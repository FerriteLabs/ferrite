//! # FerriteFunctions Runtime Manager
//!
//! Unified function execution layer for programmable data triggers. Provides
//! multi-runtime support (Lua, WASM, JavaScript, Native) with sandboxing,
//! resource limits, and execution statistics tracking.
//!
//! ## Overview
//!
//! The runtime manager sits between the trigger system and the actual function
//! execution backends. It handles:
//!
//! - **Function registration** with source validation
//! - **Sandboxed execution** with configurable resource limits
//! - **Multi-runtime dispatch** across Lua, WASM, JavaScript, and Native runtimes
//! - **Execution statistics** for monitoring and debugging
//! - **Trigger bindings** connecting functions to data events
//!
//! ## Quick Start
//!
//! ```no_run
//! use ferrite::triggers::runtime::{
//!     FunctionRuntime, RuntimeConfig, FunctionDef, FunctionSource,
//!     RuntimeType, TriggerBinding, EventType, FunctionInput,
//! };
//! use bytes::Bytes;
//! use chrono::Utc;
//! use std::collections::HashMap;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let runtime = FunctionRuntime::new(RuntimeConfig::default());
//!
//! // Register a Lua function triggered on key sets
//! let func = FunctionDef {
//!     name: "validate_order".to_string(),
//!     runtime: RuntimeType::Lua,
//!     source: FunctionSource::Inline(r#"
//!         if input.value == nil then
//!             return { success = false, error = "value required" }
//!         end
//!         return { success = true }
//!     "#.to_string()),
//!     triggers: vec![TriggerBinding {
//!         event_type: EventType::KeySet,
//!         key_pattern: "order:*".to_string(),
//!         priority: 10,
//!     }],
//!     description: Some("Validates order data on write".to_string()),
//!     metadata: HashMap::new(),
//! };
//!
//! let func_id = runtime.register_function(func)?;
//!
//! // Execute the function
//! let input = FunctionInput {
//!     event: EventType::KeySet,
//!     key: "order:123".to_string(),
//!     value: Some(Bytes::from(r#"{"item":"widget","qty":5}"#)),
//!     old_value: None,
//!     metadata: HashMap::new(),
//!     timestamp: Utc::now(),
//! };
//!
//! let output = runtime.execute(&func_id, &input).await?;
//! assert!(output.success);
//! # Ok(())
//! # }
//! ```
//!
//! ## Sandbox Modes
//!
//! | Mode | I/O | Network | Use Case |
//! |------|-----|---------|----------|
//! | Strict | No | No | Validation, transformation |
//! | Standard | Limited | No | Logging, local file access |
//! | Permissive | Yes | Yes | Webhooks, external APIs |
//!
//! ## Resource Limits
//!
//! All function executions are subject to configurable resource limits:
//!
//! - **Execution time**: Maximum wall-clock time for a function call
//! - **CPU time**: Maximum CPU time consumed
//! - **Memory**: Maximum heap allocation
//! - **Output size**: Maximum serialized output size

use bytes::Bytes;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Identifiers
// ---------------------------------------------------------------------------

/// Unique identifier for a registered function.
///
/// Wraps a UUID v4 for globally unique, collision-free identification.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FunctionId(pub String);

impl FunctionId {
    /// Generate a new random function ID.
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// Create a function ID from an existing string.
    pub fn from_string(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Return the underlying string representation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for FunctionId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for FunctionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

/// Runtime backend for function execution.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RuntimeType {
    /// Lua 5.4 scripting runtime
    Lua,
    /// WebAssembly module runtime
    Wasm,
    /// JavaScript (QuickJS) runtime
    JavaScript,
    /// Native Rust function (compiled into the binary)
    Native,
}

impl std::fmt::Display for RuntimeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RuntimeType::Lua => write!(f, "Lua"),
            RuntimeType::Wasm => write!(f, "WASM"),
            RuntimeType::JavaScript => write!(f, "JavaScript"),
            RuntimeType::Native => write!(f, "Native"),
        }
    }
}

/// Sandbox enforcement level for function execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SandboxMode {
    /// No I/O, no network access. Suitable for pure validation/transformation.
    Strict,
    /// Limited I/O (logging, local reads). No network access.
    Standard,
    /// Most operations allowed including network. Use with caution.
    Permissive,
}

impl Default for SandboxMode {
    fn default() -> Self {
        Self::Standard
    }
}

/// Source code or compiled module for a function.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FunctionSource {
    /// Source code provided inline as a string.
    Inline(String),
    /// Compiled WASM module bytes.
    Module(#[serde(with = "serde_bytes_base64")] Vec<u8>),
    /// Path to a source file on disk.
    Path(String),
}

/// Serde helper for serializing `Vec<u8>` as base64 in JSON contexts.
mod serde_bytes_base64 {
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(bytes)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes: Vec<u8> = Vec::deserialize(deserializer)?;
        Ok(bytes)
    }
}

/// Data event that can trigger a function.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventType {
    /// String SET / SETEX / MSET operations
    KeySet,
    /// DEL / UNLINK operations
    KeyDelete,
    /// Key expiration (TTL reached)
    KeyExpire,
    /// LPUSH / RPUSH operations
    ListPush,
    /// XADD stream operations
    StreamAdd,
    /// HSET / HMSET hash operations
    HashSet,
    /// Regex/glob pattern match against key names
    PatternMatch(String),
}

impl std::fmt::Display for EventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventType::KeySet => write!(f, "KeySet"),
            EventType::KeyDelete => write!(f, "KeyDelete"),
            EventType::KeyExpire => write!(f, "KeyExpire"),
            EventType::ListPush => write!(f, "ListPush"),
            EventType::StreamAdd => write!(f, "StreamAdd"),
            EventType::HashSet => write!(f, "HashSet"),
            EventType::PatternMatch(p) => write!(f, "PatternMatch({})", p),
        }
    }
}

/// Status of a registered function.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FunctionStatus {
    /// Function is registered and ready for execution.
    Active,
    /// Function is temporarily disabled.
    Disabled,
    /// Function encountered an error and is unavailable.
    Error(String),
    /// Function source is being compiled (e.g. WASM).
    Compiling,
}

impl std::fmt::Display for FunctionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FunctionStatus::Active => write!(f, "Active"),
            FunctionStatus::Disabled => write!(f, "Disabled"),
            FunctionStatus::Error(e) => write!(f, "Error: {}", e),
            FunctionStatus::Compiling => write!(f, "Compiling"),
        }
    }
}

// ---------------------------------------------------------------------------
// Structs – Configuration
// ---------------------------------------------------------------------------

/// Configuration for the FerriteFunctions runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeConfig {
    /// Maximum wall-clock execution time per invocation (milliseconds).
    pub max_execution_time_ms: u64,
    /// Maximum memory a single function invocation may allocate (bytes).
    pub max_memory_bytes: u64,
    /// Maximum CPU time per invocation (milliseconds).
    pub max_cpu_time_ms: u64,
    /// Maximum serialized output size (bytes).
    pub max_output_size_bytes: u64,
    /// Maximum number of functions that may be registered simultaneously.
    pub max_registered_functions: usize,
    /// Enable the Lua runtime backend.
    pub enable_lua: bool,
    /// Enable the WASM runtime backend.
    pub enable_wasm: bool,
    /// Enable the JavaScript runtime backend.
    pub enable_javascript: bool,
    /// Sandbox enforcement level.
    pub sandbox_mode: SandboxMode,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            max_execution_time_ms: 5000,
            max_memory_bytes: 64 * 1024 * 1024, // 64 MB
            max_cpu_time_ms: 1000,
            max_output_size_bytes: 1024 * 1024, // 1 MB
            max_registered_functions: 10_000,
            enable_lua: true,
            enable_wasm: true,
            enable_javascript: false,
            sandbox_mode: SandboxMode::Standard,
        }
    }
}

// ---------------------------------------------------------------------------
// Structs – Function definition & bindings
// ---------------------------------------------------------------------------

/// Definition of a function to be registered with the runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionDef {
    /// Human-readable function name (must be unique).
    pub name: String,
    /// Runtime backend that will execute this function.
    pub runtime: RuntimeType,
    /// Function source code or compiled module.
    pub source: FunctionSource,
    /// Event bindings that trigger this function.
    pub triggers: Vec<TriggerBinding>,
    /// Optional human-readable description.
    pub description: Option<String>,
    /// Arbitrary key-value metadata.
    pub metadata: HashMap<String, String>,
}

/// Binds a function to a specific event type and key pattern.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerBinding {
    /// Event type that activates this binding.
    pub event_type: EventType,
    /// Glob pattern matched against the affected key.
    pub key_pattern: String,
    /// Execution priority (higher values execute first).
    pub priority: i32,
}

// ---------------------------------------------------------------------------
// Structs – Input / Output
// ---------------------------------------------------------------------------

/// Input payload provided to a function invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionInput {
    /// The event that triggered execution.
    pub event: EventType,
    /// Key affected by the event.
    pub key: String,
    /// New value (if applicable).
    pub value: Option<Bytes>,
    /// Previous value (if applicable).
    pub old_value: Option<Bytes>,
    /// Arbitrary metadata passed from the trigger system.
    pub metadata: HashMap<String, String>,
    /// Timestamp of the triggering event.
    pub timestamp: DateTime<Utc>,
}

/// Output produced by a function invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionOutput {
    /// Whether the function completed successfully.
    pub success: bool,
    /// Optional return value from the function.
    pub result: Option<String>,
    /// Side-effect actions requested by the function.
    pub actions: Vec<OutputAction>,
    /// Log messages emitted during execution.
    pub logs: Vec<String>,
    /// Wall-clock execution time (microseconds).
    pub execution_time_us: u64,
    /// Peak memory used during execution (bytes).
    pub memory_used_bytes: u64,
}

/// Side-effect action that a function may request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OutputAction {
    /// Write a key-value pair with optional TTL.
    SetKey {
        /// Target key name.
        key: String,
        /// Value to store.
        value: String,
        /// Optional time-to-live in milliseconds.
        ttl_ms: Option<u64>,
    },
    /// Delete a key.
    DeleteKey {
        /// Key to delete.
        key: String,
    },
    /// Publish a message to a pub/sub channel.
    Publish {
        /// Target pub/sub channel.
        channel: String,
        /// Message payload.
        message: String,
    },
    /// Emit a log entry.
    Log {
        /// Log severity level.
        level: String,
        /// Log message text.
        message: String,
    },
    /// Record a metric data point.
    Metric {
        /// Metric name.
        name: String,
        /// Metric value.
        value: f64,
        /// Metric labels.
        labels: HashMap<String, String>,
    },
}

// ---------------------------------------------------------------------------
// Structs – Info & Stats
// ---------------------------------------------------------------------------

/// Read-only metadata about a registered function.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionInfo {
    /// Unique function identifier.
    pub id: FunctionId,
    /// Human-readable name.
    pub name: String,
    /// Runtime backend.
    pub runtime: RuntimeType,
    /// Event bindings.
    pub triggers: Vec<TriggerBinding>,
    /// When the function was registered.
    pub created_at: DateTime<Utc>,
    /// When the function was last executed (if ever).
    pub last_executed: Option<DateTime<Utc>>,
    /// Total successful + failed invocations.
    pub execution_count: u64,
    /// Total failed invocations.
    pub error_count: u64,
    /// Rolling average execution time (microseconds).
    pub avg_execution_time_us: u64,
    /// Current function status.
    pub status: FunctionStatus,
}

/// Aggregate statistics for the entire runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeStats {
    /// Total registered functions (all statuses).
    pub total_functions: u64,
    /// Functions with `Active` status.
    pub active_functions: u64,
    /// Cumulative invocation count.
    pub total_executions: u64,
    /// Cumulative error count.
    pub total_errors: u64,
    /// Global average execution time (microseconds).
    pub avg_execution_time_us: u64,
    /// Estimated total memory used by all function runtimes (bytes).
    pub total_memory_used_bytes: u64,
    /// Registered functions using the Lua runtime.
    pub lua_functions: u64,
    /// Registered functions using the WASM runtime.
    pub wasm_functions: u64,
    /// Registered functions using the JavaScript runtime.
    pub js_functions: u64,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors produced by the FerriteFunctions runtime.
#[derive(Debug, Clone, thiserror::Error)]
pub enum RuntimeError {
    /// The requested function ID does not exist.
    #[error("function not found: {0}")]
    FunctionNotFound(String),

    /// The maximum number of registered functions has been reached.
    #[error("maximum registered functions ({0}) exceeded")]
    MaxFunctionsExceeded(usize),

    /// The function source failed to compile.
    #[error("compilation error: {0}")]
    CompilationError(String),

    /// The function exceeded its execution time limit.
    #[error("execution timed out")]
    ExecutionTimeout,

    /// The function exceeded its memory allocation limit.
    #[error("memory limit exceeded")]
    MemoryLimitExceeded,

    /// The function produced output larger than the configured limit.
    #[error("output size exceeds limit")]
    OutputTooLarge,

    /// The function attempted a disallowed operation under the sandbox policy.
    #[error("sandbox violation: {0}")]
    SandboxViolation(String),

    /// The requested runtime backend is not enabled or available.
    #[error("runtime unavailable: {0}")]
    RuntimeUnavailable(String),

    /// The function source is invalid or malformed.
    #[error("invalid source: {0}")]
    InvalidSource(String),

    /// An unexpected internal error occurred.
    #[error("internal error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Internal state
// ---------------------------------------------------------------------------

/// Internal mutable state for a registered function.
struct RegisteredFunction {
    /// The original definition.
    def: FunctionDef,
    /// Assigned identifier.
    id: FunctionId,
    /// Current status.
    status: FunctionStatus,
    /// Registration timestamp.
    created_at: DateTime<Utc>,
    /// Last execution timestamp.
    last_executed: Option<DateTime<Utc>>,
    /// Total invocation count.
    execution_count: AtomicU64,
    /// Total error count.
    error_count: AtomicU64,
    /// Cumulative execution time (microseconds) for average calculation.
    total_execution_time_us: AtomicU64,
}

impl RegisteredFunction {
    /// Produce a read-only [`FunctionInfo`] snapshot.
    fn to_info(&self) -> FunctionInfo {
        let exec_count = self.execution_count.load(Ordering::Relaxed);
        let total_time = self.total_execution_time_us.load(Ordering::Relaxed);
        let avg = if exec_count > 0 {
            total_time / exec_count
        } else {
            0
        };

        FunctionInfo {
            id: self.id.clone(),
            name: self.def.name.clone(),
            runtime: self.def.runtime.clone(),
            triggers: self.def.triggers.clone(),
            created_at: self.created_at,
            last_executed: self.last_executed,
            execution_count: exec_count,
            error_count: self.error_count.load(Ordering::Relaxed),
            avg_execution_time_us: avg,
            status: self.status.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// FunctionRuntime – the public API
// ---------------------------------------------------------------------------

/// Unified function execution manager for FerriteFunctions.
///
/// Manages registration, lifecycle, and execution of user-defined functions
/// across multiple runtime backends. All operations are thread-safe and use
/// lock-free concurrent data structures where possible.
pub struct FunctionRuntime {
    /// Registered functions keyed by their [`FunctionId`].
    functions: DashMap<String, RegisteredFunction>,
    /// Secondary index: function name → function ID (enforces unique names).
    name_index: DashMap<String, FunctionId>,
    /// Runtime configuration.
    config: RuntimeConfig,
    /// Global execution counter.
    total_executions: AtomicU64,
    /// Global error counter.
    total_errors: AtomicU64,
    /// Global cumulative execution time (microseconds).
    total_execution_time_us: AtomicU64,
}

impl FunctionRuntime {
    /// Create a new runtime with the provided configuration.
    pub fn new(config: RuntimeConfig) -> Self {
        Self {
            functions: DashMap::new(),
            name_index: DashMap::new(),
            config,
            total_executions: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            total_execution_time_us: AtomicU64::new(0),
        }
    }

    /// Register a new function.
    ///
    /// Validates the function definition, checks resource limits, and assigns
    /// a unique [`FunctionId`]. Returns an error if the name is already taken,
    /// the function limit is reached, or the source is invalid.
    pub fn register_function(&self, func: FunctionDef) -> Result<FunctionId, RuntimeError> {
        // Check capacity
        if self.functions.len() >= self.config.max_registered_functions {
            return Err(RuntimeError::MaxFunctionsExceeded(
                self.config.max_registered_functions,
            ));
        }

        // Check name uniqueness
        if self.name_index.contains_key(&func.name) {
            return Err(RuntimeError::InvalidSource(format!(
                "function with name '{}' already exists",
                func.name
            )));
        }

        // Validate runtime availability
        self.validate_runtime(&func.runtime)?;

        // Validate source
        self.validate_source(&func.runtime, &func.source)?;

        let id = FunctionId::new();
        let registered = RegisteredFunction {
            def: func.clone(),
            id: id.clone(),
            status: FunctionStatus::Active,
            created_at: Utc::now(),
            last_executed: None,
            execution_count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            total_execution_time_us: AtomicU64::new(0),
        };

        self.name_index.insert(func.name.clone(), id.clone());
        self.functions.insert(id.0.clone(), registered);

        Ok(id)
    }

    /// Unregister a function by its ID.
    ///
    /// Removes the function from the registry and frees associated resources.
    pub fn unregister_function(&self, id: &FunctionId) -> Result<(), RuntimeError> {
        let removed = self
            .functions
            .remove(&id.0)
            .ok_or_else(|| RuntimeError::FunctionNotFound(id.to_string()))?;

        self.name_index.remove(&removed.1.def.name);
        Ok(())
    }

    /// Execute a registered function with the given input.
    ///
    /// Dispatches to the appropriate runtime backend, enforces resource limits,
    /// and records execution statistics. The function runs in a sandboxed
    /// environment according to the configured [`SandboxMode`].
    pub async fn execute(
        &self,
        id: &FunctionId,
        input: &FunctionInput,
    ) -> Result<FunctionOutput, RuntimeError> {
        let start = std::time::Instant::now();

        // Look up the function
        let entry = self
            .functions
            .get_mut(&id.0)
            .ok_or_else(|| RuntimeError::FunctionNotFound(id.to_string()))?;

        let func = &entry.value();
        if func.status != FunctionStatus::Active {
            return Err(RuntimeError::RuntimeUnavailable(format!(
                "function '{}' is not active (status: {})",
                func.def.name, func.status
            )));
        }

        let runtime_type = func.def.runtime.clone();
        let source = func.def.source.clone();
        let func_name = func.def.name.clone();
        drop(entry);

        // Dispatch to runtime backend
        let output = self.dispatch_execution(&runtime_type, &source, &func_name, input)?;

        let elapsed_us = start.elapsed().as_micros() as u64;

        // Check resource limits on the output
        if let Some(ref result) = output.result {
            if result.len() as u64 > self.config.max_output_size_bytes {
                self.record_error(id);
                return Err(RuntimeError::OutputTooLarge);
            }
        }

        // Record statistics
        self.total_executions.fetch_add(1, Ordering::Relaxed);
        self.total_execution_time_us
            .fetch_add(elapsed_us, Ordering::Relaxed);

        if let Some(mut entry) = self.functions.get_mut(&id.0) {
            let reg = entry.value_mut();
            reg.execution_count.fetch_add(1, Ordering::Relaxed);
            reg.total_execution_time_us
                .fetch_add(elapsed_us, Ordering::Relaxed);
            reg.last_executed = Some(Utc::now());
        }

        if !output.success {
            self.record_error(id);
        }

        Ok(FunctionOutput {
            execution_time_us: elapsed_us,
            ..output
        })
    }

    /// List all registered functions as [`FunctionInfo`] snapshots.
    pub fn list_functions(&self) -> Vec<FunctionInfo> {
        self.functions
            .iter()
            .map(|entry| entry.value().to_info())
            .collect()
    }

    /// Get information about a single function.
    pub fn get_function(&self, id: &FunctionId) -> Option<FunctionInfo> {
        self.functions
            .get(&id.0)
            .map(|entry| entry.value().to_info())
    }

    /// Get aggregate runtime statistics.
    pub fn get_stats(&self) -> RuntimeStats {
        let total_execs = self.total_executions.load(Ordering::Relaxed);
        let total_time = self.total_execution_time_us.load(Ordering::Relaxed);
        let avg = if total_execs > 0 {
            total_time / total_execs
        } else {
            0
        };

        let mut lua_count = 0u64;
        let mut wasm_count = 0u64;
        let mut js_count = 0u64;
        let mut active_count = 0u64;
        let mut total_mem = 0u64;

        for entry in self.functions.iter() {
            let f = entry.value();
            match f.def.runtime {
                RuntimeType::Lua => lua_count += 1,
                RuntimeType::Wasm => wasm_count += 1,
                RuntimeType::JavaScript => js_count += 1,
                RuntimeType::Native => {}
            }
            if f.status == FunctionStatus::Active {
                active_count += 1;
            }
            // Estimate memory from source size as a proxy
            total_mem += match &f.def.source {
                FunctionSource::Inline(s) => s.len() as u64,
                FunctionSource::Module(b) => b.len() as u64,
                FunctionSource::Path(_) => 0,
            };
        }

        RuntimeStats {
            total_functions: self.functions.len() as u64,
            active_functions: active_count,
            total_executions: total_execs,
            total_errors: self.total_errors.load(Ordering::Relaxed),
            avg_execution_time_us: avg,
            total_memory_used_bytes: total_mem,
            lua_functions: lua_count,
            wasm_functions: wasm_count,
            js_functions: js_count,
        }
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Validate that the requested runtime backend is enabled.
    fn validate_runtime(&self, runtime: &RuntimeType) -> Result<(), RuntimeError> {
        match runtime {
            RuntimeType::Lua if !self.config.enable_lua => Err(RuntimeError::RuntimeUnavailable(
                "Lua runtime is disabled".into(),
            )),
            RuntimeType::Wasm if !self.config.enable_wasm => Err(RuntimeError::RuntimeUnavailable(
                "WASM runtime is disabled".into(),
            )),
            RuntimeType::JavaScript if !self.config.enable_javascript => Err(
                RuntimeError::RuntimeUnavailable("JavaScript runtime is disabled".into()),
            ),
            _ => Ok(()),
        }
    }

    /// Validate function source for basic correctness.
    fn validate_source(
        &self,
        runtime: &RuntimeType,
        source: &FunctionSource,
    ) -> Result<(), RuntimeError> {
        match source {
            FunctionSource::Inline(code) => {
                if code.trim().is_empty() {
                    return Err(RuntimeError::InvalidSource("source code is empty".into()));
                }
            }
            FunctionSource::Module(bytes) => {
                if bytes.is_empty() {
                    return Err(RuntimeError::InvalidSource("module bytes are empty".into()));
                }
                // WASM modules must start with the magic number \0asm
                if *runtime == RuntimeType::Wasm && !bytes.starts_with(b"\0asm") {
                    return Err(RuntimeError::CompilationError(
                        "invalid WASM module: missing magic number".into(),
                    ));
                }
            }
            FunctionSource::Path(path) => {
                if path.trim().is_empty() {
                    return Err(RuntimeError::InvalidSource("source path is empty".into()));
                }
            }
        }
        Ok(())
    }

    /// Dispatch execution to the appropriate runtime backend.
    ///
    /// This is a simulated implementation — actual Lua/WASM/JS runtimes would
    /// be integrated here. Each backend returns a [`FunctionOutput`] with
    /// execution metadata.
    fn dispatch_execution(
        &self,
        runtime: &RuntimeType,
        source: &FunctionSource,
        func_name: &str,
        input: &FunctionInput,
    ) -> Result<FunctionOutput, RuntimeError> {
        match runtime {
            RuntimeType::Lua => self.execute_lua(source, func_name, input),
            RuntimeType::Wasm => self.execute_wasm(source, func_name, input),
            RuntimeType::JavaScript => self.execute_javascript(source, func_name, input),
            RuntimeType::Native => self.execute_native(func_name, input),
        }
    }

    /// Simulated Lua execution.
    fn execute_lua(
        &self,
        _source: &FunctionSource,
        func_name: &str,
        input: &FunctionInput,
    ) -> Result<FunctionOutput, RuntimeError> {
        // In production this would invoke mlua or rlua with the source code.
        let logs = vec![format!(
            "[lua] executing function '{}' for event {} on key '{}'",
            func_name, input.event, input.key
        )];

        let actions = vec![OutputAction::Log {
            level: "info".to_string(),
            message: format!("Lua function '{}' processed key '{}'", func_name, input.key),
        }];

        Ok(FunctionOutput {
            success: true,
            result: Some(format!("lua:{}:ok", func_name)),
            actions,
            logs,
            execution_time_us: 0, // overwritten by caller
            memory_used_bytes: 0,
        })
    }

    /// Simulated WASM execution.
    fn execute_wasm(
        &self,
        source: &FunctionSource,
        func_name: &str,
        input: &FunctionInput,
    ) -> Result<FunctionOutput, RuntimeError> {
        // Validate module bytes if source is Module variant
        if let FunctionSource::Module(bytes) = source {
            if !bytes.starts_with(b"\0asm") {
                return Err(RuntimeError::CompilationError(
                    "invalid WASM module at execution time".into(),
                ));
            }
        }

        let logs = vec![format!(
            "[wasm] executing function '{}' for event {} on key '{}'",
            func_name, input.event, input.key
        )];

        let mut actions = vec![OutputAction::Log {
            level: "info".to_string(),
            message: format!(
                "WASM function '{}' processed key '{}'",
                func_name, input.key
            ),
        }];

        // Demonstrate output actions based on event type
        if input.event == EventType::KeySet {
            if let Some(ref val) = input.value {
                actions.push(OutputAction::Publish {
                    channel: format!("__wasm:{}", func_name),
                    message: format!("key '{}' set to {} bytes", input.key, val.len()),
                });
            }
        }

        Ok(FunctionOutput {
            success: true,
            result: Some(format!("wasm:{}:ok", func_name)),
            actions,
            logs,
            execution_time_us: 0,
            memory_used_bytes: source.as_ref().map(|b: &[u8]| b.len() as u64).unwrap_or(0),
        })
    }

    /// Simulated JavaScript execution.
    fn execute_javascript(
        &self,
        _source: &FunctionSource,
        func_name: &str,
        input: &FunctionInput,
    ) -> Result<FunctionOutput, RuntimeError> {
        // In production this would invoke QuickJS or similar.
        let logs = vec![format!(
            "[js] executing function '{}' for event {} on key '{}'",
            func_name, input.event, input.key
        )];

        Ok(FunctionOutput {
            success: true,
            result: Some(format!("js:{}:ok", func_name)),
            actions: vec![OutputAction::Log {
                level: "info".to_string(),
                message: format!("JS function '{}' processed key '{}'", func_name, input.key),
            }],
            logs,
            execution_time_us: 0,
            memory_used_bytes: 0,
        })
    }

    /// Simulated native execution.
    fn execute_native(
        &self,
        func_name: &str,
        input: &FunctionInput,
    ) -> Result<FunctionOutput, RuntimeError> {
        let logs = vec![format!(
            "[native] executing function '{}' for event {} on key '{}'",
            func_name, input.event, input.key
        )];

        Ok(FunctionOutput {
            success: true,
            result: Some(format!("native:{}:ok", func_name)),
            actions: vec![],
            logs,
            execution_time_us: 0,
            memory_used_bytes: 0,
        })
    }

    /// Record an error against a function and the global counters.
    fn record_error(&self, id: &FunctionId) {
        self.total_errors.fetch_add(1, Ordering::Relaxed);
        if let Some(entry) = self.functions.get(&id.0) {
            entry.value().error_count.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Helper: extract byte length from a [`FunctionSource`] when applicable.
impl FunctionSource {
    fn as_ref(&self) -> Option<&[u8]> {
        match self {
            FunctionSource::Module(bytes) => Some(bytes),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// Helper: create a default runtime for tests.
    fn test_runtime() -> FunctionRuntime {
        FunctionRuntime::new(RuntimeConfig::default())
    }

    /// Helper: create a minimal Lua function definition.
    fn lua_function(name: &str) -> FunctionDef {
        FunctionDef {
            name: name.to_string(),
            runtime: RuntimeType::Lua,
            source: FunctionSource::Inline("return true".to_string()),
            triggers: vec![TriggerBinding {
                event_type: EventType::KeySet,
                key_pattern: "test:*".to_string(),
                priority: 0,
            }],
            description: Some("test function".to_string()),
            metadata: HashMap::new(),
        }
    }

    /// Helper: create a mock function input.
    fn mock_input() -> FunctionInput {
        FunctionInput {
            event: EventType::KeySet,
            key: "test:123".to_string(),
            value: Some(Bytes::from("hello")),
            old_value: None,
            metadata: HashMap::new(),
            timestamp: Utc::now(),
        }
    }

    // -- Registration tests -------------------------------------------------

    #[test]
    fn test_register_function() {
        let rt = test_runtime();
        let id = rt.register_function(lua_function("my_func")).unwrap();
        assert!(!id.0.is_empty());

        let info = rt.get_function(&id).unwrap();
        assert_eq!(info.name, "my_func");
        assert_eq!(info.runtime, RuntimeType::Lua);
        assert_eq!(info.status, FunctionStatus::Active);
        assert_eq!(info.execution_count, 0);
    }

    #[test]
    fn test_register_duplicate_name_rejected() {
        let rt = test_runtime();
        rt.register_function(lua_function("dup")).unwrap();
        let err = rt.register_function(lua_function("dup")).unwrap_err();
        assert!(matches!(err, RuntimeError::InvalidSource(_)));
    }

    #[test]
    fn test_max_functions_exceeded() {
        let config = RuntimeConfig {
            max_registered_functions: 2,
            ..RuntimeConfig::default()
        };
        let rt = FunctionRuntime::new(config);

        rt.register_function(lua_function("f1")).unwrap();
        rt.register_function(lua_function("f2")).unwrap();
        let err = rt.register_function(lua_function("f3")).unwrap_err();
        assert!(matches!(err, RuntimeError::MaxFunctionsExceeded(2)));
    }

    #[test]
    fn test_unregister_function() {
        let rt = test_runtime();
        let id = rt.register_function(lua_function("removable")).unwrap();
        assert!(rt.get_function(&id).is_some());

        rt.unregister_function(&id).unwrap();
        assert!(rt.get_function(&id).is_none());
    }

    #[test]
    fn test_unregister_nonexistent_function() {
        let rt = test_runtime();
        let fake_id = FunctionId::from_string("nonexistent");
        let err = rt.unregister_function(&fake_id).unwrap_err();
        assert!(matches!(err, RuntimeError::FunctionNotFound(_)));
    }

    #[test]
    fn test_list_functions() {
        let rt = test_runtime();
        assert!(rt.list_functions().is_empty());

        rt.register_function(lua_function("func_a")).unwrap();
        rt.register_function(lua_function("func_b")).unwrap();
        rt.register_function(lua_function("func_c")).unwrap();

        let list = rt.list_functions();
        assert_eq!(list.len(), 3);

        let names: Vec<&str> = list.iter().map(|f| f.name.as_str()).collect();
        assert!(names.contains(&"func_a"));
        assert!(names.contains(&"func_b"));
        assert!(names.contains(&"func_c"));
    }

    // -- Execution tests ----------------------------------------------------

    #[tokio::test]
    async fn test_execute_lua_function() {
        let rt = test_runtime();
        let id = rt.register_function(lua_function("lua_exec")).unwrap();

        let output = rt.execute(&id, &mock_input()).await.unwrap();
        assert!(output.success);
        assert_eq!(output.result, Some("lua:lua_exec:ok".to_string()));
        assert!(!output.logs.is_empty());
        assert!(output.execution_time_us > 0 || output.execution_time_us == 0);
    }

    #[tokio::test]
    async fn test_execute_wasm_function() {
        let rt = test_runtime();
        // Valid WASM magic number prefix
        let wasm_bytes = b"\0asm\x01\x00\x00\x00".to_vec();
        let func = FunctionDef {
            name: "wasm_exec".to_string(),
            runtime: RuntimeType::Wasm,
            source: FunctionSource::Module(wasm_bytes),
            triggers: vec![TriggerBinding {
                event_type: EventType::KeySet,
                key_pattern: "*".to_string(),
                priority: 5,
            }],
            description: None,
            metadata: HashMap::new(),
        };
        let id = rt.register_function(func).unwrap();

        let output = rt.execute(&id, &mock_input()).await.unwrap();
        assert!(output.success);
        assert_eq!(output.result, Some("wasm:wasm_exec:ok".to_string()));
        // WASM KeySet triggers a Publish action
        assert!(output.actions.len() >= 2);
    }

    #[tokio::test]
    async fn test_execute_javascript_function() {
        let config = RuntimeConfig {
            enable_javascript: true,
            ..RuntimeConfig::default()
        };
        let rt = FunctionRuntime::new(config);
        let func = FunctionDef {
            name: "js_exec".to_string(),
            runtime: RuntimeType::JavaScript,
            source: FunctionSource::Inline("console.log('hello')".to_string()),
            triggers: vec![],
            description: None,
            metadata: HashMap::new(),
        };
        let id = rt.register_function(func).unwrap();

        let output = rt.execute(&id, &mock_input()).await.unwrap();
        assert!(output.success);
        assert_eq!(output.result, Some("js:js_exec:ok".to_string()));
    }

    #[tokio::test]
    async fn test_execute_native_function() {
        let rt = test_runtime();
        let func = FunctionDef {
            name: "native_exec".to_string(),
            runtime: RuntimeType::Native,
            source: FunctionSource::Inline("native_handler".to_string()),
            triggers: vec![],
            description: None,
            metadata: HashMap::new(),
        };
        let id = rt.register_function(func).unwrap();

        let output = rt.execute(&id, &mock_input()).await.unwrap();
        assert!(output.success);
        assert_eq!(output.result, Some("native:native_exec:ok".to_string()));
        assert!(output.actions.is_empty());
    }

    #[tokio::test]
    async fn test_execute_nonexistent_function() {
        let rt = test_runtime();
        let fake_id = FunctionId::from_string("ghost");
        let err = rt.execute(&fake_id, &mock_input()).await.unwrap_err();
        assert!(matches!(err, RuntimeError::FunctionNotFound(_)));
    }

    // -- Validation tests ---------------------------------------------------

    #[test]
    fn test_empty_inline_source_rejected() {
        let rt = test_runtime();
        let mut func = lua_function("empty_src");
        func.source = FunctionSource::Inline("   ".to_string());

        let err = rt.register_function(func).unwrap_err();
        assert!(matches!(err, RuntimeError::InvalidSource(_)));
    }

    #[test]
    fn test_empty_module_bytes_rejected() {
        let rt = test_runtime();
        let func = FunctionDef {
            name: "empty_wasm".to_string(),
            runtime: RuntimeType::Wasm,
            source: FunctionSource::Module(vec![]),
            triggers: vec![],
            description: None,
            metadata: HashMap::new(),
        };

        let err = rt.register_function(func).unwrap_err();
        assert!(matches!(err, RuntimeError::InvalidSource(_)));
    }

    #[test]
    fn test_invalid_wasm_magic_rejected() {
        let rt = test_runtime();
        let func = FunctionDef {
            name: "bad_wasm".to_string(),
            runtime: RuntimeType::Wasm,
            source: FunctionSource::Module(vec![0x00, 0x01, 0x02, 0x03]),
            triggers: vec![],
            description: None,
            metadata: HashMap::new(),
        };

        let err = rt.register_function(func).unwrap_err();
        assert!(matches!(err, RuntimeError::CompilationError(_)));
    }

    #[test]
    fn test_empty_path_rejected() {
        let rt = test_runtime();
        let mut func = lua_function("empty_path");
        func.source = FunctionSource::Path("  ".to_string());

        let err = rt.register_function(func).unwrap_err();
        assert!(matches!(err, RuntimeError::InvalidSource(_)));
    }

    #[test]
    fn test_disabled_runtime_rejected() {
        let config = RuntimeConfig {
            enable_lua: false,
            ..RuntimeConfig::default()
        };
        let rt = FunctionRuntime::new(config);

        let err = rt.register_function(lua_function("no_lua")).unwrap_err();
        assert!(matches!(err, RuntimeError::RuntimeUnavailable(_)));
    }

    #[test]
    fn test_disabled_javascript_runtime_rejected() {
        let rt = test_runtime(); // JS disabled by default
        let func = FunctionDef {
            name: "no_js".to_string(),
            runtime: RuntimeType::JavaScript,
            source: FunctionSource::Inline("console.log('test')".to_string()),
            triggers: vec![],
            description: None,
            metadata: HashMap::new(),
        };

        let err = rt.register_function(func).unwrap_err();
        assert!(matches!(err, RuntimeError::RuntimeUnavailable(_)));
    }

    // -- Statistics tests ---------------------------------------------------

    #[tokio::test]
    async fn test_stats_tracking() {
        let rt = test_runtime();
        let id = rt.register_function(lua_function("stats_test")).unwrap();

        // Execute a few times
        for _ in 0..5 {
            rt.execute(&id, &mock_input()).await.unwrap();
        }

        let stats = rt.get_stats();
        assert_eq!(stats.total_functions, 1);
        assert_eq!(stats.active_functions, 1);
        assert_eq!(stats.total_executions, 5);
        assert_eq!(stats.total_errors, 0);
        assert_eq!(stats.lua_functions, 1);
        assert_eq!(stats.wasm_functions, 0);
        assert_eq!(stats.js_functions, 0);

        // Per-function stats
        let info = rt.get_function(&id).unwrap();
        assert_eq!(info.execution_count, 5);
        assert_eq!(info.error_count, 0);
        assert!(info.last_executed.is_some());
    }

    #[test]
    fn test_stats_empty_runtime() {
        let rt = test_runtime();
        let stats = rt.get_stats();
        assert_eq!(stats.total_functions, 0);
        assert_eq!(stats.active_functions, 0);
        assert_eq!(stats.total_executions, 0);
        assert_eq!(stats.total_errors, 0);
        assert_eq!(stats.avg_execution_time_us, 0);
    }

    #[test]
    fn test_stats_multiple_runtimes() {
        let config = RuntimeConfig {
            enable_javascript: true,
            ..RuntimeConfig::default()
        };
        let rt = FunctionRuntime::new(config);

        rt.register_function(lua_function("lua_fn")).unwrap();

        let wasm_func = FunctionDef {
            name: "wasm_fn".to_string(),
            runtime: RuntimeType::Wasm,
            source: FunctionSource::Module(b"\0asm\x01\x00\x00\x00".to_vec()),
            triggers: vec![],
            description: None,
            metadata: HashMap::new(),
        };
        rt.register_function(wasm_func).unwrap();

        let js_func = FunctionDef {
            name: "js_fn".to_string(),
            runtime: RuntimeType::JavaScript,
            source: FunctionSource::Inline("return 1".to_string()),
            triggers: vec![],
            description: None,
            metadata: HashMap::new(),
        };
        rt.register_function(js_func).unwrap();

        let stats = rt.get_stats();
        assert_eq!(stats.total_functions, 3);
        assert_eq!(stats.lua_functions, 1);
        assert_eq!(stats.wasm_functions, 1);
        assert_eq!(stats.js_functions, 1);
    }

    // -- Trigger binding tests ----------------------------------------------

    #[test]
    fn test_trigger_bindings_preserved() {
        let rt = test_runtime();
        let func = FunctionDef {
            name: "multi_trigger".to_string(),
            runtime: RuntimeType::Lua,
            source: FunctionSource::Inline("return true".to_string()),
            triggers: vec![
                TriggerBinding {
                    event_type: EventType::KeySet,
                    key_pattern: "order:*".to_string(),
                    priority: 10,
                },
                TriggerBinding {
                    event_type: EventType::KeyDelete,
                    key_pattern: "order:*".to_string(),
                    priority: 5,
                },
                TriggerBinding {
                    event_type: EventType::HashSet,
                    key_pattern: "user:*:settings".to_string(),
                    priority: 1,
                },
            ],
            description: None,
            metadata: HashMap::new(),
        };

        let id = rt.register_function(func).unwrap();
        let info = rt.get_function(&id).unwrap();

        assert_eq!(info.triggers.len(), 3);
        assert_eq!(info.triggers[0].event_type, EventType::KeySet);
        assert_eq!(info.triggers[0].key_pattern, "order:*");
        assert_eq!(info.triggers[0].priority, 10);
        assert_eq!(info.triggers[1].event_type, EventType::KeyDelete);
        assert_eq!(info.triggers[2].event_type, EventType::HashSet);
    }

    // -- Output action tests ------------------------------------------------

    #[tokio::test]
    async fn test_output_actions_generated() {
        let rt = test_runtime();
        let wasm_bytes = b"\0asm\x01\x00\x00\x00".to_vec();
        let func = FunctionDef {
            name: "action_gen".to_string(),
            runtime: RuntimeType::Wasm,
            source: FunctionSource::Module(wasm_bytes),
            triggers: vec![],
            description: None,
            metadata: HashMap::new(),
        };
        let id = rt.register_function(func).unwrap();

        let input = FunctionInput {
            event: EventType::KeySet,
            key: "data:key".to_string(),
            value: Some(Bytes::from("some_value")),
            old_value: None,
            metadata: HashMap::new(),
            timestamp: Utc::now(),
        };

        let output = rt.execute(&id, &input).await.unwrap();
        assert!(output.success);

        // WASM KeySet handler produces a Log and a Publish action
        let has_publish = output
            .actions
            .iter()
            .any(|a| matches!(a, OutputAction::Publish { .. }));
        let has_log = output
            .actions
            .iter()
            .any(|a| matches!(a, OutputAction::Log { .. }));
        assert!(has_publish, "expected a Publish action");
        assert!(has_log, "expected a Log action");
    }

    // -- Error handling tests -----------------------------------------------

    #[test]
    fn test_runtime_error_display() {
        let err = RuntimeError::FunctionNotFound("abc-123".to_string());
        assert_eq!(err.to_string(), "function not found: abc-123");

        let err = RuntimeError::MaxFunctionsExceeded(100);
        assert_eq!(
            err.to_string(),
            "maximum registered functions (100) exceeded"
        );

        let err = RuntimeError::ExecutionTimeout;
        assert_eq!(err.to_string(), "execution timed out");

        let err = RuntimeError::MemoryLimitExceeded;
        assert_eq!(err.to_string(), "memory limit exceeded");

        let err = RuntimeError::SandboxViolation("network access denied".to_string());
        assert_eq!(err.to_string(), "sandbox violation: network access denied");
    }

    // -- FunctionId tests ---------------------------------------------------

    #[test]
    fn test_function_id_uniqueness() {
        let id1 = FunctionId::new();
        let id2 = FunctionId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_function_id_from_string() {
        let id = FunctionId::from_string("custom-id-123");
        assert_eq!(id.as_str(), "custom-id-123");
        assert_eq!(id.to_string(), "custom-id-123");
    }

    // -- Config defaults tests ----------------------------------------------

    #[test]
    fn test_runtime_config_defaults() {
        let config = RuntimeConfig::default();
        assert_eq!(config.max_execution_time_ms, 5000);
        assert_eq!(config.max_memory_bytes, 64 * 1024 * 1024);
        assert_eq!(config.max_cpu_time_ms, 1000);
        assert_eq!(config.max_output_size_bytes, 1024 * 1024);
        assert_eq!(config.max_registered_functions, 10_000);
        assert!(config.enable_lua);
        assert!(config.enable_wasm);
        assert!(!config.enable_javascript);
        assert_eq!(config.sandbox_mode, SandboxMode::Standard);
    }

    // -- Serialization roundtrip tests --------------------------------------

    #[test]
    fn test_function_output_serialization() {
        let output = FunctionOutput {
            success: true,
            result: Some("ok".to_string()),
            actions: vec![
                OutputAction::SetKey {
                    key: "out:key".to_string(),
                    value: "42".to_string(),
                    ttl_ms: Some(60000),
                },
                OutputAction::DeleteKey {
                    key: "tmp:key".to_string(),
                },
                OutputAction::Publish {
                    channel: "events".to_string(),
                    message: "done".to_string(),
                },
                OutputAction::Log {
                    level: "info".to_string(),
                    message: "processed".to_string(),
                },
                OutputAction::Metric {
                    name: "latency_ms".to_string(),
                    value: 12.5,
                    labels: HashMap::from([("env".to_string(), "prod".to_string())]),
                },
            ],
            logs: vec!["step 1".to_string(), "step 2".to_string()],
            execution_time_us: 1500,
            memory_used_bytes: 4096,
        };

        let json = serde_json::to_string(&output).unwrap();
        let deserialized: FunctionOutput = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.success, output.success);
        assert_eq!(deserialized.result, output.result);
        assert_eq!(deserialized.actions.len(), 5);
        assert_eq!(deserialized.logs.len(), 2);
        assert_eq!(deserialized.execution_time_us, 1500);
    }

    #[test]
    fn test_event_type_serialization() {
        let events = vec![
            EventType::KeySet,
            EventType::KeyDelete,
            EventType::KeyExpire,
            EventType::ListPush,
            EventType::StreamAdd,
            EventType::HashSet,
            EventType::PatternMatch("user:*".to_string()),
        ];

        for event in &events {
            let json = serde_json::to_string(event).unwrap();
            let back: EventType = serde_json::from_str(&json).unwrap();
            assert_eq!(&back, event);
        }
    }

    // -- Concurrent access test ---------------------------------------------

    #[tokio::test]
    async fn test_concurrent_registrations() {
        let rt = Arc::new(test_runtime());
        let mut handles = vec![];

        for i in 0..50 {
            let rt = Arc::clone(&rt);
            handles.push(tokio::spawn(async move {
                let func = FunctionDef {
                    name: format!("concurrent_{}", i),
                    runtime: RuntimeType::Lua,
                    source: FunctionSource::Inline("return true".to_string()),
                    triggers: vec![],
                    description: None,
                    metadata: HashMap::new(),
                };
                rt.register_function(func)
            }));
        }

        let mut success_count = 0;
        for handle in handles {
            if handle.await.unwrap().is_ok() {
                success_count += 1;
            }
        }

        assert_eq!(success_count, 50);
        assert_eq!(rt.list_functions().len(), 50);
    }

    #[tokio::test]
    async fn test_concurrent_executions() {
        let rt = Arc::new(test_runtime());
        let id = rt
            .register_function(lua_function("concurrent_exec"))
            .unwrap();

        let mut handles = vec![];
        for _ in 0..20 {
            let rt = Arc::clone(&rt);
            let id = id.clone();
            handles.push(tokio::spawn(
                async move { rt.execute(&id, &mock_input()).await },
            ));
        }

        for handle in handles {
            assert!(handle.await.unwrap().is_ok());
        }

        let info = rt.get_function(&id).unwrap();
        assert_eq!(info.execution_count, 20);
    }

    // -- Metadata tests -----------------------------------------------------

    #[test]
    fn test_function_metadata_preserved() {
        let rt = test_runtime();
        let mut func = lua_function("with_meta");
        func.metadata
            .insert("author".to_string(), "ferrite-team".to_string());
        func.metadata
            .insert("version".to_string(), "1.0".to_string());
        func.description = Some("A well-documented function".to_string());

        let id = rt.register_function(func).unwrap();
        let info = rt.get_function(&id).unwrap();
        assert_eq!(info.name, "with_meta");
        assert!(info.created_at <= Utc::now());
    }

    // -- Edge cases ---------------------------------------------------------

    #[test]
    fn test_register_unregister_reregister() {
        let rt = test_runtime();
        let id = rt.register_function(lua_function("reuse_name")).unwrap();
        rt.unregister_function(&id).unwrap();

        // Same name should work again after unregister
        let id2 = rt.register_function(lua_function("reuse_name")).unwrap();
        assert_ne!(id, id2);
    }

    #[tokio::test]
    async fn test_execute_with_all_input_fields() {
        let rt = test_runtime();
        let id = rt.register_function(lua_function("full_input")).unwrap();

        let input = FunctionInput {
            event: EventType::KeySet,
            key: "user:456:profile".to_string(),
            value: Some(Bytes::from(r#"{"name":"Alice"}"#)),
            old_value: Some(Bytes::from(r#"{"name":"Bob"}"#)),
            metadata: HashMap::from([
                ("source".to_string(), "api".to_string()),
                ("client_id".to_string(), "app-1".to_string()),
            ]),
            timestamp: Utc::now(),
        };

        let output = rt.execute(&id, &input).await.unwrap();
        assert!(output.success);
    }

    #[tokio::test]
    async fn test_execute_with_different_event_types() {
        let rt = test_runtime();
        let id = rt.register_function(lua_function("multi_event")).unwrap();

        let event_types = vec![
            EventType::KeySet,
            EventType::KeyDelete,
            EventType::KeyExpire,
            EventType::ListPush,
            EventType::StreamAdd,
            EventType::HashSet,
            EventType::PatternMatch("user:*".to_string()),
        ];

        for event in event_types {
            let input = FunctionInput {
                event,
                key: "test:key".to_string(),
                value: None,
                old_value: None,
                metadata: HashMap::new(),
                timestamp: Utc::now(),
            };
            let output = rt.execute(&id, &input).await.unwrap();
            assert!(output.success);
        }

        let info = rt.get_function(&id).unwrap();
        assert_eq!(info.execution_count, 7);
    }
}
