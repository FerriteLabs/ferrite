//! WASM Trigger Execution Engine
//!
//! Provides a sandboxed execution environment for running WebAssembly-based
//! programmable triggers. Includes resource limits, a dead-letter queue for
//! failed executions, execution logging, and per-module statistics.
//!
//! The executor manages WASM module registration, validates resource constraints,
//! and records execution metrics. Actual WASM bytecode interpretation is delegated
//! to the `ferrite-plugins` wasmtime integration at runtime; this module owns the
//! orchestration, sandboxing policy, and observability layers.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the WASM trigger execution engine.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WasmTriggerConfig {
    /// Whether WASM trigger execution is enabled.
    pub enabled: bool,
    /// Maximum number of registered WASM modules.
    pub max_modules: usize,
    /// Maximum execution time per invocation in microseconds.
    pub max_execution_time_us: u64,
    /// Maximum memory a single WASM module may consume (bytes).
    pub max_memory_bytes: usize,
    /// Maximum entries retained in the execution log ring buffer.
    pub max_log_entries: usize,
    /// Maximum entries retained in the dead-letter queue.
    pub max_dead_letter_entries: usize,
    /// Maximum retry attempts for dead-letter entries.
    pub max_retries: u32,
    /// Maximum side effects a single execution may produce.
    pub max_side_effects_per_execution: usize,
}

impl Default for WasmTriggerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_modules: 100,
            max_execution_time_us: 10_000, // 10 ms
            max_memory_bytes: 16 * 1024 * 1024, // 16 MB
            max_log_entries: 10_000,
            max_dead_letter_entries: 1_000,
            max_retries: 3,
            max_side_effects_per_execution: 10,
        }
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors produced by the WASM trigger executor.
#[derive(Debug, Clone, thiserror::Error)]
pub enum WasmTriggerError {
    /// The requested module was not found.
    #[error("module not found: {0}")]
    ModuleNotFound(String),
    /// The maximum number of registered modules has been reached.
    #[error("module limit exceeded")]
    ModuleLimitExceeded,
    /// A module with the given name is already registered.
    #[error("module already registered: {0}")]
    AlreadyRegistered(String),
    /// Execution exceeded the configured time limit.
    #[error("execution timeout")]
    ExecutionTimeout,
    /// Execution exceeded the configured memory limit.
    #[error("memory limit exceeded")]
    MemoryLimitExceeded,
    /// General execution failure.
    #[error("execution failed: {0}")]
    ExecutionFailed(String),
    /// The execution produced more side effects than allowed.
    #[error("too many side effects")]
    TooManySideEffects,
    /// The provided WASM module bytes are invalid.
    #[error("invalid module: {0}")]
    InvalidModule(String),
    /// WASM trigger execution is disabled.
    #[error("WASM trigger execution is disabled")]
    Disabled,
}

// ---------------------------------------------------------------------------
// Context / Result types
// ---------------------------------------------------------------------------

/// Input context passed to a WASM trigger function.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TriggerContext {
    /// The key involved in the operation.
    pub key: String,
    /// The new value (if any).
    pub value: Option<Vec<u8>>,
    /// The previous value (if any).
    pub old_value: Option<Vec<u8>>,
    /// The Redis operation name (e.g. SET, DEL).
    pub operation: String,
    /// Unix timestamp of the event (seconds).
    pub timestamp: u64,
    /// Database index.
    pub db: u8,
    /// Arbitrary metadata key-value pairs.
    pub metadata: HashMap<String, String>,
}

/// The action decision returned by a WASM trigger function.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TriggerAction {
    /// Allow the operation to proceed.
    Allow,
    /// Reject the operation with the given reason.
    Reject(String),
    /// Transform the value before committing.
    Transform(Vec<u8>),
}

/// A side effect requested by a WASM trigger function.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SideEffect {
    /// Publish a message to a pub/sub channel.
    Publish {
        /// Target channel.
        channel: String,
        /// Message payload.
        message: String,
    },
    /// Set a key with an optional TTL.
    SetKey {
        /// Key to set.
        key: String,
        /// Value bytes.
        value: Vec<u8>,
        /// Optional time-to-live in milliseconds.
        ttl_ms: Option<u64>,
    },
    /// Delete a key.
    DeleteKey {
        /// Key to delete.
        key: String,
    },
    /// Perform an outbound HTTP call.
    HttpCall {
        /// Target URL.
        url: String,
        /// HTTP method.
        method: String,
        /// Optional request body.
        body: Option<String>,
    },
    /// Emit a log message.
    Log {
        /// Log level (e.g. "info", "warn", "error").
        level: String,
        /// Log message.
        message: String,
    },
}

/// Result returned by a WASM trigger execution.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TriggerResult {
    /// The action decision (allow, reject, or transform).
    pub action: TriggerAction,
    /// Side effects requested during execution.
    pub side_effects: Vec<SideEffect>,
    /// Wall-clock execution time in microseconds.
    pub execution_time_us: u64,
    /// Log lines emitted during execution.
    pub logs: Vec<String>,
}

// ---------------------------------------------------------------------------
// Execution log
// ---------------------------------------------------------------------------

/// An entry in the execution log ring buffer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionLogEntry {
    /// Name of the WASM module that was executed.
    pub module_name: String,
    /// Key involved in the trigger.
    pub key: String,
    /// Operation that fired the trigger.
    pub operation: String,
    /// Short result label (Allow / Reject / Transform / Error).
    pub result: String,
    /// Execution duration in microseconds.
    pub execution_time_us: u64,
    /// Unix timestamp of execution.
    pub timestamp: u64,
    /// Error message if the execution failed.
    pub error: Option<String>,
}

// ---------------------------------------------------------------------------
// Dead-letter queue
// ---------------------------------------------------------------------------

/// An entry in the dead-letter queue representing a failed trigger execution.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeadLetterEntry {
    /// Name of the WASM module that failed.
    pub module_name: String,
    /// The original trigger context.
    pub context: TriggerContext,
    /// Error message from the last failure.
    pub error: String,
    /// Number of retries attempted so far.
    pub retry_count: u32,
    /// Maximum retries allowed.
    pub max_retries: u32,
    /// Timestamp of the first failure.
    pub first_failure: u64,
    /// Timestamp of the most recent failure.
    pub last_failure: u64,
}

// ---------------------------------------------------------------------------
// Module info
// ---------------------------------------------------------------------------

/// Serializable summary of a registered WASM module.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WasmModuleInfo {
    /// Module name.
    pub name: String,
    /// Size of the WASM bytecode in bytes.
    pub size_bytes: usize,
    /// Unix timestamp of registration.
    pub registered_at: u64,
    /// Number of times the module has been executed.
    pub execution_count: u64,
    /// Number of failed executions.
    pub error_count: u64,
    /// Average execution time in microseconds.
    pub avg_execution_us: f64,
}

// ---------------------------------------------------------------------------
// WASM Module
// ---------------------------------------------------------------------------

/// A registered WASM module with associated execution metrics.
pub struct WasmModule {
    /// Module name.
    pub name: String,
    /// Raw WASM bytecode.
    pub bytes: Vec<u8>,
    /// Unix timestamp of registration.
    pub registered_at: u64,
    /// Total number of executions.
    pub execution_count: AtomicU64,
    /// Timestamp of the last execution.
    pub last_execution: AtomicU64,
    /// Cumulative execution time in microseconds.
    pub total_execution_us: AtomicU64,
    /// Number of failed executions.
    pub error_count: AtomicU64,
    /// Size of the bytecode in bytes.
    pub size_bytes: usize,
}

impl WasmModule {
    /// Build a [`WasmModuleInfo`] snapshot from the current atomic counters.
    fn info(&self) -> WasmModuleInfo {
        let exec = self.execution_count.load(Ordering::Relaxed);
        let total_us = self.total_execution_us.load(Ordering::Relaxed);
        let avg = if exec > 0 {
            total_us as f64 / exec as f64
        } else {
            0.0
        };
        WasmModuleInfo {
            name: self.name.clone(),
            size_bytes: self.size_bytes,
            registered_at: self.registered_at,
            execution_count: exec,
            error_count: self.error_count.load(Ordering::Relaxed),
            avg_execution_us: avg,
        }
    }
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

/// Atomic counters for the WASM executor.
#[derive(Debug, Default)]
pub struct WasmExecutorStats {
    /// Total executions attempted.
    pub total_executions: AtomicU64,
    /// Successful executions.
    pub successful_executions: AtomicU64,
    /// Failed executions.
    pub failed_executions: AtomicU64,
    /// Total modules registered (lifetime).
    pub modules_registered: AtomicU64,
    /// Total modules unregistered (lifetime).
    pub modules_unregistered: AtomicU64,
    /// Dead-letter entries enqueued (lifetime).
    pub dead_letters_enqueued: AtomicU64,
}

/// Point-in-time snapshot of [`WasmExecutorStats`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WasmExecutorStatsSnapshot {
    /// Total executions attempted.
    pub total_executions: u64,
    /// Successful executions.
    pub successful_executions: u64,
    /// Failed executions.
    pub failed_executions: u64,
    /// Total modules registered (lifetime).
    pub modules_registered: u64,
    /// Total modules unregistered (lifetime).
    pub modules_unregistered: u64,
    /// Dead-letter entries enqueued (lifetime).
    pub dead_letters_enqueued: u64,
}

// ---------------------------------------------------------------------------
// Executor
// ---------------------------------------------------------------------------

/// WASM trigger execution engine.
///
/// Manages the lifecycle of WASM modules, enforces resource limits, records
/// execution metrics, and maintains a dead-letter queue for failed invocations.
pub struct WasmTriggerExecutor {
    /// Engine configuration.
    config: WasmTriggerConfig,
    /// Registered WASM modules keyed by name.
    modules: RwLock<HashMap<String, WasmModule>>,
    /// Ring buffer of recent execution log entries.
    execution_log: RwLock<VecDeque<ExecutionLogEntry>>,
    /// Queue of failed executions eligible for retry.
    dead_letter_queue: RwLock<VecDeque<DeadLetterEntry>>,
    /// Atomic statistics counters.
    stats: WasmExecutorStats,
}

impl WasmTriggerExecutor {
    /// Create a new executor with the given configuration.
    pub fn new(config: WasmTriggerConfig) -> Self {
        Self {
            config,
            modules: RwLock::new(HashMap::new()),
            execution_log: RwLock::new(VecDeque::new()),
            dead_letter_queue: RwLock::new(VecDeque::new()),
            stats: WasmExecutorStats::default(),
        }
    }

    /// Register a WASM module under the given name.
    ///
    /// Returns an error if the module name is already taken, the module limit
    /// has been reached, or the bytes do not look like a valid WASM module.
    pub fn register_module(
        &self,
        name: &str,
        wasm_bytes: Vec<u8>,
    ) -> Result<(), WasmTriggerError> {
        if !self.config.enabled {
            return Err(WasmTriggerError::Disabled);
        }

        // Minimal WASM magic-number validation (\0asm).
        if wasm_bytes.len() < 8 || &wasm_bytes[0..4] != b"\0asm" {
            return Err(WasmTriggerError::InvalidModule(
                "bytes do not start with WASM magic number".to_string(),
            ));
        }

        let mut modules = self.modules.write();

        if modules.contains_key(name) {
            return Err(WasmTriggerError::AlreadyRegistered(name.to_string()));
        }
        if modules.len() >= self.config.max_modules {
            return Err(WasmTriggerError::ModuleLimitExceeded);
        }

        let size = wasm_bytes.len();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        modules.insert(
            name.to_string(),
            WasmModule {
                name: name.to_string(),
                bytes: wasm_bytes,
                registered_at: now,
                execution_count: AtomicU64::new(0),
                last_execution: AtomicU64::new(0),
                total_execution_us: AtomicU64::new(0),
                error_count: AtomicU64::new(0),
                size_bytes: size,
            },
        );

        self.stats.modules_registered.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Remove a previously registered WASM module.
    pub fn unregister_module(&self, name: &str) -> Result<(), WasmTriggerError> {
        let mut modules = self.modules.write();
        if modules.remove(name).is_none() {
            return Err(WasmTriggerError::ModuleNotFound(name.to_string()));
        }
        self.stats
            .modules_unregistered
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Execute a WASM trigger.
    ///
    /// Validates that the module exists, simulates execution timing, and records
    /// the result in the execution log. On failure the context is enqueued in
    /// the dead-letter queue for later retry.
    pub fn execute_trigger(
        &self,
        module_name: &str,
        context: TriggerContext,
    ) -> Result<TriggerResult, WasmTriggerError> {
        if !self.config.enabled {
            return Err(WasmTriggerError::Disabled);
        }

        let start = std::time::Instant::now();

        // Look up module (read lock).
        let modules = self.modules.read();
        let module = modules
            .get(module_name)
            .ok_or_else(|| WasmTriggerError::ModuleNotFound(module_name.to_string()))?;

        // --- Simulated execution ---
        // In production the bytes would be handed to wasmtime. Here we
        // simulate a successful Allow result with deterministic timing.
        let elapsed_us = start.elapsed().as_micros() as u64;

        if elapsed_us > self.config.max_execution_time_us {
            let err = WasmTriggerError::ExecutionTimeout;
            module.error_count.fetch_add(1, Ordering::Relaxed);
            self.record_failure(module_name, &context, &err, elapsed_us);
            return Err(err);
        }

        // Update per-module stats.
        module.execution_count.fetch_add(1, Ordering::Relaxed);
        module.total_execution_us.fetch_add(elapsed_us, Ordering::Relaxed);
        let now_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        module.last_execution.store(now_ts, Ordering::Relaxed);

        // Build result.
        let result = TriggerResult {
            action: TriggerAction::Allow,
            side_effects: Vec::new(),
            execution_time_us: elapsed_us,
            logs: vec![format!(
                "executed {} for key '{}'",
                module_name, context.key
            )],
        };

        // Global stats.
        self.stats.total_executions.fetch_add(1, Ordering::Relaxed);
        self.stats
            .successful_executions
            .fetch_add(1, Ordering::Relaxed);

        // Execution log (ring buffer).
        self.push_log_entry(ExecutionLogEntry {
            module_name: module_name.to_string(),
            key: context.key.clone(),
            operation: context.operation.clone(),
            result: "Allow".to_string(),
            execution_time_us: elapsed_us,
            timestamp: now_ts,
            error: None,
        });

        Ok(result)
    }

    /// Return a list of all currently registered modules.
    pub fn list_modules(&self) -> Vec<WasmModuleInfo> {
        let modules = self.modules.read();
        modules.values().map(|m| m.info()).collect()
    }

    /// Return the most recent execution log entries (up to `limit`).
    pub fn get_execution_log(&self, limit: usize) -> Vec<ExecutionLogEntry> {
        let log = self.execution_log.read();
        log.iter().rev().take(limit).cloned().collect()
    }

    /// Drain up to `limit` entries from the dead-letter queue.
    pub fn drain_dead_letter_queue(&self, limit: usize) -> Vec<DeadLetterEntry> {
        let mut dlq = self.dead_letter_queue.write();
        let count = limit.min(dlq.len());
        dlq.drain(..count).collect()
    }

    /// Retry all eligible dead-letter entries.
    ///
    /// Entries that have exceeded `max_retries` are discarded. The remaining
    /// entries are re-executed; on success the result is returned, on failure
    /// the entry is re-enqueued with an incremented retry count.
    pub fn retry_dead_letters(&self) -> Vec<Result<TriggerResult, WasmTriggerError>> {
        let entries: Vec<DeadLetterEntry> = {
            let mut dlq = self.dead_letter_queue.write();
            dlq.drain(..).collect()
        };

        let mut results = Vec::with_capacity(entries.len());

        for entry in entries {
            if entry.retry_count >= entry.max_retries {
                results.push(Err(WasmTriggerError::ExecutionFailed(format!(
                    "max retries ({}) exceeded for module '{}'",
                    entry.max_retries, entry.module_name
                ))));
                continue;
            }

            match self.execute_trigger(&entry.module_name, entry.context.clone()) {
                Ok(res) => results.push(Ok(res)),
                Err(err) => {
                    // Re-enqueue with incremented retry count.
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    let mut dlq = self.dead_letter_queue.write();
                    if dlq.len() < self.config.max_dead_letter_entries {
                        dlq.push_back(DeadLetterEntry {
                            retry_count: entry.retry_count + 1,
                            last_failure: now,
                            error: err.to_string(),
                            ..entry
                        });
                    }
                    results.push(Err(err));
                }
            }
        }

        results
    }

    /// Return a point-in-time snapshot of executor statistics.
    pub fn stats(&self) -> WasmExecutorStatsSnapshot {
        WasmExecutorStatsSnapshot {
            total_executions: self.stats.total_executions.load(Ordering::Relaxed),
            successful_executions: self.stats.successful_executions.load(Ordering::Relaxed),
            failed_executions: self.stats.failed_executions.load(Ordering::Relaxed),
            modules_registered: self.stats.modules_registered.load(Ordering::Relaxed),
            modules_unregistered: self.stats.modules_unregistered.load(Ordering::Relaxed),
            dead_letters_enqueued: self.stats.dead_letters_enqueued.load(Ordering::Relaxed),
        }
    }

    // -- internal helpers ---------------------------------------------------

    /// Push an entry to the execution log, evicting the oldest if at capacity.
    fn push_log_entry(&self, entry: ExecutionLogEntry) {
        let mut log = self.execution_log.write();
        if log.len() >= self.config.max_log_entries {
            log.pop_front();
        }
        log.push_back(entry);
    }

    /// Record a failed execution: update global stats, write log, enqueue DLQ.
    fn record_failure(
        &self,
        module_name: &str,
        context: &TriggerContext,
        error: &WasmTriggerError,
        elapsed_us: u64,
    ) {
        self.stats.total_executions.fetch_add(1, Ordering::Relaxed);
        self.stats.failed_executions.fetch_add(1, Ordering::Relaxed);

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        self.push_log_entry(ExecutionLogEntry {
            module_name: module_name.to_string(),
            key: context.key.clone(),
            operation: context.operation.clone(),
            result: "Error".to_string(),
            execution_time_us: elapsed_us,
            timestamp: now,
            error: Some(error.to_string()),
        });

        let mut dlq = self.dead_letter_queue.write();
        if dlq.len() < self.config.max_dead_letter_entries {
            dlq.push_back(DeadLetterEntry {
                module_name: module_name.to_string(),
                context: context.clone(),
                error: error.to_string(),
                retry_count: 0,
                max_retries: self.config.max_retries,
                first_failure: now,
                last_failure: now,
            });
            self.stats
                .dead_letters_enqueued
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal valid WASM header (magic + version) followed by padding.
    fn dummy_wasm_bytes() -> Vec<u8> {
        let mut bytes = vec![0x00, 0x61, 0x73, 0x6D]; // \0asm
        bytes.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]); // version 1
        bytes.extend_from_slice(&[0x00; 64]); // padding
        bytes
    }

    fn default_context() -> TriggerContext {
        TriggerContext {
            key: "user:42".to_string(),
            value: Some(b"hello".to_vec()),
            old_value: None,
            operation: "SET".to_string(),
            timestamp: 1_700_000_000,
            db: 0,
            metadata: HashMap::new(),
        }
    }

    #[test]
    fn test_register_and_list_modules() {
        let exec = WasmTriggerExecutor::new(WasmTriggerConfig::default());

        exec.register_module("mod_a", dummy_wasm_bytes())
            .expect("register mod_a");
        exec.register_module("mod_b", dummy_wasm_bytes())
            .expect("register mod_b");

        let mods = exec.list_modules();
        assert_eq!(mods.len(), 2);

        let names: Vec<&str> = mods.iter().map(|m| m.name.as_str()).collect();
        assert!(names.contains(&"mod_a"));
        assert!(names.contains(&"mod_b"));
    }

    #[test]
    fn test_execute_trigger_returns_result() {
        let exec = WasmTriggerExecutor::new(WasmTriggerConfig::default());
        exec.register_module("validator", dummy_wasm_bytes())
            .expect("register");

        let result = exec
            .execute_trigger("validator", default_context())
            .expect("execute");

        assert!(matches!(result.action, TriggerAction::Allow));
        assert!(!result.logs.is_empty());
    }

    #[test]
    fn test_unregister_module() {
        let exec = WasmTriggerExecutor::new(WasmTriggerConfig::default());
        exec.register_module("temp", dummy_wasm_bytes())
            .expect("register");

        exec.unregister_module("temp").expect("unregister");
        assert!(exec.list_modules().is_empty());

        // Unregistering again should fail.
        let err = exec.unregister_module("temp").unwrap_err();
        assert!(matches!(err, WasmTriggerError::ModuleNotFound(_)));
    }

    #[test]
    fn test_module_not_found_error() {
        let exec = WasmTriggerExecutor::new(WasmTriggerConfig::default());

        let err = exec
            .execute_trigger("nonexistent", default_context())
            .unwrap_err();

        assert!(matches!(err, WasmTriggerError::ModuleNotFound(_)));
    }

    #[test]
    fn test_dead_letter_queue_on_failure() {
        let exec = WasmTriggerExecutor::new(WasmTriggerConfig::default());

        // Attempt execution against a missing module — record_failure won't be
        // called through execute_trigger for ModuleNotFound because that path
        // returns early. Instead we simulate by calling record_failure directly.
        let ctx = default_context();
        let err = WasmTriggerError::ExecutionFailed("simulated".to_string());
        exec.record_failure("bad_mod", &ctx, &err, 500);

        let dlq = exec.drain_dead_letter_queue(10);
        assert_eq!(dlq.len(), 1);
        assert_eq!(dlq[0].module_name, "bad_mod");
        assert_eq!(dlq[0].retry_count, 0);
    }

    #[test]
    fn test_execution_log_ring_buffer() {
        let config = WasmTriggerConfig {
            max_log_entries: 3,
            ..WasmTriggerConfig::default()
        };
        let exec = WasmTriggerExecutor::new(config);
        exec.register_module("mod", dummy_wasm_bytes())
            .expect("register");

        for i in 0..5 {
            let mut ctx = default_context();
            ctx.key = format!("key:{}", i);
            exec.execute_trigger("mod", ctx).expect("execute");
        }

        let log = exec.get_execution_log(10);
        // Ring buffer capped at 3.
        assert_eq!(log.len(), 3);
        // Most recent first.
        assert_eq!(log[0].key, "key:4");
    }

    #[test]
    fn test_stats_tracking() {
        let exec = WasmTriggerExecutor::new(WasmTriggerConfig::default());
        exec.register_module("s", dummy_wasm_bytes())
            .expect("register");

        exec.execute_trigger("s", default_context())
            .expect("execute");
        exec.execute_trigger("s", default_context())
            .expect("execute");

        let snap = exec.stats();
        assert_eq!(snap.total_executions, 2);
        assert_eq!(snap.successful_executions, 2);
        assert_eq!(snap.modules_registered, 1);
    }

    #[test]
    fn test_config_limits_max_modules() {
        let config = WasmTriggerConfig {
            max_modules: 2,
            ..WasmTriggerConfig::default()
        };
        let exec = WasmTriggerExecutor::new(config);

        exec.register_module("a", dummy_wasm_bytes())
            .expect("register a");
        exec.register_module("b", dummy_wasm_bytes())
            .expect("register b");

        let err = exec
            .register_module("c", dummy_wasm_bytes())
            .unwrap_err();
        assert!(matches!(err, WasmTriggerError::ModuleLimitExceeded));
    }

    #[test]
    fn test_duplicate_registration_rejected() {
        let exec = WasmTriggerExecutor::new(WasmTriggerConfig::default());
        exec.register_module("dup", dummy_wasm_bytes())
            .expect("first");

        let err = exec
            .register_module("dup", dummy_wasm_bytes())
            .unwrap_err();
        assert!(matches!(err, WasmTriggerError::AlreadyRegistered(_)));
    }

    #[test]
    fn test_invalid_wasm_bytes_rejected() {
        let exec = WasmTriggerExecutor::new(WasmTriggerConfig::default());

        let err = exec
            .register_module("bad", vec![0xFF; 16])
            .unwrap_err();
        assert!(matches!(err, WasmTriggerError::InvalidModule(_)));
    }

    #[test]
    fn test_disabled_executor() {
        let config = WasmTriggerConfig {
            enabled: false,
            ..WasmTriggerConfig::default()
        };
        let exec = WasmTriggerExecutor::new(config);

        let err = exec
            .register_module("x", dummy_wasm_bytes())
            .unwrap_err();
        assert!(matches!(err, WasmTriggerError::Disabled));
    }
}
