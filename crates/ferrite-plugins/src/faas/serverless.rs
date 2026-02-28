//! Serverless Function Runtime
//!
//! Execute JavaScript/TypeScript functions inside Ferrite, triggered
//! by data events. Sub-millisecond cold start via lightweight runtime.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use thiserror::Error;

// ── Configuration ──────────────────────────────────────────────────────────

/// Global configuration for the serverless runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionConfig {
    /// Maximum number of deployed functions.
    pub max_functions: usize,
    /// Default maximum execution time per invocation.
    pub max_execution_time: Duration,
    /// Default memory limit per invocation (bytes).
    pub max_memory_bytes: u64,
    /// Enable sandboxed execution.
    pub sandbox_enabled: bool,
    /// Allow outbound network access from functions.
    pub enable_network: bool,
}

impl Default for FunctionConfig {
    fn default() -> Self {
        Self {
            max_functions: 1000,
            max_execution_time: Duration::from_secs(5),
            max_memory_bytes: 64 * 1024 * 1024,
            sandbox_enabled: true,
            enable_network: false,
        }
    }
}

// ── Core types ─────────────────────────────────────────────────────────────

/// Supported function runtimes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum FunctionRuntime {
    /// JavaScript (V8-backed).
    JavaScript,
    /// TypeScript (transpiled to JS).
    TypeScript,
    /// Python (embedded interpreter).
    Python,
    /// WebAssembly module.
    Wasm,
}

impl FunctionRuntime {
    /// Parse a runtime name from a string.
    pub fn from_str_name(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "js" | "javascript" => Some(Self::JavaScript),
            "ts" | "typescript" => Some(Self::TypeScript),
            "python" | "py" => Some(Self::Python),
            "wasm" | "webassembly" => Some(Self::Wasm),
            _ => None,
        }
    }

    /// Display name.
    pub fn name(&self) -> &'static str {
        match self {
            Self::JavaScript => "javascript",
            Self::TypeScript => "typescript",
            Self::Python => "python",
            Self::Wasm => "wasm",
        }
    }
}

/// Events that can trigger a function.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TriggerEvent {
    /// Triggered when a key is set.
    OnSet,
    /// Triggered when a key is deleted.
    OnDel,
    /// Triggered when a key expires.
    OnExpire,
    /// Triggered on pub/sub publish.
    OnPublish,
    /// Triggered on a cron schedule.
    OnSchedule {
        /// Cron expression.
        cron: String,
    },
    /// Triggered by an HTTP request.
    OnHttp {
        /// HTTP path.
        path: String,
        /// HTTP method.
        method: String,
    },
    /// Triggered by a stream entry.
    OnStream {
        /// Stream name.
        stream: String,
    },
    /// Manually invoked.
    Manual,
}

impl TriggerEvent {
    /// Parse a trigger event name.
    pub fn from_str_name(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "onset" => Some(Self::OnSet),
            "ondel" => Some(Self::OnDel),
            "onexpire" => Some(Self::OnExpire),
            "onpublish" => Some(Self::OnPublish),
            "manual" => Some(Self::Manual),
            _ => None,
        }
    }
}

/// A function trigger definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionTrigger {
    /// Event that fires this trigger.
    pub event: TriggerEvent,
    /// Optional key pattern filter.
    pub key_pattern: Option<String>,
    /// Optional filter expression.
    pub filter: Option<String>,
    /// Optional debounce duration.
    pub debounce: Option<Duration>,
}

/// Full definition of a deployed function.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionDefinition {
    /// Function name (unique identifier).
    pub name: String,
    /// Execution runtime.
    pub runtime: FunctionRuntime,
    /// Source code or path.
    pub source: String,
    /// Trigger definitions.
    pub triggers: Vec<FunctionTrigger>,
    /// Per-invocation timeout.
    pub timeout: Duration,
    /// Per-invocation memory limit (bytes).
    pub memory_limit: u64,
    /// Environment variables.
    pub env: HashMap<String, String>,
    /// Deployment timestamp (epoch ms).
    pub created_at: u64,
    /// Last update timestamp (epoch ms).
    pub updated_at: u64,
    /// Definition version (incremented on redeploy).
    pub version: u32,
    /// Whether the function is enabled.
    pub enabled: bool,
}

/// Execution status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FunctionStatus {
    /// Completed successfully.
    Success,
    /// Completed with an error.
    Error(String),
    /// Exceeded the time limit.
    Timeout,
    /// Killed due to memory limit.
    OomKilled,
}

/// Result of a single function invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionResult {
    /// Name of the invoked function.
    pub function_name: String,
    /// Unique execution identifier.
    pub execution_id: String,
    /// Execution status.
    pub status: FunctionStatus,
    /// Output value, if any.
    pub output: Option<serde_json::Value>,
    /// Log lines produced.
    pub logs: Vec<String>,
    /// Wall-clock execution duration in milliseconds.
    pub duration_ms: u64,
    /// Approximate memory used (bytes).
    pub memory_used: u64,
}

/// Summary information for listing functions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionInfo {
    /// Function name.
    pub name: String,
    /// Runtime type.
    pub runtime: FunctionRuntime,
    /// Number of triggers.
    pub triggers_count: usize,
    /// Current version.
    pub version: u32,
    /// Whether the function is enabled.
    pub enabled: bool,
    /// Total invocation count.
    pub invocations: u64,
    /// Average execution duration (ms).
    pub avg_duration_ms: f64,
    /// Last invocation timestamp (epoch ms), if any.
    pub last_invoked: Option<u64>,
}

/// Aggregate runtime statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeStats {
    /// Number of deployed functions.
    pub deployed_functions: usize,
    /// Total invocations across all functions.
    pub total_invocations: u64,
    /// Currently executing invocations.
    pub active_executions: u32,
    /// Average cold-start latency (ms).
    pub avg_cold_start_ms: f64,
    /// Average execution latency (ms).
    pub avg_execution_ms: f64,
    /// Total errors.
    pub errors: u64,
    /// Total timeouts.
    pub timeouts: u64,
}

// ── Errors ─────────────────────────────────────────────────────────────────

/// Errors produced by the serverless runtime.
#[derive(Debug, Error)]
pub enum FunctionError {
    /// Function not found.
    #[error("function not found: {0}")]
    NotFound(String),
    /// Function already exists.
    #[error("function already exists: {0}")]
    AlreadyExists(String),
    /// Source compilation failed.
    #[error("compilation failed: {0}")]
    CompilationFailed(String),
    /// Runtime execution error.
    #[error("runtime error: {0}")]
    RuntimeError(String),
    /// Execution timed out.
    #[error("function execution timed out")]
    Timeout,
    /// Out of memory.
    #[error("function killed: out of memory")]
    OomKilled,
    /// Maximum function count reached.
    #[error("maximum number of functions reached ({0})")]
    MaxFunctions(usize),
    /// Invalid source code.
    #[error("invalid source: {0}")]
    InvalidSource(String),
}

// ── Internal state ─────────────────────────────────────────────────────────

/// Per-function bookkeeping.
struct FunctionState {
    definition: FunctionDefinition,
    invocations: AtomicU64,
    total_duration_ms: AtomicU64,
    errors: AtomicU64,
    last_invoked: RwLock<Option<u64>>,
    recent_results: RwLock<Vec<FunctionResult>>,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn make_execution_id() -> String {
    use std::sync::atomic::AtomicU64 as Counter;
    static SEQ: Counter = Counter::new(1);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    format!("exec-{}-{}", now_ms(), seq)
}

// ── ServerlessRuntime ──────────────────────────────────────────────────────

/// Thread-safe serverless function runtime.
pub struct ServerlessRuntime {
    config: FunctionConfig,
    functions: RwLock<HashMap<String, Arc<FunctionState>>>,
    total_invocations: AtomicU64,
    active_executions: AtomicU32,
    total_errors: AtomicU64,
    total_timeouts: AtomicU64,
    total_cold_start_ms: AtomicU64,
    cold_start_count: AtomicU64,
}

impl ServerlessRuntime {
    /// Create a new serverless runtime.
    pub fn new(config: FunctionConfig) -> Self {
        Self {
            config,
            functions: RwLock::new(HashMap::new()),
            total_invocations: AtomicU64::new(0),
            active_executions: AtomicU32::new(0),
            total_errors: AtomicU64::new(0),
            total_timeouts: AtomicU64::new(0),
            total_cold_start_ms: AtomicU64::new(0),
            cold_start_count: AtomicU64::new(0),
        }
    }

    /// Deploy a function. Returns the function name as identifier.
    pub fn deploy(&self, def: FunctionDefinition) -> Result<String, FunctionError> {
        let mut fns = self.functions.write();

        if fns.len() >= self.config.max_functions && !fns.contains_key(&def.name) {
            return Err(FunctionError::MaxFunctions(self.config.max_functions));
        }

        if def.source.trim().is_empty() {
            return Err(FunctionError::InvalidSource(
                "source cannot be empty".to_string(),
            ));
        }

        let name = def.name.clone();
        let state = Arc::new(FunctionState {
            definition: def,
            invocations: AtomicU64::new(0),
            total_duration_ms: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            last_invoked: RwLock::new(None),
            recent_results: RwLock::new(Vec::new()),
        });

        if fns.contains_key(&name) {
            return Err(FunctionError::AlreadyExists(name));
        }

        fns.insert(name.clone(), state);
        Ok(name)
    }

    /// Invoke a function synchronously (simulated execution).
    pub fn invoke(
        &self,
        name: &str,
        event: serde_json::Value,
    ) -> Result<FunctionResult, FunctionError> {
        let fns = self.functions.read();
        let state = fns
            .get(name)
            .ok_or_else(|| FunctionError::NotFound(name.to_string()))?
            .clone();
        drop(fns);

        if !state.definition.enabled {
            return Err(FunctionError::RuntimeError(format!(
                "function '{}' is disabled",
                name
            )));
        }

        let cold_start = Instant::now();
        self.active_executions.fetch_add(1, Ordering::Relaxed);
        self.total_invocations.fetch_add(1, Ordering::Relaxed);

        let cold_start_ms = cold_start.elapsed().as_millis() as u64;
        self.total_cold_start_ms
            .fetch_add(cold_start_ms, Ordering::Relaxed);
        self.cold_start_count.fetch_add(1, Ordering::Relaxed);

        // Simulated execution: parse source as a JSON object to produce output.
        let exec_start = Instant::now();
        let output = match serde_json::from_str::<serde_json::Value>(&state.definition.source) {
            Ok(val) => Some(val),
            Err(_) => {
                // Treat non-JSON source as a simple echo function.
                Some(serde_json::json!({
                    "echo": state.definition.source,
                    "event": event,
                    "runtime": state.definition.runtime.name(),
                }))
            }
        };

        let duration_ms = exec_start.elapsed().as_millis() as u64;
        self.active_executions.fetch_sub(1, Ordering::Relaxed);

        state.invocations.fetch_add(1, Ordering::Relaxed);
        state
            .total_duration_ms
            .fetch_add(duration_ms, Ordering::Relaxed);
        *state.last_invoked.write() = Some(now_ms());

        let result = FunctionResult {
            function_name: name.to_string(),
            execution_id: make_execution_id(),
            status: FunctionStatus::Success,
            output,
            logs: vec![format!("Executed {} (simulated)", name)],
            duration_ms,
            memory_used: 0,
        };

        let mut recent = state.recent_results.write();
        recent.push(result.clone());
        if recent.len() > 100 {
            recent.remove(0);
        }

        Ok(result)
    }

    /// Fire-and-forget invocation. Returns an execution id.
    pub fn invoke_async(
        &self,
        name: &str,
        event: serde_json::Value,
    ) -> Result<String, FunctionError> {
        let result = self.invoke(name, event)?;
        Ok(result.execution_id)
    }

    /// Remove a deployed function.
    pub fn undeploy(&self, name: &str) -> Result<(), FunctionError> {
        let mut fns = self.functions.write();
        if fns.remove(name).is_none() {
            return Err(FunctionError::NotFound(name.to_string()));
        }
        Ok(())
    }

    /// Get a function definition.
    pub fn get_function(&self, name: &str) -> Option<FunctionDefinition> {
        self.functions
            .read()
            .get(name)
            .map(|s| s.definition.clone())
    }

    /// List all deployed functions.
    pub fn list_functions(&self) -> Vec<FunctionInfo> {
        self.functions
            .read()
            .values()
            .map(|s| {
                let invocations = s.invocations.load(Ordering::Relaxed);
                let total_dur = s.total_duration_ms.load(Ordering::Relaxed);
                let avg = if invocations > 0 {
                    total_dur as f64 / invocations as f64
                } else {
                    0.0
                };
                FunctionInfo {
                    name: s.definition.name.clone(),
                    runtime: s.definition.runtime.clone(),
                    triggers_count: s.definition.triggers.len(),
                    version: s.definition.version,
                    enabled: s.definition.enabled,
                    invocations,
                    avg_duration_ms: avg,
                    last_invoked: *s.last_invoked.read(),
                }
            })
            .collect()
    }

    /// Get execution result by id from recent history.
    pub fn get_result(&self, execution_id: &str) -> Option<FunctionResult> {
        let fns = self.functions.read();
        for state in fns.values() {
            let recent = state.recent_results.read();
            for r in recent.iter().rev() {
                if r.execution_id == execution_id {
                    return Some(r.clone());
                }
            }
        }
        None
    }

    /// Get recent executions for a function.
    pub fn recent_executions(&self, name: &str, limit: usize) -> Vec<FunctionResult> {
        self.functions
            .read()
            .get(name)
            .map(|s| {
                let recent = s.recent_results.read();
                recent.iter().rev().take(limit).cloned().collect()
            })
            .unwrap_or_default()
    }

    /// Enable a function.
    pub fn enable(&self, name: &str) -> Result<(), FunctionError> {
        let fns = self.functions.read();
        let _state = fns
            .get(name)
            .ok_or_else(|| FunctionError::NotFound(name.to_string()))?;
        // We need mutable access to the definition — drop read, take write.
        drop(fns);

        let mut fns = self.functions.write();
        if let Some(s) = fns.get_mut(name) {
            let def = &mut Arc::get_mut(s)
                .ok_or_else(|| FunctionError::RuntimeError("concurrent access".to_string()))?
                .definition;
            def.enabled = true;
            def.updated_at = now_ms();
        }
        Ok(())
    }

    /// Disable a function.
    pub fn disable(&self, name: &str) -> Result<(), FunctionError> {
        let fns = self.functions.read();
        let _state = fns
            .get(name)
            .ok_or_else(|| FunctionError::NotFound(name.to_string()))?;
        drop(fns);

        let mut fns = self.functions.write();
        if let Some(s) = fns.get_mut(name) {
            let def = &mut Arc::get_mut(s)
                .ok_or_else(|| FunctionError::RuntimeError("concurrent access".to_string()))?
                .definition;
            def.enabled = false;
            def.updated_at = now_ms();
        }
        Ok(())
    }

    /// Return aggregate runtime statistics.
    pub fn stats(&self) -> RuntimeStats {
        let total_inv = self.total_invocations.load(Ordering::Relaxed);
        let cold_count = self.cold_start_count.load(Ordering::Relaxed);
        let cold_total = self.total_cold_start_ms.load(Ordering::Relaxed);

        let fns = self.functions.read();
        let mut total_dur = 0u64;
        let mut total_fn_inv = 0u64;
        for s in fns.values() {
            total_dur += s.total_duration_ms.load(Ordering::Relaxed);
            total_fn_inv += s.invocations.load(Ordering::Relaxed);
        }

        RuntimeStats {
            deployed_functions: fns.len(),
            total_invocations: total_inv,
            active_executions: self.active_executions.load(Ordering::Relaxed),
            avg_cold_start_ms: if cold_count > 0 {
                cold_total as f64 / cold_count as f64
            } else {
                0.0
            },
            avg_execution_ms: if total_fn_inv > 0 {
                total_dur as f64 / total_fn_inv as f64
            } else {
                0.0
            },
            errors: self.total_errors.load(Ordering::Relaxed),
            timeouts: self.total_timeouts.load(Ordering::Relaxed),
        }
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_runtime() -> ServerlessRuntime {
        ServerlessRuntime::new(FunctionConfig::default())
    }

    fn test_definition(name: &str) -> FunctionDefinition {
        FunctionDefinition {
            name: name.to_string(),
            runtime: FunctionRuntime::JavaScript,
            source: r#"{"result": "ok"}"#.to_string(),
            triggers: vec![FunctionTrigger {
                event: TriggerEvent::Manual,
                key_pattern: None,
                filter: None,
                debounce: None,
            }],
            timeout: Duration::from_secs(5),
            memory_limit: 64 * 1024 * 1024,
            env: HashMap::new(),
            created_at: now_ms(),
            updated_at: now_ms(),
            version: 1,
            enabled: true,
        }
    }

    #[test]
    fn test_deploy_and_list() {
        let rt = test_runtime();
        let id = rt.deploy(test_definition("hello")).expect("deploy");
        assert_eq!(id, "hello");
        let fns = rt.list_functions();
        assert_eq!(fns.len(), 1);
        assert_eq!(fns[0].name, "hello");
    }

    #[test]
    fn test_deploy_duplicate_fails() {
        let rt = test_runtime();
        rt.deploy(test_definition("dup")).expect("first");
        let result = rt.deploy(test_definition("dup"));
        assert!(matches!(result, Err(FunctionError::AlreadyExists(_))));
    }

    #[test]
    fn test_invoke_success() {
        let rt = test_runtime();
        rt.deploy(test_definition("fn1")).expect("deploy");
        let result = rt
            .invoke("fn1", serde_json::json!({"key": "val"}))
            .expect("invoke");
        assert_eq!(result.function_name, "fn1");
        assert!(matches!(result.status, FunctionStatus::Success));
        assert!(result.output.is_some());
    }

    #[test]
    fn test_invoke_not_found() {
        let rt = test_runtime();
        let result = rt.invoke("missing", serde_json::json!({}));
        assert!(matches!(result, Err(FunctionError::NotFound(_))));
    }

    #[test]
    fn test_undeploy() {
        let rt = test_runtime();
        rt.deploy(test_definition("rm")).expect("deploy");
        rt.undeploy("rm").expect("undeploy");
        assert!(rt.get_function("rm").is_none());
    }

    #[test]
    fn test_recent_executions() {
        let rt = test_runtime();
        rt.deploy(test_definition("hist")).expect("deploy");
        rt.invoke("hist", serde_json::json!({})).expect("inv1");
        rt.invoke("hist", serde_json::json!({})).expect("inv2");

        let recent = rt.recent_executions("hist", 10);
        assert_eq!(recent.len(), 2);
    }

    #[test]
    fn test_invoke_async() {
        let rt = test_runtime();
        rt.deploy(test_definition("async_fn")).expect("deploy");
        let exec_id = rt
            .invoke_async("async_fn", serde_json::json!({}))
            .expect("async invoke");
        assert!(exec_id.starts_with("exec-"));

        let result = rt.get_result(&exec_id);
        assert!(result.is_some());
    }

    #[test]
    fn test_stats() {
        let rt = test_runtime();
        rt.deploy(test_definition("s1")).expect("deploy");
        rt.invoke("s1", serde_json::json!({})).expect("invoke");

        let stats = rt.stats();
        assert_eq!(stats.deployed_functions, 1);
        assert_eq!(stats.total_invocations, 1);
    }

    #[test]
    fn test_empty_source_fails() {
        let rt = test_runtime();
        let mut def = test_definition("bad");
        def.source = "  ".to_string();
        let result = rt.deploy(def);
        assert!(matches!(result, Err(FunctionError::InvalidSource(_))));
    }

    #[test]
    fn test_get_function() {
        let rt = test_runtime();
        rt.deploy(test_definition("gf")).expect("deploy");
        let def = rt.get_function("gf");
        assert!(def.is_some());
        assert_eq!(def.as_ref().map(|d| d.name.as_str()), Some("gf"));
    }

    #[test]
    fn test_function_runtime_parse() {
        assert!(matches!(
            FunctionRuntime::from_str_name("js"),
            Some(FunctionRuntime::JavaScript)
        ));
        assert!(matches!(
            FunctionRuntime::from_str_name("python"),
            Some(FunctionRuntime::Python)
        ));
        assert!(matches!(
            FunctionRuntime::from_str_name("wasm"),
            Some(FunctionRuntime::Wasm)
        ));
        assert!(FunctionRuntime::from_str_name("unknown").is_none());
    }
}
