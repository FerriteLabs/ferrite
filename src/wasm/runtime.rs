//! WASM Runtime
//!
//! Wraps the wasmtime `Engine`, `Linker`, and `Store` to provide a complete
//! execution environment for user-defined WASM functions.
//!
//! Key features:
//! - Module compilation and caching (keyed by content hash)
//! - Fuel-based CPU limiting (configurable per-call)
//! - Memory limits (configurable per-module, default 16 MB)
//! - Execution timeout (default 5 seconds)
//! - Host function bindings for Ferrite store operations

use std::collections::HashMap;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use wasmtime::{Config, Engine, Linker, Module, Store, Val};

use ferrite_plugins::wasm::{
    FunctionPermissions, ResourceLimits, ResourceUsage, WasmError, WasmValue,
};

use super::host::{self, HostState};

/// Configuration for the WASM runtime.
#[derive(Clone, Debug)]
pub struct WasmConfig {
    /// Default memory limit per instance in bytes (default: 16 MB).
    pub default_memory_limit: usize,
    /// Default fuel limit per execution (default: 10_000_000).
    pub default_fuel_limit: u64,
    /// Default execution timeout (default: 5 seconds).
    pub default_timeout: Duration,
    /// Whether to enable module caching (default: true).
    pub enable_cache: bool,
}

impl Default for WasmConfig {
    fn default() -> Self {
        Self {
            default_memory_limit: 16 * 1024 * 1024, // 16 MB
            default_fuel_limit: 10_000_000,
            default_timeout: Duration::from_secs(5),
            enable_cache: true,
        }
    }
}

/// Result of executing a WASM function.
#[derive(Clone, Debug)]
pub struct ExecutionResult {
    /// Return values from the function.
    pub values: Vec<WasmValue>,
    /// Resource usage during execution.
    pub usage: ResourceUsage,
    /// Wall-clock execution time.
    pub duration: Duration,
    /// Whether the execution succeeded.
    pub success: bool,
    /// Error message if execution failed.
    pub error: Option<String>,
}

impl ExecutionResult {
    /// Create a successful result.
    pub fn success(values: Vec<WasmValue>, usage: ResourceUsage, duration: Duration) -> Self {
        Self {
            values,
            usage,
            duration,
            success: true,
            error: None,
        }
    }

    /// Create a failed result.
    pub fn failure(error: String, usage: ResourceUsage, duration: Duration) -> Self {
        Self {
            values: Vec::new(),
            usage,
            duration,
            success: false,
            error: Some(error),
        }
    }

    /// Get the first return value as i32.
    pub fn as_i32(&self) -> Option<i32> {
        self.values.first().and_then(|v| v.as_i32())
    }

    /// Get the first return value as i64.
    pub fn as_i64(&self) -> Option<i64> {
        self.values.first().and_then(|v| v.as_i64())
    }
}

/// Metadata about a compiled WASM module.
#[derive(Clone, Debug)]
pub struct ModuleInfo {
    /// Module name.
    pub name: String,
    /// Content hash of the WASM bytes.
    pub hash: String,
    /// Size of the WASM bytes.
    pub size: usize,
    /// Exported function names.
    pub exports: Vec<String>,
}

/// Runtime statistics.
#[derive(Clone, Debug, Default)]
pub struct RuntimeStats {
    /// Total modules compiled.
    pub modules_compiled: u64,
    /// Total executions.
    pub total_executions: u64,
    /// Successful executions.
    pub successful_executions: u64,
    /// Failed executions.
    pub failed_executions: u64,
    /// Cache hits during module lookup.
    pub cache_hits: u64,
    /// Cache misses during module lookup.
    pub cache_misses: u64,
}

/// The core WASM runtime that manages compilation, caching, and execution.
///
/// Thread-safe: the `Engine` is `Send + Sync`, and mutable state is behind `RwLock`.
pub struct WasmRuntime {
    /// Wasmtime engine configured with fuel metering.
    engine: Engine,
    /// Pre-configured linker with host function bindings.
    linker: Linker<HostState>,
    /// Compiled module cache keyed by content hash.
    modules: RwLock<HashMap<String, Module>>,
    /// Runtime configuration.
    config: WasmConfig,
    /// Execution statistics.
    stats: RwLock<RuntimeStats>,
}

impl WasmRuntime {
    /// Create a new WASM runtime with the given configuration.
    ///
    /// This sets up the wasmtime engine with fuel metering enabled and
    /// registers all Ferrite host functions in the linker.
    pub fn new(config: WasmConfig) -> Result<Self, WasmError> {
        let mut engine_config = Config::new();
        engine_config.consume_fuel(true);

        let engine = Engine::new(&engine_config)
            .map_err(|e| WasmError::Internal(format!("failed to create wasmtime engine: {}", e)))?;

        let mut linker = Linker::new(&engine);
        host::register_host_functions(&mut linker).map_err(|e| {
            WasmError::Internal(format!("failed to register host functions: {}", e))
        })?;

        Ok(Self {
            engine,
            linker,
            modules: RwLock::new(HashMap::new()),
            config,
            stats: RwLock::new(RuntimeStats::default()),
        })
    }

    /// Create a runtime with default configuration.
    pub fn with_defaults() -> Result<Self, WasmError> {
        Self::new(WasmConfig::default())
    }

    /// Compile and register a WASM module.
    ///
    /// Returns the content hash that can be used to look up the module later.
    /// If a module with the same hash is already cached, this is a no-op.
    pub fn register_module(&self, name: &str, wasm_bytes: &[u8]) -> Result<String, WasmError> {
        // Validate magic number
        validate_wasm_bytes(wasm_bytes)?;

        let hash = calculate_hash(wasm_bytes);

        // Check cache
        {
            let cache = self.modules.read();
            if cache.contains_key(&hash) {
                self.stats.write().cache_hits += 1;
                return Ok(hash);
            }
        }

        // Compile
        let module = Module::new(&self.engine, wasm_bytes)
            .map_err(|e| WasmError::CompilationError(format!("{}", e)))?;

        // Cache
        {
            let mut cache = self.modules.write();
            cache.insert(hash.clone(), module);
        }

        {
            let mut stats = self.stats.write();
            stats.modules_compiled += 1;
            stats.cache_misses += 1;
        }

        tracing::info!(module = name, hash = %hash, "WASM module compiled and cached");

        Ok(hash)
    }

    /// Execute a function in a WASM module.
    ///
    /// Creates a fresh `Store` per call with the given resource limits and
    /// host state. The module is looked up from the cache by hash.
    pub fn execute(
        &self,
        module_hash: &str,
        function_name: &str,
        args: &[WasmValue],
        db: u8,
        permissions: &FunctionPermissions,
        limits: &ResourceLimits,
    ) -> ExecutionResult {
        let start = Instant::now();
        let mut usage = ResourceUsage::new();

        // Look up the compiled module
        let module = {
            let cache = self.modules.read();
            match cache.get(module_hash) {
                Some(m) => m.clone(),
                None => {
                    return ExecutionResult::failure(
                        format!("module not found in cache: {}", module_hash),
                        usage,
                        start.elapsed(),
                    );
                }
            }
        };

        // Create a store with host state
        let host_state = HostState::new(db, permissions.clone(), limits.max_memory);
        let mut store = Store::new(&self.engine, host_state);

        // Set fuel limit
        if let Err(e) = store.set_fuel(limits.max_fuel) {
            self.record_execution(false, start.elapsed());
            return ExecutionResult::failure(
                format!("failed to set fuel limit: {}", e),
                usage,
                start.elapsed(),
            );
        }

        // Instantiate the module with host function bindings
        let instance = match self.linker.instantiate(&mut store, &module) {
            Ok(i) => i,
            Err(e) => {
                self.record_execution(false, start.elapsed());
                return ExecutionResult::failure(
                    format!("failed to instantiate module: {}", e),
                    usage,
                    start.elapsed(),
                );
            }
        };

        // Look up the exported function
        let func = match instance.get_func(&mut store, function_name) {
            Some(f) => f,
            None => {
                self.record_execution(false, start.elapsed());
                return ExecutionResult::failure(
                    format!("function '{}' not found in module", function_name),
                    usage,
                    start.elapsed(),
                );
            }
        };

        // Convert arguments to wasmtime Val
        let wasm_args: Vec<Val> = args
            .iter()
            .map(|v| match v {
                WasmValue::I32(i) => Val::I32(*i),
                WasmValue::I64(i) => Val::I64(*i),
                WasmValue::F32(f) => Val::F32(f.to_bits()),
                WasmValue::F64(f) => Val::F64(f.to_bits()),
                WasmValue::Bytes(_) => Val::I32(0), // Bytes are passed via shared memory
            })
            .collect();

        // Prepare result slots based on the function signature
        let func_ty = func.ty(&store);
        let mut results = vec![Val::I32(0); func_ty.results().len()];

        // Check timeout before calling
        let timeout = limits.max_time;
        if start.elapsed() > timeout {
            self.record_execution(false, start.elapsed());
            return ExecutionResult::failure(
                format!("setup exceeded timeout ({:?})", timeout),
                usage,
                start.elapsed(),
            );
        }

        // Call the function
        match func.call(&mut store, &wasm_args, &mut results) {
            Ok(()) => {
                // Convert results back
                let result_values: Vec<WasmValue> = results
                    .iter()
                    .map(|v| match v {
                        Val::I32(i) => WasmValue::I32(*i),
                        Val::I64(i) => WasmValue::I64(*i),
                        Val::F32(f) => WasmValue::F32(f32::from_bits(*f)),
                        Val::F64(f) => WasmValue::F64(f64::from_bits(*f)),
                        _ => WasmValue::I32(0),
                    })
                    .collect();

                // Record fuel usage
                let fuel_remaining = store.get_fuel().unwrap_or(0);
                let fuel_consumed = limits.max_fuel.saturating_sub(fuel_remaining);
                usage.record_fuel(fuel_consumed);

                let duration = start.elapsed();
                usage.set_execution_time(duration);
                self.record_execution(true, duration);

                ExecutionResult::success(result_values, usage, duration)
            }
            Err(e) => {
                let duration = start.elapsed();
                self.record_execution(false, duration);

                let error_msg = format!("{}", e);
                let classified = if error_msg.contains("fuel") {
                    "fuel limit exceeded during execution".to_string()
                } else if duration > timeout {
                    format!("execution timeout after {:?}", timeout)
                } else {
                    format!("WASM execution error: {}", e)
                };

                ExecutionResult::failure(classified, usage, duration)
            }
        }
    }

    /// List all cached module hashes.
    pub fn list_modules(&self) -> Vec<String> {
        self.modules.read().keys().cloned().collect()
    }

    /// Remove a module from the cache.
    pub fn remove_module(&self, hash: &str) -> Result<(), WasmError> {
        let mut cache = self.modules.write();
        if cache.remove(hash).is_none() {
            return Err(WasmError::FunctionNotFound(format!(
                "module hash not in cache: {}",
                hash
            )));
        }
        Ok(())
    }

    /// Get runtime statistics.
    pub fn stats(&self) -> RuntimeStats {
        self.stats.read().clone()
    }

    /// Get the runtime configuration.
    pub fn config(&self) -> &WasmConfig {
        &self.config
    }

    /// Record an execution in the stats.
    fn record_execution(&self, success: bool, _duration: Duration) {
        let mut stats = self.stats.write();
        stats.total_executions += 1;
        if success {
            stats.successful_executions += 1;
        } else {
            stats.failed_executions += 1;
        }
    }

    /// Parse exports from a compiled module.
    pub fn get_module_exports(&self, module_hash: &str) -> Result<Vec<String>, WasmError> {
        let cache = self.modules.read();
        let module = cache
            .get(module_hash)
            .ok_or_else(|| WasmError::FunctionNotFound(format!("module hash: {}", module_hash)))?;

        Ok(module
            .exports()
            .filter_map(|e| {
                if e.ty().func().is_some() {
                    Some(e.name().to_string())
                } else {
                    None
                }
            })
            .collect())
    }
}

/// Validate that the bytes look like a WASM module.
fn validate_wasm_bytes(bytes: &[u8]) -> Result<(), WasmError> {
    if bytes.len() < 8 {
        return Err(WasmError::InvalidModule("module too small".to_string()));
    }
    // WASM magic: \0asm
    if &bytes[0..4] != b"\x00asm" {
        return Err(WasmError::InvalidModule(
            "invalid WASM magic number".to_string(),
        ));
    }
    // Version 1
    if bytes[4..8] != [0x01, 0x00, 0x00, 0x00] {
        return Err(WasmError::InvalidModule(format!(
            "unsupported WASM version: {:?}",
            &bytes[4..8]
        )));
    }
    Ok(())
}

/// Calculate FNV-1a hash of data, returned as a hex string.
pub(crate) fn calculate_hash(data: &[u8]) -> String {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET;
    for byte in data {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash ^= data.len() as u64;
    hash = hash.wrapping_mul(FNV_PRIME);

    format!("{:016x}", hash)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal valid WASM module that exports an `add` function: (i32, i32) -> i32
    fn wasm_add_module() -> Vec<u8> {
        // This is a hand-assembled minimal WASM module:
        //   (module
        //     (func $add (export "add") (param i32 i32) (result i32)
        //       local.get 0
        //       local.get 1
        //       i32.add)
        //     (func $main (export "main") (result i32)
        //       i32.const 0))
        vec![
            0x00, 0x61, 0x73, 0x6D, // magic
            0x01, 0x00, 0x00, 0x00, // version
            // Type section
            0x01, 0x0C, // section id=1, size=12
            0x02, // 2 types
            0x60, 0x02, 0x7F, 0x7F, 0x01, 0x7F, // (i32, i32) -> i32
            0x60, 0x00, 0x01, 0x7F, // () -> i32
            // Function section
            0x03, 0x03, // section id=3, size=3
            0x02, // 2 functions
            0x00, // func 0 uses type 0
            0x01, // func 1 uses type 1
            // Export section
            0x07, 0x11, // section id=7, size=17
            0x02, // 2 exports
            0x03, 0x61, 0x64, 0x64, // "add"
            0x00, 0x00, // func index 0
            0x04, 0x6D, 0x61, 0x69, 0x6E, // "main"
            0x00, 0x01, // func index 1
            // Code section
            0x0A, 0x0F, // section id=10, size=15
            0x02, // 2 functions
            // func 0 body (add): local.get 0, local.get 1, i32.add, end
            0x07, // body size=7
            0x00, // 0 local declarations
            0x20, 0x00, // local.get 0
            0x20, 0x01, // local.get 1
            0x6A, // i32.add
            0x0B, // end
            // func 1 body (main): i32.const 0, end
            0x04, // body size=4
            0x00, // 0 local declarations
            0x41, 0x00, // i32.const 0
            0x0B, // end
        ]
    }

    #[test]
    fn test_runtime_creation() {
        let runtime = WasmRuntime::with_defaults();
        assert!(runtime.is_ok());
    }

    #[test]
    fn test_register_module() {
        let runtime = WasmRuntime::with_defaults().expect("runtime creation failed");
        let bytes = wasm_add_module();
        let hash = runtime.register_module("test_add", &bytes);
        assert!(hash.is_ok(), "register_module failed: {:?}", hash.err());
    }

    #[test]
    fn test_register_module_invalid_magic() {
        let runtime = WasmRuntime::with_defaults().expect("runtime creation failed");
        let bytes = vec![0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00];
        let result = runtime.register_module("bad", &bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_register_module_too_small() {
        let runtime = WasmRuntime::with_defaults().expect("runtime creation failed");
        let result = runtime.register_module("tiny", &[0x00, 0x61]);
        assert!(result.is_err());
    }

    #[test]
    fn test_execute_add() {
        let runtime = WasmRuntime::with_defaults().expect("runtime creation failed");
        let bytes = wasm_add_module();
        let hash = runtime
            .register_module("add", &bytes)
            .expect("register failed");

        let result = runtime.execute(
            &hash,
            "add",
            &[WasmValue::I32(3), WasmValue::I32(4)],
            0,
            &FunctionPermissions::default(),
            &ResourceLimits::default(),
        );

        assert!(result.success, "execution failed: {:?}", result.error);
        assert_eq!(result.as_i32(), Some(7));
    }

    #[test]
    fn test_execute_main() {
        let runtime = WasmRuntime::with_defaults().expect("runtime creation failed");
        let bytes = wasm_add_module();
        let hash = runtime
            .register_module("main_test", &bytes)
            .expect("register failed");

        let result = runtime.execute(
            &hash,
            "main",
            &[],
            0,
            &FunctionPermissions::default(),
            &ResourceLimits::default(),
        );

        assert!(result.success, "execution failed: {:?}", result.error);
        assert_eq!(result.as_i32(), Some(0));
    }

    #[test]
    fn test_execute_function_not_found() {
        let runtime = WasmRuntime::with_defaults().expect("runtime creation failed");
        let bytes = wasm_add_module();
        let hash = runtime
            .register_module("test", &bytes)
            .expect("register failed");

        let result = runtime.execute(
            &hash,
            "nonexistent",
            &[],
            0,
            &FunctionPermissions::default(),
            &ResourceLimits::default(),
        );

        assert!(!result.success);
        assert!(result
            .error
            .as_ref()
            .is_some_and(|e| e.contains("not found")));
    }

    #[test]
    fn test_execute_module_not_cached() {
        let runtime = WasmRuntime::with_defaults().expect("runtime creation failed");

        let result = runtime.execute(
            "nonexistent_hash",
            "main",
            &[],
            0,
            &FunctionPermissions::default(),
            &ResourceLimits::default(),
        );

        assert!(!result.success);
        assert!(result
            .error
            .as_ref()
            .is_some_and(|e| e.contains("not found")));
    }

    #[test]
    fn test_fuel_exhaustion() {
        let runtime = WasmRuntime::with_defaults().expect("runtime creation failed");
        let bytes = wasm_add_module();
        let hash = runtime
            .register_module("fuel_test", &bytes)
            .expect("register failed");

        let limits = ResourceLimits {
            max_fuel: 1, // impossibly low
            ..Default::default()
        };

        let result = runtime.execute(
            &hash,
            "add",
            &[WasmValue::I32(1), WasmValue::I32(2)],
            0,
            &FunctionPermissions::default(),
            &limits,
        );

        assert!(!result.success);
        // The error should mention fuel
        assert!(
            result.error.as_ref().is_some_and(|e| e.contains("fuel")),
            "Expected fuel error, got: {:?}",
            result.error
        );
    }

    #[test]
    fn test_module_caching() {
        let runtime = WasmRuntime::with_defaults().expect("runtime creation failed");
        let bytes = wasm_add_module();

        let hash1 = runtime
            .register_module("first", &bytes)
            .expect("register failed");
        let hash2 = runtime
            .register_module("second", &bytes)
            .expect("register failed");

        // Same bytes should yield same hash
        assert_eq!(hash1, hash2);

        let stats = runtime.stats();
        assert_eq!(stats.modules_compiled, 1);
        assert_eq!(stats.cache_hits, 1);
    }

    #[test]
    fn test_remove_module() {
        let runtime = WasmRuntime::with_defaults().expect("runtime creation failed");
        let bytes = wasm_add_module();
        let hash = runtime
            .register_module("rm_test", &bytes)
            .expect("register failed");

        assert!(runtime.remove_module(&hash).is_ok());
        assert!(runtime.remove_module(&hash).is_err());
    }

    #[test]
    fn test_list_modules() {
        let runtime = WasmRuntime::with_defaults().expect("runtime creation failed");
        assert!(runtime.list_modules().is_empty());

        let bytes = wasm_add_module();
        let hash = runtime
            .register_module("list_test", &bytes)
            .expect("register failed");

        let modules = runtime.list_modules();
        assert_eq!(modules.len(), 1);
        assert!(modules.contains(&hash));
    }

    #[test]
    fn test_get_module_exports() {
        let runtime = WasmRuntime::with_defaults().expect("runtime creation failed");
        let bytes = wasm_add_module();
        let hash = runtime
            .register_module("exports_test", &bytes)
            .expect("register failed");

        let exports = runtime
            .get_module_exports(&hash)
            .expect("get_exports failed");
        assert!(exports.contains(&"add".to_string()));
        assert!(exports.contains(&"main".to_string()));
    }

    #[test]
    fn test_runtime_stats() {
        let runtime = WasmRuntime::with_defaults().expect("runtime creation failed");
        let bytes = wasm_add_module();
        let hash = runtime
            .register_module("stats_test", &bytes)
            .expect("register failed");

        // Execute a few times
        for _ in 0..3 {
            runtime.execute(
                &hash,
                "main",
                &[],
                0,
                &FunctionPermissions::default(),
                &ResourceLimits::default(),
            );
        }

        let stats = runtime.stats();
        assert_eq!(stats.total_executions, 3);
        assert_eq!(stats.successful_executions, 3);
        assert_eq!(stats.failed_executions, 0);
    }

    #[test]
    fn test_concurrent_execution() {
        use std::sync::Arc;
        use std::thread;

        let runtime = Arc::new(WasmRuntime::with_defaults().expect("runtime creation failed"));
        let bytes = wasm_add_module();
        let hash = runtime
            .register_module("concurrent_test", &bytes)
            .expect("register failed");

        let mut handles = Vec::new();
        for i in 0..4 {
            let rt = Arc::clone(&runtime);
            let h = hash.clone();
            handles.push(thread::spawn(move || {
                for j in 0..10 {
                    let result = rt.execute(
                        &h,
                        "add",
                        &[WasmValue::I32(i), WasmValue::I32(j)],
                        0,
                        &FunctionPermissions::default(),
                        &ResourceLimits::default(),
                    );
                    assert!(result.success, "thread {} iter {} failed", i, j);
                    assert_eq!(result.as_i32(), Some(i + j));
                }
            }));
        }

        for handle in handles {
            handle.join().expect("thread panicked");
        }

        let stats = runtime.stats();
        assert_eq!(stats.total_executions, 40);
        assert_eq!(stats.successful_executions, 40);
    }

    #[test]
    fn test_calculate_hash_deterministic() {
        let data = b"hello world";
        let h1 = calculate_hash(data);
        let h2 = calculate_hash(data);
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 16);
    }

    #[test]
    fn test_calculate_hash_different_data() {
        let h1 = calculate_hash(b"hello");
        let h2 = calculate_hash(b"world");
        assert_ne!(h1, h2);
    }
}
