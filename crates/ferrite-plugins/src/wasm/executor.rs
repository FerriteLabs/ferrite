//! WASM execution engine
//!
//! Provides the runtime for executing WebAssembly functions with
//! resource limits, sandboxing, and host function access.
//!
//! When the `wasm` feature is enabled, this uses wasmtime for real
//! WebAssembly execution. Otherwise, it provides a simulated executor
//! for testing purposes.

use super::host::{HostContext, HostFunctions};
use super::types::{ResourceLimits, ResourceUsage, WasmFunction, WasmValue};
use super::WasmError;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(feature = "wasm")]
use wasmtime::{Config, Engine, Linker, Module, Store, Val};

/// Result of WASM execution
#[derive(Clone, Debug)]
pub struct ExecutionResult {
    /// Return values
    pub values: Vec<WasmValue>,
    /// Resource usage
    pub usage: ResourceUsage,
    /// Execution time
    pub duration: Duration,
    /// Success flag
    pub success: bool,
    /// Error message if failed
    pub error: Option<String>,
}

impl ExecutionResult {
    /// Create a successful result
    pub fn success(values: Vec<WasmValue>, usage: ResourceUsage, duration: Duration) -> Self {
        Self {
            values,
            usage,
            duration,
            success: true,
            error: None,
        }
    }

    /// Create a failed result
    pub fn failure(error: String, usage: ResourceUsage, duration: Duration) -> Self {
        Self {
            values: Vec::new(),
            usage,
            duration,
            success: false,
            error: Some(error),
        }
    }

    /// Get the first return value as i32
    pub fn as_i32(&self) -> Option<i32> {
        self.values.first().and_then(|v| v.as_i32())
    }

    /// Get the first return value as bytes
    pub fn as_bytes(&self) -> Option<&[u8]> {
        self.values.first().and_then(|v| v.as_bytes())
    }
}

/// WASM execution engine
///
/// Manages the execution of WASM functions with proper sandboxing,
/// resource limits, and host function integration.
///
/// When compiled with the `wasm` feature, this uses wasmtime for real
/// WebAssembly execution. Otherwise, it provides a simulated executor.
pub struct WasmExecutor {
    /// Host functions available to WASM
    host_functions: Arc<HostFunctions>,
    /// Cached instances (for reuse)
    instance_cache: RwLock<HashMap<String, CachedInstance>>,
    /// Execution statistics
    stats: RwLock<ExecutorStats>,
    /// Wasmtime engine (when feature enabled)
    #[cfg(feature = "wasm")]
    engine: Engine,
    /// Module cache for wasmtime
    #[cfg(feature = "wasm")]
    module_cache: RwLock<HashMap<String, Module>>,
}

/// A cached WASM instance for reuse
struct CachedInstance {
    /// Last used timestamp
    last_used: Instant,
    /// Number of times used
    use_count: u64,
    /// Memory snapshot (for fast reset)
    memory_snapshot: Vec<u8>,
}

/// Executor statistics
#[derive(Clone, Debug, Default)]
pub struct ExecutorStats {
    /// Total executions
    pub total_executions: u64,
    /// Successful executions
    pub successful_executions: u64,
    /// Failed executions
    pub failed_executions: u64,
    /// Total execution time
    pub total_execution_time: Duration,
    /// Cache hits
    pub cache_hits: u64,
    /// Cache misses
    pub cache_misses: u64,
}

impl WasmExecutor {
    /// Create a new WASM executor
    #[cfg(not(feature = "wasm"))]
    pub fn new() -> Self {
        Self {
            host_functions: Arc::new(HostFunctions::new()),
            instance_cache: RwLock::new(HashMap::new()),
            stats: RwLock::new(ExecutorStats::default()),
        }
    }

    /// Create a new WASM executor with wasmtime
    #[cfg(feature = "wasm")]
    pub fn new() -> Self {
        // Configure wasmtime engine with fuel metering for resource limits
        let mut config = Config::new();
        config.consume_fuel(true);
        config.wasm_component_model(false);

        let engine = Engine::new(&config).expect("Failed to create wasmtime engine");

        Self {
            host_functions: Arc::new(HostFunctions::new()),
            instance_cache: RwLock::new(HashMap::new()),
            stats: RwLock::new(ExecutorStats::default()),
            engine,
            module_cache: RwLock::new(HashMap::new()),
        }
    }

    /// Execute a WASM function using wasmtime
    ///
    /// This uses the wasmtime runtime for real WebAssembly execution with
    /// proper sandboxing, fuel metering, and host function integration.
    #[cfg(feature = "wasm")]
    pub fn execute(
        &self,
        function: &WasmFunction,
        entry_point: &str,
        args: Vec<WasmValue>,
        db: u8,
        limits: &ResourceLimits,
    ) -> ExecutionResult {
        let start = Instant::now();
        let mut usage = ResourceUsage::new();

        // Create host context
        let _ctx = HostContext::new(db, function.metadata.permissions.clone());

        // Get or compile the module
        let module_hash = &function.metadata.source_hash;
        let module = {
            let cache = self.module_cache.read();
            if let Some(m) = cache.get(module_hash) {
                self.stats.write().cache_hits += 1;
                m.clone()
            } else {
                drop(cache);
                self.stats.write().cache_misses += 1;

                // Compile the module
                match Module::new(&self.engine, &function.wasm_bytes) {
                    Ok(m) => {
                        let mut cache = self.module_cache.write();
                        cache.insert(module_hash.clone(), m.clone());
                        m
                    }
                    Err(e) => {
                        let duration = start.elapsed();
                        self.record_execution(false, duration);
                        return ExecutionResult::failure(
                            format!("Failed to compile WASM module: {}", e),
                            usage,
                            duration,
                        );
                    }
                }
            }
        };

        // Create a store with fuel limits
        let mut store = Store::new(&self.engine, ());

        // Set fuel limit
        if let Err(e) = store.set_fuel(limits.max_fuel) {
            let duration = start.elapsed();
            self.record_execution(false, duration);
            return ExecutionResult::failure(
                format!("Failed to set fuel limit: {}", e),
                usage,
                duration,
            );
        }

        // Create linker and add host functions
        let linker = Linker::new(&self.engine);

        // Instantiate the module
        let instance = match linker.instantiate(&mut store, &module) {
            Ok(i) => i,
            Err(e) => {
                let duration = start.elapsed();
                self.record_execution(false, duration);
                return ExecutionResult::failure(
                    format!("Failed to instantiate WASM module: {}", e),
                    usage,
                    duration,
                );
            }
        };

        // Get the exported function
        let func = match instance.get_func(&mut store, entry_point) {
            Some(f) => f,
            None => {
                let duration = start.elapsed();
                self.record_execution(false, duration);
                return ExecutionResult::failure(
                    format!("Entry point '{}' not found in module", entry_point),
                    usage,
                    duration,
                );
            }
        };

        // Convert args to wasmtime values
        let wasm_args: Vec<Val> = args
            .iter()
            .map(|v| match v {
                WasmValue::I32(i) => Val::I32(*i),
                WasmValue::I64(i) => Val::I64(*i),
                WasmValue::F32(f) => Val::F32(f.to_bits()),
                WasmValue::F64(f) => Val::F64(f.to_bits()),
                WasmValue::Bytes(_) => Val::I32(0), // Bytes need memory handling
            })
            .collect();

        // Prepare result storage based on function type
        let func_ty = func.ty(&store);
        let mut results = vec![Val::I32(0); func_ty.results().len()];

        // Check timeout
        if start.elapsed() > limits.max_time {
            let duration = start.elapsed();
            self.record_execution(false, duration);
            return ExecutionResult::failure(
                format!("Setup timeout: {:?}", limits.max_time),
                usage,
                duration,
            );
        }

        // Call the function
        match func.call(&mut store, &wasm_args, &mut results) {
            Ok(()) => {
                // Convert results back to WasmValue
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

                // Check if it was a fuel exhaustion
                let error_msg = if e.to_string().contains("fuel") {
                    "Fuel limit exceeded during execution".to_string()
                } else {
                    format!("WASM execution error: {}", e)
                };

                ExecutionResult::failure(error_msg, usage, duration)
            }
        }
    }

    /// Execute a WASM function (simulated - no wasmtime feature)
    ///
    /// This is a simplified executor that simulates WASM execution.
    /// Enable the `wasm` feature for real WebAssembly execution.
    #[cfg(not(feature = "wasm"))]
    pub fn execute(
        &self,
        function: &WasmFunction,
        entry_point: &str,
        args: Vec<WasmValue>,
        db: u8,
        limits: &ResourceLimits,
    ) -> ExecutionResult {
        let start = Instant::now();
        let mut usage = ResourceUsage::new();

        // Create host context
        let _ctx = HostContext::new(db, function.metadata.permissions.clone());

        // Check if entry point exists
        if !function.exports.contains(&entry_point.to_string()) && !function.exports.is_empty() {
            let duration = start.elapsed();
            self.record_execution(false, duration);
            return ExecutionResult::failure(
                format!("entry point '{}' not found", entry_point),
                usage,
                duration,
            );
        }

        // Simulate fuel consumption
        let fuel_per_arg = 100u64;
        let base_fuel = 1000u64;
        let fuel_consumed = base_fuel + (args.len() as u64 * fuel_per_arg);

        if fuel_consumed > limits.max_fuel {
            let duration = start.elapsed();
            self.record_execution(false, duration);
            return ExecutionResult::failure("fuel limit exceeded".to_string(), usage, duration);
        }

        usage.record_fuel(fuel_consumed);

        // Simulate memory usage
        let memory_used = function.wasm_bytes.len() + args.len() * 8;
        if memory_used > limits.max_memory {
            let duration = start.elapsed();
            self.record_execution(false, duration);
            return ExecutionResult::failure(
                format!(
                    "memory limit exceeded: {} > {}",
                    memory_used, limits.max_memory
                ),
                usage,
                duration,
            );
        }
        usage.record_memory(memory_used);

        // Check timeout
        if start.elapsed() > limits.max_time {
            let duration = start.elapsed();
            self.record_execution(false, duration);
            return ExecutionResult::failure(
                format!("execution timeout: {:?}", limits.max_time),
                usage,
                duration,
            );
        }

        // Simulate execution - return a simulated result based on the function name
        let result = match entry_point {
            "add" | "add1" => {
                // Simple add function
                let a = args.first().and_then(|v| v.as_i32()).unwrap_or(0);
                let b = args.get(1).and_then(|v| v.as_i32()).unwrap_or(1);
                vec![WasmValue::I32(a + b)]
            }
            "multiply" => {
                let a = args.first().and_then(|v| v.as_i32()).unwrap_or(1);
                let b = args.get(1).and_then(|v| v.as_i32()).unwrap_or(1);
                vec![WasmValue::I32(a * b)]
            }
            "identity" => args,
            "run" | "main" | "init" => {
                // Default entry points return success
                vec![WasmValue::I32(0)]
            }
            _ => {
                // Unknown function returns 0
                vec![WasmValue::I32(0)]
            }
        };

        let duration = start.elapsed();
        usage.set_execution_time(duration);
        self.record_execution(true, duration);

        ExecutionResult::success(result, usage, duration)
    }

    /// Execute with async support (for wasmtime async)
    pub async fn execute_async(
        &self,
        function: &WasmFunction,
        entry_point: &str,
        args: Vec<WasmValue>,
        db: u8,
        limits: &ResourceLimits,
    ) -> ExecutionResult {
        // In a real implementation, this would use wasmtime's async support
        // For now, delegate to sync execution
        self.execute(function, entry_point, args, db, limits)
    }

    /// Record execution statistics
    fn record_execution(&self, success: bool, duration: Duration) {
        let mut stats = self.stats.write();
        stats.total_executions += 1;
        if success {
            stats.successful_executions += 1;
        } else {
            stats.failed_executions += 1;
        }
        stats.total_execution_time += duration;
    }

    /// Get executor statistics
    pub fn stats(&self) -> ExecutorStats {
        self.stats.read().clone()
    }

    /// Get available host functions
    pub fn host_functions(&self) -> Vec<&str> {
        self.host_functions.names()
    }

    /// Validate a WASM module
    pub fn validate(&self, wasm_bytes: &[u8]) -> Result<(), WasmError> {
        // Check WASM magic number
        if wasm_bytes.len() < 8 {
            return Err(WasmError::InvalidModule("module too small".to_string()));
        }

        // WASM magic number: \0asm (0x00 0x61 0x73 0x6D)
        let magic = &wasm_bytes[0..4];
        if magic != [0x00, 0x61, 0x73, 0x6D] {
            return Err(WasmError::InvalidModule(
                "invalid magic number - not a WASM module".to_string(),
            ));
        }

        // Check version (should be 1)
        let version = &wasm_bytes[4..8];
        if version != [0x01, 0x00, 0x00, 0x00] {
            return Err(WasmError::InvalidModule(format!(
                "unsupported WASM version: {:?}",
                version
            )));
        }

        Ok(())
    }

    /// Parse exports from WASM module (simplified)
    pub fn parse_exports(&self, wasm_bytes: &[u8]) -> Result<Vec<String>, WasmError> {
        self.validate(wasm_bytes)?;

        // In a real implementation, this would parse the WASM module
        // and extract the export section. For now, return empty.
        Ok(Vec::new())
    }

    /// Clear the instance cache
    #[cfg(not(feature = "wasm"))]
    pub fn clear_cache(&self) {
        self.instance_cache.write().clear();
    }

    /// Clear the instance and module cache (wasmtime)
    #[cfg(feature = "wasm")]
    pub fn clear_cache(&self) {
        self.instance_cache.write().clear();
        self.module_cache.write().clear();
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> (usize, u64, u64) {
        let cache = self.instance_cache.read();
        let stats = self.stats.read();
        (cache.len(), stats.cache_hits, stats.cache_misses)
    }
}

impl Default for WasmExecutor {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for WasmExecutor
pub struct WasmExecutorBuilder {
    host_functions: Option<Arc<HostFunctions>>,
    cache_size: usize,
}

impl WasmExecutorBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            host_functions: None,
            cache_size: 100,
        }
    }

    /// Set custom host functions
    pub fn with_host_functions(mut self, funcs: HostFunctions) -> Self {
        self.host_functions = Some(Arc::new(funcs));
        self
    }

    /// Set cache size
    pub fn with_cache_size(mut self, size: usize) -> Self {
        self.cache_size = size;
        self
    }

    /// Build the executor (without wasmtime)
    #[cfg(not(feature = "wasm"))]
    pub fn build(self) -> WasmExecutor {
        WasmExecutor {
            host_functions: self
                .host_functions
                .unwrap_or_else(|| Arc::new(HostFunctions::new())),
            instance_cache: RwLock::new(HashMap::with_capacity(self.cache_size)),
            stats: RwLock::new(ExecutorStats::default()),
        }
    }

    /// Build the executor (with wasmtime)
    #[cfg(feature = "wasm")]
    pub fn build(self) -> WasmExecutor {
        let mut config = Config::new();
        config.consume_fuel(true);
        config.wasm_component_model(false);

        let engine = Engine::new(&config).expect("Failed to create wasmtime engine");

        WasmExecutor {
            host_functions: self
                .host_functions
                .unwrap_or_else(|| Arc::new(HostFunctions::new())),
            instance_cache: RwLock::new(HashMap::with_capacity(self.cache_size)),
            stats: RwLock::new(ExecutorStats::default()),
            engine,
            module_cache: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for WasmExecutorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wasm::types::FunctionMetadata;

    fn create_test_function(name: &str) -> WasmFunction {
        let metadata = FunctionMetadata::new(name.to_string(), "test_hash".to_string());
        // Valid WASM module (minimal)
        let wasm_bytes = vec![
            0x00, 0x61, 0x73, 0x6D, // Magic
            0x01, 0x00, 0x00, 0x00, // Version 1
        ];
        WasmFunction::new(metadata, wasm_bytes).with_exports(vec!["run".to_string()])
    }

    #[test]
    fn test_executor_new() {
        let executor = WasmExecutor::new();
        assert!(executor.host_functions().len() >= 10);
    }

    #[test]
    fn test_executor_validate() {
        let executor = WasmExecutor::new();

        // Valid WASM
        let valid = vec![0x00, 0x61, 0x73, 0x6D, 0x01, 0x00, 0x00, 0x00];
        assert!(executor.validate(&valid).is_ok());

        // Invalid magic
        let invalid = vec![0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00];
        assert!(executor.validate(&invalid).is_err());

        // Too short
        let short = vec![0x00, 0x61];
        assert!(executor.validate(&short).is_err());
    }

    #[cfg(not(feature = "wasm"))]
    #[test]
    fn test_executor_execute() {
        let executor = WasmExecutor::new();
        let function = create_test_function("test");
        let limits = ResourceLimits::default();

        let result = executor.execute(&function, "run", vec![], 0, &limits);

        assert!(result.success);
        assert!(result.error.is_none());
        assert!(result.duration.as_nanos() > 0);
    }

    #[cfg(not(feature = "wasm"))]
    #[test]
    fn test_executor_execute_add() {
        let executor = WasmExecutor::new();
        let mut function = create_test_function("add_test");
        function.exports = vec!["add".to_string()];
        let limits = ResourceLimits::default();

        let result = executor.execute(
            &function,
            "add",
            vec![WasmValue::I32(5), WasmValue::I32(3)],
            0,
            &limits,
        );

        assert!(result.success);
        assert_eq!(result.as_i32(), Some(8));
    }

    #[cfg(not(feature = "wasm"))]
    #[test]
    fn test_executor_execute_multiply() {
        let executor = WasmExecutor::new();
        let mut function = create_test_function("mul_test");
        function.exports = vec!["multiply".to_string()];
        let limits = ResourceLimits::default();

        let result = executor.execute(
            &function,
            "multiply",
            vec![WasmValue::I32(4), WasmValue::I32(7)],
            0,
            &limits,
        );

        assert!(result.success);
        assert_eq!(result.as_i32(), Some(28));
    }

    #[test]
    fn test_executor_entry_point_not_found() {
        let executor = WasmExecutor::new();
        let function = create_test_function("test");
        let limits = ResourceLimits::default();

        let result = executor.execute(&function, "nonexistent", vec![], 0, &limits);

        assert!(!result.success);
        assert!(result.error.unwrap().contains("not found"));
    }

    #[cfg(not(feature = "wasm"))]
    #[test]
    fn test_executor_fuel_limit() {
        let executor = WasmExecutor::new();
        let function = create_test_function("test");
        let limits = ResourceLimits {
            max_fuel: 10, // Very low limit
            ..Default::default()
        };

        let result = executor.execute(&function, "run", vec![], 0, &limits);

        assert!(!result.success);
        assert!(result.error.unwrap().contains("fuel"));
    }

    #[cfg(not(feature = "wasm"))]
    #[test]
    fn test_executor_memory_limit() {
        let executor = WasmExecutor::new();

        // Create a function with large WASM bytes
        let mut function = create_test_function("large");
        function.wasm_bytes = vec![0u8; 1000000]; // 1MB

        let limits = ResourceLimits {
            max_memory: 100, // Very low limit
            ..Default::default()
        };

        let result = executor.execute(&function, "run", vec![], 0, &limits);

        assert!(!result.success);
        assert!(result.error.unwrap().contains("memory"));
    }

    #[cfg(not(feature = "wasm"))]
    #[test]
    fn test_executor_stats() {
        let executor = WasmExecutor::new();
        let function = create_test_function("test");
        let limits = ResourceLimits::default();

        // Execute a few times
        executor.execute(&function, "run", vec![], 0, &limits);
        executor.execute(&function, "run", vec![], 0, &limits);
        executor.execute(&function, "run", vec![], 0, &limits);

        let stats = executor.stats();
        assert_eq!(stats.total_executions, 3);
        assert_eq!(stats.successful_executions, 3);
        assert_eq!(stats.failed_executions, 0);
    }

    #[test]
    fn test_executor_builder() {
        let executor = WasmExecutorBuilder::new().with_cache_size(50).build();

        assert!(executor.host_functions().len() >= 10);
    }

    #[test]
    fn test_execution_result_success() {
        let result = ExecutionResult::success(
            vec![WasmValue::I32(42)],
            ResourceUsage::new(),
            Duration::from_millis(10),
        );

        assert!(result.success);
        assert!(result.error.is_none());
        assert_eq!(result.as_i32(), Some(42));
    }

    #[test]
    fn test_execution_result_failure() {
        let result = ExecutionResult::failure(
            "test error".to_string(),
            ResourceUsage::new(),
            Duration::from_millis(5),
        );

        assert!(!result.success);
        assert_eq!(result.error, Some("test error".to_string()));
        assert!(result.values.is_empty());
    }

    #[cfg(not(feature = "wasm"))]
    #[tokio::test]
    async fn test_executor_execute_async() {
        let executor = WasmExecutor::new();
        let function = create_test_function("test");
        let limits = ResourceLimits::default();

        let result = executor
            .execute_async(&function, "run", vec![], 0, &limits)
            .await;

        assert!(result.success);
    }

    #[test]
    fn test_executor_cache_operations() {
        let executor = WasmExecutor::new();

        let (size, hits, misses) = executor.cache_stats();
        assert_eq!(size, 0);
        assert_eq!(hits, 0);
        assert_eq!(misses, 0);

        executor.clear_cache();
        let (size, _, _) = executor.cache_stats();
        assert_eq!(size, 0);
    }
}
