//! UDF Registry
//!
//! Maps user-defined function names to WASM modules and entry points.
//! Thread-safe via `parking_lot::RwLock`. The registry stores function
//! metadata alongside the module hash used to look up the compiled module
//! in the `WasmRuntime` cache.
//!
//! This is the integration-layer registry that lives in the top-level crate
//! and bridges the FUNCTION/FCALL commands to the WASM runtime.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use parking_lot::RwLock;

use ferrite_plugins::wasm::{
    FunctionMetadata, FunctionPermissions, ResourceLimits, WasmError, WasmValue,
};

use super::runtime::{ExecutionResult, WasmRuntime};

/// A registered user-defined function.
#[derive(Clone, Debug)]
pub struct RegisteredFunction {
    /// Function name.
    pub name: String,
    /// Module hash in the runtime cache.
    pub module_hash: String,
    /// Default entry point in the WASM module.
    pub entry_point: String,
    /// Function metadata.
    pub metadata: FunctionMetadata,
    /// Raw WASM bytes (kept for FUNCTION DUMP/RESTORE).
    pub wasm_bytes: Vec<u8>,
    /// Exported functions from the module.
    pub exports: Vec<String>,
    /// Number of times called (atomic update behind lock).
    pub call_count: u64,
    /// Total execution time.
    pub total_execution_time: Duration,
    /// Last error from the most recent call.
    pub last_error: Option<String>,
}

/// Statistics for the UDF registry.
#[derive(Clone, Debug, Default)]
pub struct RegistryStats {
    /// Number of loaded functions.
    pub function_count: usize,
    /// Total WASM bytes across all functions.
    pub total_wasm_size: usize,
    /// Total calls across all functions.
    pub total_calls: u64,
    /// Total successful calls.
    pub successful_calls: u64,
    /// Total failed calls.
    pub failed_calls: u64,
}

/// Information about a registered function (for FUNCTION LIST / WASM.INFO).
#[derive(Clone, Debug)]
pub struct FunctionInfo {
    /// Function name.
    pub name: String,
    /// Module hash.
    pub module_hash: String,
    /// Entry point.
    pub entry_point: String,
    /// Description.
    pub description: Option<String>,
    /// Source hash from metadata.
    pub source_hash: String,
    /// Creation time.
    pub created_at: SystemTime,
    /// Last modified time.
    pub modified_at: SystemTime,
    /// Number of calls.
    pub call_count: u64,
    /// Average execution time in milliseconds.
    pub avg_execution_time_ms: f64,
    /// Exported functions.
    pub exports: Vec<String>,
    /// Permissions.
    pub permissions: FunctionPermissions,
    /// Resource limits.
    pub limits: ResourceLimits,
    /// WASM module size in bytes.
    pub wasm_size: usize,
}

/// Thread-safe UDF registry.
///
/// Stores registered functions and dispatches calls to the `WasmRuntime`.
pub struct UdfRegistry {
    /// Registered functions keyed by name.
    functions: RwLock<HashMap<String, RegisteredFunction>>,
    /// The WASM runtime.
    runtime: Arc<WasmRuntime>,
    /// Maximum number of functions allowed.
    max_functions: usize,
    /// Statistics.
    stats: RwLock<RegistryStats>,
}

impl UdfRegistry {
    /// Create a new UDF registry with a WASM runtime.
    pub fn new(runtime: Arc<WasmRuntime>) -> Self {
        Self {
            functions: RwLock::new(HashMap::new()),
            runtime,
            max_functions: 1000,
            stats: RwLock::new(RegistryStats::default()),
        }
    }

    /// Create with a custom function limit.
    pub fn with_max_functions(runtime: Arc<WasmRuntime>, max_functions: usize) -> Self {
        Self {
            functions: RwLock::new(HashMap::new()),
            runtime,
            max_functions,
            stats: RwLock::new(RegistryStats::default()),
        }
    }

    /// Register a WASM function.
    ///
    /// Compiles the WASM bytes, caches the module, and registers the function
    /// under the given name. If `replace` is true, an existing function with
    /// the same name will be replaced.
    pub fn load(
        &self,
        name: &str,
        wasm_bytes: Vec<u8>,
        metadata: Option<FunctionMetadata>,
        replace: bool,
    ) -> Result<(), WasmError> {
        // Check capacity
        {
            let functions = self.functions.read();
            if !replace && functions.contains_key(name) {
                return Err(WasmError::FunctionExists(name.to_string()));
            }
            if functions.len() >= self.max_functions && !functions.contains_key(name) {
                return Err(WasmError::ResourceLimitExceeded(format!(
                    "maximum functions ({}) reached",
                    self.max_functions
                )));
            }
        }

        // Compile and cache in the runtime
        let module_hash = self.runtime.register_module(name, &wasm_bytes)?;

        // Get exports
        let exports = self
            .runtime
            .get_module_exports(&module_hash)
            .unwrap_or_default();

        // Build metadata
        let source_hash = module_hash.clone();
        let metadata = metadata
            .unwrap_or_else(|| FunctionMetadata::new(name.to_string(), source_hash.clone()));

        // Determine entry point: prefer "main", then first export, then the function name
        let entry_point = if exports.contains(&"main".to_string()) {
            "main".to_string()
        } else if let Some(first) = exports.first() {
            first.clone()
        } else {
            name.to_string()
        };

        let func = RegisteredFunction {
            name: name.to_string(),
            module_hash,
            entry_point,
            metadata,
            wasm_bytes,
            exports,
            call_count: 0,
            total_execution_time: Duration::ZERO,
            last_error: None,
        };

        self.functions
            .write()
            .insert(name.to_string(), func);

        // Update stats
        self.update_stats();

        tracing::info!(function = name, "WASM UDF registered");
        Ok(())
    }

    /// Unload (delete) a function.
    pub fn delete(&self, name: &str) -> Result<(), WasmError> {
        let removed = self.functions.write().remove(name);
        if removed.is_none() {
            return Err(WasmError::FunctionNotFound(name.to_string()));
        }
        self.update_stats();
        tracing::info!(function = name, "WASM UDF deleted");
        Ok(())
    }

    /// Call a registered function (FCALL).
    pub fn call(
        &self,
        name: &str,
        keys: &[bytes::Bytes],
        args: &[bytes::Bytes],
        db: u8,
    ) -> Result<ExecutionResult, WasmError> {
        // Look up the function
        let (module_hash, entry_point, permissions, limits) = {
            let functions = self.functions.read();
            let func = functions
                .get(name)
                .ok_or_else(|| WasmError::FunctionNotFound(name.to_string()))?;

            (
                func.module_hash.clone(),
                func.entry_point.clone(),
                func.metadata.permissions.clone(),
                func.metadata.limits.clone(),
            )
        };

        // Convert args to WasmValues
        let mut wasm_args: Vec<WasmValue> = Vec::with_capacity(keys.len() + args.len());

        // Keys first
        for key in keys {
            wasm_args.push(WasmValue::Bytes(key.to_vec()));
        }

        // Then regular args, trying to parse as numbers
        for arg in args {
            let arg_str = String::from_utf8_lossy(arg);
            if let Ok(i) = arg_str.parse::<i64>() {
                wasm_args.push(WasmValue::I64(i));
            } else if let Ok(f) = arg_str.parse::<f64>() {
                wasm_args.push(WasmValue::F64(f));
            } else {
                wasm_args.push(WasmValue::Bytes(arg.to_vec()));
            }
        }

        // For simple numeric args, pass them directly as i32/i64
        // For the common case of pure-i32 functions, extract i32 args
        let simple_args: Vec<WasmValue> = wasm_args
            .iter()
            .map(|v| match v {
                WasmValue::I64(i) if i32::try_from(*i).is_ok() => {
                    WasmValue::I32(*i as i32)
                }
                other => other.clone(),
            })
            .collect();

        // Execute
        let result =
            self.runtime
                .execute(&module_hash, &entry_point, &simple_args, db, &permissions, &limits);

        // Update call stats
        {
            let mut functions = self.functions.write();
            if let Some(func) = functions.get_mut(name) {
                func.call_count += 1;
                func.total_execution_time += result.duration;
                if !result.success {
                    func.last_error = result.error.clone();
                }
            }
        }

        // Update global stats
        {
            let mut stats = self.stats.write();
            stats.total_calls += 1;
            if result.success {
                stats.successful_calls += 1;
            } else {
                stats.failed_calls += 1;
            }
        }

        Ok(result)
    }

    /// Call a registered function in read-only mode (FCALL_RO).
    ///
    /// Enforces read-only permissions regardless of the function's
    /// configured permissions.
    pub fn call_ro(
        &self,
        name: &str,
        keys: &[bytes::Bytes],
        args: &[bytes::Bytes],
        db: u8,
    ) -> Result<ExecutionResult, WasmError> {
        // Look up the function but override permissions to read-only
        let (module_hash, entry_point, _permissions, limits) = {
            let functions = self.functions.read();
            let func = functions
                .get(name)
                .ok_or_else(|| WasmError::FunctionNotFound(name.to_string()))?;
            (
                func.module_hash.clone(),
                func.entry_point.clone(),
                func.metadata.permissions.clone(),
                func.metadata.limits.clone(),
            )
        };

        let ro_permissions = FunctionPermissions::read_only();

        let mut wasm_args: Vec<WasmValue> = Vec::with_capacity(keys.len() + args.len());
        for key in keys {
            wasm_args.push(WasmValue::Bytes(key.to_vec()));
        }
        for arg in args {
            let arg_str = String::from_utf8_lossy(arg);
            if let Ok(i) = arg_str.parse::<i64>() {
                wasm_args.push(WasmValue::I64(i));
            } else if let Ok(f) = arg_str.parse::<f64>() {
                wasm_args.push(WasmValue::F64(f));
            } else {
                wasm_args.push(WasmValue::Bytes(arg.to_vec()));
            }
        }

        let simple_args: Vec<WasmValue> = wasm_args
            .iter()
            .map(|v| match v {
                WasmValue::I64(i) if i32::try_from(*i).is_ok() => {
                    WasmValue::I32(*i as i32)
                }
                other => other.clone(),
            })
            .collect();

        let result = self.runtime.execute(
            &module_hash,
            &entry_point,
            &simple_args,
            db,
            &ro_permissions,
            &limits,
        );

        // Update stats
        {
            let mut functions = self.functions.write();
            if let Some(func) = functions.get_mut(name) {
                func.call_count += 1;
                func.total_execution_time += result.duration;
            }
        }

        Ok(result)
    }

    /// Check if a function exists.
    pub fn exists(&self, name: &str) -> bool {
        self.functions.read().contains_key(name)
    }

    /// List all function names.
    pub fn list(&self) -> Vec<String> {
        self.functions.read().keys().cloned().collect()
    }

    /// Get information about a function.
    pub fn info(&self, name: &str) -> Option<FunctionInfo> {
        let functions = self.functions.read();
        functions.get(name).map(|f| {
            let avg_time = if f.call_count > 0 {
                f.total_execution_time.as_millis() as f64 / f.call_count as f64
            } else {
                0.0
            };

            FunctionInfo {
                name: f.name.clone(),
                module_hash: f.module_hash.clone(),
                entry_point: f.entry_point.clone(),
                description: f.metadata.description.clone(),
                source_hash: f.metadata.source_hash.clone(),
                created_at: f.metadata.created_at,
                modified_at: f.metadata.modified_at,
                call_count: f.call_count,
                avg_execution_time_ms: avg_time,
                exports: f.exports.clone(),
                permissions: f.metadata.permissions.clone(),
                limits: f.metadata.limits.clone(),
                wasm_size: f.wasm_bytes.len(),
            }
        })
    }

    /// Get the number of registered functions.
    pub fn len(&self) -> usize {
        self.functions.read().len()
    }

    /// Check if the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.functions.read().is_empty()
    }

    /// Get registry statistics.
    pub fn stats(&self) -> RegistryStats {
        self.stats.read().clone()
    }

    /// Flush (delete) all functions.
    pub fn flush(&self) {
        self.functions.write().clear();
        let mut stats = self.stats.write();
        stats.function_count = 0;
        stats.total_wasm_size = 0;
    }

    /// Update internal stats from the functions map.
    fn update_stats(&self) {
        let functions = self.functions.read();
        let mut stats = self.stats.write();
        stats.function_count = functions.len();
        stats.total_wasm_size = functions.values().map(|f| f.wasm_bytes.len()).sum();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wasm::runtime::WasmConfig;

    /// Minimal valid WASM module exporting `add(i32, i32) -> i32` and `main() -> i32`.
    fn wasm_add_module() -> Vec<u8> {
        vec![
            0x00, 0x61, 0x73, 0x6D, 0x01, 0x00, 0x00, 0x00, // magic + version
            // Type section
            0x01, 0x0C, 0x02, 0x60, 0x02, 0x7F, 0x7F, 0x01, 0x7F, 0x60, 0x00, 0x01, 0x7F,
            // Function section
            0x03, 0x03, 0x02, 0x00, 0x01,
            // Export section
            0x07, 0x11, 0x02, 0x03, 0x61, 0x64, 0x64, 0x00, 0x00, 0x04, 0x6D, 0x61, 0x69, 0x6E,
            0x00, 0x01,
            // Code section
            0x0A, 0x0F, 0x02, 0x07, 0x00, 0x20, 0x00, 0x20, 0x01, 0x6A, 0x0B, 0x04, 0x00, 0x41,
            0x00, 0x0B,
        ]
    }

    fn create_registry() -> UdfRegistry {
        let runtime =
            Arc::new(WasmRuntime::new(WasmConfig::default()).expect("runtime creation failed"));
        UdfRegistry::new(runtime)
    }

    #[test]
    fn test_registry_creation() {
        let registry = create_registry();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn test_load_function() {
        let registry = create_registry();
        let result = registry.load("myadd", wasm_add_module(), None, false);
        assert!(result.is_ok(), "load failed: {:?}", result.err());
        assert!(registry.exists("myadd"));
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn test_load_duplicate_without_replace() {
        let registry = create_registry();
        registry
            .load("myfunc", wasm_add_module(), None, false)
            .expect("first load failed");

        let result = registry.load("myfunc", wasm_add_module(), None, false);
        assert!(matches!(result, Err(WasmError::FunctionExists(_))));
    }

    #[test]
    fn test_load_duplicate_with_replace() {
        let registry = create_registry();
        registry
            .load("myfunc", wasm_add_module(), None, false)
            .expect("first load failed");

        let result = registry.load("myfunc", wasm_add_module(), None, true);
        assert!(result.is_ok());
    }

    #[test]
    fn test_delete_function() {
        let registry = create_registry();
        registry
            .load("todelete", wasm_add_module(), None, false)
            .expect("load failed");

        assert!(registry.delete("todelete").is_ok());
        assert!(!registry.exists("todelete"));
    }

    #[test]
    fn test_delete_nonexistent() {
        let registry = create_registry();
        let result = registry.delete("ghost");
        assert!(matches!(result, Err(WasmError::FunctionNotFound(_))));
    }

    #[test]
    fn test_call_function() {
        let registry = create_registry();
        registry
            .load("adder", wasm_add_module(), None, false)
            .expect("load failed");

        // The module exports "add" and "main"; the registry picks "main" as default.
        // We need to call "add" explicitly. Let's test that the registry picks
        // "main" if available.
        let result = registry
            .call("adder", &[], &[], 0)
            .expect("call failed");

        // "main" returns 0
        assert!(result.success, "execution failed: {:?}", result.error);
        assert_eq!(result.as_i32(), Some(0));
    }

    #[test]
    fn test_call_nonexistent_function() {
        let registry = create_registry();
        let result = registry.call("ghost", &[], &[], 0);
        assert!(matches!(result, Err(WasmError::FunctionNotFound(_))));
    }

    #[test]
    fn test_list_functions() {
        let registry = create_registry();
        registry
            .load("func_a", wasm_add_module(), None, false)
            .expect("load a failed");
        registry
            .load("func_b", wasm_add_module(), None, false)
            .expect("load b failed");

        let names = registry.list();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"func_a".to_string()));
        assert!(names.contains(&"func_b".to_string()));
    }

    #[test]
    fn test_function_info() {
        let registry = create_registry();
        let metadata = FunctionMetadata::new("info_test".to_string(), "custom_hash".to_string())
            .with_description("A test function".to_string());

        registry
            .load("info_test", wasm_add_module(), Some(metadata), false)
            .expect("load failed");

        let info = registry.info("info_test").expect("info should exist");
        assert_eq!(info.name, "info_test");
        assert_eq!(info.description, Some("A test function".to_string()));
        assert!(info.exports.contains(&"add".to_string()));
        assert!(info.exports.contains(&"main".to_string()));
        assert_eq!(info.call_count, 0);
    }

    #[test]
    fn test_function_info_nonexistent() {
        let registry = create_registry();
        assert!(registry.info("ghost").is_none());
    }

    #[test]
    fn test_registry_stats() {
        let registry = create_registry();

        registry
            .load("s1", wasm_add_module(), None, false)
            .expect("load failed");
        registry
            .load("s2", wasm_add_module(), None, false)
            .expect("load failed");

        let stats = registry.stats();
        assert_eq!(stats.function_count, 2);
        assert!(stats.total_wasm_size > 0);
    }

    #[test]
    fn test_flush() {
        let registry = create_registry();
        registry
            .load("f1", wasm_add_module(), None, false)
            .expect("load failed");
        registry
            .load("f2", wasm_add_module(), None, false)
            .expect("load failed");

        assert_eq!(registry.len(), 2);
        registry.flush();
        assert!(registry.is_empty());
    }

    #[test]
    fn test_call_updates_stats() {
        let registry = create_registry();
        registry
            .load("counter_test", wasm_add_module(), None, false)
            .expect("load failed");

        // Call three times
        for _ in 0..3 {
            let _ = registry.call("counter_test", &[], &[], 0);
        }

        let info = registry.info("counter_test").expect("info should exist");
        assert_eq!(info.call_count, 3);

        let stats = registry.stats();
        assert_eq!(stats.total_calls, 3);
        assert_eq!(stats.successful_calls, 3);
    }

    #[test]
    fn test_call_ro() {
        let registry = create_registry();
        registry
            .load("ro_test", wasm_add_module(), None, false)
            .expect("load failed");

        let result = registry
            .call_ro("ro_test", &[], &[], 0)
            .expect("call_ro failed");
        assert!(result.success);
    }

    #[test]
    fn test_load_invalid_wasm() {
        let registry = create_registry();
        let result = registry.load("bad", vec![0, 0, 0, 0, 0, 0, 0, 0], None, false);
        assert!(result.is_err());
    }

    #[test]
    fn test_concurrent_access() {
        use std::thread;

        let runtime =
            Arc::new(WasmRuntime::new(WasmConfig::default()).expect("runtime creation failed"));
        let registry = Arc::new(UdfRegistry::new(runtime));

        registry
            .load("concurrent", wasm_add_module(), None, false)
            .expect("load failed");

        let mut handles = Vec::new();
        for _ in 0..4 {
            let reg = Arc::clone(&registry);
            handles.push(thread::spawn(move || {
                for _ in 0..10 {
                    let result = reg.call("concurrent", &[], &[], 0);
                    assert!(result.is_ok());
                }
            }));
        }

        for handle in handles {
            handle.join().expect("thread panicked");
        }

        let info = registry.info("concurrent").expect("info should exist");
        assert_eq!(info.call_count, 40);
    }
}
