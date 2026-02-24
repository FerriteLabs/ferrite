//! WASM function registry
//!
//! Manages the lifecycle of WASM functions including loading,
//! caching, execution, versioning, and dependency tracking.

use super::executor::{ExecutionResult, WasmExecutor};
use super::hot_reload::InflightTracker;
use super::observability::WasmMetricsRegistry;
use super::types::{
    FunctionDependency, FunctionMetadata, FunctionPermissions, FunctionVersion, ResourceLimits,
    WasmFunction, WasmValue,
};
use super::WasmError;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

/// Registry for WASM functions
///
/// Manages loaded WASM modules and provides execution capabilities.
///
/// # Example
///
/// ```ignore
/// let registry = FunctionRegistry::new()?;
///
/// // Load a function from bytes
/// registry.load("myfilter", wasm_bytes, metadata)?;
///
/// // Call the function
/// let result = registry.call("myfilter", "run", keys, args).await?;
///
/// // Unload when done
/// registry.unload("myfilter")?;
/// ```
pub struct FunctionRegistry {
    /// Loaded functions (latest version)
    functions: RwLock<HashMap<String, Arc<WasmFunction>>>,
    /// Version history: name -> (version -> function)
    versions: RwLock<HashMap<String, HashMap<FunctionVersion, Arc<WasmFunction>>>>,
    /// WASM executor
    executor: Arc<WasmExecutor>,
    /// Default resource limits
    #[allow(dead_code)] // Planned for v0.2 — stored for default module resource limits
    default_limits: ResourceLimits,
    /// Maximum number of functions
    max_functions: usize,
    /// Maximum WASM instances across all modules
    max_instances: usize,
    /// Current active instance count
    active_instances: RwLock<usize>,
    /// Per-function metrics
    metrics: Arc<WasmMetricsRegistry>,
    /// In-flight call tracker for graceful hot reload
    inflight: Arc<InflightTracker>,
}

impl FunctionRegistry {
    /// Create a new function registry
    pub fn new() -> Self {
        Self {
            functions: RwLock::new(HashMap::new()),
            versions: RwLock::new(HashMap::new()),
            executor: Arc::new(WasmExecutor::new()),
            default_limits: ResourceLimits::default(),
            max_functions: 1000,
            max_instances: 256,
            active_instances: RwLock::new(0),
            metrics: Arc::new(WasmMetricsRegistry::new()),
            inflight: Arc::new(InflightTracker::new()),
        }
    }

    /// Create with custom limits
    pub fn with_limits(limits: ResourceLimits) -> Self {
        Self {
            functions: RwLock::new(HashMap::new()),
            versions: RwLock::new(HashMap::new()),
            executor: Arc::new(WasmExecutor::new()),
            default_limits: limits,
            max_functions: 1000,
            max_instances: 256,
            active_instances: RwLock::new(0),
            metrics: Arc::new(WasmMetricsRegistry::new()),
            inflight: Arc::new(InflightTracker::new()),
        }
    }

    /// Create with custom max instances per server
    pub fn with_max_instances(mut self, max: usize) -> Self {
        self.max_instances = max;
        self
    }

    /// Get the metrics registry
    pub fn metrics(&self) -> &Arc<WasmMetricsRegistry> {
        &self.metrics
    }

    /// Get the in-flight tracker (for hot reload)
    pub fn inflight_tracker(&self) -> &Arc<InflightTracker> {
        &self.inflight
    }

    /// Load a function from WASM bytes
    pub fn load(
        &self,
        name: &str,
        wasm_bytes: Vec<u8>,
        metadata: Option<FunctionMetadata>,
    ) -> Result<(), WasmError> {
        // Check capacity
        {
            let functions = self.functions.read();
            if functions.len() >= self.max_functions && !functions.contains_key(name) {
                return Err(WasmError::ResourceLimitExceeded(format!(
                    "maximum functions ({}) exceeded",
                    self.max_functions
                )));
            }
        }

        // Check instance limit
        {
            let instances = self.active_instances.read();
            if *instances >= self.max_instances {
                return Err(WasmError::InstanceLimitExceeded {
                    limit: self.max_instances,
                });
            }
        }

        // Validate WASM
        self.executor.validate(&wasm_bytes)?;

        // Calculate source hash
        let source_hash = calculate_hash(&wasm_bytes);

        // Parse exports
        let exports = self.executor.parse_exports(&wasm_bytes)?;

        // Create metadata
        let metadata = metadata
            .unwrap_or_else(|| FunctionMetadata::new(name.to_string(), source_hash.clone()));

        // Validate dependencies
        self.check_dependencies(&metadata.dependencies)?;

        // Create function
        let version = metadata.version.clone();
        let function = Arc::new(WasmFunction::new(metadata, wasm_bytes).with_exports(exports));

        // Store in main map and version history
        let is_new = !self.functions.read().contains_key(name);
        self.functions
            .write()
            .insert(name.to_string(), Arc::clone(&function));

        // Store versioned copy
        self.versions
            .write()
            .entry(name.to_string())
            .or_default()
            .insert(version, function);

        if is_new {
            *self.active_instances.write() += 1;
        }

        // Initialize metrics for this function
        self.metrics.get_or_create(name);

        tracing::debug!(function = name, "WASM function loaded");

        Ok(())
    }

    /// Load a function from a file
    pub fn load_file(
        &self,
        name: &str,
        path: &Path,
        metadata: Option<FunctionMetadata>,
    ) -> Result<(), WasmError> {
        let wasm_bytes = std::fs::read(path)
            .map_err(|e| WasmError::InvalidModule(format!("failed to read file: {}", e)))?;

        self.load(name, wasm_bytes, metadata)
    }

    /// Load a function from base64
    pub fn load_base64(
        &self,
        name: &str,
        base64_data: &str,
        metadata: Option<FunctionMetadata>,
    ) -> Result<(), WasmError> {
        use base64::Engine;

        let wasm_bytes = base64::engine::general_purpose::STANDARD
            .decode(base64_data)
            .map_err(|e| WasmError::InvalidModule(format!("invalid base64: {}", e)))?;

        self.load(name, wasm_bytes, metadata)
    }

    /// Unload a function
    pub fn unload(&self, name: &str) -> Result<(), WasmError> {
        if self.functions.write().remove(name).is_none() {
            return Err(WasmError::FunctionNotFound(name.to_string()));
        }
        self.versions.write().remove(name);
        self.metrics.remove(name);
        let mut instances = self.active_instances.write();
        *instances = instances.saturating_sub(1);
        Ok(())
    }

    /// Reload a function (atomic replace), preserving stats and version history
    pub fn reload(
        &self,
        name: &str,
        wasm_bytes: Vec<u8>,
        metadata: Option<FunctionMetadata>,
    ) -> Result<(), WasmError> {
        // Validate first
        self.executor.validate(&wasm_bytes)?;

        // Check if exists
        if !self.functions.read().contains_key(name) {
            return Err(WasmError::FunctionNotFound(name.to_string()));
        }

        // Calculate source hash
        let source_hash = calculate_hash(&wasm_bytes);

        // Parse exports
        let exports = self.executor.parse_exports(&wasm_bytes)?;

        // Create metadata, preserving stats from old function
        let old_func = self.functions.read().get(name).cloned();
        let mut new_metadata = metadata
            .unwrap_or_else(|| FunctionMetadata::new(name.to_string(), source_hash.clone()));

        if let Some(old) = old_func {
            // Preserve call statistics
            new_metadata.call_count = old.metadata.call_count;
            new_metadata.total_execution_time_ms = old.metadata.total_execution_time_ms;
            new_metadata.created_at = old.metadata.created_at;
        }

        new_metadata.modified_at = SystemTime::now();

        // Validate dependencies
        self.check_dependencies(&new_metadata.dependencies)?;

        // Create and store new function
        let version = new_metadata.version.clone();
        let function = Arc::new(WasmFunction::new(new_metadata, wasm_bytes).with_exports(exports));

        self.functions
            .write()
            .insert(name.to_string(), Arc::clone(&function));

        // Store in version history
        self.versions
            .write()
            .entry(name.to_string())
            .or_default()
            .insert(version, function);

        Ok(())
    }

    /// Get a function by name
    pub fn get(&self, name: &str) -> Option<Arc<WasmFunction>> {
        self.functions.read().get(name).cloned()
    }

    /// Get a specific version of a function
    pub fn get_version(&self, name: &str, version: &FunctionVersion) -> Option<Arc<WasmFunction>> {
        self.versions
            .read()
            .get(name)
            .and_then(|vs| vs.get(version).cloned())
    }

    /// List all available versions for a function
    pub fn list_versions(&self, name: &str) -> Vec<FunctionVersion> {
        self.versions
            .read()
            .get(name)
            .map(|vs| {
                let mut versions: Vec<_> = vs.keys().cloned().collect();
                versions.sort_by(|a, b| {
                    a.major
                        .cmp(&b.major)
                        .then(a.minor.cmp(&b.minor))
                        .then(a.patch.cmp(&b.patch))
                });
                versions
            })
            .unwrap_or_default()
    }

    /// Check if a function exists
    pub fn exists(&self, name: &str) -> bool {
        self.functions.read().contains_key(name)
    }

    /// List all function names
    pub fn list(&self) -> Vec<String> {
        self.functions.read().keys().cloned().collect()
    }

    /// Get function info
    pub fn info(&self, name: &str) -> Option<FunctionInfo> {
        self.functions.read().get(name).map(|f| {
            let metrics_snap = self.metrics.snapshot(name);
            FunctionInfo {
                name: f.metadata.name.clone(),
                description: f.metadata.description.clone(),
                author: f.metadata.author.clone(),
                version: f.metadata.version.clone(),
                source_hash: f.metadata.source_hash.clone(),
                created_at: f.metadata.created_at,
                modified_at: f.metadata.modified_at,
                call_count: metrics_snap
                    .as_ref()
                    .map(|m| m.call_count)
                    .unwrap_or(f.metadata.call_count),
                avg_execution_time_ms: metrics_snap
                    .as_ref()
                    .map(|m| m.avg_duration.as_secs_f64() * 1000.0)
                    .unwrap_or(f.metadata.avg_execution_time_ms()),
                error_rate: metrics_snap.as_ref().map(|m| m.error_rate).unwrap_or(0.0),
                exports: f.exports.clone(),
                permissions: f.metadata.permissions.clone(),
                limits: f.metadata.limits.clone(),
                wasm_size: f.wasm_bytes.len(),
                available_versions: self.list_versions(name),
                dependencies: f.metadata.dependencies.clone(),
            }
        })
    }

    /// Call a function with in-flight tracking and metrics
    pub fn call(
        &self,
        name: &str,
        entry_point: &str,
        args: Vec<WasmValue>,
        db: u8,
    ) -> Result<ExecutionResult, WasmError> {
        let function = self
            .get(name)
            .ok_or_else(|| WasmError::FunctionNotFound(name.to_string()))?;

        // Track in-flight call
        let _guard = self.inflight.begin_call(name);
        let fn_metrics = self.metrics.get_or_create(name);

        let limits = &function.metadata.limits;
        let result = self
            .executor
            .execute(&function, entry_point, args, db, limits);

        // Record metrics
        if result.success {
            fn_metrics.record_success(
                result.duration,
                result.usage.memory_peak as u64,
                result.usage.fuel_consumed,
            );
        } else {
            fn_metrics.record_error(result.duration);
        }

        Ok(result)
    }

    /// Call a function with custom limits
    pub fn call_with_limits(
        &self,
        name: &str,
        entry_point: &str,
        args: Vec<WasmValue>,
        db: u8,
        limits: &ResourceLimits,
    ) -> Result<ExecutionResult, WasmError> {
        let function = self
            .get(name)
            .ok_or_else(|| WasmError::FunctionNotFound(name.to_string()))?;

        let _guard = self.inflight.begin_call(name);
        let fn_metrics = self.metrics.get_or_create(name);

        let result = self
            .executor
            .execute(&function, entry_point, args, db, limits);

        if result.success {
            fn_metrics.record_success(
                result.duration,
                result.usage.memory_peak as u64,
                result.usage.fuel_consumed,
            );
        } else {
            fn_metrics.record_error(result.duration);
        }

        Ok(result)
    }

    /// Call a function asynchronously
    pub async fn call_async(
        &self,
        name: &str,
        entry_point: &str,
        args: Vec<WasmValue>,
        db: u8,
    ) -> Result<ExecutionResult, WasmError> {
        let function = self
            .get(name)
            .ok_or_else(|| WasmError::FunctionNotFound(name.to_string()))?;

        let _guard = self.inflight.begin_call(name);
        let fn_metrics = self.metrics.get_or_create(name);

        let limits = &function.metadata.limits;
        let result = self
            .executor
            .execute_async(&function, entry_point, args, db, limits)
            .await;

        if result.success {
            fn_metrics.record_success(
                result.duration,
                result.usage.memory_peak as u64,
                result.usage.fuel_consumed,
            );
        } else {
            fn_metrics.record_error(result.duration);
        }

        Ok(result)
    }

    /// Get registry statistics
    pub fn stats(&self) -> RegistryStats {
        let functions = self.functions.read();
        let executor_stats = self.executor.stats();

        RegistryStats {
            function_count: functions.len(),
            total_wasm_size: functions.values().map(|f| f.wasm_bytes.len()).sum(),
            total_executions: executor_stats.total_executions,
            successful_executions: executor_stats.successful_executions,
            failed_executions: executor_stats.failed_executions,
        }
    }

    /// Get the number of loaded functions
    pub fn len(&self) -> usize {
        self.functions.read().len()
    }

    /// Check if the registry is empty
    pub fn is_empty(&self) -> bool {
        self.functions.read().is_empty()
    }

    /// Clear all functions
    pub fn clear(&self) {
        self.functions.write().clear();
        self.versions.write().clear();
        *self.active_instances.write() = 0;
    }

    /// Check that all dependencies of a function are satisfied
    fn check_dependencies(&self, deps: &[FunctionDependency]) -> Result<(), WasmError> {
        let functions = self.functions.read();
        for dep in deps {
            if dep.optional {
                continue;
            }
            let func = functions.get(&dep.name);
            match func {
                None => {
                    return Err(WasmError::DependencyNotSatisfied(format!(
                        "required function '{}' not loaded",
                        dep.name
                    )));
                }
                Some(f) => {
                    if let Some(ref min_ver) = dep.min_version {
                        if !f.metadata.version.is_compatible(min_ver) {
                            return Err(WasmError::VersionConflict(format!(
                                "function '{}' version {} not compatible with required {}",
                                dep.name, f.metadata.version, min_ver
                            )));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Get the current number of active instances
    pub fn active_instances(&self) -> usize {
        *self.active_instances.read()
    }

    /// Get the maximum instances per server
    pub fn max_instances(&self) -> usize {
        self.max_instances
    }
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Calculate a hash of data using FNV-1a algorithm
fn calculate_hash(data: &[u8]) -> String {
    // FNV-1a 64-bit hash
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET;
    for byte in data {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }

    // Also incorporate length to reduce collisions
    hash ^= data.len() as u64;
    hash = hash.wrapping_mul(FNV_PRIME);

    // Format as hex string
    format!("{:016x}", hash)
}

/// Information about a loaded function
#[derive(Clone, Debug)]
pub struct FunctionInfo {
    /// Function name
    pub name: String,
    /// Description
    pub description: Option<String>,
    /// Author
    pub author: Option<String>,
    /// Current version
    pub version: FunctionVersion,
    /// Source hash
    pub source_hash: String,
    /// Creation time
    pub created_at: SystemTime,
    /// Last modified time
    pub modified_at: SystemTime,
    /// Number of calls
    pub call_count: u64,
    /// Average execution time
    pub avg_execution_time_ms: f64,
    /// Error rate (0.0 to 1.0)
    pub error_rate: f64,
    /// Exported functions
    pub exports: Vec<String>,
    /// Permissions
    pub permissions: FunctionPermissions,
    /// Resource limits
    pub limits: ResourceLimits,
    /// WASM module size
    pub wasm_size: usize,
    /// Available versions
    pub available_versions: Vec<FunctionVersion>,
    /// Dependencies
    pub dependencies: Vec<FunctionDependency>,
}

/// Registry statistics
#[derive(Clone, Debug)]
pub struct RegistryStats {
    /// Number of loaded functions
    pub function_count: usize,
    /// Total WASM bytes loaded
    pub total_wasm_size: usize,
    /// Total executions
    pub total_executions: u64,
    /// Successful executions
    pub successful_executions: u64,
    /// Failed executions
    pub failed_executions: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_wasm_bytes() -> Vec<u8> {
        vec![
            0x00, 0x61, 0x73, 0x6D, // Magic
            0x01, 0x00, 0x00, 0x00, // Version 1
        ]
    }

    #[test]
    fn test_registry_new() {
        let registry = FunctionRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn test_registry_load() {
        let registry = FunctionRegistry::new();

        registry.load("test", valid_wasm_bytes(), None).unwrap();

        assert!(registry.exists("test"));
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn test_registry_load_invalid() {
        let registry = FunctionRegistry::new();

        let result = registry.load("test", vec![0, 0, 0, 0], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_registry_load_with_metadata() {
        let registry = FunctionRegistry::new();

        let metadata = FunctionMetadata::new("test".to_string(), "custom_hash".to_string())
            .with_description("Test function".to_string());

        registry
            .load("test", valid_wasm_bytes(), Some(metadata))
            .unwrap();

        let info = registry.info("test").unwrap();
        assert_eq!(info.description, Some("Test function".to_string()));
    }

    #[test]
    fn test_registry_unload() {
        let registry = FunctionRegistry::new();

        registry.load("test", valid_wasm_bytes(), None).unwrap();
        assert!(registry.exists("test"));

        registry.unload("test").unwrap();
        assert!(!registry.exists("test"));
    }

    #[test]
    fn test_registry_unload_nonexistent() {
        let registry = FunctionRegistry::new();

        let result = registry.unload("nonexistent");
        assert!(matches!(result, Err(WasmError::FunctionNotFound(_))));
    }

    #[test]
    fn test_registry_reload() {
        let registry = FunctionRegistry::new();

        registry.load("test", valid_wasm_bytes(), None).unwrap();

        // Reload with new bytes
        let new_bytes = vec![
            0x00, 0x61, 0x73, 0x6D, 0x01, 0x00, 0x00, 0x00,
            0x00, // Extra byte to make it different
        ];

        registry.reload("test", new_bytes, None).unwrap();

        let info = registry.info("test").unwrap();
        assert_eq!(info.wasm_size, 9);
    }

    #[test]
    fn test_registry_reload_nonexistent() {
        let registry = FunctionRegistry::new();

        let result = registry.reload("nonexistent", valid_wasm_bytes(), None);
        assert!(matches!(result, Err(WasmError::FunctionNotFound(_))));
    }

    #[test]
    fn test_registry_list() {
        let registry = FunctionRegistry::new();

        registry.load("func1", valid_wasm_bytes(), None).unwrap();
        registry.load("func2", valid_wasm_bytes(), None).unwrap();
        registry.load("func3", valid_wasm_bytes(), None).unwrap();

        let names = registry.list();
        assert_eq!(names.len(), 3);
        assert!(names.contains(&"func1".to_string()));
        assert!(names.contains(&"func2".to_string()));
        assert!(names.contains(&"func3".to_string()));
    }

    #[test]
    fn test_registry_info() {
        let registry = FunctionRegistry::new();

        registry.load("test", valid_wasm_bytes(), None).unwrap();

        let info = registry.info("test").unwrap();
        assert_eq!(info.name, "test");
        assert_eq!(info.wasm_size, 8);
        assert_eq!(info.call_count, 0);
    }

    #[cfg(not(feature = "wasm"))]
    #[test]
    fn test_registry_call() {
        let registry = FunctionRegistry::new();

        let mut metadata = FunctionMetadata::new("test".to_string(), "hash".to_string());
        registry
            .load("test", valid_wasm_bytes(), Some(metadata))
            .unwrap();

        let result = registry.call("test", "run", vec![], 0).unwrap();
        assert!(result.success);
    }

    #[test]
    fn test_registry_call_nonexistent() {
        let registry = FunctionRegistry::new();

        let result = registry.call("nonexistent", "run", vec![], 0);
        assert!(matches!(result, Err(WasmError::FunctionNotFound(_))));
    }

    #[cfg(not(feature = "wasm"))]
    #[test]
    fn test_registry_call_with_args() {
        let registry = FunctionRegistry::new();

        registry.load("test", valid_wasm_bytes(), None).unwrap();

        // Add exports to the function
        {
            let mut functions = registry.functions.write();
            let func = functions.get_mut("test").unwrap();
            let inner = Arc::make_mut(func);
            inner.exports = vec!["add".to_string()];
        }

        let result = registry
            .call("test", "add", vec![WasmValue::I32(5), WasmValue::I32(3)], 0)
            .unwrap();

        assert!(result.success);
        assert_eq!(result.as_i32(), Some(8));
    }

    #[cfg(not(feature = "wasm"))]
    #[tokio::test]
    async fn test_registry_call_async() {
        let registry = FunctionRegistry::new();

        registry.load("test", valid_wasm_bytes(), None).unwrap();

        let result = registry.call_async("test", "run", vec![], 0).await.unwrap();
        assert!(result.success);
    }

    #[test]
    fn test_registry_stats() {
        let registry = FunctionRegistry::new();

        registry.load("func1", valid_wasm_bytes(), None).unwrap();
        registry.load("func2", valid_wasm_bytes(), None).unwrap();

        let stats = registry.stats();
        assert_eq!(stats.function_count, 2);
        assert_eq!(stats.total_wasm_size, 16);
    }

    #[test]
    fn test_registry_clear() {
        let registry = FunctionRegistry::new();

        registry.load("func1", valid_wasm_bytes(), None).unwrap();
        registry.load("func2", valid_wasm_bytes(), None).unwrap();

        assert_eq!(registry.len(), 2);

        registry.clear();

        assert!(registry.is_empty());
    }

    #[test]
    fn test_registry_load_base64() {
        let registry = FunctionRegistry::new();

        // Base64 encode valid WASM bytes
        use base64::Engine;
        let encoded = base64::engine::general_purpose::STANDARD.encode(valid_wasm_bytes());

        registry.load_base64("test", &encoded, None).unwrap();

        assert!(registry.exists("test"));
    }

    #[test]
    fn test_registry_load_base64_invalid() {
        let registry = FunctionRegistry::new();

        let result = registry.load_base64("test", "not valid base64!!!", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_calculate_hash() {
        let hash1 = calculate_hash(b"hello");
        let hash2 = calculate_hash(b"hello");
        let hash3 = calculate_hash(b"world");

        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);
        assert_eq!(hash1.len(), 16); // FNV-1a 64-bit hex length
    }

    #[test]
    fn test_function_info() {
        let registry = FunctionRegistry::new();

        let metadata = FunctionMetadata::new("test".to_string(), "hash".to_string())
            .with_description("A test function".to_string())
            .with_author("alice".to_string())
            .with_permissions(FunctionPermissions::read_only())
            .with_limits(ResourceLimits::quick());

        registry
            .load("test", valid_wasm_bytes(), Some(metadata))
            .unwrap();

        let info = registry.info("test").unwrap();
        assert_eq!(info.name, "test");
        assert_eq!(info.description, Some("A test function".to_string()));
        assert_eq!(info.author, Some("alice".to_string()));
        assert!(!info.permissions.allow_write);
        assert_eq!(info.limits.max_fuel, ResourceLimits::quick().max_fuel);
    }

    #[test]
    fn test_versioning() {
        let registry = FunctionRegistry::new();

        // Load v1
        let meta_v1 = FunctionMetadata::new("myfn".to_string(), "hash_v1".to_string())
            .with_version(FunctionVersion::new(1, 0, 0));
        registry
            .load("myfn", valid_wasm_bytes(), Some(meta_v1))
            .unwrap();

        // Reload with v1.1
        let meta_v1_1 = FunctionMetadata::new("myfn".to_string(), "hash_v1_1".to_string())
            .with_version(FunctionVersion::new(1, 1, 0));
        registry
            .reload("myfn", valid_wasm_bytes(), Some(meta_v1_1))
            .unwrap();

        let versions = registry.list_versions("myfn");
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0], FunctionVersion::new(1, 0, 0));
        assert_eq!(versions[1], FunctionVersion::new(1, 1, 0));

        // Get specific version
        let v1 = registry.get_version("myfn", &FunctionVersion::new(1, 0, 0));
        assert!(v1.is_some());
    }

    #[test]
    fn test_dependency_tracking() {
        let registry = FunctionRegistry::new();

        // Load the dependency first
        let helper_meta = FunctionMetadata::new("helper".to_string(), "hash".to_string())
            .with_version(FunctionVersion::new(1, 0, 0));
        registry
            .load("helper", valid_wasm_bytes(), Some(helper_meta))
            .unwrap();

        // Load a function that depends on helper
        let dep = FunctionDependency::required("helper".to_string())
            .with_min_version(FunctionVersion::new(1, 0, 0));
        let meta = FunctionMetadata::new("consumer".to_string(), "hash2".to_string())
            .with_dependencies(vec![dep]);
        registry
            .load("consumer", valid_wasm_bytes(), Some(meta))
            .unwrap();

        assert!(registry.exists("consumer"));
    }

    #[test]
    fn test_dependency_not_satisfied() {
        let registry = FunctionRegistry::new();

        let dep = FunctionDependency::required("missing_fn".to_string());
        let meta = FunctionMetadata::new("consumer".to_string(), "hash".to_string())
            .with_dependencies(vec![dep]);

        let result = registry.load("consumer", valid_wasm_bytes(), Some(meta));
        assert!(matches!(result, Err(WasmError::DependencyNotSatisfied(_))));
    }

    #[test]
    fn test_optional_dependency_ok_when_missing() {
        let registry = FunctionRegistry::new();

        let dep = FunctionDependency::optional("missing_fn".to_string());
        let meta = FunctionMetadata::new("consumer".to_string(), "hash".to_string())
            .with_dependencies(vec![dep]);

        // Optional deps don't cause failure
        let result = registry.load("consumer", valid_wasm_bytes(), Some(meta));
        assert!(result.is_ok());
    }

    #[test]
    fn test_instance_limit() {
        let registry = FunctionRegistry::new().with_max_instances(2);

        registry.load("fn1", valid_wasm_bytes(), None).unwrap();
        registry.load("fn2", valid_wasm_bytes(), None).unwrap();

        let result = registry.load("fn3", valid_wasm_bytes(), None);
        assert!(matches!(
            result,
            Err(WasmError::InstanceLimitExceeded { .. })
        ));

        // Unload one should allow a new one
        registry.unload("fn1").unwrap();
        assert!(registry.load("fn3", valid_wasm_bytes(), None).is_ok());
    }

    #[test]
    fn test_active_instances_tracking() {
        let registry = FunctionRegistry::new();

        assert_eq!(registry.active_instances(), 0);

        registry.load("fn1", valid_wasm_bytes(), None).unwrap();
        assert_eq!(registry.active_instances(), 1);

        registry.load("fn2", valid_wasm_bytes(), None).unwrap();
        assert_eq!(registry.active_instances(), 2);

        registry.unload("fn1").unwrap();
        assert_eq!(registry.active_instances(), 1);

        registry.clear();
        assert_eq!(registry.active_instances(), 0);
    }

    #[cfg(not(feature = "wasm"))]
    #[test]
    fn test_metrics_recorded_on_call() {
        let registry = FunctionRegistry::new();
        registry.load("test", valid_wasm_bytes(), None).unwrap();

        registry.call("test", "run", vec![], 0).unwrap();
        registry.call("test", "run", vec![], 0).unwrap();

        let snap = registry.metrics().snapshot("test").unwrap();
        assert_eq!(snap.call_count, 2);
        assert_eq!(snap.success_count, 2);
        assert_eq!(snap.error_count, 0);
    }

    #[test]
    fn test_inflight_tracker_accessible() {
        let registry = FunctionRegistry::new();
        let tracker = registry.inflight_tracker();
        assert_eq!(tracker.inflight_count("any_fn"), 0);
    }
}
