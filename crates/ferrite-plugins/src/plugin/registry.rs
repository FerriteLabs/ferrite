//! Plugin Registry
//!
//! Manages loaded plugins and their lifecycle.

use super::manifest::PluginManifest;
use super::PluginError;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

/// Plugin registry managing loaded plugins
pub struct PluginRegistry {
    /// Loaded plugins
    plugins: RwLock<HashMap<String, Arc<LoadedPlugin>>>,
    /// Maximum number of plugins
    max_plugins: usize,
    /// Statistics
    stats: RwLock<RegistryStats>,
}

impl PluginRegistry {
    /// Create a new registry
    pub fn new(max_plugins: usize) -> Self {
        Self {
            plugins: RwLock::new(HashMap::new()),
            max_plugins,
            stats: RwLock::new(RegistryStats::default()),
        }
    }

    /// Register a plugin
    pub fn register(&self, plugin: LoadedPlugin) -> Result<(), PluginError> {
        let mut plugins = self.plugins.write();

        // Check capacity
        if plugins.len() >= self.max_plugins && !plugins.contains_key(&plugin.name) {
            return Err(PluginError::CapacityExceeded(self.max_plugins));
        }

        // Check for duplicates
        if plugins.contains_key(&plugin.name) {
            return Err(PluginError::AlreadyExists(plugin.name.clone()));
        }

        let name = plugin.name.clone();
        plugins.insert(name, Arc::new(plugin));

        // Update stats
        let mut stats = self.stats.write();
        stats.total_plugins = plugins.len();
        stats.active_plugins = plugins
            .values()
            .filter(|p| p.state == PluginState::Active)
            .count();

        Ok(())
    }

    /// Unregister a plugin
    pub fn unregister(&self, name: &str) -> Result<(), PluginError> {
        let mut plugins = self.plugins.write();

        if plugins.remove(name).is_none() {
            return Err(PluginError::NotFound(name.to_string()));
        }

        // Update stats
        let mut stats = self.stats.write();
        stats.total_plugins = plugins.len();
        stats.active_plugins = plugins
            .values()
            .filter(|p| p.state == PluginState::Active)
            .count();

        Ok(())
    }

    /// Update a plugin (for hot reload)
    pub fn update(&self, name: &str, plugin: LoadedPlugin) -> Result<(), PluginError> {
        let mut plugins = self.plugins.write();

        if !plugins.contains_key(name) {
            return Err(PluginError::NotFound(name.to_string()));
        }

        plugins.insert(name.to_string(), Arc::new(plugin));

        Ok(())
    }

    /// Get a plugin by name
    pub fn get(&self, name: &str) -> Option<Arc<LoadedPlugin>> {
        self.plugins.read().get(name).cloned()
    }

    /// Check if a plugin exists
    pub fn exists(&self, name: &str) -> bool {
        self.plugins.read().contains_key(name)
    }

    /// List all plugins
    pub fn list(&self) -> Vec<PluginInfo> {
        self.plugins
            .read()
            .values()
            .map(|p| PluginInfo::from_loaded(p))
            .collect()
    }

    /// Get plugin info
    pub fn info(&self, name: &str) -> Option<PluginInfo> {
        self.plugins
            .read()
            .get(name)
            .map(|p| PluginInfo::from_loaded(p))
    }

    /// Get plugins by state
    pub fn by_state(&self, state: PluginState) -> Vec<PluginInfo> {
        self.plugins
            .read()
            .values()
            .filter(|p| p.state == state)
            .map(|p| PluginInfo::from_loaded(p))
            .collect()
    }

    /// Set plugin state
    pub fn set_state(&self, name: &str, state: PluginState) -> Result<(), PluginError> {
        let mut plugins = self.plugins.write();

        let plugin = plugins
            .get_mut(name)
            .ok_or_else(|| PluginError::NotFound(name.to_string()))?;

        // Get mutable reference
        let plugin = Arc::make_mut(plugin);
        plugin.state = state.clone();
        plugin.state_changed_at = SystemTime::now();

        // Update stats
        drop(plugins);
        let plugins = self.plugins.read();
        let mut stats = self.stats.write();
        stats.active_plugins = plugins
            .values()
            .filter(|p| p.state == PluginState::Active)
            .count();
        stats.failed_plugins = plugins
            .values()
            .filter(|p| p.state == PluginState::Failed)
            .count();

        Ok(())
    }

    /// Get registry statistics
    pub fn stats(&self) -> PluginStats {
        let stats = self.stats.read();
        PluginStats {
            total_plugins: stats.total_plugins,
            active_plugins: stats.active_plugins,
            failed_plugins: stats.failed_plugins,
            total_commands: stats.total_commands,
            total_executions: stats.total_executions,
        }
    }

    /// Get number of plugins
    pub fn len(&self) -> usize {
        self.plugins.read().len()
    }

    /// Check if registry is empty
    pub fn is_empty(&self) -> bool {
        self.plugins.read().is_empty()
    }

    /// Clear all plugins
    pub fn clear(&self) {
        self.plugins.write().clear();
        let mut stats = self.stats.write();
        stats.total_plugins = 0;
        stats.active_plugins = 0;
    }
}

impl Default for PluginRegistry {
    fn default() -> Self {
        Self::new(100)
    }
}

/// Internal registry statistics
#[derive(Default)]
struct RegistryStats {
    total_plugins: usize,
    active_plugins: usize,
    failed_plugins: usize,
    total_commands: usize,
    total_executions: u64,
}

/// A loaded plugin instance
#[derive(Clone, Debug)]
pub struct LoadedPlugin {
    /// Plugin name
    pub name: String,
    /// Plugin manifest
    pub manifest: PluginManifest,
    /// Plugin state
    pub state: PluginState,
    /// WASM bytecode
    pub wasm_bytes: Vec<u8>,
    /// Source hash
    pub source_hash: String,
    /// Load time
    pub loaded_at: SystemTime,
    /// State change time
    pub state_changed_at: SystemTime,
    /// Execution statistics
    pub exec_stats: ExecutionStats,
}

impl LoadedPlugin {
    /// Create a new loaded plugin
    pub fn new(
        name: String,
        manifest: PluginManifest,
        wasm_bytes: Vec<u8>,
        source_hash: String,
    ) -> Self {
        let now = SystemTime::now();
        Self {
            name,
            manifest,
            state: PluginState::Active,
            wasm_bytes,
            source_hash,
            loaded_at: now,
            state_changed_at: now,
            exec_stats: ExecutionStats::default(),
        }
    }

    /// Check if plugin is active
    pub fn is_active(&self) -> bool {
        self.state == PluginState::Active
    }

    /// Get size of WASM bytecode
    pub fn wasm_size(&self) -> usize {
        self.wasm_bytes.len()
    }

    /// Record an execution
    pub fn record_execution(&mut self, success: bool, duration_ms: u64) {
        self.exec_stats.total_calls += 1;
        if success {
            self.exec_stats.successful_calls += 1;
        } else {
            self.exec_stats.failed_calls += 1;
        }
        self.exec_stats.total_time_ms += duration_ms;
        self.exec_stats.last_called = Some(SystemTime::now());
    }
}

/// Plugin state
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PluginState {
    /// Plugin is loading
    Loading,
    /// Plugin is active and running
    Active,
    /// Plugin is disabled
    Disabled,
    /// Plugin failed to load or run
    Failed,
    /// Plugin is being unloaded
    Unloading,
}

impl std::fmt::Display for PluginState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PluginState::Loading => write!(f, "loading"),
            PluginState::Active => write!(f, "active"),
            PluginState::Disabled => write!(f, "disabled"),
            PluginState::Failed => write!(f, "failed"),
            PluginState::Unloading => write!(f, "unloading"),
        }
    }
}

/// Execution statistics for a plugin
#[derive(Clone, Debug, Default)]
pub struct ExecutionStats {
    /// Total calls
    pub total_calls: u64,
    /// Successful calls
    pub successful_calls: u64,
    /// Failed calls
    pub failed_calls: u64,
    /// Total execution time in milliseconds
    pub total_time_ms: u64,
    /// Last call time
    pub last_called: Option<SystemTime>,
}

impl ExecutionStats {
    /// Get average execution time in milliseconds
    pub fn avg_time_ms(&self) -> f64 {
        if self.total_calls == 0 {
            0.0
        } else {
            self.total_time_ms as f64 / self.total_calls as f64
        }
    }

    /// Get success rate
    pub fn success_rate(&self) -> f64 {
        if self.total_calls == 0 {
            1.0
        } else {
            self.successful_calls as f64 / self.total_calls as f64
        }
    }
}

/// Plugin information (summary view)
#[derive(Clone, Debug)]
pub struct PluginInfo {
    /// Plugin name
    pub name: String,
    /// Version string
    pub version: String,
    /// Description
    pub description: String,
    /// Author
    pub author: Option<String>,
    /// Current state
    pub state: PluginState,
    /// WASM size in bytes
    pub wasm_size: usize,
    /// Number of commands
    pub command_count: usize,
    /// Number of hooks
    pub hook_count: usize,
    /// Load time
    pub loaded_at: SystemTime,
    /// Total executions
    pub total_executions: u64,
    /// Average execution time
    pub avg_execution_time_ms: f64,
}

impl PluginInfo {
    /// Create from loaded plugin
    pub fn from_loaded(plugin: &LoadedPlugin) -> Self {
        Self {
            name: plugin.name.clone(),
            version: plugin.manifest.version.to_string(),
            description: plugin.manifest.description.clone(),
            author: plugin.manifest.author.clone(),
            state: plugin.state.clone(),
            wasm_size: plugin.wasm_bytes.len(),
            command_count: plugin.manifest.commands.len(),
            hook_count: plugin.manifest.hooks.len(),
            loaded_at: plugin.loaded_at,
            total_executions: plugin.exec_stats.total_calls,
            avg_execution_time_ms: plugin.exec_stats.avg_time_ms(),
        }
    }
}

/// Plugin statistics
#[derive(Clone, Debug)]
pub struct PluginStats {
    /// Total plugins loaded
    pub total_plugins: usize,
    /// Active plugins
    pub active_plugins: usize,
    /// Failed plugins
    pub failed_plugins: usize,
    /// Total commands registered
    pub total_commands: usize,
    /// Total executions across all plugins
    pub total_executions: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::manifest::Version;

    fn create_test_plugin(name: &str) -> LoadedPlugin {
        LoadedPlugin::new(
            name.to_string(),
            PluginManifest::new(name, Version::new(1, 0, 0)),
            vec![0x00, 0x61, 0x73, 0x6D, 0x01, 0x00, 0x00, 0x00],
            "test_hash".to_string(),
        )
    }

    #[test]
    fn test_registry_new() {
        let registry = PluginRegistry::new(10);
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn test_registry_register() {
        let registry = PluginRegistry::new(10);
        let plugin = create_test_plugin("test");

        registry.register(plugin).unwrap();

        assert!(registry.exists("test"));
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn test_registry_register_duplicate() {
        let registry = PluginRegistry::new(10);
        let plugin1 = create_test_plugin("test");
        let plugin2 = create_test_plugin("test");

        registry.register(plugin1).unwrap();
        let result = registry.register(plugin2);

        assert!(matches!(result, Err(PluginError::AlreadyExists(_))));
    }

    #[test]
    fn test_registry_capacity() {
        let registry = PluginRegistry::new(2);

        registry.register(create_test_plugin("test1")).unwrap();
        registry.register(create_test_plugin("test2")).unwrap();
        let result = registry.register(create_test_plugin("test3"));

        assert!(matches!(result, Err(PluginError::CapacityExceeded(_))));
    }

    #[test]
    fn test_registry_unregister() {
        let registry = PluginRegistry::new(10);
        registry.register(create_test_plugin("test")).unwrap();

        assert!(registry.exists("test"));
        registry.unregister("test").unwrap();
        assert!(!registry.exists("test"));
    }

    #[test]
    fn test_registry_unregister_nonexistent() {
        let registry = PluginRegistry::new(10);
        let result = registry.unregister("nonexistent");
        assert!(matches!(result, Err(PluginError::NotFound(_))));
    }

    #[test]
    fn test_registry_get() {
        let registry = PluginRegistry::new(10);
        registry.register(create_test_plugin("test")).unwrap();

        let plugin = registry.get("test").unwrap();
        assert_eq!(plugin.name, "test");

        assert!(registry.get("nonexistent").is_none());
    }

    #[test]
    fn test_registry_list() {
        let registry = PluginRegistry::new(10);
        registry.register(create_test_plugin("plugin1")).unwrap();
        registry.register(create_test_plugin("plugin2")).unwrap();

        let list = registry.list();
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn test_registry_set_state() {
        let registry = PluginRegistry::new(10);
        registry.register(create_test_plugin("test")).unwrap();

        assert_eq!(registry.get("test").unwrap().state, PluginState::Active);

        registry.set_state("test", PluginState::Disabled).unwrap();
        assert_eq!(registry.get("test").unwrap().state, PluginState::Disabled);
    }

    #[test]
    fn test_registry_by_state() {
        let registry = PluginRegistry::new(10);
        registry.register(create_test_plugin("active1")).unwrap();
        registry.register(create_test_plugin("active2")).unwrap();
        registry.register(create_test_plugin("disabled")).unwrap();

        registry
            .set_state("disabled", PluginState::Disabled)
            .unwrap();

        let active = registry.by_state(PluginState::Active);
        assert_eq!(active.len(), 2);

        let disabled = registry.by_state(PluginState::Disabled);
        assert_eq!(disabled.len(), 1);
    }

    #[test]
    fn test_registry_stats() {
        let registry = PluginRegistry::new(10);
        registry.register(create_test_plugin("test1")).unwrap();
        registry.register(create_test_plugin("test2")).unwrap();
        registry.set_state("test2", PluginState::Failed).unwrap();

        let stats = registry.stats();
        assert_eq!(stats.total_plugins, 2);
        assert_eq!(stats.active_plugins, 1);
        assert_eq!(stats.failed_plugins, 1);
    }

    #[test]
    fn test_registry_clear() {
        let registry = PluginRegistry::new(10);
        registry.register(create_test_plugin("test1")).unwrap();
        registry.register(create_test_plugin("test2")).unwrap();

        assert_eq!(registry.len(), 2);
        registry.clear();
        assert!(registry.is_empty());
    }

    #[test]
    fn test_loaded_plugin_new() {
        let plugin = create_test_plugin("test");
        assert_eq!(plugin.name, "test");
        assert!(plugin.is_active());
        assert_eq!(plugin.wasm_size(), 8);
    }

    #[test]
    fn test_loaded_plugin_record_execution() {
        let mut plugin = create_test_plugin("test");

        plugin.record_execution(true, 10);
        plugin.record_execution(true, 20);
        plugin.record_execution(false, 5);

        assert_eq!(plugin.exec_stats.total_calls, 3);
        assert_eq!(plugin.exec_stats.successful_calls, 2);
        assert_eq!(plugin.exec_stats.failed_calls, 1);
        assert_eq!(plugin.exec_stats.total_time_ms, 35);
    }

    #[test]
    fn test_execution_stats() {
        let mut stats = ExecutionStats::default();

        assert_eq!(stats.avg_time_ms(), 0.0);
        assert_eq!(stats.success_rate(), 1.0);

        stats.total_calls = 4;
        stats.successful_calls = 3;
        stats.total_time_ms = 100;

        assert_eq!(stats.avg_time_ms(), 25.0);
        assert_eq!(stats.success_rate(), 0.75);
    }

    #[test]
    fn test_plugin_state_display() {
        assert_eq!(PluginState::Active.to_string(), "active");
        assert_eq!(PluginState::Disabled.to_string(), "disabled");
        assert_eq!(PluginState::Failed.to_string(), "failed");
    }

    #[test]
    fn test_plugin_info_from_loaded() {
        let plugin = create_test_plugin("test");
        let info = PluginInfo::from_loaded(&plugin);

        assert_eq!(info.name, "test");
        assert_eq!(info.version, "1.0.0");
        assert_eq!(info.state, PluginState::Active);
        assert_eq!(info.wasm_size, 8);
    }
}
