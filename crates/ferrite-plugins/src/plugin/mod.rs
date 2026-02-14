//! Plugin System
//!
//! Extensibility framework for Ferrite, allowing custom commands,
//! data types, storage backends, and event handlers.
//!
//! # Features
//!
//! - **Custom Commands**: Add new Redis-like commands
//! - **Custom Data Types**: Define new data structures
//! - **Storage Backends**: Integrate external storage systems
//! - **Event Handlers**: React to server events
//! - **WASM Plugins**: Portable plugin format using WebAssembly
//!
//! # Example
//!
//! ```ignore
//! use ferrite::plugin::{PluginManager, PluginManifest};
//!
//! let manager = PluginManager::new(PluginConfig::default());
//!
//! // Load a plugin
//! manager.load("my-plugin", plugin_bytes)?;
//!
//! // Call plugin command
//! let result = manager.execute_command("MY.COMMAND", args).await?;
//! ```

pub mod hooks;
pub mod hot_reload;
mod loader;
mod manifest;
pub mod marketplace;
mod registry;
mod sandbox;
pub mod sdk;

pub use hooks::{EventHook, HookContext, HookResult};
pub use hot_reload::{HotReloadConfig, HotReloader, ReloadEvent, ReloadEventType};
pub use loader::{PluginLoader, PluginSource};
pub use manifest::{
    CommandSpec, DataTypeSpec, Dependency, HookSpec, PluginCapability, PluginManifest,
    PluginPermission, Version,
};
pub use marketplace::{
    MarketplaceEntry, MarketplaceError, MarketplaceStats, PluginCategory, PluginMarketplace,
    SearchQuery, SortBy, VersionResolver,
};
pub use registry::{LoadedPlugin, PluginInfo, PluginRegistry, PluginState, PluginStats};
pub use sandbox::{PluginSandbox, SandboxConfig, SandboxLimits};
pub use sdk::{
    CommandContext as SdkCommandContext, PluginBuilder, PluginDefinition, PluginResponse,
};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Plugin system configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PluginConfig {
    /// Enable plugin system
    pub enabled: bool,
    /// Directory for plugin files
    pub plugin_dir: String,
    /// Auto-load plugins from directory
    pub auto_load: bool,
    /// Maximum number of plugins
    pub max_plugins: usize,
    /// Sandbox configuration
    pub sandbox: SandboxConfig,
    /// Hot reload support
    pub hot_reload: bool,
    /// Plugin timeout in milliseconds
    pub timeout_ms: u64,
}

impl Default for PluginConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            plugin_dir: "./plugins".to_string(),
            auto_load: true,
            max_plugins: 100,
            sandbox: SandboxConfig::default(),
            hot_reload: true,
            timeout_ms: 5000,
        }
    }
}

impl PluginConfig {
    /// Create a minimal configuration
    pub fn minimal() -> Self {
        Self {
            enabled: true,
            plugin_dir: "./plugins".to_string(),
            auto_load: false,
            max_plugins: 10,
            sandbox: SandboxConfig::restrictive(),
            hot_reload: false,
            timeout_ms: 1000,
        }
    }

    /// Create a development configuration with relaxed restrictions
    pub fn development() -> Self {
        Self {
            enabled: true,
            plugin_dir: "./plugins".to_string(),
            auto_load: true,
            max_plugins: 100,
            sandbox: SandboxConfig::permissive(),
            hot_reload: true,
            timeout_ms: 30000,
        }
    }
}

/// Main plugin manager
pub struct PluginManager {
    /// Configuration
    config: PluginConfig,
    /// Plugin registry
    registry: Arc<PluginRegistry>,
    /// Plugin loader
    loader: Arc<PluginLoader>,
    /// Event hooks
    hooks: Arc<RwLock<EventHookRegistry>>,
    /// Command registry (plugin commands)
    commands: Arc<RwLock<HashMap<String, CommandHandler>>>,
    /// Data type registry
    data_types: Arc<RwLock<HashMap<String, DataTypeHandler>>>,
}

impl PluginManager {
    /// Create a new plugin manager
    pub fn new(config: PluginConfig) -> Self {
        let registry = Arc::new(PluginRegistry::new(config.max_plugins));
        let loader = Arc::new(PluginLoader::new(config.sandbox.clone()));

        Self {
            config,
            registry,
            loader,
            hooks: Arc::new(RwLock::new(EventHookRegistry::new())),
            commands: Arc::new(RwLock::new(HashMap::new())),
            data_types: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Load a plugin from bytes
    pub async fn load(
        &self,
        name: &str,
        bytes: Vec<u8>,
        manifest: Option<PluginManifest>,
    ) -> Result<(), PluginError> {
        // Load via loader (validates and sandboxes)
        let plugin = self.loader.load(name, bytes, manifest)?;

        // Check capabilities and register handlers
        self.register_plugin_handlers(&plugin).await?;

        // Add to registry
        self.registry.register(plugin)?;

        Ok(())
    }

    /// Load a plugin from a file path
    pub async fn load_file(&self, path: &str) -> Result<String, PluginError> {
        let bytes = std::fs::read(path)
            .map_err(|e| PluginError::LoadError(format!("failed to read file: {}", e)))?;

        // Try to load manifest from adjacent .toml file
        let manifest_path = format!("{}.toml", path.trim_end_matches(".wasm"));
        let manifest = std::fs::read_to_string(&manifest_path)
            .ok()
            .and_then(|s| toml::from_str(&s).ok());

        // Derive name from file
        let name = std::path::Path::new(path)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string();

        self.load(&name, bytes, manifest).await?;
        Ok(name)
    }

    /// Load all plugins from the plugin directory
    pub async fn load_all(&self) -> Result<Vec<String>, PluginError> {
        if !self.config.auto_load {
            return Ok(vec![]);
        }

        let dir = std::path::Path::new(&self.config.plugin_dir);
        if !dir.exists() {
            return Ok(vec![]);
        }

        let mut loaded = vec![];
        let entries = std::fs::read_dir(dir)
            .map_err(|e| PluginError::LoadError(format!("failed to read plugin dir: {}", e)))?;

        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "wasm") {
                match self.load_file(path.to_str().unwrap_or_default()).await {
                    Ok(name) => loaded.push(name),
                    Err(e) => {
                        tracing::warn!("failed to load plugin {:?}: {}", path, e);
                    }
                }
            }
        }

        Ok(loaded)
    }

    /// Unload a plugin
    pub async fn unload(&self, name: &str) -> Result<(), PluginError> {
        // Get plugin before removing
        let plugin = self
            .registry
            .get(name)
            .ok_or_else(|| PluginError::NotFound(name.to_string()))?;

        // Unregister handlers
        self.unregister_plugin_handlers(&plugin).await?;

        // Remove from registry
        self.registry.unregister(name)?;

        Ok(())
    }

    /// Reload a plugin (hot reload)
    pub async fn reload(&self, name: &str, bytes: Vec<u8>) -> Result<(), PluginError> {
        if !self.config.hot_reload {
            return Err(PluginError::HotReloadDisabled);
        }

        // Get existing plugin
        let existing = self
            .registry
            .get(name)
            .ok_or_else(|| PluginError::NotFound(name.to_string()))?;

        // Unregister old handlers
        self.unregister_plugin_handlers(&existing).await?;

        // Load new version
        let manifest = existing.manifest.clone();
        let plugin = self.loader.load(name, bytes, Some(manifest))?;

        // Register new handlers
        self.register_plugin_handlers(&plugin).await?;

        // Update registry
        self.registry.update(name, plugin)?;

        Ok(())
    }

    /// Execute a plugin command
    pub async fn execute_command(
        &self,
        command: &str,
        args: Vec<String>,
    ) -> Result<CommandResult, PluginError> {
        let commands = self.commands.read().await;
        let handler = commands
            .get(command)
            .ok_or_else(|| PluginError::CommandNotFound(command.to_string()))?;

        let ctx = CommandContext {
            command: command.to_string(),
            args,
            db: 0,
            client_id: None,
        };

        // Execute with timeout
        let timeout = std::time::Duration::from_millis(self.config.timeout_ms);
        match tokio::time::timeout(timeout, handler.execute(ctx)).await {
            Ok(result) => result,
            Err(_) => Err(PluginError::Timeout(self.config.timeout_ms)),
        }
    }

    /// Trigger an event hook
    pub async fn trigger_hook(
        &self,
        event: EventType,
        context: HookContext,
    ) -> Result<HookResult, PluginError> {
        let hooks = self.hooks.read().await;
        hooks.trigger(event, context).await
    }

    /// Get plugin info
    pub fn get_plugin(&self, name: &str) -> Option<PluginInfo> {
        self.registry.info(name)
    }

    /// List all plugins
    pub fn list_plugins(&self) -> Vec<PluginInfo> {
        self.registry.list()
    }

    /// Get plugin statistics
    pub fn stats(&self) -> PluginManagerStats {
        let registry_stats = self.registry.stats();
        PluginManagerStats {
            total_plugins: registry_stats.total_plugins,
            active_plugins: registry_stats.active_plugins,
            failed_plugins: registry_stats.failed_plugins,
            total_commands: self.commands.try_read().map_or(0, |c| c.len()),
            total_hooks: self.hooks.try_read().map_or(0, |h| h.count()),
        }
    }

    /// Enable a plugin
    pub fn enable(&self, name: &str) -> Result<(), PluginError> {
        self.registry.set_state(name, PluginState::Active)
    }

    /// Disable a plugin
    pub fn disable(&self, name: &str) -> Result<(), PluginError> {
        self.registry.set_state(name, PluginState::Disabled)
    }

    /// Register plugin handlers
    async fn register_plugin_handlers(&self, plugin: &LoadedPlugin) -> Result<(), PluginError> {
        // Register commands
        for cmd in &plugin.manifest.commands {
            let handler = CommandHandler::from_plugin(plugin.name.clone(), cmd.clone());
            self.commands
                .write()
                .await
                .insert(cmd.name.to_uppercase(), handler);
        }

        // Register data types
        for dt in &plugin.manifest.data_types {
            let handler = DataTypeHandler::from_plugin(plugin.name.clone(), dt.clone());
            self.data_types
                .write()
                .await
                .insert(dt.name.to_uppercase(), handler);
        }

        // Register hooks
        for hook in &plugin.manifest.hooks {
            let event_hook = EventHook::from_plugin(plugin.name.clone(), hook.clone());
            self.hooks.write().await.register(event_hook)?;
        }

        Ok(())
    }

    /// Unregister plugin handlers
    async fn unregister_plugin_handlers(&self, plugin: &LoadedPlugin) -> Result<(), PluginError> {
        // Unregister commands
        let mut commands = self.commands.write().await;
        for cmd in &plugin.manifest.commands {
            commands.remove(&cmd.name.to_uppercase());
        }

        // Unregister data types
        let mut data_types = self.data_types.write().await;
        for dt in &plugin.manifest.data_types {
            data_types.remove(&dt.name.to_uppercase());
        }

        // Unregister hooks
        self.hooks.write().await.unregister_plugin(&plugin.name)?;

        Ok(())
    }
}

impl Default for PluginManager {
    fn default() -> Self {
        Self::new(PluginConfig::default())
    }
}

/// Command handler for plugin commands
pub struct CommandHandler {
    /// Plugin name
    plugin_name: String,
    /// Command specification
    spec: CommandSpec,
}

impl CommandHandler {
    /// Create from plugin
    fn from_plugin(plugin_name: String, spec: CommandSpec) -> Self {
        Self { plugin_name, spec }
    }

    /// Execute the command
    async fn execute(&self, ctx: CommandContext) -> Result<CommandResult, PluginError> {
        // Validate arity
        if let Some(min) = self.spec.min_args {
            if ctx.args.len() < min {
                return Err(PluginError::InvalidArguments(format!(
                    "expected at least {} arguments, got {}",
                    min,
                    ctx.args.len()
                )));
            }
        }
        if let Some(max) = self.spec.max_args {
            if ctx.args.len() > max {
                return Err(PluginError::InvalidArguments(format!(
                    "expected at most {} arguments, got {}",
                    max,
                    ctx.args.len()
                )));
            }
        }

        // Simulated execution (in real impl would call WASM)
        Ok(CommandResult {
            value: ResultValue::Ok,
            modified_keys: vec![],
        })
    }
}

/// Data type handler for plugin data types
pub struct DataTypeHandler {
    /// Plugin name
    plugin_name: String,
    /// Data type specification
    spec: DataTypeSpec,
}

impl DataTypeHandler {
    /// Create from plugin
    fn from_plugin(plugin_name: String, spec: DataTypeSpec) -> Self {
        Self { plugin_name, spec }
    }
}

/// Event hook registry
pub struct EventHookRegistry {
    /// Hooks by event type
    hooks: HashMap<EventType, Vec<EventHook>>,
}

impl EventHookRegistry {
    /// Create new registry
    fn new() -> Self {
        Self {
            hooks: HashMap::new(),
        }
    }

    /// Register a hook
    fn register(&mut self, hook: EventHook) -> Result<(), PluginError> {
        self.hooks.entry(hook.event.clone()).or_default().push(hook);
        Ok(())
    }

    /// Unregister all hooks for a plugin
    fn unregister_plugin(&mut self, plugin_name: &str) -> Result<(), PluginError> {
        for hooks in self.hooks.values_mut() {
            hooks.retain(|h| h.plugin_name != plugin_name);
        }
        Ok(())
    }

    /// Trigger hooks for an event
    async fn trigger(
        &self,
        event: EventType,
        context: HookContext,
    ) -> Result<HookResult, PluginError> {
        let hooks = match self.hooks.get(&event) {
            Some(h) => h,
            None => return Ok(HookResult::Continue),
        };

        for hook in hooks {
            match hook.execute(&context).await {
                Ok(HookResult::Continue) => continue,
                Ok(HookResult::Abort(reason)) => return Ok(HookResult::Abort(reason)),
                Ok(HookResult::Modified(data)) => return Ok(HookResult::Modified(data)),
                Err(e) => {
                    tracing::warn!("hook {} failed: {}", hook.name, e);
                    // Continue on hook failure
                }
            }
        }

        Ok(HookResult::Continue)
    }

    /// Count total hooks
    fn count(&self) -> usize {
        self.hooks.values().map(|v| v.len()).sum()
    }
}

/// Command execution context
#[derive(Clone, Debug)]
pub struct CommandContext {
    /// Command name
    pub command: String,
    /// Arguments
    pub args: Vec<String>,
    /// Database number
    pub db: u8,
    /// Client ID
    pub client_id: Option<u64>,
}

/// Command result
#[derive(Clone, Debug)]
pub struct CommandResult {
    /// Result value
    pub value: ResultValue,
    /// Keys modified
    pub modified_keys: Vec<String>,
}

/// Result value types
#[derive(Clone, Debug)]
pub enum ResultValue {
    /// OK response
    Ok,
    /// String value
    String(String),
    /// Integer value
    Integer(i64),
    /// Bulk string
    Bulk(Vec<u8>),
    /// Array of values
    Array(Vec<ResultValue>),
    /// Null
    Null,
    /// Error
    Error(String),
}

/// Event types for hooks
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EventType {
    /// Before command execution
    BeforeCommand,
    /// After command execution
    AfterCommand,
    /// On key write
    OnKeyWrite,
    /// On key read
    OnKeyRead,
    /// On key expiry
    OnKeyExpiry,
    /// On key delete
    OnKeyDelete,
    /// On client connect
    OnClientConnect,
    /// On client disconnect
    OnClientDisconnect,
    /// On server startup
    OnServerStartup,
    /// On server shutdown
    OnServerShutdown,
    /// Custom event
    Custom(String),
}

/// Plugin manager statistics
#[derive(Clone, Debug)]
pub struct PluginManagerStats {
    /// Total plugins loaded
    pub total_plugins: usize,
    /// Active plugins
    pub active_plugins: usize,
    /// Failed plugins
    pub failed_plugins: usize,
    /// Total registered commands
    pub total_commands: usize,
    /// Total registered hooks
    pub total_hooks: usize,
}

/// Plugin errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum PluginError {
    /// Plugin not found
    #[error("plugin not found: {0}")]
    NotFound(String),

    /// Plugin already exists
    #[error("plugin already exists: {0}")]
    AlreadyExists(String),

    /// Load error
    #[error("failed to load plugin: {0}")]
    LoadError(String),

    /// Invalid manifest
    #[error("invalid manifest: {0}")]
    InvalidManifest(String),

    /// Permission denied
    #[error("permission denied: {0}")]
    PermissionDenied(String),

    /// Command not found
    #[error("command not found: {0}")]
    CommandNotFound(String),

    /// Invalid arguments
    #[error("invalid arguments: {0}")]
    InvalidArguments(String),

    /// Execution error
    #[error("execution error: {0}")]
    ExecutionError(String),

    /// Timeout
    #[error("execution timed out after {0}ms")]
    Timeout(u64),

    /// Hot reload disabled
    #[error("hot reload is disabled")]
    HotReloadDisabled,

    /// Sandbox violation
    #[error("sandbox violation: {0}")]
    SandboxViolation(String),

    /// Dependency error
    #[error("dependency error: {0}")]
    DependencyError(String),

    /// Capacity exceeded
    #[error("maximum plugins ({0}) exceeded")]
    CapacityExceeded(usize),

    /// Invalid state
    #[error("invalid plugin state: {0}")]
    InvalidState(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plugin_config_default() {
        let config = PluginConfig::default();
        assert!(config.enabled);
        assert!(config.hot_reload);
        assert_eq!(config.max_plugins, 100);
    }

    #[test]
    fn test_plugin_config_minimal() {
        let config = PluginConfig::minimal();
        assert!(config.enabled);
        assert!(!config.hot_reload);
        assert_eq!(config.max_plugins, 10);
    }

    #[test]
    fn test_plugin_manager_new() {
        let manager = PluginManager::new(PluginConfig::default());
        assert_eq!(manager.stats().total_plugins, 0);
    }

    #[test]
    fn test_event_hook_registry() {
        let mut registry = EventHookRegistry::new();
        assert_eq!(registry.count(), 0);

        let hook = EventHook {
            name: "test_hook".to_string(),
            plugin_name: "test_plugin".to_string(),
            event: EventType::BeforeCommand,
            priority: 0,
        };

        registry.register(hook).unwrap();
        assert_eq!(registry.count(), 1);

        registry.unregister_plugin("test_plugin").unwrap();
        assert_eq!(registry.count(), 0);
    }

    #[test]
    fn test_event_type_equality() {
        assert_eq!(EventType::BeforeCommand, EventType::BeforeCommand);
        assert_ne!(EventType::BeforeCommand, EventType::AfterCommand);
        assert_eq!(
            EventType::Custom("test".to_string()),
            EventType::Custom("test".to_string())
        );
    }

    #[test]
    fn test_result_value() {
        let ok = ResultValue::Ok;
        let s = ResultValue::String("hello".to_string());
        let i = ResultValue::Integer(42);
        let n = ResultValue::Null;

        // Just verify they can be constructed
        match ok {
            ResultValue::Ok => {}
            _ => panic!("expected Ok"),
        }
        match s {
            ResultValue::String(v) => assert_eq!(v, "hello"),
            _ => panic!("expected String"),
        }
        match i {
            ResultValue::Integer(v) => assert_eq!(v, 42),
            _ => panic!("expected Integer"),
        }
        match n {
            ResultValue::Null => {}
            _ => panic!("expected Null"),
        }
    }
}
