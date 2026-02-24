//! Redis Module API Compatibility Layer
//!
//! Provides types and registries that mirror the Redis Module API so that
//! existing Redis module *concepts* (commands, custom data types, lifecycle
//! hooks) can be expressed within Ferrite's plugin system.
//!
//! # Overview
//!
//! This module does **not** load native Redis `.so` modules. Instead it
//! provides a Rust-native compatibility layer that mirrors the key Redis
//! Module API concepts:
//!
//! - [`RedisModuleCtx`](api::RedisModuleCtx) — context passed to command callbacks
//! - [`RedisModuleString`](api::RedisModuleString) — string type for the module API
//! - [`RedisModuleKey`](api::RedisModuleKey) — key handle for read/write operations
//! - [`ModuleCommand`](commands::ModuleCommand) — command registration & dispatch
//! - [`RedisModuleTypeDef`](types::RedisModuleTypeDef) — custom data type registration
//!
//! # Example
//!
//! ```rust,ignore
//! use ferrite_plugins::redis_module::{
//!     RedisModule, ModuleInfo,
//!     api::{RedisModuleCtx, RedisModuleString, Status},
//!     commands::{ModuleCommand, CommandFlag},
//! };
//!
//! fn my_set(ctx: &mut RedisModuleCtx, args: &[RedisModuleString]) -> Status {
//!     ctx.reply_with_simple_string("OK");
//!     Status::Ok
//! }
//!
//! let mut module = RedisModule::new(ModuleInfo {
//!     name: "mymodule".into(),
//!     version: 1,
//!     api_version: 1,
//! });
//!
//! module.register_command(
//!     ModuleCommand::new("MYMOD.SET", my_set, vec![CommandFlag::Write]),
//! );
//! ```

pub mod api;
pub mod commands;
pub mod types;

use std::fmt;
use std::sync::Arc;

use parking_lot::RwLock;

use api::Status;
use commands::{ModuleCommand, ModuleCommandRegistry};
use types::{ModuleTypeRegistry, RedisModuleTypeDef};

/// Metadata describing a loaded module.
#[derive(Debug, Clone)]
pub struct ModuleInfo {
    /// Human-readable module name
    pub name: String,
    /// Module version (integer, e.g. `1`)
    pub version: u32,
    /// Redis Module API version this module targets
    pub api_version: u32,
}

/// Lifecycle state of a module.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModuleState {
    /// Module has been registered but not yet initialised
    Registered,
    /// Module is fully loaded and active
    Loaded,
    /// Module is being unloaded
    Unloading,
    /// Module has been unloaded
    Unloaded,
    /// Module failed to load
    Failed,
}

impl fmt::Display for ModuleState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ModuleState::Registered => write!(f, "registered"),
            ModuleState::Loaded => write!(f, "loaded"),
            ModuleState::Unloading => write!(f, "unloading"),
            ModuleState::Unloaded => write!(f, "unloaded"),
            ModuleState::Failed => write!(f, "failed"),
        }
    }
}

/// A Redis-compatible module managed by the compatibility layer.
pub struct RedisModule {
    /// Module metadata
    info: ModuleInfo,
    /// Current lifecycle state
    state: ModuleState,
    /// Command registry
    commands: Arc<ModuleCommandRegistry>,
    /// Custom type registry
    types: Arc<ModuleTypeRegistry>,
}

impl RedisModule {
    /// Create a new module in the [`ModuleState::Registered`] state.
    pub fn new(info: ModuleInfo) -> Self {
        Self {
            info,
            state: ModuleState::Registered,
            commands: Arc::new(ModuleCommandRegistry::new()),
            types: Arc::new(ModuleTypeRegistry::new()),
        }
    }

    /// Module metadata.
    pub fn info(&self) -> &ModuleInfo {
        &self.info
    }

    /// Current lifecycle state.
    pub fn state(&self) -> ModuleState {
        self.state
    }

    /// Register a command with this module.
    pub fn register_command(&self, command: ModuleCommand) -> Status {
        self.commands.register(command)
    }

    /// Register a custom data type with this module.
    pub fn register_type(
        &self,
        type_def: RedisModuleTypeDef,
    ) -> Result<(), types::ModuleTypeError> {
        self.types.register(type_def)
    }

    /// Reference to the command registry.
    pub fn command_registry(&self) -> &ModuleCommandRegistry {
        &self.commands
    }

    /// Reference to the type registry.
    pub fn type_registry(&self) -> &ModuleTypeRegistry {
        &self.types
    }

    /// Transition to [`ModuleState::Loaded`].
    pub fn mark_loaded(&mut self) {
        self.state = ModuleState::Loaded;
    }

    /// Begin unloading the module.
    pub fn begin_unload(&mut self) {
        self.state = ModuleState::Unloading;
    }

    /// Finish unloading the module.
    pub fn finish_unload(&mut self) {
        self.state = ModuleState::Unloaded;
    }

    /// Mark the module as failed.
    pub fn mark_failed(&mut self) {
        self.state = ModuleState::Failed;
    }
}

impl fmt::Debug for RedisModule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisModule")
            .field("info", &self.info)
            .field("state", &self.state)
            .field("commands", &self.commands.len())
            .field("types", &self.types.len())
            .finish()
    }
}

/// Top-level manager for all loaded Redis-compatible modules.
pub struct RedisModuleManager {
    modules: RwLock<Vec<RedisModule>>,
}

impl RedisModuleManager {
    /// Create a new manager with no modules.
    pub fn new() -> Self {
        Self {
            modules: RwLock::new(Vec::new()),
        }
    }

    /// Load (register + mark loaded) a module.
    pub fn load(&self, info: ModuleInfo) -> Result<(), RedisModuleError> {
        let mut modules = self.modules.write();
        if modules.iter().any(|m| m.info.name == info.name) {
            return Err(RedisModuleError::AlreadyLoaded(info.name));
        }

        let mut module = RedisModule::new(info);
        module.mark_loaded();
        modules.push(module);
        Ok(())
    }

    /// Unload a module by name.
    pub fn unload(&self, name: &str) -> Result<(), RedisModuleError> {
        let mut modules = self.modules.write();
        let idx = modules
            .iter()
            .position(|m| m.info.name == name)
            .ok_or_else(|| RedisModuleError::NotFound(name.to_string()))?;

        modules[idx].begin_unload();
        modules[idx].finish_unload();
        modules.remove(idx);
        Ok(())
    }

    /// Get a snapshot of module info for all loaded modules.
    pub fn list(&self) -> Vec<ModuleInfo> {
        self.modules.read().iter().map(|m| m.info.clone()).collect()
    }

    /// Number of loaded modules.
    pub fn len(&self) -> usize {
        self.modules.read().len()
    }

    /// Whether there are no loaded modules.
    pub fn is_empty(&self) -> bool {
        self.modules.read().is_empty()
    }
}

impl Default for RedisModuleManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Errors from the module manager.
#[derive(Debug, Clone, thiserror::Error)]
pub enum RedisModuleError {
    /// Module already loaded
    #[error("module already loaded: {0}")]
    AlreadyLoaded(String),
    /// Module not found
    #[error("module not found: {0}")]
    NotFound(String),
    /// Module load failure
    #[error("module load failed: {0}")]
    LoadFailed(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use commands::CommandFlag;

    fn test_info(name: &str) -> ModuleInfo {
        ModuleInfo {
            name: name.to_string(),
            version: 1,
            api_version: api::REDISMODULE_API_VERSION,
        }
    }

    #[test]
    fn test_module_lifecycle() {
        let mut module = RedisModule::new(test_info("test"));
        assert_eq!(module.state(), ModuleState::Registered);

        module.mark_loaded();
        assert_eq!(module.state(), ModuleState::Loaded);

        module.begin_unload();
        assert_eq!(module.state(), ModuleState::Unloading);

        module.finish_unload();
        assert_eq!(module.state(), ModuleState::Unloaded);
    }

    #[test]
    fn test_module_register_command() {
        let module = RedisModule::new(test_info("mymod"));
        let cmd = ModuleCommand::new(
            "MYMOD.PING",
            |ctx, _| {
                ctx.reply_with_simple_string("PONG");
                Status::Ok
            },
            vec![CommandFlag::ReadOnly, CommandFlag::Fast],
        );
        assert_eq!(module.register_command(cmd), Status::Ok);
        assert_eq!(module.command_registry().len(), 1);
    }

    #[test]
    fn test_module_register_type() {
        let module = RedisModule::new(test_info("mymod"));
        let td = types::RedisModuleTypeDef::new("mytype", 1);
        assert!(module.register_type(td).is_ok());
        assert_eq!(module.type_registry().len(), 1);
    }

    #[test]
    fn test_manager_load_unload() {
        let mgr = RedisModuleManager::new();

        mgr.load(test_info("mod_a")).unwrap();
        mgr.load(test_info("mod_b")).unwrap();
        assert_eq!(mgr.len(), 2);

        // Duplicate load
        assert!(mgr.load(test_info("mod_a")).is_err());

        mgr.unload("mod_a").unwrap();
        assert_eq!(mgr.len(), 1);

        // Not found
        assert!(mgr.unload("mod_a").is_err());
    }

    #[test]
    fn test_manager_list() {
        let mgr = RedisModuleManager::new();
        mgr.load(test_info("alpha")).unwrap();
        mgr.load(test_info("beta")).unwrap();

        let mut names: Vec<_> = mgr.list().iter().map(|i| i.name.clone()).collect();
        names.sort();
        assert_eq!(names, vec!["alpha", "beta"]);
    }

    #[test]
    fn test_module_state_display() {
        assert_eq!(ModuleState::Registered.to_string(), "registered");
        assert_eq!(ModuleState::Loaded.to_string(), "loaded");
        assert_eq!(ModuleState::Failed.to_string(), "failed");
    }
}
