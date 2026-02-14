//! Plugin Extension SDK
//!
//! High-level builder API for creating Ferrite plugins. Plugin authors use
//! this SDK to define commands, data types, event hooks, and capabilities
//! in a type-safe, ergonomic way.
//!
//! # Example
//! ```rust,ignore
//! use ferrite::plugin::sdk::PluginBuilder;
//!
//! let plugin = PluginBuilder::new("my-plugin", "1.0.0")
//!     .description("A custom rate limiter")
//!     .author("dev@example.com")
//!     .command("RATELIMIT.CHECK", |ctx| {
//!         // Command implementation
//!         Ok(ctx.reply_ok())
//!     })
//!     .on_key_write(|ctx| {
//!         // React to writes
//!         Ok(())
//!     })
//!     .build();
//! ```

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::manifest::{
    CommandSpec, DataTypeSpec, Dependency, HookSpec, PluginCapability, PluginManifest,
    PluginPermission, Version, VersionReq,
};
use super::sandbox::SandboxConfig;
use super::EventType;

/// Context passed to plugin command handlers
#[derive(Debug, Clone)]
pub struct CommandContext {
    /// Command arguments
    pub args: Vec<String>,
    /// Database index
    pub db: u8,
    /// Client ID
    pub client_id: u64,
}

impl CommandContext {
    /// Get argument at index
    pub fn arg(&self, index: usize) -> Option<&str> {
        self.args.get(index).map(|s| s.as_str())
    }

    /// Get number of arguments
    pub fn arg_count(&self) -> usize {
        self.args.len()
    }
}

/// Response types a plugin command can return
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PluginResponse {
    /// Simple OK response
    Ok,
    /// String response
    String(String),
    /// Integer response
    Integer(i64),
    /// Bulk data response
    Bulk(Vec<u8>),
    /// Array response
    Array(Vec<PluginResponse>),
    /// Null response
    Null,
    /// Error response
    Error(String),
}

/// Type-erased command handler
pub type CommandHandler =
    Box<dyn Fn(&CommandContext) -> Result<PluginResponse, String> + Send + Sync>;

/// Type-erased event hook handler
pub type HookHandler = Box<dyn Fn(&SdkHookContext) -> Result<(), String> + Send + Sync>;

/// Context for event hooks
#[derive(Debug, Clone)]
pub struct SdkHookContext {
    /// Event type that triggered this hook
    pub event: String,
    /// Key involved (if applicable)
    pub key: Option<String>,
    /// Value involved (if applicable)
    pub value: Option<Vec<u8>>,
    /// Database index
    pub db: u8,
}

/// Builder for constructing plugins with a fluent API
pub struct PluginBuilder {
    manifest: PluginManifest,
    commands: HashMap<String, CommandHandler>,
    hooks: Vec<(EventType, HookHandler)>,
    sandbox_config: SandboxConfig,
}

impl PluginBuilder {
    /// Start building a new plugin
    pub fn new(name: &str, version: &str) -> Self {
        let v = parse_version(version);
        Self {
            manifest: PluginManifest::new(name, v),
            commands: HashMap::new(),
            hooks: Vec::new(),
            sandbox_config: SandboxConfig::default(),
        }
    }

    /// Set plugin description
    pub fn description(mut self, desc: &str) -> Self {
        self.manifest = self.manifest.with_description(desc);
        self
    }

    /// Set plugin author
    pub fn author(mut self, author: &str) -> Self {
        self.manifest = self.manifest.with_author(author);
        self
    }

    /// Request a permission
    pub fn permission(mut self, permission: PluginPermission) -> Self {
        self.manifest = self.manifest.with_permission(permission);
        self
    }

    /// Declare a capability
    pub fn capability(mut self, capability: PluginCapability) -> Self {
        self.manifest = self.manifest.with_capability(capability);
        self
    }

    /// Add a dependency on another plugin
    pub fn depends_on(mut self, name: &str, _version: &str) -> Self {
        self.manifest = self.manifest.with_dependency(Dependency {
            name: name.to_string(),
            version: VersionReq::Any,
            optional: false,
        });
        self
    }

    /// Register a new command
    pub fn command<F>(mut self, name: &str, handler: F) -> Self
    where
        F: Fn(&CommandContext) -> Result<PluginResponse, String> + Send + Sync + 'static,
    {
        let spec = CommandSpec {
            name: name.to_uppercase(),
            description: String::new(),
            min_args: None,
            max_args: None,
            args: Vec::new(),
            flags: Vec::new(),
            is_write: true,
            is_admin: false,
            category: None,
            complexity: None,
        };
        self.manifest = self.manifest.with_command(spec);
        self.commands.insert(name.to_uppercase(), Box::new(handler));
        self
    }

    /// Register a read-only command
    pub fn read_command<F>(mut self, name: &str, handler: F) -> Self
    where
        F: Fn(&CommandContext) -> Result<PluginResponse, String> + Send + Sync + 'static,
    {
        let spec = CommandSpec {
            name: name.to_uppercase(),
            description: String::new(),
            min_args: None,
            max_args: None,
            args: Vec::new(),
            flags: Vec::new(),
            is_write: false,
            is_admin: false,
            category: None,
            complexity: None,
        };
        self.manifest = self.manifest.with_command(spec);
        self.commands.insert(name.to_uppercase(), Box::new(handler));
        self
    }

    /// Register a data type
    pub fn data_type(mut self, name: &str) -> Self {
        self.manifest = self.manifest.with_data_type(DataTypeSpec {
            name: name.to_string(),
            description: String::new(),
            commands: Vec::new(),
            encoding: super::manifest::DataEncoding::Binary,
        });
        self
    }

    /// Register an event hook
    pub fn on_event<F>(mut self, event: EventType, handler: F) -> Self
    where
        F: Fn(&SdkHookContext) -> Result<(), String> + Send + Sync + 'static,
    {
        let spec = HookSpec {
            name: format!("{:?}", event),
            event: format!("{:?}", event),
            priority: 100,
            filter: None,
        };
        self.manifest = self.manifest.with_hook(spec);
        self.hooks.push((event, Box::new(handler)));
        self
    }

    /// Register a hook for key write events
    pub fn on_key_write<F>(self, handler: F) -> Self
    where
        F: Fn(&SdkHookContext) -> Result<(), String> + Send + Sync + 'static,
    {
        self.on_event(EventType::OnKeyWrite, handler)
    }

    /// Register a hook for key read events
    pub fn on_key_read<F>(self, handler: F) -> Self
    where
        F: Fn(&SdkHookContext) -> Result<(), String> + Send + Sync + 'static,
    {
        self.on_event(EventType::OnKeyRead, handler)
    }

    /// Register a hook for key expiry events
    pub fn on_key_expiry<F>(self, handler: F) -> Self
    where
        F: Fn(&SdkHookContext) -> Result<(), String> + Send + Sync + 'static,
    {
        self.on_event(EventType::OnKeyExpiry, handler)
    }

    /// Set sandbox configuration
    pub fn sandbox(mut self, config: SandboxConfig) -> Self {
        self.sandbox_config = config;
        self
    }

    /// Build the plugin definition
    pub fn build(self) -> PluginDefinition {
        PluginDefinition {
            manifest: self.manifest,
            commands: self.commands,
            hooks: self.hooks,
            sandbox_config: self.sandbox_config,
        }
    }
}

/// A fully defined plugin ready for registration
pub struct PluginDefinition {
    /// Plugin manifest with metadata
    pub manifest: PluginManifest,
    /// Command handlers
    pub commands: HashMap<String, CommandHandler>,
    /// Event hook handlers
    pub hooks: Vec<(EventType, HookHandler)>,
    /// Sandbox configuration
    pub sandbox_config: SandboxConfig,
}

impl PluginDefinition {
    /// Get the plugin name
    pub fn name(&self) -> &str {
        &self.manifest.name
    }

    /// Get the plugin version
    pub fn version(&self) -> &Version {
        &self.manifest.version
    }

    /// Get command names
    pub fn command_names(&self) -> Vec<String> {
        self.commands.keys().cloned().collect()
    }

    /// Execute a command
    pub fn execute_command(
        &self,
        name: &str,
        ctx: &CommandContext,
    ) -> Result<PluginResponse, String> {
        let handler = self
            .commands
            .get(&name.to_uppercase())
            .ok_or_else(|| format!("unknown command: {}", name))?;
        handler(ctx)
    }

    /// Validate the plugin definition
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        if let Err(e) = self.manifest.validate() {
            errors.push(format!("manifest error: {}", e));
        }

        if self.commands.is_empty() && self.hooks.is_empty() {
            errors.push("plugin must define at least one command or hook".to_string());
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

fn parse_version(s: &str) -> Version {
    let parts: Vec<u32> = s.split('.').filter_map(|p| p.parse().ok()).collect();
    Version::new(
        parts.first().copied().unwrap_or(0),
        parts.get(1).copied().unwrap_or(0),
        parts.get(2).copied().unwrap_or(0),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_simple_plugin() {
        let plugin = PluginBuilder::new("test-plugin", "1.0.0")
            .description("A test plugin")
            .author("test@example.com")
            .command("TEST.CMD", |_ctx| Ok(PluginResponse::Ok))
            .build();

        assert_eq!(plugin.name(), "test-plugin");
        assert_eq!(plugin.version().major, 1);
        assert!(plugin.command_names().contains(&"TEST.CMD".to_string()));
    }

    #[test]
    fn test_execute_command() {
        let plugin = PluginBuilder::new("echo-plugin", "1.0.0")
            .command("ECHO.UPPER", |ctx| {
                let arg = ctx.arg(0).unwrap_or("").to_uppercase();
                Ok(PluginResponse::String(arg))
            })
            .build();

        let ctx = CommandContext {
            args: vec!["hello".to_string()],
            db: 0,
            client_id: 1,
        };

        match plugin.execute_command("ECHO.UPPER", &ctx).unwrap() {
            PluginResponse::String(s) => assert_eq!(s, "HELLO"),
            other => panic!("unexpected response: {:?}", other),
        }
    }

    #[test]
    fn test_read_command() {
        let plugin = PluginBuilder::new("read-plugin", "0.1.0")
            .read_command("MY.GET", |_ctx| Ok(PluginResponse::Null))
            .build();

        assert!(plugin.command_names().contains(&"MY.GET".to_string()));
    }

    #[test]
    fn test_event_hooks() {
        let plugin = PluginBuilder::new("hook-plugin", "1.0.0")
            .on_key_write(|_ctx| Ok(()))
            .on_key_read(|_ctx| Ok(()))
            .build();

        assert_eq!(plugin.hooks.len(), 2);
    }

    #[test]
    fn test_plugin_validation_empty() {
        let plugin = PluginBuilder::new("empty-plugin", "1.0.0").build();
        assert!(plugin.validate().is_err());
    }

    #[test]
    fn test_plugin_validation_with_command() {
        let plugin = PluginBuilder::new("valid-plugin", "1.0.0")
            .command("MY.CMD", |_| Ok(PluginResponse::Ok))
            .build();
        assert!(plugin.validate().is_ok());
    }

    #[test]
    fn test_plugin_with_dependencies() {
        let plugin = PluginBuilder::new("dep-plugin", "1.0.0")
            .depends_on("base-plugin", ">=1.0.0")
            .command("DEP.CMD", |_| Ok(PluginResponse::Ok))
            .build();

        assert!(!plugin.manifest.dependencies.is_empty());
    }

    #[test]
    fn test_command_context() {
        let ctx = CommandContext {
            args: vec!["a".to_string(), "b".to_string()],
            db: 0,
            client_id: 42,
        };
        assert_eq!(ctx.arg(0), Some("a"));
        assert_eq!(ctx.arg(1), Some("b"));
        assert_eq!(ctx.arg(2), None);
        assert_eq!(ctx.arg_count(), 2);
    }

    #[test]
    fn test_parse_version() {
        let v = parse_version("1.2.3");
        assert_eq!(v.major, 1);
        assert_eq!(v.minor, 2);
        assert_eq!(v.patch, 3);
    }

    #[test]
    fn test_plugin_response_variants() {
        let _ok = PluginResponse::Ok;
        let _str = PluginResponse::String("test".to_string());
        let _int = PluginResponse::Integer(42);
        let _null = PluginResponse::Null;
        let _err = PluginResponse::Error("oops".to_string());
        let _arr = PluginResponse::Array(vec![PluginResponse::Ok]);
        let _bulk = PluginResponse::Bulk(vec![1, 2, 3]);
    }
}
