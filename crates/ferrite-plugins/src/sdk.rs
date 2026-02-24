//! # Plugin SDK
//!
//! Provides a stable, versioned API for plugin authors to build Ferrite plugins.
//! The SDK offers type-safe traits for command handlers, lifecycle hooks, and custom
//! data types, along with manifest validation and metadata generation.
//!
//! # Example
//!
//! ```rust,ignore
//! use ferrite_plugins::sdk::*;
//!
//! let manifest = PluginManifest {
//!     name: "my-plugin".into(),
//!     version: "1.0.0".into(),
//!     description: "A sample plugin".into(),
//!     author: "Author".into(),
//!     ..Default::default()
//! };
//!
//! let mut sdk = PluginSdk::new(manifest);
//! sdk.register_command("MYCMD", Box::new(MyCommandHandler));
//! sdk.register_hook(HookPoint::OnStartup, Box::new(MyHookHandler));
//!
//! let issues = sdk.validate();
//! let metadata = sdk.build_metadata();
//! ```

use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Current SDK version string.
pub const SDK_VERSION: &str = "0.1.0";

/// Current API version number. Incremented on breaking changes.
pub const API_VERSION: u32 = 1;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors produced by the Plugin SDK.
#[derive(Debug, Clone, thiserror::Error)]
pub enum PluginSdkError {
    /// The plugin manifest is invalid.
    #[error("invalid manifest: {0}")]
    ManifestInvalid(String),

    /// A version conflict was detected between dependencies.
    #[error("version conflict: {0}")]
    VersionConflict(String),

    /// The plugin lacks a required permission.
    #[error("permission denied: {0}")]
    PermissionDenied(String),

    /// A command with the same name is already registered.
    #[error("command already exists: {0}")]
    CommandExists(String),

    /// A runtime error occurred during plugin execution.
    #[error("runtime error: {0}")]
    RuntimeError(String),

    /// An internal SDK error.
    #[error("internal error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Manifest & related types
// ---------------------------------------------------------------------------

/// Declarative manifest describing a Ferrite plugin.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PluginManifest {
    /// Unique plugin name (kebab-case recommended).
    pub name: String,

    /// Semantic version string (e.g. `"1.2.3"`).
    pub version: String,

    /// Human-readable description.
    pub description: String,

    /// Author name or organisation.
    pub author: String,

    /// SPDX license identifier (e.g. `"MIT"`, `"Apache-2.0"`).
    pub license: String,

    /// Minimum compatible Ferrite version (inclusive).
    pub min_ferrite_version: Option<String>,

    /// Maximum compatible Ferrite version (exclusive).
    pub max_ferrite_version: Option<String>,

    /// Other plugins this plugin depends on.
    pub dependencies: Vec<PluginDependency>,

    /// Permissions the plugin requires to operate.
    pub permissions: Vec<Permission>,

    /// Module entry point (e.g. `"plugin.wasm"`, `"init.lua"`).
    pub entry_point: String,

    /// Runtime used to execute the plugin.
    pub runtime: RuntimeType,
}

impl Default for PluginManifest {
    fn default() -> Self {
        Self {
            name: String::new(),
            version: "0.1.0".into(),
            description: String::new(),
            author: String::new(),
            license: "MIT".into(),
            min_ferrite_version: None,
            max_ferrite_version: None,
            dependencies: Vec::new(),
            permissions: Vec::new(),
            entry_point: String::new(),
            runtime: RuntimeType::Native,
        }
    }
}

/// A dependency on another plugin.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PluginDependency {
    /// Name of the required plugin.
    pub name: String,

    /// Semver version requirement (e.g. `">=1.0.0"`).
    pub version_req: String,

    /// When `true` the dependency is not required at runtime.
    pub optional: bool,
}

/// Permissions a plugin may request.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Permission {
    /// Read key values from the store.
    ReadKeys,
    /// Write / mutate key values.
    WriteKeys,
    /// Subscribe to pub/sub channels.
    Subscribe,
    /// Publish messages to pub/sub channels.
    Publish,
    /// Make outbound network connections.
    Network,
    /// Access the host filesystem.
    FileSystem,
    /// Execute external processes.
    Exec,
}

/// Runtime environment used to execute a plugin.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RuntimeType {
    /// Compiled Rust / native code (loaded via dynamic linking).
    Native,
    /// WebAssembly module.
    Wasm,
    /// Lua scripting.
    Lua,
    /// JavaScript / V8 isolate.
    JavaScript,
}

impl Default for RuntimeType {
    fn default() -> Self {
        Self::Native
    }
}

// ---------------------------------------------------------------------------
// Plugin values
// ---------------------------------------------------------------------------

/// A dynamically-typed value exchanged between the host and plugins.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PluginValue {
    /// No value.
    Null,
    /// Boolean value.
    Bool(bool),
    /// 64-bit signed integer.
    Int(i64),
    /// 64-bit floating-point number.
    Float(f64),
    /// UTF-8 string.
    String(String),
    /// Raw byte buffer.
    Bytes(Vec<u8>),
    /// Ordered list of values.
    Array(Vec<PluginValue>),
    /// Ordered list of key-value pairs.
    Map(Vec<(String, PluginValue)>),
}

// ---------------------------------------------------------------------------
// Command handler
// ---------------------------------------------------------------------------

/// Trait implemented by plugins to handle custom commands.
#[async_trait]
pub trait CommandHandler: Send + Sync {
    /// Execute the command with the given arguments.
    async fn execute(&self, args: &[PluginValue]) -> Result<PluginValue, PluginSdkError>;

    /// Return the canonical command name (uppercase by convention).
    fn name(&self) -> &str;

    /// Return the command arity.
    ///
    /// Positive values indicate a fixed number of arguments.
    /// Negative values indicate a minimum (`-N` means at least `N - 1`).
    fn arity(&self) -> i32;

    /// Return command flags (e.g. `"readonly"`, `"fast"`).
    fn flags(&self) -> &[&str] {
        &[]
    }
}

// ---------------------------------------------------------------------------
// Hooks
// ---------------------------------------------------------------------------

/// Points in the Ferrite lifecycle where plugins can attach behaviour.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HookPoint {
    /// Invoked before a command is executed.
    BeforeCommand,
    /// Invoked after a command completes.
    AfterCommand,
    /// Invoked when a key expires via TTL.
    OnKeyExpire,
    /// Invoked when a key is evicted by the memory policy.
    OnKeyEvict,
    /// Invoked when a new client connects.
    OnClientConnect,
    /// Invoked when a client disconnects.
    OnClientDisconnect,
    /// Invoked during replication synchronisation.
    OnReplicationSync,
    /// Invoked once during server startup.
    OnStartup,
    /// Invoked during graceful shutdown.
    OnShutdown,
}

/// Context passed to hook handlers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HookContext {
    /// The hook point being triggered.
    pub hook: HookPoint,

    /// Timestamp when the hook was triggered.
    pub timestamp: DateTime<Utc>,

    /// Client identifier, if applicable.
    pub client_id: Option<String>,

    /// Command name that triggered the hook, if applicable.
    pub command: Option<String>,

    /// Key involved in the event, if applicable.
    pub key: Option<String>,

    /// Arbitrary metadata supplied by the runtime.
    pub metadata: HashMap<String, String>,
}

/// Result returned by a hook handler to control execution flow.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum HookResult {
    /// Allow normal processing to continue.
    Continue,
    /// Abort the current operation with a reason.
    Abort(String),
    /// Replace the result with the supplied value.
    Modified(PluginValue),
}

/// Trait implemented by plugins to handle lifecycle hooks.
pub trait HookHandler: Send + Sync {
    /// Process the hook event and return a result.
    fn handle(&self, ctx: &HookContext) -> HookResult;
}

// ---------------------------------------------------------------------------
// Data type definitions
// ---------------------------------------------------------------------------

/// Definition of a custom data type exposed by a plugin.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataTypeDef {
    /// Unique type name.
    pub name: String,

    /// Version of this type definition.
    pub version: String,

    /// Name of the serialisation function in the plugin module.
    pub serialize_fn: String,

    /// Name of the deserialisation function in the plugin module.
    pub deserialize_fn: String,

    /// RDB type identifier (must be unique across loaded plugins).
    pub rdb_type_id: u8,
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// Severity of a validation issue found in a plugin.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IssueSeverity {
    /// A blocking issue — the plugin cannot be loaded.
    Error,
    /// A non-blocking concern that should be addressed.
    Warning,
    /// An informational note.
    Info,
}

/// A single validation issue reported during manifest / SDK checks.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValidationIssue {
    /// How severe the issue is.
    pub severity: IssueSeverity,

    /// Human-readable description of the issue.
    pub message: String,

    /// The component that triggered the issue (e.g. `"manifest"`, `"command:GET"`).
    pub component: String,
}

// ---------------------------------------------------------------------------
// Metadata
// ---------------------------------------------------------------------------

/// Metadata generated from the manifest and registered components.
///
/// This is the machine-readable summary exchanged with the Ferrite runtime at
/// load time.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PluginMetadata {
    /// Unique identifier for this metadata snapshot.
    pub id: String,

    /// Plugin name (copied from manifest).
    pub name: String,

    /// Plugin version (copied from manifest).
    pub version: String,

    /// Names of registered commands.
    pub commands: Vec<String>,

    /// Names of registered custom data types.
    pub data_types: Vec<String>,

    /// Hook points with at least one registered handler.
    pub hooks: Vec<HookPoint>,

    /// Total number of registered components (commands + data types + hooks).
    pub total_components: usize,

    /// SDK API version the plugin was built against.
    pub api_version: u32,
}

// ---------------------------------------------------------------------------
// PluginSdk — main entry point
// ---------------------------------------------------------------------------

/// Main entry point for building a Ferrite plugin.
///
/// Plugin authors create an instance of `PluginSdk`, register their commands,
/// data types, and hooks, then call [`validate`](PluginSdk::validate) and
/// [`build_metadata`](PluginSdk::build_metadata) before handing the result to
/// the Ferrite runtime.
pub struct PluginSdk {
    manifest: PluginManifest,
    commands: HashMap<String, Box<dyn CommandHandler>>,
    data_types: HashMap<String, DataTypeDef>,
    hooks: HashMap<HookPoint, Vec<Box<dyn HookHandler>>>,
}

impl PluginSdk {
    /// Create a new SDK instance from a [`PluginManifest`].
    pub fn new(manifest: PluginManifest) -> Self {
        Self {
            manifest,
            commands: HashMap::new(),
            data_types: HashMap::new(),
            hooks: HashMap::new(),
        }
    }

    /// Register a custom command handler.
    ///
    /// Returns an error if a command with the same name is already registered.
    pub fn register_command(
        &mut self,
        name: &str,
        handler: Box<dyn CommandHandler>,
    ) -> Result<(), PluginSdkError> {
        if self.commands.contains_key(name) {
            return Err(PluginSdkError::CommandExists(name.to_string()));
        }
        self.commands.insert(name.to_string(), handler);
        Ok(())
    }

    /// Register a custom data type definition.
    pub fn register_data_type(&mut self, name: &str, type_def: DataTypeDef) {
        self.data_types.insert(name.to_string(), type_def);
    }

    /// Register a hook handler for the given [`HookPoint`].
    ///
    /// Multiple handlers can be registered for the same hook point.
    pub fn register_hook(&mut self, hook: HookPoint, handler: Box<dyn HookHandler>) {
        self.hooks.entry(hook).or_default().push(handler);
    }

    /// Validate the manifest and registered components.
    ///
    /// Returns a list of [`ValidationIssue`]s. An empty list indicates the
    /// plugin is ready to load.
    pub fn validate(&self) -> Vec<ValidationIssue> {
        let mut issues = Vec::new();

        // -- Manifest checks ------------------------------------------------
        if self.manifest.name.is_empty() {
            issues.push(ValidationIssue {
                severity: IssueSeverity::Error,
                message: "plugin name must not be empty".into(),
                component: "manifest".into(),
            });
        }

        if self.manifest.version.is_empty() {
            issues.push(ValidationIssue {
                severity: IssueSeverity::Error,
                message: "plugin version must not be empty".into(),
                component: "manifest".into(),
            });
        }

        if self.manifest.description.is_empty() {
            issues.push(ValidationIssue {
                severity: IssueSeverity::Warning,
                message: "plugin description is empty".into(),
                component: "manifest".into(),
            });
        }

        if self.manifest.author.is_empty() {
            issues.push(ValidationIssue {
                severity: IssueSeverity::Warning,
                message: "plugin author is empty".into(),
                component: "manifest".into(),
            });
        }

        if self.manifest.entry_point.is_empty() && self.manifest.runtime != RuntimeType::Native {
            issues.push(ValidationIssue {
                severity: IssueSeverity::Error,
                message: "entry_point is required for non-native runtimes".into(),
                component: "manifest".into(),
            });
        }

        // -- Dependency checks ----------------------------------------------
        for dep in &self.manifest.dependencies {
            if dep.name.is_empty() {
                issues.push(ValidationIssue {
                    severity: IssueSeverity::Error,
                    message: "dependency name must not be empty".into(),
                    component: "dependency".into(),
                });
            }
            if dep.version_req.is_empty() {
                issues.push(ValidationIssue {
                    severity: IssueSeverity::Warning,
                    message: format!("dependency '{}' has no version requirement", dep.name),
                    component: format!("dependency:{}", dep.name),
                });
            }
        }

        // -- Permission checks ----------------------------------------------
        if self.manifest.permissions.is_empty() && !self.commands.is_empty() {
            issues.push(ValidationIssue {
                severity: IssueSeverity::Info,
                message: "plugin registers commands but declares no permissions".into(),
                component: "permissions".into(),
            });
        }

        // -- Command checks -------------------------------------------------
        for (name, handler) in &self.commands {
            if handler.name() != name {
                issues.push(ValidationIssue {
                    severity: IssueSeverity::Warning,
                    message: format!(
                        "command registered as '{}' but handler reports name '{}'",
                        name,
                        handler.name()
                    ),
                    component: format!("command:{}", name),
                });
            }
        }

        // -- Data type checks -----------------------------------------------
        for (name, type_def) in &self.data_types {
            if type_def.serialize_fn.is_empty() || type_def.deserialize_fn.is_empty() {
                issues.push(ValidationIssue {
                    severity: IssueSeverity::Error,
                    message: format!(
                        "data type '{}' must specify both serialize and deserialize functions",
                        name
                    ),
                    component: format!("data_type:{}", name),
                });
            }
        }

        issues
    }

    /// Build a [`PluginMetadata`] snapshot from the current SDK state.
    pub fn build_metadata(&self) -> PluginMetadata {
        let commands: Vec<String> = self.commands.keys().cloned().collect();
        let data_types: Vec<String> = self.data_types.keys().cloned().collect();
        let hooks: Vec<HookPoint> = self.hooks.keys().cloned().collect();
        let total_components = commands.len() + data_types.len() + hooks.len();

        PluginMetadata {
            id: Uuid::new_v4().to_string(),
            name: self.manifest.name.clone(),
            version: self.manifest.version.clone(),
            commands,
            data_types,
            hooks,
            total_components,
            api_version: API_VERSION,
        }
    }

    /// Return a reference to the manifest.
    pub fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Helpers ------------------------------------------------------------

    fn sample_manifest() -> PluginManifest {
        PluginManifest {
            name: "test-plugin".into(),
            version: "1.0.0".into(),
            description: "A test plugin".into(),
            author: "Test Author".into(),
            license: "MIT".into(),
            min_ferrite_version: Some("0.5.0".into()),
            max_ferrite_version: None,
            dependencies: vec![],
            permissions: vec![Permission::ReadKeys, Permission::WriteKeys],
            entry_point: "plugin.wasm".into(),
            runtime: RuntimeType::Wasm,
        }
    }

    struct EchoCommand;

    #[async_trait]
    impl CommandHandler for EchoCommand {
        async fn execute(&self, args: &[PluginValue]) -> Result<PluginValue, PluginSdkError> {
            Ok(PluginValue::Array(args.to_vec()))
        }
        fn name(&self) -> &str {
            "ECHO"
        }
        fn arity(&self) -> i32 {
            -1
        }
        fn flags(&self) -> &[&str] {
            &["readonly", "fast"]
        }
    }

    struct StartupHook;

    impl HookHandler for StartupHook {
        fn handle(&self, _ctx: &HookContext) -> HookResult {
            HookResult::Continue
        }
    }

    struct AbortHook;

    impl HookHandler for AbortHook {
        fn handle(&self, _ctx: &HookContext) -> HookResult {
            HookResult::Abort("blocked by policy".into())
        }
    }

    fn sample_data_type() -> DataTypeDef {
        DataTypeDef {
            name: "BitMap".into(),
            version: "1.0.0".into(),
            serialize_fn: "bitmap_serialize".into(),
            deserialize_fn: "bitmap_deserialize".into(),
            rdb_type_id: 42,
        }
    }

    fn hook_context(hook: HookPoint) -> HookContext {
        HookContext {
            hook,
            timestamp: Utc::now(),
            client_id: None,
            command: None,
            key: None,
            metadata: HashMap::new(),
        }
    }

    // -- Constants ----------------------------------------------------------

    #[test]
    fn test_sdk_version_constants() {
        assert_eq!(SDK_VERSION, "0.1.0");
        assert_eq!(API_VERSION, 1);
    }

    // -- Manifest -----------------------------------------------------------

    #[test]
    fn test_manifest_creation() {
        let m = sample_manifest();
        assert_eq!(m.name, "test-plugin");
        assert_eq!(m.version, "1.0.0");
        assert_eq!(m.runtime, RuntimeType::Wasm);
        assert_eq!(m.permissions.len(), 2);
    }

    #[test]
    fn test_manifest_default() {
        let m = PluginManifest::default();
        assert!(m.name.is_empty());
        assert_eq!(m.version, "0.1.0");
        assert_eq!(m.runtime, RuntimeType::Native);
    }

    #[test]
    fn test_manifest_serialization() {
        let m = sample_manifest();
        let json = serde_json::to_string(&m).expect("serialize");
        let deserialized: PluginManifest = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(deserialized.name, m.name);
        assert_eq!(deserialized.version, m.version);
    }

    // -- Validation ---------------------------------------------------------

    #[test]
    fn test_validate_valid_manifest() {
        let sdk = PluginSdk::new(sample_manifest());
        let issues = sdk.validate();
        let errors: Vec<_> = issues
            .iter()
            .filter(|i| i.severity == IssueSeverity::Error)
            .collect();
        assert!(errors.is_empty(), "expected no errors: {:?}", errors);
    }

    #[test]
    fn test_validate_empty_name() {
        let mut manifest = sample_manifest();
        manifest.name = String::new();
        let sdk = PluginSdk::new(manifest);
        let issues = sdk.validate();
        assert!(issues
            .iter()
            .any(|i| i.severity == IssueSeverity::Error && i.message.contains("name")));
    }

    #[test]
    fn test_validate_empty_version() {
        let mut manifest = sample_manifest();
        manifest.version = String::new();
        let sdk = PluginSdk::new(manifest);
        let issues = sdk.validate();
        assert!(issues
            .iter()
            .any(|i| i.severity == IssueSeverity::Error && i.message.contains("version")));
    }

    #[test]
    fn test_validate_missing_entry_point_non_native() {
        let mut manifest = sample_manifest();
        manifest.entry_point = String::new();
        manifest.runtime = RuntimeType::Wasm;
        let sdk = PluginSdk::new(manifest);
        let issues = sdk.validate();
        assert!(issues
            .iter()
            .any(|i| i.severity == IssueSeverity::Error && i.message.contains("entry_point")));
    }

    #[test]
    fn test_validate_missing_entry_point_native_ok() {
        let mut manifest = sample_manifest();
        manifest.entry_point = String::new();
        manifest.runtime = RuntimeType::Native;
        let sdk = PluginSdk::new(manifest);
        let issues = sdk.validate();
        assert!(
            !issues
                .iter()
                .any(|i| i.severity == IssueSeverity::Error && i.message.contains("entry_point")),
            "native runtime should not require entry_point"
        );
    }

    #[test]
    fn test_validate_empty_description_warning() {
        let mut manifest = sample_manifest();
        manifest.description = String::new();
        let sdk = PluginSdk::new(manifest);
        let issues = sdk.validate();
        assert!(issues
            .iter()
            .any(|i| i.severity == IssueSeverity::Warning && i.message.contains("description")));
    }

    // -- Dependency validation ----------------------------------------------

    #[test]
    fn test_validate_dependency_empty_name() {
        let mut manifest = sample_manifest();
        manifest.dependencies.push(PluginDependency {
            name: String::new(),
            version_req: ">=1.0".into(),
            optional: false,
        });
        let sdk = PluginSdk::new(manifest);
        let issues = sdk.validate();
        assert!(issues
            .iter()
            .any(|i| i.severity == IssueSeverity::Error && i.component == "dependency"));
    }

    #[test]
    fn test_validate_dependency_empty_version_warning() {
        let mut manifest = sample_manifest();
        manifest.dependencies.push(PluginDependency {
            name: "other-plugin".into(),
            version_req: String::new(),
            optional: false,
        });
        let sdk = PluginSdk::new(manifest);
        let issues = sdk.validate();
        assert!(issues
            .iter()
            .any(|i| i.severity == IssueSeverity::Warning
                && i.component == "dependency:other-plugin"));
    }

    // -- Command registration -----------------------------------------------

    #[test]
    fn test_register_command() {
        let mut sdk = PluginSdk::new(sample_manifest());
        let result = sdk.register_command("ECHO", Box::new(EchoCommand));
        assert!(result.is_ok());
    }

    #[test]
    fn test_register_duplicate_command() {
        let mut sdk = PluginSdk::new(sample_manifest());
        sdk.register_command("ECHO", Box::new(EchoCommand)).unwrap();
        let result = sdk.register_command("ECHO", Box::new(EchoCommand));
        assert!(matches!(result, Err(PluginSdkError::CommandExists(_))));
    }

    #[tokio::test]
    async fn test_command_execute() {
        let handler = EchoCommand;
        let args = vec![PluginValue::String("hello".into()), PluginValue::Int(42)];
        let result = handler.execute(&args).await.unwrap();
        assert_eq!(result, PluginValue::Array(args));
    }

    #[test]
    fn test_command_handler_properties() {
        let handler = EchoCommand;
        assert_eq!(handler.name(), "ECHO");
        assert_eq!(handler.arity(), -1);
        assert_eq!(handler.flags(), &["readonly", "fast"]);
    }

    // -- Hook registration --------------------------------------------------

    #[test]
    fn test_register_hook() {
        let mut sdk = PluginSdk::new(sample_manifest());
        sdk.register_hook(HookPoint::OnStartup, Box::new(StartupHook));
        let metadata = sdk.build_metadata();
        assert!(metadata.hooks.contains(&HookPoint::OnStartup));
    }

    #[test]
    fn test_register_multiple_hooks_same_point() {
        let mut sdk = PluginSdk::new(sample_manifest());
        sdk.register_hook(HookPoint::BeforeCommand, Box::new(StartupHook));
        sdk.register_hook(HookPoint::BeforeCommand, Box::new(AbortHook));
        // Both registered; only one hook point in metadata.
        let metadata = sdk.build_metadata();
        assert_eq!(
            metadata
                .hooks
                .iter()
                .filter(|h| **h == HookPoint::BeforeCommand)
                .count(),
            1
        );
    }

    #[test]
    fn test_hook_handler_continue() {
        let handler = StartupHook;
        let ctx = hook_context(HookPoint::OnStartup);
        assert_eq!(handler.handle(&ctx), HookResult::Continue);
    }

    #[test]
    fn test_hook_handler_abort() {
        let handler = AbortHook;
        let ctx = hook_context(HookPoint::BeforeCommand);
        match handler.handle(&ctx) {
            HookResult::Abort(reason) => assert_eq!(reason, "blocked by policy"),
            other => panic!("expected Abort, got {:?}", other),
        }
    }

    // -- Data type registration ---------------------------------------------

    #[test]
    fn test_register_data_type() {
        let mut sdk = PluginSdk::new(sample_manifest());
        sdk.register_data_type("BitMap", sample_data_type());
        let metadata = sdk.build_metadata();
        assert!(metadata.data_types.contains(&"BitMap".to_string()));
    }

    #[test]
    fn test_validate_data_type_missing_fns() {
        let mut sdk = PluginSdk::new(sample_manifest());
        sdk.register_data_type(
            "BadType",
            DataTypeDef {
                name: "BadType".into(),
                version: "1.0.0".into(),
                serialize_fn: String::new(),
                deserialize_fn: String::new(),
                rdb_type_id: 99,
            },
        );
        let issues = sdk.validate();
        assert!(issues
            .iter()
            .any(|i| i.severity == IssueSeverity::Error && i.component == "data_type:BadType"));
    }

    // -- Metadata generation ------------------------------------------------

    #[test]
    fn test_build_metadata_empty() {
        let sdk = PluginSdk::new(sample_manifest());
        let meta = sdk.build_metadata();
        assert_eq!(meta.name, "test-plugin");
        assert_eq!(meta.version, "1.0.0");
        assert_eq!(meta.api_version, API_VERSION);
        assert!(meta.commands.is_empty());
        assert!(meta.data_types.is_empty());
        assert!(meta.hooks.is_empty());
        assert_eq!(meta.total_components, 0);
    }

    #[test]
    fn test_build_metadata_with_components() {
        let mut sdk = PluginSdk::new(sample_manifest());
        sdk.register_command("ECHO", Box::new(EchoCommand)).unwrap();
        sdk.register_data_type("BitMap", sample_data_type());
        sdk.register_hook(HookPoint::OnStartup, Box::new(StartupHook));
        sdk.register_hook(HookPoint::OnShutdown, Box::new(StartupHook));

        let meta = sdk.build_metadata();
        assert!(meta.commands.contains(&"ECHO".to_string()));
        assert!(meta.data_types.contains(&"BitMap".to_string()));
        assert!(meta.hooks.contains(&HookPoint::OnStartup));
        assert!(meta.hooks.contains(&HookPoint::OnShutdown));
        assert_eq!(meta.total_components, 4); // 1 cmd + 1 dt + 2 hooks
        assert!(!meta.id.is_empty());
    }

    // -- Permission checks --------------------------------------------------

    #[test]
    fn test_permissions_no_permissions_with_commands_info() {
        let mut manifest = sample_manifest();
        manifest.permissions.clear();
        let mut sdk = PluginSdk::new(manifest);
        sdk.register_command("ECHO", Box::new(EchoCommand)).unwrap();
        let issues = sdk.validate();
        assert!(issues
            .iter()
            .any(|i| i.severity == IssueSeverity::Info && i.message.contains("permissions")));
    }

    #[test]
    fn test_permission_equality() {
        assert_eq!(Permission::ReadKeys, Permission::ReadKeys);
        assert_ne!(Permission::ReadKeys, Permission::WriteKeys);
    }

    #[test]
    fn test_permission_serialization() {
        let perm = Permission::Network;
        let json = serde_json::to_string(&perm).unwrap();
        let deserialized: Permission = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, perm);
    }

    // -- Error handling -----------------------------------------------------

    #[test]
    fn test_error_display() {
        let err = PluginSdkError::ManifestInvalid("missing name".into());
        assert_eq!(err.to_string(), "invalid manifest: missing name");

        let err = PluginSdkError::CommandExists("GET".into());
        assert_eq!(err.to_string(), "command already exists: GET");
    }

    #[test]
    fn test_error_variants() {
        let errors: Vec<PluginSdkError> = vec![
            PluginSdkError::ManifestInvalid("bad".into()),
            PluginSdkError::VersionConflict("mismatch".into()),
            PluginSdkError::PermissionDenied("no access".into()),
            PluginSdkError::CommandExists("PING".into()),
            PluginSdkError::RuntimeError("crash".into()),
            PluginSdkError::Internal("oops".into()),
        ];
        for err in &errors {
            assert!(!err.to_string().is_empty());
        }
        assert_eq!(errors.len(), 6);
    }

    // -- PluginValue --------------------------------------------------------

    #[test]
    fn test_plugin_value_variants() {
        let values = vec![
            PluginValue::Null,
            PluginValue::Bool(true),
            PluginValue::Int(42),
            PluginValue::Float(3.14),
            PluginValue::String("hello".into()),
            PluginValue::Bytes(vec![0xDE, 0xAD]),
            PluginValue::Array(vec![PluginValue::Int(1)]),
            PluginValue::Map(vec![("key".into(), PluginValue::Bool(false))]),
        ];
        for v in &values {
            let json = serde_json::to_string(v).unwrap();
            let back: PluginValue = serde_json::from_str(&json).unwrap();
            assert_eq!(&back, v);
        }
    }

    // -- HookPoint completeness ---------------------------------------------

    #[test]
    fn test_hook_point_variants() {
        let hooks = vec![
            HookPoint::BeforeCommand,
            HookPoint::AfterCommand,
            HookPoint::OnKeyExpire,
            HookPoint::OnKeyEvict,
            HookPoint::OnClientConnect,
            HookPoint::OnClientDisconnect,
            HookPoint::OnReplicationSync,
            HookPoint::OnStartup,
            HookPoint::OnShutdown,
        ];
        assert_eq!(hooks.len(), 9);
        // All distinct
        let mut set = std::collections::HashSet::new();
        for h in &hooks {
            assert!(set.insert(h));
        }
    }

    // -- RuntimeType --------------------------------------------------------

    #[test]
    fn test_runtime_type_variants() {
        let types = vec![
            RuntimeType::Native,
            RuntimeType::Wasm,
            RuntimeType::Lua,
            RuntimeType::JavaScript,
        ];
        for t in &types {
            let json = serde_json::to_string(t).unwrap();
            let back: RuntimeType = serde_json::from_str(&json).unwrap();
            assert_eq!(&back, t);
        }
    }
}
