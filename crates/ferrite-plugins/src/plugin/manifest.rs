//! Plugin Manifest
//!
//! Defines the plugin metadata and capabilities.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Plugin manifest containing metadata and capabilities
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct PluginManifest {
    /// Plugin name
    pub name: String,
    /// Plugin version
    pub version: Version,
    /// Plugin description
    #[serde(default)]
    pub description: String,
    /// Plugin author
    pub author: Option<String>,
    /// Plugin license
    pub license: Option<String>,
    /// Plugin homepage
    pub homepage: Option<String>,
    /// Plugin repository
    pub repository: Option<String>,
    /// Minimum Ferrite version required
    pub min_ferrite_version: Option<Version>,
    /// Plugin dependencies
    #[serde(default)]
    pub dependencies: Vec<Dependency>,
    /// Required permissions
    #[serde(default)]
    pub permissions: Vec<PluginPermission>,
    /// Plugin capabilities
    #[serde(default)]
    pub capabilities: Vec<PluginCapability>,
    /// Custom commands provided
    #[serde(default)]
    pub commands: Vec<CommandSpec>,
    /// Custom data types provided
    #[serde(default)]
    pub data_types: Vec<DataTypeSpec>,
    /// Event hooks
    #[serde(default)]
    pub hooks: Vec<HookSpec>,
    /// Configuration schema
    pub config_schema: Option<ConfigSchema>,
    /// Additional metadata
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

impl PluginManifest {
    /// Create a new manifest with required fields
    pub fn new(name: &str, version: Version) -> Self {
        Self {
            name: name.to_string(),
            version,
            description: String::new(),
            ..Default::default()
        }
    }

    /// Builder method: set description
    pub fn with_description(mut self, description: &str) -> Self {
        self.description = description.to_string();
        self
    }

    /// Builder method: set author
    pub fn with_author(mut self, author: &str) -> Self {
        self.author = Some(author.to_string());
        self
    }

    /// Builder method: add permission
    pub fn with_permission(mut self, permission: PluginPermission) -> Self {
        self.permissions.push(permission);
        self
    }

    /// Builder method: add capability
    pub fn with_capability(mut self, capability: PluginCapability) -> Self {
        self.capabilities.push(capability);
        self
    }

    /// Builder method: add command
    pub fn with_command(mut self, command: CommandSpec) -> Self {
        self.commands.push(command);
        self
    }

    /// Builder method: add data type
    pub fn with_data_type(mut self, data_type: DataTypeSpec) -> Self {
        self.data_types.push(data_type);
        self
    }

    /// Builder method: add hook
    pub fn with_hook(mut self, hook: HookSpec) -> Self {
        self.hooks.push(hook);
        self
    }

    /// Builder method: add dependency
    pub fn with_dependency(mut self, dependency: Dependency) -> Self {
        self.dependencies.push(dependency);
        self
    }

    /// Validate the manifest
    pub fn validate(&self) -> Result<(), ManifestError> {
        // Name must not be empty
        if self.name.is_empty() {
            return Err(ManifestError::InvalidName(
                "name cannot be empty".to_string(),
            ));
        }

        // Name must be valid identifier
        if !is_valid_identifier(&self.name) {
            return Err(ManifestError::InvalidName(format!(
                "invalid plugin name: {}",
                self.name
            )));
        }

        // Check command names
        for cmd in &self.commands {
            if !is_valid_command_name(&cmd.name) {
                return Err(ManifestError::InvalidCommand(format!(
                    "invalid command name: {}",
                    cmd.name
                )));
            }
        }

        // Check data type names
        for dt in &self.data_types {
            if !is_valid_identifier(&dt.name) {
                return Err(ManifestError::InvalidDataType(format!(
                    "invalid data type name: {}",
                    dt.name
                )));
            }
        }

        Ok(())
    }

    /// Check if manifest requires a specific permission
    pub fn requires_permission(&self, permission: &PluginPermission) -> bool {
        self.permissions.contains(permission)
    }

    /// Check if manifest has a specific capability
    pub fn has_capability(&self, capability: &PluginCapability) -> bool {
        self.capabilities.contains(capability)
    }
}

/// Semantic version
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Version {
    /// Major version
    pub major: u32,
    /// Minor version
    pub minor: u32,
    /// Patch version
    pub patch: u32,
    /// Pre-release identifier
    pub pre_release: Option<String>,
}

impl Version {
    /// Create a new version
    pub fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            patch,
            pre_release: None,
        }
    }

    /// Parse from string
    pub fn parse(s: &str) -> Result<Self, ManifestError> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() < 2 {
            return Err(ManifestError::InvalidVersion(s.to_string()));
        }

        let major = parts[0]
            .parse()
            .map_err(|_| ManifestError::InvalidVersion(s.to_string()))?;
        let minor = parts[1]
            .parse()
            .map_err(|_| ManifestError::InvalidVersion(s.to_string()))?;
        let patch = if parts.len() > 2 {
            // Handle pre-release suffix like "1.0.0-beta"
            let patch_str = parts[2].split('-').next().unwrap_or("0");
            patch_str
                .parse()
                .map_err(|_| ManifestError::InvalidVersion(s.to_string()))?
        } else {
            0
        };

        let pre_release = if parts.len() > 2 && parts[2].contains('-') {
            Some(parts[2].split('-').skip(1).collect::<Vec<_>>().join("-"))
        } else {
            None
        };

        Ok(Self {
            major,
            minor,
            patch,
            pre_release,
        })
    }

    /// Check if this version is compatible with another
    pub fn is_compatible_with(&self, other: &Version) -> bool {
        // Same major version and >= minor version
        self.major == other.major && self.minor >= other.minor
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.pre_release {
            Some(pre) => write!(f, "{}.{}.{}-{}", self.major, self.minor, self.patch, pre),
            None => write!(f, "{}.{}.{}", self.major, self.minor, self.patch),
        }
    }
}

/// Plugin dependency
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Dependency {
    /// Dependency name
    pub name: String,
    /// Version requirement
    pub version: VersionReq,
    /// Is optional
    pub optional: bool,
}

impl Dependency {
    /// Create a required dependency
    pub fn required(name: &str, version: VersionReq) -> Self {
        Self {
            name: name.to_string(),
            version,
            optional: false,
        }
    }

    /// Create an optional dependency
    pub fn optional(name: &str, version: VersionReq) -> Self {
        Self {
            name: name.to_string(),
            version,
            optional: true,
        }
    }
}

/// Version requirement
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum VersionReq {
    /// Exact version
    Exact(Version),
    /// Minimum version
    AtLeast(Version),
    /// Range (min, max)
    Range(Version, Version),
    /// Any version
    Any,
}

impl VersionReq {
    /// Check if a version matches this requirement
    pub fn matches(&self, version: &Version) -> bool {
        match self {
            VersionReq::Exact(v) => version == v,
            VersionReq::AtLeast(v) => {
                version.major > v.major
                    || (version.major == v.major && version.minor > v.minor)
                    || (version.major == v.major
                        && version.minor == v.minor
                        && version.patch >= v.patch)
            }
            VersionReq::Range(min, max) => {
                Self::AtLeast(min.clone()).matches(version)
                    && (version.major < max.major
                        || (version.major == max.major && version.minor < max.minor)
                        || (version.major == max.major
                            && version.minor == max.minor
                            && version.patch <= max.patch))
            }
            VersionReq::Any => true,
        }
    }
}

/// Plugin permission
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PluginPermission {
    /// Read data
    ReadData,
    /// Write data
    WriteData,
    /// Delete data
    DeleteData,
    /// Execute commands
    ExecuteCommands,
    /// Access configuration
    AccessConfig,
    /// Network access
    NetworkAccess,
    /// Filesystem access
    FilesystemAccess,
    /// Create subprocesses
    SpawnProcess,
    /// Access metrics
    AccessMetrics,
    /// Manage other plugins
    ManagePlugins,
    /// Admin operations
    Admin,
    /// Custom permission
    Custom(String),
}

impl PluginPermission {
    /// Check if this is a dangerous permission
    pub fn is_dangerous(&self) -> bool {
        matches!(
            self,
            PluginPermission::FilesystemAccess
                | PluginPermission::SpawnProcess
                | PluginPermission::ManagePlugins
                | PluginPermission::Admin
        )
    }
}

/// Plugin capability
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PluginCapability {
    /// Provides commands
    Commands,
    /// Provides data types
    DataTypes,
    /// Provides event hooks
    EventHooks,
    /// Provides storage backend
    StorageBackend,
    /// Provides authentication
    Authentication,
    /// Provides authorization
    Authorization,
    /// Provides metrics
    Metrics,
    /// Provides logging
    Logging,
    /// Custom capability
    Custom(String),
}

/// Command specification
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommandSpec {
    /// Command name
    pub name: String,
    /// Command description
    pub description: String,
    /// Minimum arguments
    pub min_args: Option<usize>,
    /// Maximum arguments
    pub max_args: Option<usize>,
    /// Argument specifications
    pub args: Vec<ArgSpec>,
    /// Command flags
    pub flags: Vec<FlagSpec>,
    /// Is write command
    pub is_write: bool,
    /// Is admin command
    pub is_admin: bool,
    /// Command category
    pub category: Option<String>,
    /// Complexity hint
    pub complexity: Option<String>,
}

impl CommandSpec {
    /// Create a new command spec
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            description: String::new(),
            min_args: None,
            max_args: None,
            args: vec![],
            flags: vec![],
            is_write: false,
            is_admin: false,
            category: None,
            complexity: None,
        }
    }

    /// Builder: set description
    pub fn with_description(mut self, desc: &str) -> Self {
        self.description = desc.to_string();
        self
    }

    /// Builder: set arity
    pub fn with_arity(mut self, min: usize, max: usize) -> Self {
        self.min_args = Some(min);
        self.max_args = Some(max);
        self
    }

    /// Builder: add argument
    pub fn with_arg(mut self, arg: ArgSpec) -> Self {
        self.args.push(arg);
        self
    }

    /// Builder: mark as write command
    pub fn write(mut self) -> Self {
        self.is_write = true;
        self
    }

    /// Builder: mark as admin command
    pub fn admin(mut self) -> Self {
        self.is_admin = true;
        self
    }
}

/// Argument specification
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArgSpec {
    /// Argument name
    pub name: String,
    /// Argument type
    pub arg_type: ArgType,
    /// Is required
    pub required: bool,
    /// Description
    pub description: Option<String>,
    /// Default value
    pub default: Option<String>,
}

impl ArgSpec {
    /// Create a required argument
    pub fn required(name: &str, arg_type: ArgType) -> Self {
        Self {
            name: name.to_string(),
            arg_type,
            required: true,
            description: None,
            default: None,
        }
    }

    /// Create an optional argument
    pub fn optional(name: &str, arg_type: ArgType) -> Self {
        Self {
            name: name.to_string(),
            arg_type,
            required: false,
            description: None,
            default: None,
        }
    }

    /// Builder: set description
    pub fn with_description(mut self, desc: &str) -> Self {
        self.description = Some(desc.to_string());
        self
    }

    /// Builder: set default value
    pub fn with_default(mut self, default: &str) -> Self {
        self.default = Some(default.to_string());
        self
    }
}

/// Argument type
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ArgType {
    /// String argument
    String,
    /// Integer argument
    Integer,
    /// Float argument
    Float,
    /// Key argument
    Key,
    /// Pattern argument
    Pattern,
    /// Enum argument with variants
    Enum(Vec<String>),
    /// Variadic (multiple values)
    Variadic(Box<ArgType>),
}

/// Flag specification
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FlagSpec {
    /// Flag name
    pub name: String,
    /// Short form (single char)
    pub short: Option<char>,
    /// Description
    pub description: Option<String>,
    /// Takes value
    pub takes_value: bool,
}

/// Data type specification
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataTypeSpec {
    /// Type name
    pub name: String,
    /// Type description
    pub description: String,
    /// Commands for this type
    pub commands: Vec<String>,
    /// Encoding format
    pub encoding: DataEncoding,
}

impl DataTypeSpec {
    /// Create a new data type spec
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            description: String::new(),
            commands: vec![],
            encoding: DataEncoding::Binary,
        }
    }

    /// Builder: set description
    pub fn with_description(mut self, desc: &str) -> Self {
        self.description = desc.to_string();
        self
    }

    /// Builder: add command
    pub fn with_command(mut self, cmd: &str) -> Self {
        self.commands.push(cmd.to_string());
        self
    }

    /// Builder: set encoding
    pub fn with_encoding(mut self, encoding: DataEncoding) -> Self {
        self.encoding = encoding;
        self
    }
}

/// Data encoding format
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DataEncoding {
    /// Raw binary
    Binary,
    /// JSON
    Json,
    /// MessagePack
    MessagePack,
    /// Protocol Buffers
    Protobuf,
    /// Custom
    Custom(String),
}

/// Hook specification
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HookSpec {
    /// Hook name
    pub name: String,
    /// Event type
    pub event: String,
    /// Priority (lower = earlier)
    pub priority: i32,
    /// Filter expression
    pub filter: Option<String>,
}

impl HookSpec {
    /// Create a new hook spec
    pub fn new(name: &str, event: &str) -> Self {
        Self {
            name: name.to_string(),
            event: event.to_string(),
            priority: 0,
            filter: None,
        }
    }

    /// Builder: set priority
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Builder: set filter
    pub fn with_filter(mut self, filter: &str) -> Self {
        self.filter = Some(filter.to_string());
        self
    }
}

/// Configuration schema for plugin settings
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ConfigSchema {
    /// Config fields
    pub fields: Vec<ConfigField>,
}

/// Configuration field
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConfigField {
    /// Field name
    pub name: String,
    /// Field type
    pub field_type: ConfigFieldType,
    /// Is required
    pub required: bool,
    /// Description
    pub description: Option<String>,
    /// Default value
    pub default: Option<String>,
}

/// Configuration field type
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ConfigFieldType {
    /// String field
    String,
    /// Integer field
    Integer,
    /// Boolean field
    Boolean,
    /// Float field
    Float,
    /// Array field
    Array(Box<ConfigFieldType>),
    /// Object field
    Object,
}

/// Manifest errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum ManifestError {
    /// Invalid name
    #[error("invalid name: {0}")]
    InvalidName(String),

    /// Invalid version
    #[error("invalid version: {0}")]
    InvalidVersion(String),

    /// Invalid command
    #[error("invalid command: {0}")]
    InvalidCommand(String),

    /// Invalid data type
    #[error("invalid data type: {0}")]
    InvalidDataType(String),

    /// Parse error
    #[error("parse error: {0}")]
    ParseError(String),
}

/// Check if a string is a valid identifier
fn is_valid_identifier(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }

    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }

    chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// Check if a string is a valid command name
fn is_valid_command_name(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }

    // Command names can have dots for namespacing (e.g., "MY.COMMAND")
    s.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_new() {
        let v = Version::new(1, 2, 3);
        assert_eq!(v.major, 1);
        assert_eq!(v.minor, 2);
        assert_eq!(v.patch, 3);
        assert_eq!(v.pre_release, None);
    }

    #[test]
    fn test_version_parse() {
        let v = Version::parse("1.2.3").unwrap();
        assert_eq!(v.major, 1);
        assert_eq!(v.minor, 2);
        assert_eq!(v.patch, 3);

        let v = Version::parse("1.0").unwrap();
        assert_eq!(v.major, 1);
        assert_eq!(v.minor, 0);
        assert_eq!(v.patch, 0);

        let v = Version::parse("2.1.0-beta").unwrap();
        assert_eq!(v.major, 2);
        assert_eq!(v.minor, 1);
        assert_eq!(v.patch, 0);
        assert_eq!(v.pre_release, Some("beta".to_string()));
    }

    #[test]
    fn test_version_display() {
        let v = Version::new(1, 2, 3);
        assert_eq!(v.to_string(), "1.2.3");

        let v = Version {
            major: 2,
            minor: 0,
            patch: 1,
            pre_release: Some("alpha".to_string()),
        };
        assert_eq!(v.to_string(), "2.0.1-alpha");
    }

    #[test]
    fn test_version_compatibility() {
        let v1 = Version::new(1, 2, 0);
        let v2 = Version::new(1, 3, 0);
        let v3 = Version::new(2, 0, 0);

        assert!(v2.is_compatible_with(&v1));
        assert!(!v1.is_compatible_with(&v2));
        assert!(!v3.is_compatible_with(&v1));
    }

    #[test]
    fn test_version_req_matches() {
        let v = Version::new(1, 2, 3);

        assert!(VersionReq::Any.matches(&v));
        assert!(VersionReq::Exact(Version::new(1, 2, 3)).matches(&v));
        assert!(!VersionReq::Exact(Version::new(1, 2, 4)).matches(&v));
        assert!(VersionReq::AtLeast(Version::new(1, 2, 0)).matches(&v));
        assert!(!VersionReq::AtLeast(Version::new(1, 3, 0)).matches(&v));
    }

    #[test]
    fn test_manifest_new() {
        let m = PluginManifest::new("test-plugin", Version::new(1, 0, 0));
        assert_eq!(m.name, "test-plugin");
        assert_eq!(m.version, Version::new(1, 0, 0));
    }

    #[test]
    fn test_manifest_builder() {
        let m = PluginManifest::new("test", Version::new(1, 0, 0))
            .with_description("Test plugin")
            .with_author("Test Author")
            .with_permission(PluginPermission::ReadData)
            .with_capability(PluginCapability::Commands);

        assert_eq!(m.description, "Test plugin");
        assert_eq!(m.author, Some("Test Author".to_string()));
        assert!(m.permissions.contains(&PluginPermission::ReadData));
        assert!(m.capabilities.contains(&PluginCapability::Commands));
    }

    #[test]
    fn test_manifest_validate() {
        let valid = PluginManifest::new("test_plugin", Version::new(1, 0, 0));
        assert!(valid.validate().is_ok());

        let invalid = PluginManifest::new("", Version::new(1, 0, 0));
        assert!(invalid.validate().is_err());

        let invalid_name = PluginManifest::new("123invalid", Version::new(1, 0, 0));
        assert!(invalid_name.validate().is_err());
    }

    #[test]
    fn test_command_spec() {
        let cmd = CommandSpec::new("MY.COMMAND")
            .with_description("A custom command")
            .with_arity(1, 3)
            .with_arg(ArgSpec::required("key", ArgType::Key))
            .write();

        assert_eq!(cmd.name, "MY.COMMAND");
        assert_eq!(cmd.min_args, Some(1));
        assert_eq!(cmd.max_args, Some(3));
        assert!(cmd.is_write);
        assert!(!cmd.is_admin);
    }

    #[test]
    fn test_data_type_spec() {
        let dt = DataTypeSpec::new("MYTYPE")
            .with_description("Custom data type")
            .with_command("MYTYPE.SET")
            .with_command("MYTYPE.GET")
            .with_encoding(DataEncoding::Json);

        assert_eq!(dt.name, "MYTYPE");
        assert_eq!(dt.commands.len(), 2);
    }

    #[test]
    fn test_permission_dangerous() {
        assert!(!PluginPermission::ReadData.is_dangerous());
        assert!(!PluginPermission::WriteData.is_dangerous());
        assert!(PluginPermission::FilesystemAccess.is_dangerous());
        assert!(PluginPermission::SpawnProcess.is_dangerous());
        assert!(PluginPermission::Admin.is_dangerous());
    }

    #[test]
    fn test_valid_identifier() {
        assert!(is_valid_identifier("test"));
        assert!(is_valid_identifier("test_plugin"));
        assert!(is_valid_identifier("test-plugin"));
        assert!(is_valid_identifier("_private"));
        assert!(!is_valid_identifier(""));
        assert!(!is_valid_identifier("123test"));
        assert!(!is_valid_identifier("test plugin"));
    }

    #[test]
    fn test_valid_command_name() {
        assert!(is_valid_command_name("GET"));
        assert!(is_valid_command_name("MY.COMMAND"));
        assert!(is_valid_command_name("MY.NESTED.COMMAND"));
        assert!(!is_valid_command_name(""));
        assert!(!is_valid_command_name("MY COMMAND"));
    }
}
