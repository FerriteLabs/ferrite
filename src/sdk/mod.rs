//! SDK Generator
//!
//! Automatic client SDK generation for multiple programming languages.
//!
//! # Features
//!
//! - Generate type-safe client libraries for Redis commands
//! - Support for TypeScript, Python, Go, Java, and Rust
//! - Automatic documentation generation
//! - OpenAPI/Swagger spec generation
//! - gRPC protobuf definitions
//!
//! # Example
//!
//! ```ignore
//! use ferrite::sdk::{SdkGenerator, Language};
//!
//! let generator = SdkGenerator::new();
//! generator.generate(Language::TypeScript, "./sdk/typescript")?;
//! generator.generate(Language::Python, "./sdk/python")?;
//! ```

#![allow(dead_code, unused_imports, unused_variables)]
pub mod ffi;
pub mod generator;
pub mod languages;
pub mod protocol_ext;
pub mod schema;
pub mod templates;
pub mod user_schema;

pub use generator::{GeneratorConfig, SdkGenerator};
pub use languages::Language;
pub use protocol_ext::{
    DistanceMetric, ExtendedCommand, ExtendedResponse, FerriteProtocolExtension, IndexType,
};
pub use schema::{ApiSchema, CommandSchema, TypeSchema};

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// SDK generation error
#[derive(Debug, Clone, thiserror::Error)]
pub enum SdkError {
    /// Template error
    #[error("template error: {0}")]
    Template(String),

    /// Schema error
    #[error("schema error: {0}")]
    Schema(String),

    /// IO error
    #[error("IO error: {0}")]
    Io(String),

    /// Language not supported
    #[error("unsupported language: {0}")]
    UnsupportedLanguage(String),

    /// Invalid configuration
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
}

/// SDK generation result
pub type Result<T> = std::result::Result<T, SdkError>;

/// SDK metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SdkMetadata {
    /// SDK name
    pub name: String,
    /// Version
    pub version: String,
    /// Description
    pub description: String,
    /// License
    pub license: String,
    /// Repository URL
    pub repository: Option<String>,
    /// Author
    pub author: Option<String>,
}

impl Default for SdkMetadata {
    fn default() -> Self {
        Self {
            name: "ferrite-client".to_string(),
            version: "0.1.0".to_string(),
            description: "Ferrite client SDK".to_string(),
            license: "MIT".to_string(),
            repository: None,
            author: None,
        }
    }
}

/// Generated SDK output
#[derive(Clone, Debug)]
pub struct GeneratedSdk {
    /// Target language
    pub language: Language,
    /// Output directory
    pub output_dir: PathBuf,
    /// Generated files
    pub files: Vec<GeneratedFile>,
    /// Metadata
    pub metadata: SdkMetadata,
}

/// A generated file
#[derive(Clone, Debug)]
pub struct GeneratedFile {
    /// File path (relative to output dir)
    pub path: PathBuf,
    /// File content
    pub content: String,
    /// File type
    pub file_type: FileType,
}

/// Type of generated file
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FileType {
    /// Source code
    Source,
    /// Configuration file
    Config,
    /// Documentation
    Documentation,
    /// Test file
    Test,
    /// Build script
    Build,
}

/// Command category for organizing SDKs
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CommandCategory {
    /// String commands
    Strings,
    /// List commands
    Lists,
    /// Set commands
    Sets,
    /// Hash commands
    Hashes,
    /// Sorted set commands
    SortedSets,
    /// Key commands
    Keys,
    /// Server commands
    Server,
    /// Pub/Sub commands
    PubSub,
    /// Transaction commands
    Transactions,
    /// Scripting commands
    Scripting,
    /// Cluster commands
    Cluster,
    /// Stream commands
    Streams,
    /// Vector commands (Ferrite extension)
    Vector,
    /// Graph commands (Ferrite extension)
    Graph,
    /// Time series commands (Ferrite extension)
    TimeSeries,
    /// Document commands (Ferrite extension)
    Document,
}

impl CommandCategory {
    /// Get all categories
    pub fn all() -> Vec<CommandCategory> {
        vec![
            CommandCategory::Strings,
            CommandCategory::Lists,
            CommandCategory::Sets,
            CommandCategory::Hashes,
            CommandCategory::SortedSets,
            CommandCategory::Keys,
            CommandCategory::Server,
            CommandCategory::PubSub,
            CommandCategory::Transactions,
            CommandCategory::Scripting,
            CommandCategory::Cluster,
            CommandCategory::Streams,
            CommandCategory::Vector,
            CommandCategory::Graph,
            CommandCategory::TimeSeries,
            CommandCategory::Document,
        ]
    }

    /// Get category name
    pub fn name(&self) -> &'static str {
        match self {
            CommandCategory::Strings => "Strings",
            CommandCategory::Lists => "Lists",
            CommandCategory::Sets => "Sets",
            CommandCategory::Hashes => "Hashes",
            CommandCategory::SortedSets => "SortedSets",
            CommandCategory::Keys => "Keys",
            CommandCategory::Server => "Server",
            CommandCategory::PubSub => "PubSub",
            CommandCategory::Transactions => "Transactions",
            CommandCategory::Scripting => "Scripting",
            CommandCategory::Cluster => "Cluster",
            CommandCategory::Streams => "Streams",
            CommandCategory::Vector => "Vector",
            CommandCategory::Graph => "Graph",
            CommandCategory::TimeSeries => "TimeSeries",
            CommandCategory::Document => "Document",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sdk_metadata_default() {
        let meta = SdkMetadata::default();
        assert_eq!(meta.name, "ferrite-client");
        assert!(!meta.version.is_empty());
    }

    #[test]
    fn test_command_category_all() {
        let categories = CommandCategory::all();
        assert!(categories.len() > 10);
    }
}
