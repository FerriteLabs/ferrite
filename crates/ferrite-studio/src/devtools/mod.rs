#![forbid(unsafe_code)]
//! Developer experience tools for Ferrite: schema visualization, migration wizard,
//! architecture templates, and interactive query builder.

pub mod migration_wizard;
pub mod query_builder;
pub mod schema_viz;
pub mod templates;

pub use migration_wizard::{
    CompatibilityReport, IncompatibleCommand, MigrationPlan, MigrationStep, MigrationWizard,
    RiskLevel,
};
pub use query_builder::{CommandHelp, QueryBuilder, QuerySuggestion};
pub use schema_viz::{DatabaseSchema, KeyPattern, SchemaSnapshot, SchemaVisualizer};
pub use templates::{ArchitectureTemplate, TemplateCategory, TemplateRegistry};
