//! Migration Wizard
//!
//! Automated migration from Redis to Ferrite with compatibility analysis.
//!
//! # Features
//!
//! - Redis compatibility analysis
//! - Data migration with progress tracking
//! - Configuration migration
//! - Replication-based live migration
//! - Rollback support
//!
//! # Example
//!
//! ```ignore
//! use ferrite::migration::{MigrationWizard, MigrationConfig};
//!
//! let wizard = MigrationWizard::new();
//!
//! // Analyze Redis instance for compatibility
//! let analysis = wizard.analyze("redis://localhost:6379").await?;
//!
//! // Run migration
//! let result = wizard.migrate(MigrationConfig {
//!     source: "redis://localhost:6379".to_string(),
//!     target: "ferrite://localhost:6380".to_string(),
//!     ..Default::default()
//! }).await?;
//! ```

pub mod analyzer;
pub mod cluster_migration;
pub mod cutover;
pub mod executor;
pub mod live;
pub mod live_migration;
pub mod planner;
pub mod progress;
/// Redis RDB file parser for snapshot-based migration.
pub mod rdb_parser;
pub mod validator;

pub use analyzer::{CompatibilityAnalyzer, CompatibilityIssue, CompatibilityReport};
pub use cluster_migration::{
    ClusterMigrationConfig, ClusterMigrationCoordinator, ClusterMigrationError,
    ClusterMigrationState, MigrationCheckpoint, SlotMigrationPhase,
};
pub use cutover::{
    CutoverConfig, CutoverError, CutoverOrchestrator, CutoverState, CutoverSummary,
    VerificationResult,
};
pub use executor::{MigrationExecutor, MigrationProgress, MigrationResult};
pub use planner::{MigrationPlan, MigrationPlanner, MigrationStep};
pub use progress::{
    DataVerifier, MigrationOutcome, MigrationPhase as ProgressPhase, MigrationProgressError,
    MigrationProgressTracker, MigrationReport, ProgressSnapshot,
};
pub use validator::{MigrationValidator, ValidationResult};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// Migration error
#[derive(Debug, Clone, thiserror::Error)]
pub enum MigrationError {
    /// Connection error
    #[error("connection error: {0}")]
    Connection(String),

    /// Compatibility error
    #[error("compatibility error: {0}")]
    Compatibility(String),

    /// Data transfer error
    #[error("data transfer error: {0}")]
    DataTransfer(String),

    /// Validation error
    #[error("validation error: {0}")]
    Validation(String),

    /// Configuration error
    #[error("configuration error: {0}")]
    Config(String),

    /// Timeout error
    #[error("operation timed out: {0}")]
    Timeout(String),

    /// Rollback error
    #[error("rollback error: {0}")]
    Rollback(String),
}

/// Migration result type
pub type Result<T> = std::result::Result<T, MigrationError>;

/// Migration configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MigrationConfig {
    /// Source Redis URL
    pub source: String,
    /// Target Ferrite URL
    pub target: String,
    /// Migration mode
    pub mode: MigrationMode,
    /// Batch size for data transfer
    pub batch_size: usize,
    /// Number of parallel workers
    pub workers: usize,
    /// Whether to verify data after migration
    pub verify: bool,
    /// Whether to keep source data after migration
    pub keep_source: bool,
    /// Key pattern filter (e.g., "user:*")
    pub key_pattern: Option<String>,
    /// Databases to migrate (empty = all)
    pub databases: Vec<u32>,
    /// Timeout for operations
    pub timeout: Duration,
    /// Enable progress reporting
    pub progress_reporting: bool,
    /// Maximum memory for buffering (bytes)
    pub max_buffer_memory: usize,
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            source: "redis://localhost:6379".to_string(),
            target: "ferrite://localhost:6380".to_string(),
            mode: MigrationMode::Snapshot,
            batch_size: 1000,
            workers: 4,
            verify: true,
            keep_source: true,
            key_pattern: None,
            databases: vec![],
            timeout: Duration::from_secs(3600),
            progress_reporting: true,
            max_buffer_memory: 256 * 1024 * 1024, // 256MB
        }
    }
}

/// Migration mode
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationMode {
    /// Snapshot-based migration (source is paused)
    Snapshot,
    /// Live migration using replication
    Live,
    /// Incremental migration (delta sync)
    Incremental,
    /// Dual-write mode (write to both during transition)
    DualWrite,
}

/// Migration wizard - main entry point
pub struct MigrationWizard {
    /// Compatibility analyzer
    analyzer: CompatibilityAnalyzer,
    /// Migration planner
    planner: MigrationPlanner,
    /// Migration executor
    executor: MigrationExecutor,
    /// Migration validator
    validator: MigrationValidator,
}

impl MigrationWizard {
    /// Create a new migration wizard
    pub fn new() -> Self {
        Self {
            analyzer: CompatibilityAnalyzer::new(),
            planner: MigrationPlanner::new(),
            executor: MigrationExecutor::new(),
            validator: MigrationValidator::new(),
        }
    }

    /// Analyze source Redis for compatibility
    pub async fn analyze(&self, source_url: &str) -> Result<CompatibilityReport> {
        self.analyzer.analyze(source_url).await
    }

    /// Create a migration plan
    pub async fn plan(&self, config: &MigrationConfig) -> Result<MigrationPlan> {
        // First analyze compatibility
        let report = self.analyzer.analyze(&config.source).await?;

        // Check for blocking issues
        let blocking_issues: Vec<_> = report
            .issues
            .iter()
            .filter(|i| i.severity == IssueSeverity::Blocking)
            .collect();

        if !blocking_issues.is_empty() {
            return Err(MigrationError::Compatibility(format!(
                "Found {} blocking issues that must be resolved before migration",
                blocking_issues.len()
            )));
        }

        // Create the plan
        self.planner.create_plan(config, &report).await
    }

    /// Execute a migration
    pub async fn migrate(&self, config: MigrationConfig) -> Result<MigrationResult> {
        // Create plan
        let plan = self.plan(&config).await?;

        // Execute migration
        let result = self.executor.execute(&plan, &config).await?;

        // Validate if requested
        if config.verify {
            let validation = self.validator.validate(&config, &result).await?;
            if !validation.success {
                return Err(MigrationError::Validation(format!(
                    "Validation failed: {} errors",
                    validation.errors.len()
                )));
            }
        }

        Ok(result)
    }

    /// Perform a dry run (no actual migration)
    pub async fn dry_run(&self, config: &MigrationConfig) -> Result<MigrationPlan> {
        let plan = self.plan(config).await?;
        Ok(plan)
    }

    /// Rollback a migration
    pub async fn rollback(&self, result: &MigrationResult) -> Result<()> {
        self.executor.rollback(result).await
    }

    /// Get migration progress
    pub fn progress(&self) -> Option<MigrationProgress> {
        self.executor.current_progress()
    }
}

impl Default for MigrationWizard {
    fn default() -> Self {
        Self::new()
    }
}

/// Issue severity level
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IssueSeverity {
    /// Information only
    Info,
    /// Warning - migration will proceed but may have issues
    Warning,
    /// Error - migration may fail or have data loss
    Error,
    /// Blocking - migration cannot proceed
    Blocking,
}

/// Supported Redis data types
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    /// String
    String,
    /// List
    List,
    /// Set
    Set,
    /// Sorted Set
    SortedSet,
    /// Hash
    Hash,
    /// Stream
    Stream,
    /// HyperLogLog
    HyperLogLog,
    /// Bitmap
    Bitmap,
    /// Geospatial
    Geo,
    /// JSON (RedisJSON)
    Json,
    /// Time series (RedisTimeSeries)
    TimeSeries,
    /// Graph (RedisGraph)
    Graph,
    /// Search (RediSearch)
    Search,
    /// Bloom filter
    Bloom,
    /// Unknown
    Unknown,
}

/// Statistics about source data
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SourceStats {
    /// Total number of keys
    pub total_keys: u64,
    /// Keys by data type
    pub keys_by_type: HashMap<DataType, u64>,
    /// Total memory usage (bytes)
    pub memory_bytes: u64,
    /// Memory by data type
    pub memory_by_type: HashMap<DataType, u64>,
    /// Number of databases in use
    pub databases_used: u32,
    /// Keys with TTL
    pub keys_with_ttl: u64,
    /// Largest key size (bytes)
    pub largest_key_bytes: u64,
    /// Redis version
    pub redis_version: String,
    /// Cluster mode enabled
    pub cluster_mode: bool,
    /// Number of nodes (if cluster)
    pub cluster_nodes: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migration_config_default() {
        let config = MigrationConfig::default();
        assert_eq!(config.batch_size, 1000);
        assert!(config.verify);
        assert!(config.keep_source);
    }

    #[test]
    fn test_wizard_creation() {
        let wizard = MigrationWizard::new();
        assert!(wizard.progress().is_none());
    }
}
