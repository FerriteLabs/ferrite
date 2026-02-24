//! Serverless/Edge Functions
//!
//! Deploy and execute functions at the edge with automatic scaling,
//! cold-start optimization, and geo-distributed execution.
//!
//! # Features
//!
//! - **Edge Deployment**: Deploy functions to edge locations globally
//! - **Auto-Scaling**: Automatic scaling based on load
//! - **Cold Start Optimization**: Pre-warming and instant startup
//! - **Geo-Distribution**: Route to nearest edge location
//! - **Event Triggers**: Execute on data changes, schedules, or HTTP
//! - **Observability**: Built-in metrics, tracing, and logging
//!
//! # Example
//!
//! ```ignore
//! use ferrite::serverless::{FunctionDeployment, EdgeRuntime, TriggerType};
//!
//! let runtime = EdgeRuntime::new(config);
//!
//! // Deploy a function
//! let deployment = FunctionDeployment::builder("process-order")
//!     .wasm_bytes(&wasm)
//!     .trigger(TriggerType::KeyPattern("orders:*".into()))
//!     .regions(vec!["us-east-1", "eu-west-1", "ap-southeast-1"])
//!     .min_instances(1)
//!     .max_instances(100)
//!     .build();
//!
//! runtime.deploy(deployment).await?;
//! ```

/// Serverless connection pooling proxy and event bridge.
pub mod connection_pool;
pub mod deployment;
pub mod edge;
pub mod runtime;
pub mod scheduler;
pub mod triggers;
pub mod wasi_adapter;

pub use deployment::{
    DeploymentConfig, DeploymentStatus, FunctionDeployment, FunctionSpec, ResourceRequirements,
    ScalingConfig,
};
pub use edge::{EdgeLocation, EdgeNetwork, EdgeNode, EdgeNodeStatus};
pub use runtime::{EdgeRuntime, ExecutionContext, InvocationRequest, InvocationResult};
pub use scheduler::{FunctionScheduler, ScheduleConfig, ScheduledTask};
pub use triggers::{EventTrigger, HttpTrigger, ScheduleTrigger, TriggerConfig, TriggerType};

use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

/// Serverless configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServerlessConfig {
    /// Enable serverless functions
    pub enabled: bool,
    /// Enable edge deployment
    pub edge_enabled: bool,
    /// Default region
    pub default_region: String,
    /// Available regions
    pub regions: Vec<String>,
    /// Cold start timeout (ms)
    pub cold_start_timeout_ms: u64,
    /// Max concurrent executions per function
    pub max_concurrent_executions: usize,
    /// Default memory limit (bytes)
    pub default_memory_limit: usize,
    /// Default timeout (ms)
    pub default_timeout_ms: u64,
    /// Enable function caching
    pub caching_enabled: bool,
    /// Function cache TTL (seconds)
    pub cache_ttl_secs: u64,
    /// Pre-warming configuration
    pub prewarm: PrewarmConfig,
    /// Metrics collection
    pub metrics_enabled: bool,
}

impl Default for ServerlessConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            edge_enabled: true,
            default_region: "us-east-1".to_string(),
            regions: vec![
                "us-east-1".to_string(),
                "us-west-2".to_string(),
                "eu-west-1".to_string(),
                "eu-central-1".to_string(),
                "ap-southeast-1".to_string(),
                "ap-northeast-1".to_string(),
            ],
            cold_start_timeout_ms: 5000,
            max_concurrent_executions: 1000,
            default_memory_limit: 128 * 1024 * 1024, // 128MB
            default_timeout_ms: 30000,               // 30 seconds
            caching_enabled: true,
            cache_ttl_secs: 300,
            prewarm: PrewarmConfig::default(),
            metrics_enabled: true,
        }
    }
}

/// Pre-warming configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrewarmConfig {
    /// Enable pre-warming
    pub enabled: bool,
    /// Minimum warm instances
    pub min_warm_instances: usize,
    /// Pre-warm on deployment
    pub prewarm_on_deploy: bool,
    /// Predictive pre-warming based on traffic patterns
    pub predictive_enabled: bool,
}

impl Default for PrewarmConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_warm_instances: 1,
            prewarm_on_deploy: true,
            predictive_enabled: false,
        }
    }
}

/// Serverless runtime metrics
#[derive(Debug, Default)]
pub struct ServerlessMetrics {
    /// Total invocations
    pub invocations: AtomicU64,
    /// Successful invocations
    pub successes: AtomicU64,
    /// Failed invocations
    pub failures: AtomicU64,
    /// Cold starts
    pub cold_starts: AtomicU64,
    /// Warm starts
    pub warm_starts: AtomicU64,
    /// Timeouts
    pub timeouts: AtomicU64,
    /// Total execution time (ms)
    pub total_execution_time_ms: AtomicU64,
    /// Active executions
    pub active_executions: AtomicU64,
}

/// Metrics snapshot
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    /// Total invocations
    pub invocations: u64,
    /// Successful invocations
    pub successes: u64,
    /// Failed invocations
    pub failures: u64,
    /// Cold starts
    pub cold_starts: u64,
    /// Warm starts
    pub warm_starts: u64,
    /// Timeouts
    pub timeouts: u64,
    /// Average execution time (ms)
    pub avg_execution_time_ms: f64,
    /// Active executions
    pub active_executions: u64,
    /// Success rate
    pub success_rate: f64,
    /// Cold start rate
    pub cold_start_rate: f64,
}

impl ServerlessMetrics {
    /// Create a snapshot
    pub fn snapshot(&self) -> MetricsSnapshot {
        let invocations = self.invocations.load(Ordering::Relaxed);
        let successes = self.successes.load(Ordering::Relaxed);
        let cold_starts = self.cold_starts.load(Ordering::Relaxed);
        let total_time = self.total_execution_time_ms.load(Ordering::Relaxed);

        let avg_time = if invocations > 0 {
            total_time as f64 / invocations as f64
        } else {
            0.0
        };

        let success_rate = if invocations > 0 {
            successes as f64 / invocations as f64
        } else {
            1.0
        };

        let cold_start_rate = if invocations > 0 {
            cold_starts as f64 / invocations as f64
        } else {
            0.0
        };

        MetricsSnapshot {
            invocations,
            successes,
            failures: self.failures.load(Ordering::Relaxed),
            cold_starts,
            warm_starts: self.warm_starts.load(Ordering::Relaxed),
            timeouts: self.timeouts.load(Ordering::Relaxed),
            avg_execution_time_ms: avg_time,
            active_executions: self.active_executions.load(Ordering::Relaxed),
            success_rate,
            cold_start_rate,
        }
    }
}

/// Serverless errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum ServerlessError {
    /// Function not found
    #[error("function not found: {0}")]
    FunctionNotFound(String),

    /// Function already exists
    #[error("function already exists: {0}")]
    FunctionExists(String),

    /// Deployment failed
    #[error("deployment failed: {0}")]
    DeploymentFailed(String),

    /// Invocation failed
    #[error("invocation failed: {0}")]
    InvocationFailed(String),

    /// Timeout
    #[error("function timed out after {0}ms")]
    Timeout(u64),

    /// Resource limit exceeded
    #[error("resource limit exceeded: {0}")]
    ResourceLimitExceeded(String),

    /// Region not available
    #[error("region not available: {0}")]
    RegionNotAvailable(String),

    /// Cold start failed
    #[error("cold start failed: {0}")]
    ColdStartFailed(String),

    /// Configuration error
    #[error("configuration error: {0}")]
    ConfigError(String),

    /// Internal error
    #[error("internal error: {0}")]
    Internal(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serverless_config_default() {
        let config = ServerlessConfig::default();
        assert!(config.enabled);
        assert!(config.edge_enabled);
        assert!(!config.regions.is_empty());
    }

    #[test]
    fn test_metrics_snapshot() {
        let metrics = ServerlessMetrics::default();
        metrics.invocations.store(100, Ordering::Relaxed);
        metrics.successes.store(95, Ordering::Relaxed);
        metrics.failures.store(5, Ordering::Relaxed);
        metrics.cold_starts.store(10, Ordering::Relaxed);
        metrics
            .total_execution_time_ms
            .store(5000, Ordering::Relaxed);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.invocations, 100);
        assert_eq!(snapshot.successes, 95);
        assert!((snapshot.success_rate - 0.95).abs() < 0.001);
        assert!((snapshot.avg_execution_time_ms - 50.0).abs() < 0.001);
    }

    #[test]
    fn test_prewarm_config_default() {
        let config = PrewarmConfig::default();
        assert!(config.enabled);
        assert!(config.prewarm_on_deploy);
    }
}
