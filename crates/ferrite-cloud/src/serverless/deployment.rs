//! Function Deployment Management
//!
//! Handle function deployments, versioning, and lifecycle management.

use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::triggers::TriggerConfig;

/// Function deployment
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FunctionDeployment {
    /// Deployment ID
    pub id: String,
    /// Function name
    pub name: String,
    /// Function specification
    pub spec: FunctionSpec,
    /// Deployment configuration
    pub config: DeploymentConfig,
    /// Target regions
    pub regions: Vec<String>,
    /// Deployment status
    pub status: DeploymentStatus,
    /// Version
    pub version: u32,
    /// Created timestamp
    pub created_at: u64,
    /// Updated timestamp
    pub updated_at: u64,
    /// Metadata
    pub metadata: HashMap<String, String>,
}

impl FunctionDeployment {
    /// Create a new deployment builder
    pub fn builder(name: &str) -> DeploymentBuilder {
        DeploymentBuilder::new(name)
    }
}

/// Deployment builder
pub struct DeploymentBuilder {
    name: String,
    spec: FunctionSpec,
    config: DeploymentConfig,
    regions: Vec<String>,
    metadata: HashMap<String, String>,
}

impl DeploymentBuilder {
    /// Create new builder
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            spec: FunctionSpec::default(),
            config: DeploymentConfig::default(),
            regions: vec![],
            metadata: HashMap::new(),
        }
    }

    /// Set WASM bytes
    pub fn wasm_bytes(mut self, bytes: &[u8]) -> Self {
        self.spec.wasm_bytes = bytes.to_vec();
        self
    }

    /// Set handler name
    pub fn handler(mut self, handler: &str) -> Self {
        self.spec.handler = handler.to_string();
        self
    }

    /// Add trigger
    pub fn trigger(mut self, trigger: TriggerConfig) -> Self {
        self.spec.triggers.push(trigger);
        self
    }

    /// Set target regions
    pub fn regions(mut self, regions: Vec<&str>) -> Self {
        self.regions = regions.into_iter().map(String::from).collect();
        self
    }

    /// Set minimum instances
    pub fn min_instances(mut self, min: usize) -> Self {
        self.config.scaling.min_instances = min;
        self
    }

    /// Set maximum instances
    pub fn max_instances(mut self, max: usize) -> Self {
        self.config.scaling.max_instances = max;
        self
    }

    /// Set memory limit
    pub fn memory_limit(mut self, bytes: usize) -> Self {
        self.config.resources.memory_limit = bytes;
        self
    }

    /// Set timeout
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.config.timeout_ms = timeout.as_millis() as u64;
        self
    }

    /// Add metadata
    pub fn metadata(mut self, key: &str, value: &str) -> Self {
        self.metadata.insert(key.to_string(), value.to_string());
        self
    }

    /// Set environment variables
    pub fn env(mut self, key: &str, value: &str) -> Self {
        self.spec
            .environment
            .insert(key.to_string(), value.to_string());
        self
    }

    /// Build the deployment
    pub fn build(self) -> FunctionDeployment {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        FunctionDeployment {
            id: format!("deploy_{}_{}", self.name, now),
            name: self.name,
            spec: self.spec,
            config: self.config,
            regions: self.regions,
            status: DeploymentStatus::Pending,
            version: 1,
            created_at: now,
            updated_at: now,
            metadata: self.metadata,
        }
    }
}

/// Function specification
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct FunctionSpec {
    /// WASM module bytes
    #[serde(skip)]
    pub wasm_bytes: Vec<u8>,
    /// WASM module hash (for verification)
    pub wasm_hash: String,
    /// Handler function name
    pub handler: String,
    /// Runtime (currently only "wasm")
    pub runtime: String,
    /// Triggers that invoke this function
    pub triggers: Vec<TriggerConfig>,
    /// Environment variables
    pub environment: HashMap<String, String>,
    /// Secrets (references, not values)
    pub secrets: Vec<String>,
}

/// Deployment configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeploymentConfig {
    /// Scaling configuration
    pub scaling: ScalingConfig,
    /// Resource requirements
    pub resources: ResourceRequirements,
    /// Timeout in milliseconds
    pub timeout_ms: u64,
    /// Retry configuration
    pub retry: RetryConfig,
    /// Enable tracing
    pub tracing_enabled: bool,
    /// Log level
    pub log_level: String,
    /// Concurrency limit per instance
    pub concurrency_limit: usize,
}

impl Default for DeploymentConfig {
    fn default() -> Self {
        Self {
            scaling: ScalingConfig::default(),
            resources: ResourceRequirements::default(),
            timeout_ms: 30000,
            retry: RetryConfig::default(),
            tracing_enabled: true,
            log_level: "info".to_string(),
            concurrency_limit: 10,
        }
    }
}

/// Scaling configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScalingConfig {
    /// Minimum instances
    pub min_instances: usize,
    /// Maximum instances
    pub max_instances: usize,
    /// Target concurrency per instance
    pub target_concurrency: usize,
    /// Scale up threshold (%)
    pub scale_up_threshold: u8,
    /// Scale down threshold (%)
    pub scale_down_threshold: u8,
    /// Scale up cooldown (seconds)
    pub scale_up_cooldown_secs: u64,
    /// Scale down cooldown (seconds)
    pub scale_down_cooldown_secs: u64,
}

impl Default for ScalingConfig {
    fn default() -> Self {
        Self {
            min_instances: 0,
            max_instances: 100,
            target_concurrency: 10,
            scale_up_threshold: 80,
            scale_down_threshold: 20,
            scale_up_cooldown_secs: 60,
            scale_down_cooldown_secs: 300,
        }
    }
}

/// Resource requirements
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResourceRequirements {
    /// Memory limit in bytes
    pub memory_limit: usize,
    /// CPU allocation (millicores)
    pub cpu_millicores: usize,
    /// Ephemeral storage limit
    pub storage_limit: usize,
    /// Network bandwidth limit (bytes/sec)
    pub network_bandwidth: usize,
}

impl Default for ResourceRequirements {
    fn default() -> Self {
        Self {
            memory_limit: 128 * 1024 * 1024,      // 128MB
            cpu_millicores: 1000,                 // 1 vCPU
            storage_limit: 512 * 1024 * 1024,     // 512MB
            network_bandwidth: 100 * 1024 * 1024, // 100MB/s
        }
    }
}

/// Retry configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Enable retries
    pub enabled: bool,
    /// Maximum retry attempts
    pub max_attempts: usize,
    /// Initial backoff (ms)
    pub initial_backoff_ms: u64,
    /// Maximum backoff (ms)
    pub max_backoff_ms: u64,
    /// Backoff multiplier
    pub backoff_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_attempts: 3,
            initial_backoff_ms: 100,
            max_backoff_ms: 10000,
            backoff_multiplier: 2.0,
        }
    }
}

/// Deployment status
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeploymentStatus {
    /// Pending deployment
    Pending,
    /// Building/compiling
    Building,
    /// Deploying to regions
    Deploying,
    /// Active and serving
    Active,
    /// Paused (not serving)
    Paused,
    /// Failed deployment
    Failed(String),
    /// Being deleted
    Deleting,
    /// Deleted
    Deleted,
}

/// Regional deployment status
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegionalDeployment {
    /// Region ID
    pub region: String,
    /// Status in this region
    pub status: DeploymentStatus,
    /// Number of active instances
    pub active_instances: usize,
    /// Ready instances
    pub ready_instances: usize,
    /// Last health check
    pub last_health_check: u64,
    /// Error message if failed
    pub error: Option<String>,
}

/// Deployment metrics
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DeploymentMetrics {
    /// Total invocations
    pub invocations: u64,
    /// Successful invocations
    pub successes: u64,
    /// Failed invocations
    pub failures: u64,
    /// Average latency (ms)
    pub avg_latency_ms: f64,
    /// P99 latency (ms)
    pub p99_latency_ms: f64,
    /// Cold starts
    pub cold_starts: u64,
    /// Active instances across all regions
    pub active_instances: usize,
    /// Memory usage (bytes)
    pub memory_usage: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deployment_builder() {
        let deployment = FunctionDeployment::builder("my-function")
            .wasm_bytes(&[0, 1, 2, 3])
            .handler("handle")
            .regions(vec!["us-east-1", "eu-west-1"])
            .min_instances(1)
            .max_instances(10)
            .memory_limit(256 * 1024 * 1024)
            .timeout(Duration::from_secs(60))
            .env("DEBUG", "true")
            .metadata("owner", "team-a")
            .build();

        assert_eq!(deployment.name, "my-function");
        assert_eq!(deployment.spec.handler, "handle");
        assert_eq!(deployment.regions.len(), 2);
        assert_eq!(deployment.config.scaling.min_instances, 1);
        assert_eq!(deployment.config.scaling.max_instances, 10);
        assert_eq!(deployment.config.resources.memory_limit, 256 * 1024 * 1024);
        assert_eq!(deployment.config.timeout_ms, 60000);
        assert_eq!(
            deployment.spec.environment.get("DEBUG"),
            Some(&"true".to_string())
        );
    }

    #[test]
    fn test_deployment_status() {
        let status = DeploymentStatus::Active;
        assert_eq!(status, DeploymentStatus::Active);

        let failed = DeploymentStatus::Failed("compile error".to_string());
        if let DeploymentStatus::Failed(msg) = failed {
            assert_eq!(msg, "compile error");
        }
    }

    #[test]
    fn test_scaling_config_default() {
        let config = ScalingConfig::default();
        assert_eq!(config.min_instances, 0);
        assert_eq!(config.max_instances, 100);
        assert_eq!(config.target_concurrency, 10);
    }

    #[test]
    fn test_resource_requirements_default() {
        let resources = ResourceRequirements::default();
        assert_eq!(resources.memory_limit, 128 * 1024 * 1024);
        assert_eq!(resources.cpu_millicores, 1000);
    }
}
