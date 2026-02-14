//! Edge Runtime
//!
//! Execute serverless functions at the edge with cold-start optimization.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use super::deployment::{DeploymentStatus, FunctionDeployment};
use super::edge::{EdgeNetwork, EdgeNode, GeoCoordinates};
use super::{ServerlessConfig, ServerlessError, ServerlessMetrics};

/// Edge runtime for executing serverless functions
pub struct EdgeRuntime {
    /// Configuration
    config: ServerlessConfig,
    /// Deployments
    deployments: RwLock<HashMap<String, FunctionDeployment>>,
    /// Edge network
    edge_network: Arc<EdgeNetwork>,
    /// Warm instances
    warm_instances: RwLock<HashMap<String, Vec<WarmInstance>>>,
    /// Metrics
    metrics: Arc<ServerlessMetrics>,
    /// Running flag
    running: AtomicBool,
}

/// Warm instance for reduced cold-start
struct WarmInstance {
    /// Instance ID
    id: String,
    /// Function ID
    function_id: String,
    /// Created at
    created_at: Instant,
    /// Last used
    last_used: Instant,
    /// In use
    in_use: AtomicBool,
}

/// Invocation request
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InvocationRequest {
    /// Function name
    pub function_name: String,
    /// Input payload
    pub payload: Vec<u8>,
    /// Request context
    pub context: ExecutionContext,
    /// Timeout override
    pub timeout_ms: Option<u64>,
    /// Async invocation (fire and forget)
    pub async_invoke: bool,
    /// Preferred region
    pub preferred_region: Option<String>,
}

/// Execution context
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ExecutionContext {
    /// Request ID
    pub request_id: String,
    /// Caller identity
    pub caller: Option<String>,
    /// Source IP
    pub source_ip: Option<String>,
    /// Source coordinates (for geo-routing)
    pub source_coords: Option<(f64, f64)>,
    /// Custom headers
    pub headers: HashMap<String, String>,
    /// Trace ID (for distributed tracing)
    pub trace_id: Option<String>,
    /// Parent span ID
    pub parent_span_id: Option<String>,
}

/// Invocation result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InvocationResult {
    /// Request ID
    pub request_id: String,
    /// Success
    pub success: bool,
    /// Output payload
    pub payload: Option<Vec<u8>>,
    /// Error message
    pub error: Option<String>,
    /// Execution duration (ms)
    pub duration_ms: u64,
    /// Cold start
    pub cold_start: bool,
    /// Region executed in
    pub region: String,
    /// Node executed on
    pub node_id: String,
    /// Memory used (bytes)
    pub memory_used: usize,
    /// Logs
    pub logs: Vec<LogEntry>,
}

/// Log entry from function execution
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogEntry {
    /// Timestamp
    pub timestamp: u64,
    /// Log level
    pub level: String,
    /// Message
    pub message: String,
}

impl EdgeRuntime {
    /// Create a new edge runtime
    pub fn new(config: ServerlessConfig) -> Self {
        let edge_network = Arc::new(EdgeNetwork::new());

        // Initialize default locations
        for location in super::edge::create_default_locations() {
            edge_network.add_location(location);
        }

        Self {
            config,
            deployments: RwLock::new(HashMap::new()),
            edge_network,
            warm_instances: RwLock::new(HashMap::new()),
            metrics: Arc::new(ServerlessMetrics::default()),
            running: AtomicBool::new(false),
        }
    }

    /// Start the runtime
    pub async fn start(&self) -> Result<(), ServerlessError> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Err(ServerlessError::Internal(
                "runtime already started".to_string(),
            ));
        }

        info!("Edge runtime started");
        Ok(())
    }

    /// Stop the runtime
    pub async fn stop(&self) -> Result<(), ServerlessError> {
        if !self.running.swap(false, Ordering::SeqCst) {
            return Err(ServerlessError::Internal("runtime not running".to_string()));
        }

        info!("Edge runtime stopped");
        Ok(())
    }

    /// Deploy a function
    pub async fn deploy(
        &self,
        mut deployment: FunctionDeployment,
    ) -> Result<String, ServerlessError> {
        let name = deployment.name.clone();

        // Check if already exists
        if self.deployments.read().contains_key(&name) {
            return Err(ServerlessError::FunctionExists(name));
        }

        // Validate deployment
        self.validate_deployment(&deployment)?;

        // Set status to deploying
        deployment.status = DeploymentStatus::Deploying;

        // Deploy to regions
        let regions = if deployment.regions.is_empty() {
            vec![self.config.default_region.clone()]
        } else {
            deployment.regions.clone()
        };

        for region in &regions {
            if !self.config.regions.contains(region) {
                return Err(ServerlessError::RegionNotAvailable(region.clone()));
            }
        }

        // Pre-warm instances if configured
        if self.config.prewarm.prewarm_on_deploy {
            self.prewarm_function(&name, self.config.prewarm.min_warm_instances)?;
        }

        // Mark as active
        deployment.status = DeploymentStatus::Active;

        // Store deployment
        let id = deployment.id.clone();
        self.deployments.write().insert(name, deployment);

        info!("Deployed function: {}", id);
        Ok(id)
    }

    /// Undeploy a function
    pub async fn undeploy(&self, name: &str) -> Result<(), ServerlessError> {
        let mut deployments = self.deployments.write();

        if let Some(mut deployment) = deployments.remove(name) {
            deployment.status = DeploymentStatus::Deleted;

            // Clean up warm instances
            self.warm_instances.write().remove(name);

            info!("Undeployed function: {}", name);
            Ok(())
        } else {
            Err(ServerlessError::FunctionNotFound(name.to_string()))
        }
    }

    /// Invoke a function
    pub async fn invoke(
        &self,
        request: InvocationRequest,
    ) -> Result<InvocationResult, ServerlessError> {
        let start = Instant::now();

        // Get deployment
        let deployment = self
            .deployments
            .read()
            .get(&request.function_name)
            .cloned()
            .ok_or_else(|| ServerlessError::FunctionNotFound(request.function_name.clone()))?;

        // Check deployment is active
        if deployment.status != DeploymentStatus::Active {
            return Err(ServerlessError::InvocationFailed(format!(
                "function not active: {:?}",
                deployment.status
            )));
        }

        // Select region
        let region = self.select_region(&request, &deployment)?;

        // Select node
        let node = self
            .edge_network
            .select_node(&region, &request.function_name)
            .ok_or_else(|| ServerlessError::InvocationFailed("no available nodes".to_string()))?;

        // Track metrics
        self.metrics.invocations.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .active_executions
            .fetch_add(1, Ordering::Relaxed);

        // Check for warm instance
        let cold_start = !self.try_use_warm_instance(&request.function_name);

        if cold_start {
            self.metrics.cold_starts.fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics.warm_starts.fetch_add(1, Ordering::Relaxed);
        }

        // Execute function (placeholder - would call actual WASM runtime)
        let result = self
            .execute_function(&request, &deployment, &node, cold_start)
            .await;

        let duration_ms = start.elapsed().as_millis() as u64;
        self.metrics
            .total_execution_time_ms
            .fetch_add(duration_ms, Ordering::Relaxed);
        self.metrics
            .active_executions
            .fetch_sub(1, Ordering::Relaxed);

        match &result {
            Ok(r) if r.success => {
                self.metrics.successes.fetch_add(1, Ordering::Relaxed);
            }
            Ok(_) | Err(_) => {
                self.metrics.failures.fetch_add(1, Ordering::Relaxed);
            }
        }

        result
    }

    fn validate_deployment(&self, deployment: &FunctionDeployment) -> Result<(), ServerlessError> {
        if deployment.name.is_empty() {
            return Err(ServerlessError::ConfigError(
                "function name required".to_string(),
            ));
        }

        if deployment.spec.wasm_bytes.is_empty() {
            return Err(ServerlessError::ConfigError(
                "WASM bytes required".to_string(),
            ));
        }

        if deployment.config.resources.memory_limit > self.config.default_memory_limit * 4 {
            return Err(ServerlessError::ResourceLimitExceeded(
                "memory limit too high".to_string(),
            ));
        }

        Ok(())
    }

    fn select_region(
        &self,
        request: &InvocationRequest,
        deployment: &FunctionDeployment,
    ) -> Result<String, ServerlessError> {
        // Use preferred region if specified and available
        if let Some(ref preferred) = request.preferred_region {
            if deployment.regions.contains(preferred) || deployment.regions.is_empty() {
                return Ok(preferred.clone());
            }
        }

        // Use geo-routing if coordinates available
        if let Some((lat, lon)) = request.context.source_coords {
            let coords = GeoCoordinates {
                latitude: lat,
                longitude: lon,
            };

            if let Some(location) = self.edge_network.find_nearest_location(&coords) {
                if deployment.regions.is_empty() || deployment.regions.contains(&location.region) {
                    return Ok(location.region);
                }
            }
        }

        // Fall back to first available region
        if let Some(region) = deployment.regions.first() {
            Ok(region.clone())
        } else {
            Ok(self.config.default_region.clone())
        }
    }

    fn try_use_warm_instance(&self, function_name: &str) -> bool {
        let mut instances = self.warm_instances.write();

        if let Some(warm_list) = instances.get_mut(function_name) {
            for instance in warm_list.iter_mut() {
                if !instance.in_use.swap(true, Ordering::SeqCst) {
                    return true;
                }
            }
        }

        false
    }

    fn prewarm_function(&self, function_name: &str, count: usize) -> Result<(), ServerlessError> {
        let mut instances = self.warm_instances.write();
        let warm_list = instances.entry(function_name.to_string()).or_default();

        let now = Instant::now();
        for i in 0..count {
            warm_list.push(WarmInstance {
                id: format!("warm_{}_{}", function_name, i),
                function_id: function_name.to_string(),
                created_at: now,
                last_used: now,
                in_use: AtomicBool::new(false),
            });
        }

        debug!("Pre-warmed {} instances for {}", count, function_name);
        Ok(())
    }

    async fn execute_function(
        &self,
        request: &InvocationRequest,
        deployment: &FunctionDeployment,
        node: &EdgeNode,
        cold_start: bool,
    ) -> Result<InvocationResult, ServerlessError> {
        let start = Instant::now();

        // Timeout
        let timeout_ms = request
            .timeout_ms
            .unwrap_or(deployment.config.timeout_ms)
            .min(self.config.default_timeout_ms);

        // Execute with timeout (placeholder for actual WASM execution)
        let timeout = Duration::from_millis(timeout_ms);

        // Simulated execution - in production this would call the WASM executor
        let execution_result = tokio::time::timeout(timeout, async {
            // Placeholder: actual execution would happen here
            // let executor = WasmExecutor::new(&deployment.spec.wasm_bytes)?;
            // executor.call(&deployment.spec.handler, &request.payload)?;
            Ok::<_, ServerlessError>(vec![])
        })
        .await;

        let duration_ms = start.elapsed().as_millis() as u64;

        match execution_result {
            Ok(Ok(output)) => Ok(InvocationResult {
                request_id: request.context.request_id.clone(),
                success: true,
                payload: Some(output),
                error: None,
                duration_ms,
                cold_start,
                region: node.location_id.clone(),
                node_id: node.id.clone(),
                memory_used: 0, // Would be tracked during execution
                logs: vec![],
            }),
            Ok(Err(e)) => Ok(InvocationResult {
                request_id: request.context.request_id.clone(),
                success: false,
                payload: None,
                error: Some(e.to_string()),
                duration_ms,
                cold_start,
                region: node.location_id.clone(),
                node_id: node.id.clone(),
                memory_used: 0,
                logs: vec![],
            }),
            Err(_) => {
                self.metrics.timeouts.fetch_add(1, Ordering::Relaxed);
                Err(ServerlessError::Timeout(timeout_ms))
            }
        }
    }

    /// Get deployment
    pub fn get_deployment(&self, name: &str) -> Option<FunctionDeployment> {
        self.deployments.read().get(name).cloned()
    }

    /// List deployments
    pub fn list_deployments(&self) -> Vec<FunctionDeployment> {
        self.deployments.read().values().cloned().collect()
    }

    /// Get metrics
    pub fn get_metrics(&self) -> super::MetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Get edge network
    pub fn edge_network(&self) -> Arc<EdgeNetwork> {
        Arc::clone(&self.edge_network)
    }

    /// Update deployment
    pub fn update_deployment(
        &self,
        name: &str,
        updater: impl FnOnce(&mut FunctionDeployment),
    ) -> Result<(), ServerlessError> {
        let mut deployments = self.deployments.write();

        if let Some(deployment) = deployments.get_mut(name) {
            updater(deployment);
            deployment.updated_at = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            Ok(())
        } else {
            Err(ServerlessError::FunctionNotFound(name.to_string()))
        }
    }
}

impl Default for EdgeRuntime {
    fn default() -> Self {
        Self::new(ServerlessConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serverless::deployment::FunctionDeployment;

    #[tokio::test]
    async fn test_runtime_lifecycle() {
        let runtime = EdgeRuntime::new(ServerlessConfig::default());

        assert!(runtime.start().await.is_ok());
        assert!(runtime.stop().await.is_ok());
    }

    #[tokio::test]
    async fn test_deploy_function() {
        let runtime = EdgeRuntime::new(ServerlessConfig::default());
        runtime.start().await.unwrap();

        let deployment = FunctionDeployment::builder("test-function")
            .wasm_bytes(&[0, 1, 2, 3])
            .handler("handle")
            .build();

        let result = runtime.deploy(deployment).await;
        assert!(result.is_ok());

        let retrieved = runtime.get_deployment("test-function");
        assert!(retrieved.is_some());
    }

    #[tokio::test]
    async fn test_undeploy_function() {
        let runtime = EdgeRuntime::new(ServerlessConfig::default());
        runtime.start().await.unwrap();

        let deployment = FunctionDeployment::builder("to-delete")
            .wasm_bytes(&[0, 1, 2, 3])
            .build();

        runtime.deploy(deployment).await.unwrap();
        assert!(runtime.undeploy("to-delete").await.is_ok());
        assert!(runtime.get_deployment("to-delete").is_none());
    }

    #[tokio::test]
    async fn test_duplicate_deployment() {
        let runtime = EdgeRuntime::new(ServerlessConfig::default());
        runtime.start().await.unwrap();

        let deployment = FunctionDeployment::builder("duplicate")
            .wasm_bytes(&[0, 1, 2, 3])
            .build();

        runtime.deploy(deployment.clone()).await.unwrap();

        let result = runtime.deploy(deployment).await;
        assert!(matches!(result, Err(ServerlessError::FunctionExists(_))));
    }

    #[test]
    fn test_execution_context_default() {
        let ctx = ExecutionContext::default();
        assert!(ctx.request_id.is_empty());
        assert!(ctx.caller.is_none());
    }
}
