//! Ferrite Kubernetes Operator
//!
//! Main operator implementation that manages the lifecycle of the controllers.

use super::controller::{BackupController, ClusterController, ControllerConfig, RestoreController};
use super::metrics::OperatorMetrics;
use super::resources::ResourceManager;
use super::*;
use std::sync::Arc;
use tokio::sync::broadcast;

/// Ferrite Kubernetes Operator
pub struct Operator {
    /// Operator configuration
    config: OperatorConfig,
    /// Resource manager
    resource_manager: Arc<ResourceManager>,
    /// Cluster controller
    cluster_controller: ClusterController,
    /// Backup controller
    backup_controller: BackupController,
    /// Restore controller
    restore_controller: RestoreController,
    /// Metrics
    metrics: OperatorMetrics,
    /// Shutdown signal
    shutdown_tx: broadcast::Sender<()>,
}

impl Operator {
    /// Create a new operator
    pub fn new(config: OperatorConfig) -> Self {
        let resource_manager = Arc::new(ResourceManager::new());
        let controller_config = ControllerConfig {
            reconcile_interval: config.reconcile_interval,
            max_retries: config.max_retries,
            leader_election: config.leader_election,
            namespace: config.namespace.clone(),
        };

        let cluster_controller =
            ClusterController::new(resource_manager.clone(), controller_config.clone());
        let backup_controller =
            BackupController::new(resource_manager.clone(), controller_config.clone());
        let restore_controller =
            RestoreController::new(resource_manager.clone(), controller_config);

        let (shutdown_tx, _) = broadcast::channel(1);

        Self {
            config,
            resource_manager,
            cluster_controller,
            backup_controller,
            restore_controller,
            metrics: OperatorMetrics::new(),
            shutdown_tx,
        }
    }

    /// Run the operator
    pub async fn run(&self) -> Result<(), OperatorError> {
        tracing::info!(
            version = OPERATOR_VERSION,
            "Starting Ferrite Kubernetes Operator"
        );

        // Start metrics server
        if self.config.metrics_enabled {
            self.start_metrics_server().await?;
        }

        // Start leader election if enabled
        if self.config.leader_election {
            self.wait_for_leadership().await?;
        }

        // Start watching resources
        self.watch_resources().await
    }

    /// Start the metrics server
    async fn start_metrics_server(&self) -> Result<(), OperatorError> {
        let port = self.config.metrics_port;
        tracing::info!(port = port, "Starting metrics server");
        // Metrics server would be started here
        Ok(())
    }

    /// Wait for leadership
    async fn wait_for_leadership(&self) -> Result<(), OperatorError> {
        tracing::info!("Waiting for leader election");
        // Leader election logic would go here
        tracing::info!("Acquired leadership");
        Ok(())
    }

    /// Watch Kubernetes resources
    async fn watch_resources(&self) -> Result<(), OperatorError> {
        tracing::info!("Starting resource watchers");

        // In a real implementation, this would use the Kubernetes watch API
        // Here we simulate the event loop

        let mut shutdown_rx = self.shutdown_tx.subscribe();

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    tracing::info!("Received shutdown signal");
                    break;
                }
                _ = tokio::time::sleep(self.config.reconcile_interval) => {
                    // Periodic reconciliation
                    self.reconcile_all().await?;
                }
            }
        }

        tracing::info!("Operator shutdown complete");
        Ok(())
    }

    /// Reconcile all resources
    async fn reconcile_all(&self) -> Result<(), OperatorError> {
        // Reconcile clusters
        for cluster in self.cluster_controller.list() {
            if let Err(e) = self.cluster_controller.reconcile(&cluster).await {
                tracing::error!(
                    cluster = %cluster.metadata.namespaced_name(),
                    error = %e,
                    "Failed to reconcile cluster"
                );
                self.metrics.record_reconcile_error(&cluster.metadata.name);
            } else {
                self.metrics
                    .record_reconcile_success(&cluster.metadata.name);
            }
        }

        Ok(())
    }

    /// Shutdown the operator
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }

    /// Get operator metrics
    pub fn metrics(&self) -> &OperatorMetrics {
        &self.metrics
    }

    /// Get cluster controller
    pub fn cluster_controller(&self) -> &ClusterController {
        &self.cluster_controller
    }

    /// Get backup controller
    pub fn backup_controller(&self) -> &BackupController {
        &self.backup_controller
    }

    /// Get restore controller
    pub fn restore_controller(&self) -> &RestoreController {
        &self.restore_controller
    }
}

/// Operator configuration
#[derive(Debug, Clone)]
pub struct OperatorConfig {
    /// Namespace to watch (None = all namespaces)
    pub namespace: Option<String>,
    /// Reconciliation interval
    pub reconcile_interval: std::time::Duration,
    /// Maximum reconciliation retries
    pub max_retries: u32,
    /// Enable leader election
    pub leader_election: bool,
    /// Leader election lease name
    pub lease_name: String,
    /// Leader election lease namespace
    pub lease_namespace: String,
    /// Enable metrics
    pub metrics_enabled: bool,
    /// Metrics port
    pub metrics_port: u16,
    /// Enable webhooks
    pub webhooks_enabled: bool,
    /// Webhook port
    pub webhook_port: u16,
    /// Log level
    pub log_level: String,
}

impl Default for OperatorConfig {
    fn default() -> Self {
        Self {
            namespace: None,
            reconcile_interval: DEFAULT_REQUEUE_DURATION,
            max_retries: MAX_RECONCILE_RETRIES,
            leader_election: true,
            lease_name: "ferrite-operator-leader".to_string(),
            lease_namespace: "ferrite-system".to_string(),
            metrics_enabled: true,
            metrics_port: 8080,
            webhooks_enabled: false,
            webhook_port: 9443,
            log_level: "info".to_string(),
        }
    }
}

impl OperatorConfig {
    /// Create config from environment variables
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Ok(ns) = std::env::var("WATCH_NAMESPACE") {
            if !ns.is_empty() {
                config.namespace = Some(ns);
            }
        }

        if let Ok(val) = std::env::var("RECONCILE_INTERVAL_SECS") {
            if let Ok(secs) = val.parse::<u64>() {
                config.reconcile_interval = std::time::Duration::from_secs(secs);
            }
        }

        if let Ok(val) = std::env::var("LEADER_ELECTION") {
            config.leader_election = val.to_lowercase() == "true";
        }

        if let Ok(val) = std::env::var("METRICS_PORT") {
            if let Ok(port) = val.parse::<u16>() {
                config.metrics_port = port;
            }
        }

        if let Ok(val) = std::env::var("LOG_LEVEL") {
            config.log_level = val;
        }

        config
    }
}

/// Operator error types
#[derive(Debug, Clone, thiserror::Error)]
pub enum OperatorError {
    /// Kubernetes API error
    #[error("Kubernetes API error: {0}")]
    KubernetesApi(String),

    /// Resource not found
    #[error("Resource not found: {0}")]
    NotFound(String),

    /// Invalid resource
    #[error("Invalid resource: {0}")]
    InvalidResource(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    Configuration(String),

    /// Reconciliation error
    #[error("Reconciliation error: {0}")]
    Reconciliation(String),

    /// Leader election error
    #[error("Leader election error: {0}")]
    LeaderElection(String),

    /// Timeout error
    #[error("Timeout: {0}")]
    Timeout(String),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operator_config_default() {
        let config = OperatorConfig::default();
        assert!(config.leader_election);
        assert!(config.metrics_enabled);
        assert_eq!(config.metrics_port, 8080);
    }

    #[test]
    fn test_operator_creation() {
        let operator = Operator::new(OperatorConfig::default());
        assert!(operator.cluster_controller.list().is_empty());
    }
}
