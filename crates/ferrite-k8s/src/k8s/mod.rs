//! Kubernetes Operator for Ferrite
//!
//! Provides a complete Kubernetes operator for managing Ferrite clusters,
//! including automatic provisioning, scaling, failover, and backup/restore.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                       Ferrite Kubernetes Operator                       │
//! ├─────────────────────────────────────────────────────────────────────────┤
//! │                                                                         │
//! │  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐       │
//! │  │  CRD Controller │   │ Cluster Manager │   │ Backup Manager  │       │
//! │  │                 │   │                 │   │                 │       │
//! │  │ - FerriteCluster│   │ - Provisioning  │   │ - Scheduled     │       │
//! │  │ - FerriteBackup │   │ - Scaling       │   │ - On-demand     │       │
//! │  │ - FerriteRestore│   │ - Failover      │   │ - Restore       │       │
//! │  └────────┬────────┘   └────────┬────────┘   └────────┬────────┘       │
//! │           │                     │                     │                 │
//! │           ▼                     ▼                     ▼                 │
//! │  ┌─────────────────────────────────────────────────────────────────┐   │
//! │  │                     Kubernetes API Server                        │   │
//! │  └─────────────────────────────────────────────────────────────────┘   │
//! │                                                                         │
//! │  ┌─────────────────────────────────────────────────────────────────┐   │
//! │  │                       Ferrite Cluster                            │   │
//! │  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │   │
//! │  │  │ Primary  │  │ Replica  │  │ Replica  │  │ Sentinel │        │   │
//! │  │  │  Pod     │  │  Pod 1   │  │  Pod 2   │  │  Pods    │        │   │
//! │  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘        │   │
//! │  └─────────────────────────────────────────────────────────────────┘   │
//! │                                                                         │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Custom Resource Definitions
//!
//! - `FerriteCluster`: Main resource for deploying Ferrite clusters
//! - `FerriteBackup`: Resource for managing backups
//! - `FerriteRestore`: Resource for restoring from backups
//!
//! # Example
//!
//! ```yaml
//! apiVersion: ferrite.io/v1
//! kind: FerriteCluster
//! metadata:
//!   name: my-ferrite-cluster
//! spec:
//!   replicas: 3
//!   version: "0.1.0"
//!   resources:
//!     memory: "4Gi"
//!     cpu: "2"
//!   persistence:
//!     enabled: true
//!     storageClass: "fast-ssd"
//!     size: "100Gi"
//!   sentinel:
//!     enabled: true
//!     replicas: 3
//! ```

#![allow(dead_code, unused_imports, unused_variables)]
pub mod conditions;
mod controller;
mod crd;
pub mod failover;
pub mod generators;
pub mod leader_election;
mod metrics;
mod operator;
mod reconciler;
mod resources;
pub mod rolling_update;
pub mod scaling;

pub use controller::{
    BackupController, ClusterController, CronJobSpec, MonitorEndpoint, PodDisruptionBudgetSpec,
    RestoreController, ServiceMonitorSpec,
};
pub use crd::{
    BackupPhase, BackupScheduleConfig, ClusterPhase, FerriteBackup, FerriteBackupSpec,
    FerriteBackupStatus, FerriteCluster, FerriteClusterSpec, FerriteClusterStatus,
    FerriteRestore, FerriteRestoreSpec, FerriteRestoreStatus, RestorePhase,
    RollingUpdateConfig,
};
pub use conditions::{
    StatusConditionManager, CONDITION_DEGRADED, CONDITION_READY, CONDITION_SCALING,
    CONDITION_UPGRADING,
};
pub use failover::{FailoverEvent, FailoverManager, FailoverPlan, HealthStatus};
pub use generators::{
    build_labels, build_selector, generate_backup_cronjob, generate_configmap,
    generate_headless_service, generate_pdb, generate_service, generate_service_monitor,
    generate_statefulset,
};
pub use leader_election::{
    ElectorState, LeaderElectionConfig, LeaderElector, LeaderEvent,
};
pub use metrics::OperatorMetrics;
pub use operator::{Operator, OperatorConfig, OperatorError};
pub use reconciler::{ReconcileAction, ReconcileResult, Reconciler};
pub use resources::{ConfigMapSpec, PodSpec, ResourceManager, ServiceSpec};
pub use rolling_update::{
    RollingUpdateManager, UpdatePlan, UpdateProgress, UpdateStepResult, UpdateStrategy,
};
pub use scaling::{
    ScaleDirection, ScalePlan, ScaleValidation, ScalingManager, SlotMigrationPhase,
    SlotMigrationState,
};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// Kubernetes namespace
pub type Namespace = String;

/// Kubernetes resource name
pub type ResourceName = String;

/// Kubernetes labels
pub type Labels = HashMap<String, String>;

/// Kubernetes annotations
pub type Annotations = HashMap<String, String>;

/// Requeue duration for reconciliation
pub const DEFAULT_REQUEUE_DURATION: Duration = Duration::from_secs(30);

/// Maximum reconciliation retries
pub const MAX_RECONCILE_RETRIES: u32 = 5;

/// Operator version
pub const OPERATOR_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Default Ferrite image
pub const DEFAULT_FERRITE_IMAGE: &str = "ferrite/ferrite:latest";

/// Common Kubernetes resource metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMeta {
    /// Resource name
    pub name: String,
    /// Namespace
    pub namespace: Option<String>,
    /// Labels
    #[serde(default)]
    pub labels: Labels,
    /// Annotations
    #[serde(default)]
    pub annotations: Annotations,
    /// Resource version for optimistic locking
    pub resource_version: Option<String>,
    /// UID
    pub uid: Option<String>,
    /// Generation
    pub generation: Option<i64>,
}

impl ObjectMeta {
    /// Create new metadata
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            namespace: None,
            labels: HashMap::new(),
            annotations: HashMap::new(),
            resource_version: None,
            uid: None,
            generation: None,
        }
    }

    /// Set namespace
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Add label
    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.insert(key.into(), value.into());
        self
    }

    /// Add annotation
    pub fn with_annotation(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.annotations.insert(key.into(), value.into());
        self
    }

    /// Get namespaced name
    pub fn namespaced_name(&self) -> String {
        if let Some(ns) = &self.namespace {
            format!("{}/{}", ns, self.name)
        } else {
            self.name.clone()
        }
    }
}

impl Default for ObjectMeta {
    fn default() -> Self {
        Self::new("")
    }
}

/// Owner reference for Kubernetes resources
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OwnerReference {
    /// API version
    pub api_version: String,
    /// Kind
    pub kind: String,
    /// Name
    pub name: String,
    /// UID
    pub uid: String,
    /// Is controller
    #[serde(default)]
    pub controller: bool,
    /// Block owner deletion
    #[serde(default)]
    pub block_owner_deletion: bool,
}

impl OwnerReference {
    /// Create owner reference from cluster
    pub fn from_cluster(cluster: &FerriteCluster) -> Self {
        Self {
            api_version: "ferrite.io/v1".to_string(),
            kind: "FerriteCluster".to_string(),
            name: cluster.metadata.name.clone(),
            uid: cluster.metadata.uid.clone().unwrap_or_default(),
            controller: true,
            block_owner_deletion: true,
        }
    }
}

/// Condition for resource status
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Condition {
    /// Type of condition
    #[serde(rename = "type")]
    pub condition_type: String,
    /// Status (True, False, Unknown)
    pub status: String,
    /// Last transition time
    pub last_transition_time: Option<String>,
    /// Reason for the condition
    pub reason: Option<String>,
    /// Human-readable message
    pub message: Option<String>,
    /// Observed generation
    pub observed_generation: Option<i64>,
}

impl Condition {
    /// Create a new condition
    pub fn new(condition_type: impl Into<String>, status: bool) -> Self {
        Self {
            condition_type: condition_type.into(),
            status: if status {
                "True".to_string()
            } else {
                "False".to_string()
            },
            last_transition_time: Some(chrono::Utc::now().to_rfc3339()),
            reason: None,
            message: None,
            observed_generation: None,
        }
    }

    /// Set reason
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    /// Set message
    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    /// Check if condition is true
    pub fn is_true(&self) -> bool {
        self.status == "True"
    }
}

/// Resource requirements
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceRequirements {
    /// Memory limit
    pub memory: String,
    /// CPU limit
    pub cpu: String,
    /// Memory request
    pub memory_request: Option<String>,
    /// CPU request
    pub cpu_request: Option<String>,
}

impl Default for ResourceRequirements {
    fn default() -> Self {
        Self {
            memory: "1Gi".to_string(),
            cpu: "1".to_string(),
            memory_request: Some("512Mi".to_string()),
            cpu_request: Some("500m".to_string()),
        }
    }
}

/// Persistence configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistenceConfig {
    /// Enable persistence
    #[serde(default)]
    pub enabled: bool,
    /// Storage class
    pub storage_class: Option<String>,
    /// Storage size
    #[serde(default = "default_storage_size")]
    pub size: String,
    /// Access modes
    #[serde(default = "default_access_modes")]
    pub access_modes: Vec<String>,
}

fn default_storage_size() -> String {
    "10Gi".to_string()
}

fn default_access_modes() -> Vec<String> {
    vec!["ReadWriteOnce".to_string()]
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            storage_class: None,
            size: default_storage_size(),
            access_modes: default_access_modes(),
        }
    }
}

/// TLS configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TlsConfig {
    /// Enable TLS
    #[serde(default)]
    pub enabled: bool,
    /// Secret name containing TLS certs
    pub secret_name: Option<String>,
    /// Certificate authority secret
    pub ca_secret_name: Option<String>,
    /// Client auth mode
    #[serde(default)]
    pub client_auth: ClientAuthMode,
}

/// Client auth mode
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClientAuthMode {
    /// No client auth
    #[default]
    None,
    /// Optional client auth
    Want,
    /// Required client auth
    Need,
}



/// Affinity rules
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AffinityConfig {
    /// Pod anti-affinity
    pub pod_anti_affinity: Option<PodAntiAffinity>,
    /// Node selector
    #[serde(default)]
    pub node_selector: HashMap<String, String>,
    /// Tolerations
    #[serde(default)]
    pub tolerations: Vec<Toleration>,
}

/// Pod anti-affinity
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PodAntiAffinity {
    /// Preferred anti-affinity
    Preferred,
    /// Required anti-affinity
    Required,
}

/// Toleration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Toleration {
    /// Key
    pub key: Option<String>,
    /// Operator
    pub operator: Option<String>,
    /// Value
    pub value: Option<String>,
    /// Effect
    pub effect: Option<String>,
    /// Toleration seconds
    pub toleration_seconds: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_meta() {
        let meta = ObjectMeta::new("test-cluster")
            .with_namespace("default")
            .with_label("app", "ferrite")
            .with_annotation("note", "test");

        assert_eq!(meta.name, "test-cluster");
        assert_eq!(meta.namespace, Some("default".to_string()));
        assert_eq!(meta.labels.get("app"), Some(&"ferrite".to_string()));
        assert_eq!(meta.namespaced_name(), "default/test-cluster");
    }

    #[test]
    fn test_condition() {
        let cond = Condition::new("Ready", true)
            .with_reason("AllReplicasReady")
            .with_message("All replicas are ready");

        assert!(cond.is_true());
        assert_eq!(cond.reason, Some("AllReplicasReady".to_string()));
    }

    #[test]
    fn test_resource_requirements() {
        let req = ResourceRequirements::default();
        assert_eq!(req.memory, "1Gi");
        assert_eq!(req.cpu, "1");
    }

    #[test]
    fn test_persistence_config() {
        let config = PersistenceConfig::default();
        assert!(config.enabled);
        assert_eq!(config.size, "10Gi");
    }
}
