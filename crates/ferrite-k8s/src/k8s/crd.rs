//! Custom Resource Definitions for Ferrite Kubernetes Operator
//!
//! Defines the CRDs for managing Ferrite clusters:
//! - FerriteCluster: Main cluster resource
//! - FerriteBackup: Backup management
//! - FerriteRestore: Restore operations

use super::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// FerriteCluster custom resource definition
///
/// Represents a Ferrite cluster deployment in Kubernetes
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FerriteCluster {
    /// API version
    pub api_version: String,
    /// Kind
    pub kind: String,
    /// Metadata
    pub metadata: ObjectMeta,
    /// Spec
    pub spec: FerriteClusterSpec,
    /// Status
    #[serde(default)]
    pub status: Option<FerriteClusterStatus>,
}

impl FerriteCluster {
    /// Create a new FerriteCluster
    pub fn new(name: impl Into<String>, spec: FerriteClusterSpec) -> Self {
        Self {
            api_version: "ferrite.io/v1".to_string(),
            kind: "FerriteCluster".to_string(),
            metadata: ObjectMeta::new(name),
            spec,
            status: None,
        }
    }

    /// Set namespace
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.metadata.namespace = Some(namespace.into());
        self
    }

    /// Get cluster phase
    pub fn phase(&self) -> ClusterPhase {
        self.status
            .as_ref()
            .map(|s| s.phase.clone())
            .unwrap_or(ClusterPhase::Pending)
    }

    /// Check if cluster is ready
    pub fn is_ready(&self) -> bool {
        self.phase() == ClusterPhase::Running
    }

    /// Get ready replicas
    pub fn ready_replicas(&self) -> i32 {
        self.status.as_ref().map(|s| s.ready_replicas).unwrap_or(0)
    }
}

/// FerriteCluster specification
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FerriteClusterSpec {
    /// Number of replicas
    #[serde(default = "default_replicas")]
    pub replicas: i32,
    /// Ferrite version
    #[serde(default = "default_version")]
    pub version: String,
    /// Container image
    pub image: Option<String>,
    /// Image pull policy
    #[serde(default = "default_pull_policy")]
    pub image_pull_policy: String,
    /// Image pull secrets
    #[serde(default)]
    pub image_pull_secrets: Vec<String>,
    /// Resource requirements
    #[serde(default)]
    pub resources: ResourceRequirements,
    /// Persistence configuration
    #[serde(default)]
    pub persistence: PersistenceConfig,
    /// TLS configuration
    #[serde(default)]
    pub tls: TlsConfig,
    /// Sentinel configuration
    #[serde(default)]
    pub sentinel: SentinelSpec,
    /// Cluster mode configuration
    #[serde(default)]
    pub cluster_mode: ClusterModeSpec,
    /// Ferrite configuration
    #[serde(default)]
    pub config: HashMap<String, String>,
    /// Affinity configuration
    #[serde(default)]
    pub affinity: AffinityConfig,
    /// Service configuration
    #[serde(default)]
    pub service: ServiceConfig,
    /// Metrics configuration
    #[serde(default)]
    pub metrics: MetricsConfig,
    /// Environment variables
    #[serde(default)]
    pub env: Vec<EnvVar>,
    /// Init containers
    #[serde(default)]
    pub init_containers: Vec<ContainerSpec>,
    /// Sidecar containers
    #[serde(default)]
    pub sidecars: Vec<ContainerSpec>,
    /// Pod security context
    pub security_context: Option<PodSecurityContext>,
    /// Service account name
    pub service_account_name: Option<String>,
    /// Priority class name
    pub priority_class_name: Option<String>,
    /// Termination grace period
    pub termination_grace_period_seconds: Option<i64>,
    /// Auth configuration
    #[serde(default)]
    pub auth: AuthConfig,
    /// Backup schedule configuration
    #[serde(default)]
    pub backup: Option<BackupScheduleConfig>,
    /// Rolling update strategy configuration
    #[serde(default)]
    pub rolling_update: RollingUpdateConfig,
}

fn default_replicas() -> i32 {
    1
}

fn default_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

fn default_pull_policy() -> String {
    "IfNotPresent".to_string()
}

impl Default for FerriteClusterSpec {
    fn default() -> Self {
        Self {
            replicas: default_replicas(),
            version: default_version(),
            image: None,
            image_pull_policy: default_pull_policy(),
            image_pull_secrets: Vec::new(),
            resources: ResourceRequirements::default(),
            persistence: PersistenceConfig::default(),
            tls: TlsConfig::default(),
            sentinel: SentinelSpec::default(),
            cluster_mode: ClusterModeSpec::default(),
            config: HashMap::new(),
            affinity: AffinityConfig::default(),
            service: ServiceConfig::default(),
            metrics: MetricsConfig::default(),
            env: Vec::new(),
            init_containers: Vec::new(),
            sidecars: Vec::new(),
            security_context: None,
            service_account_name: None,
            priority_class_name: None,
            termination_grace_period_seconds: None,
            auth: AuthConfig::default(),
            backup: None,
            rolling_update: RollingUpdateConfig::default(),
        }
    }
}

impl FerriteClusterSpec {
    /// Get the container image
    pub fn get_image(&self) -> String {
        self.image
            .clone()
            .unwrap_or_else(|| format!("ferrite/ferrite:{}", self.version))
    }
}

/// FerriteCluster status
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FerriteClusterStatus {
    /// Current phase
    #[serde(default)]
    pub phase: ClusterPhase,
    /// Number of replicas
    pub replicas: i32,
    /// Number of ready replicas
    pub ready_replicas: i32,
    /// Current leader
    pub leader: Option<String>,
    /// Cluster endpoint
    pub endpoint: Option<String>,
    /// Conditions
    #[serde(default)]
    pub conditions: Vec<Condition>,
    /// Observed generation
    pub observed_generation: Option<i64>,
    /// Last update time
    pub last_update_time: Option<String>,
    /// Members
    #[serde(default)]
    pub members: Vec<ClusterMember>,
}

impl FerriteClusterStatus {
    /// Add or update condition
    pub fn set_condition(&mut self, condition: Condition) {
        if let Some(pos) = self
            .conditions
            .iter()
            .position(|c| c.condition_type == condition.condition_type)
        {
            self.conditions[pos] = condition;
        } else {
            self.conditions.push(condition);
        }
    }

    /// Get condition by type
    pub fn get_condition(&self, condition_type: &str) -> Option<&Condition> {
        self.conditions
            .iter()
            .find(|c| c.condition_type == condition_type)
    }

    /// Check if condition is true
    pub fn is_condition_true(&self, condition_type: &str) -> bool {
        self.get_condition(condition_type)
            .map(|c| c.is_true())
            .unwrap_or(false)
    }
}

/// Cluster phase
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ClusterPhase {
    /// Pending creation
    #[default]
    Pending,
    /// Creating resources
    Creating,
    /// Running
    Running,
    /// Scaling
    Scaling,
    /// Upgrading
    Upgrading,
    /// Failing over
    FailingOver,
    /// Failed
    Failed,
    /// Terminating
    Terminating,
}

impl std::fmt::Display for ClusterPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterPhase::Pending => write!(f, "Pending"),
            ClusterPhase::Creating => write!(f, "Creating"),
            ClusterPhase::Running => write!(f, "Running"),
            ClusterPhase::Scaling => write!(f, "Scaling"),
            ClusterPhase::Upgrading => write!(f, "Upgrading"),
            ClusterPhase::FailingOver => write!(f, "FailingOver"),
            ClusterPhase::Failed => write!(f, "Failed"),
            ClusterPhase::Terminating => write!(f, "Terminating"),
        }
    }
}

/// Cluster member
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterMember {
    /// Pod name
    pub name: String,
    /// Pod IP
    pub ip: Option<String>,
    /// Role (primary/replica)
    pub role: MemberRole,
    /// Ready status
    pub ready: bool,
    /// Last seen time
    pub last_seen: Option<String>,
}

/// Member role
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MemberRole {
    /// Primary node
    Primary,
    /// Replica node
    Replica,
    /// Unknown role
    Unknown,
}

/// Sentinel specification
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SentinelSpec {
    /// Enable sentinel
    #[serde(default)]
    pub enabled: bool,
    /// Number of sentinel replicas
    #[serde(default = "default_sentinel_replicas")]
    pub replicas: i32,
    /// Sentinel resources
    pub resources: Option<ResourceRequirements>,
    /// Sentinel configuration
    #[serde(default)]
    pub config: HashMap<String, String>,
    /// Quorum size
    pub quorum: Option<i32>,
    /// Down after milliseconds
    pub down_after_milliseconds: Option<i64>,
    /// Failover timeout
    pub failover_timeout: Option<i64>,
}

fn default_sentinel_replicas() -> i32 {
    3
}

impl Default for SentinelSpec {
    fn default() -> Self {
        Self {
            enabled: false,
            replicas: default_sentinel_replicas(),
            resources: None,
            config: HashMap::new(),
            quorum: None,
            down_after_milliseconds: None,
            failover_timeout: None,
        }
    }
}

/// Cluster mode specification
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterModeSpec {
    /// Enable cluster mode
    #[serde(default)]
    pub enabled: bool,
    /// Number of shards
    #[serde(default = "default_shards")]
    pub shards: i32,
    /// Replicas per shard
    #[serde(default = "default_replicas_per_shard")]
    pub replicas_per_shard: i32,
}

fn default_shards() -> i32 {
    3
}

fn default_replicas_per_shard() -> i32 {
    1
}

/// Service configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceConfig {
    /// Service type
    #[serde(default = "default_service_type")]
    pub service_type: String,
    /// Annotations
    #[serde(default)]
    pub annotations: HashMap<String, String>,
    /// Load balancer IP
    pub load_balancer_ip: Option<String>,
    /// External traffic policy
    pub external_traffic_policy: Option<String>,
    /// Port
    #[serde(default = "default_port")]
    pub port: i32,
}

fn default_service_type() -> String {
    "ClusterIP".to_string()
}

fn default_port() -> i32 {
    6379
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            service_type: default_service_type(),
            annotations: HashMap::new(),
            load_balancer_ip: None,
            external_traffic_policy: None,
            port: default_port(),
        }
    }
}

/// Metrics configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricsConfig {
    /// Enable metrics
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Metrics port
    #[serde(default = "default_metrics_port")]
    pub port: i32,
    /// Service monitor for Prometheus Operator
    #[serde(default)]
    pub service_monitor: ServiceMonitorConfig,
}

fn default_true() -> bool {
    true
}

fn default_metrics_port() -> i32 {
    9187
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: default_metrics_port(),
            service_monitor: ServiceMonitorConfig::default(),
        }
    }
}

/// Service monitor configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceMonitorConfig {
    /// Create service monitor
    #[serde(default)]
    pub enabled: bool,
    /// Scrape interval
    pub interval: Option<String>,
    /// Labels
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

/// Environment variable
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnvVar {
    /// Variable name
    pub name: String,
    /// Direct value
    pub value: Option<String>,
    /// Value from secret/configmap
    pub value_from: Option<EnvVarSource>,
}

/// Environment variable source
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnvVarSource {
    /// Secret key reference
    pub secret_key_ref: Option<KeyRef>,
    /// ConfigMap key reference
    pub config_map_key_ref: Option<KeyRef>,
}

/// Key reference
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KeyRef {
    /// Name of the secret/configmap
    pub name: String,
    /// Key within the resource
    pub key: String,
}

/// Container specification
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerSpec {
    /// Container name
    pub name: String,
    /// Container image
    pub image: String,
    /// Command
    #[serde(default)]
    pub command: Vec<String>,
    /// Args
    #[serde(default)]
    pub args: Vec<String>,
    /// Environment variables
    #[serde(default)]
    pub env: Vec<EnvVar>,
    /// Resources
    pub resources: Option<ResourceRequirements>,
    /// Volume mounts
    #[serde(default)]
    pub volume_mounts: Vec<VolumeMount>,
}

/// Volume mount
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeMount {
    /// Mount name
    pub name: String,
    /// Mount path
    pub mount_path: String,
    /// Read only
    #[serde(default)]
    pub read_only: bool,
    /// Sub path
    pub sub_path: Option<String>,
}

/// Pod security context
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodSecurityContext {
    /// Run as user
    pub run_as_user: Option<i64>,
    /// Run as group
    pub run_as_group: Option<i64>,
    /// FS group
    pub fs_group: Option<i64>,
    /// Run as non-root
    pub run_as_non_root: Option<bool>,
}

/// Authentication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthConfig {
    /// Enable authentication
    #[serde(default)]
    pub enabled: bool,
    /// Password secret name
    pub password_secret: Option<String>,
    /// Password secret key
    #[serde(default = "default_password_key")]
    pub password_key: String,
}

fn default_password_key() -> String {
    "password".to_string()
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            password_secret: None,
            password_key: default_password_key(),
        }
    }
}

// ============================================================================
// RollingUpdateConfig
// ============================================================================

/// Rolling update strategy configuration for version upgrades.
///
/// Controls how pods are replaced during a rolling update, including
/// health-check timeouts, concurrency limits, and rollback behaviour.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RollingUpdateConfig {
    /// Maximum number of pods that can be unavailable during the update.
    /// Can be an absolute number (e.g. `"1"`) or percentage (e.g. `"25%"`).
    #[serde(default = "default_max_unavailable")]
    pub max_unavailable: String,
    /// Maximum number of extra pods that can be created above the desired
    /// replica count during the update.
    #[serde(default = "default_max_surge")]
    pub max_surge: String,
    /// Seconds to wait for a pod to become healthy after update before
    /// considering it failed.
    #[serde(default = "default_health_check_timeout_secs")]
    pub health_check_timeout_secs: u64,
    /// Maximum consecutive pod failures before triggering an automatic rollback.
    #[serde(default = "default_max_consecutive_failures")]
    pub max_consecutive_failures: u32,
    /// Whether to run a pre-update cluster health check (abort if degraded).
    #[serde(default = "default_true_val")]
    pub pre_update_health_check: bool,
    /// Whether to run a post-update verification pass after all pods are updated.
    #[serde(default = "default_true_val")]
    pub post_update_verify: bool,
}

fn default_max_unavailable() -> String {
    "1".to_string()
}

fn default_max_surge() -> String {
    "0".to_string()
}

fn default_health_check_timeout_secs() -> u64 {
    60
}

fn default_max_consecutive_failures() -> u32 {
    1
}

fn default_true_val() -> bool {
    true
}

impl Default for RollingUpdateConfig {
    fn default() -> Self {
        Self {
            max_unavailable: default_max_unavailable(),
            max_surge: default_max_surge(),
            health_check_timeout_secs: default_health_check_timeout_secs(),
            max_consecutive_failures: default_max_consecutive_failures(),
            pre_update_health_check: true,
            post_update_verify: true,
        }
    }
}

impl RollingUpdateConfig {
    /// Parse `max_unavailable` into an absolute count for a given desired replica count.
    pub fn max_unavailable_count(&self, replicas: i32) -> i32 {
        parse_int_or_percent(&self.max_unavailable, replicas).max(1)
    }

    /// Parse `max_surge` into an absolute count for a given desired replica count.
    pub fn max_surge_count(&self, replicas: i32) -> i32 {
        parse_int_or_percent(&self.max_surge, replicas)
    }
}

/// Parse a value that can be either an absolute integer or a percentage string.
fn parse_int_or_percent(value: &str, total: i32) -> i32 {
    if let Some(pct_str) = value.strip_suffix('%') {
        if let Ok(pct) = pct_str.parse::<f64>() {
            return ((pct / 100.0) * total as f64).ceil() as i32;
        }
    }
    value.parse::<i32>().unwrap_or(1)
}

// ============================================================================
// BackupScheduleConfig (cluster-level backup configuration)
// ============================================================================

/// Backup schedule configuration embedded in a FerriteCluster spec.
///
/// Defines automatic backup scheduling and storage for the cluster.
/// For one-off backups, use the `FerriteBackup` CRD instead.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupScheduleConfig {
    /// Enable automatic scheduled backups
    #[serde(default)]
    pub enabled: bool,
    /// Cron expression for backup schedule (e.g. `"0 2 * * *"` for daily at 2 AM)
    pub schedule: Option<String>,
    /// Backup storage destination
    pub destination: Option<BackupStorage>,
    /// Retention policy for automatic backups
    #[serde(default)]
    pub retention: RetentionPolicy,
}

// ============================================================================
// FerriteBackup CRD
// ============================================================================

/// FerriteBackup custom resource definition
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FerriteBackup {
    /// API version
    pub api_version: String,
    /// Kind
    pub kind: String,
    /// Metadata
    pub metadata: ObjectMeta,
    /// Spec
    pub spec: FerriteBackupSpec,
    /// Status
    #[serde(default)]
    pub status: Option<FerriteBackupStatus>,
}

impl FerriteBackup {
    /// Create a new FerriteBackup
    pub fn new(name: impl Into<String>, spec: FerriteBackupSpec) -> Self {
        Self {
            api_version: "ferrite.io/v1".to_string(),
            kind: "FerriteBackup".to_string(),
            metadata: ObjectMeta::new(name),
            spec,
            status: None,
        }
    }

    /// Get backup phase
    pub fn phase(&self) -> BackupPhase {
        self.status
            .as_ref()
            .map(|s| s.phase.clone())
            .unwrap_or(BackupPhase::Pending)
    }
}

/// FerriteBackup specification
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FerriteBackupSpec {
    /// Cluster to backup
    pub cluster_name: String,
    /// Backup storage location
    pub storage: BackupStorage,
    /// Schedule (cron format) for scheduled backups
    pub schedule: Option<String>,
    /// Retention policy
    #[serde(default)]
    pub retention: RetentionPolicy,
    /// Backup type
    #[serde(default)]
    pub backup_type: BackupType,
}

/// Backup storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupStorage {
    /// Storage type
    #[serde(rename = "type")]
    pub storage_type: BackupStorageType,
    /// S3 configuration
    pub s3: Option<S3Config>,
    /// GCS configuration
    pub gcs: Option<GcsConfig>,
    /// Azure Blob configuration
    pub azure: Option<AzureConfig>,
    /// Local PVC configuration
    pub local: Option<LocalConfig>,
}

/// Backup storage type
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackupStorageType {
    /// AWS S3
    S3,
    /// Google Cloud Storage
    Gcs,
    /// Azure Blob Storage
    Azure,
    /// Local PVC
    Local,
}

/// S3 configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct S3Config {
    /// Bucket name
    pub bucket: String,
    /// Region
    pub region: Option<String>,
    /// Endpoint (for S3-compatible storage)
    pub endpoint: Option<String>,
    /// Path prefix
    pub prefix: Option<String>,
    /// Credentials secret
    pub credentials_secret: String,
}

/// GCS configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GcsConfig {
    /// Bucket name
    pub bucket: String,
    /// Path prefix
    pub prefix: Option<String>,
    /// Credentials secret
    pub credentials_secret: String,
}

/// Azure Blob configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AzureConfig {
    /// Container name
    pub container: String,
    /// Path prefix
    pub prefix: Option<String>,
    /// Credentials secret
    pub credentials_secret: String,
}

/// Local configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalConfig {
    /// PVC name
    pub pvc_name: String,
    /// Sub path
    pub path: Option<String>,
}

/// Retention policy
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetentionPolicy {
    /// Keep last N backups
    #[serde(default = "default_keep_last")]
    pub keep_last: i32,
    /// Keep daily backups for N days
    pub keep_daily: Option<i32>,
    /// Keep weekly backups for N weeks
    pub keep_weekly: Option<i32>,
    /// Keep monthly backups for N months
    pub keep_monthly: Option<i32>,
}

fn default_keep_last() -> i32 {
    5
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            keep_last: default_keep_last(),
            keep_daily: None,
            keep_weekly: None,
            keep_monthly: None,
        }
    }
}

/// Backup type
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackupType {
    /// Full RDB backup
    #[default]
    Full,
    /// AOF backup
    Aof,
    /// Both RDB and AOF
    Both,
}

/// FerriteBackup status
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FerriteBackupStatus {
    /// Current phase
    #[serde(default)]
    pub phase: BackupPhase,
    /// Start time
    pub start_time: Option<String>,
    /// Completion time
    pub completion_time: Option<String>,
    /// Backup size in bytes
    pub backup_size: Option<i64>,
    /// Error message
    pub error: Option<String>,
    /// Backup location
    pub backup_location: Option<String>,
    /// Conditions
    #[serde(default)]
    pub conditions: Vec<Condition>,
}

/// Backup phase
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum BackupPhase {
    /// Pending
    #[default]
    Pending,
    /// In progress
    InProgress,
    /// Completed
    Completed,
    /// Failed
    Failed,
}

// ============================================================================
// FerriteRestore CRD
// ============================================================================

/// FerriteRestore custom resource definition
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FerriteRestore {
    /// API version
    pub api_version: String,
    /// Kind
    pub kind: String,
    /// Metadata
    pub metadata: ObjectMeta,
    /// Spec
    pub spec: FerriteRestoreSpec,
    /// Status
    #[serde(default)]
    pub status: Option<FerriteRestoreStatus>,
}

impl FerriteRestore {
    /// Create a new FerriteRestore
    pub fn new(name: impl Into<String>, spec: FerriteRestoreSpec) -> Self {
        Self {
            api_version: "ferrite.io/v1".to_string(),
            kind: "FerriteRestore".to_string(),
            metadata: ObjectMeta::new(name),
            spec,
            status: None,
        }
    }

    /// Get restore phase
    pub fn phase(&self) -> RestorePhase {
        self.status
            .as_ref()
            .map(|s| s.phase.clone())
            .unwrap_or(RestorePhase::Pending)
    }
}

/// FerriteRestore specification
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FerriteRestoreSpec {
    /// Target cluster name
    pub cluster_name: String,
    /// Backup to restore from
    pub backup_name: Option<String>,
    /// Direct backup location
    pub backup_location: Option<String>,
    /// Storage configuration (if not using backup_name)
    pub storage: Option<BackupStorage>,
    /// Restore options
    #[serde(default)]
    pub options: RestoreOptions,
}

/// Restore options
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RestoreOptions {
    /// Replace existing data
    #[serde(default)]
    pub replace_existing: bool,
    /// Key pattern to restore
    pub key_pattern: Option<String>,
    /// Skip keys matching pattern
    pub skip_pattern: Option<String>,
}

/// FerriteRestore status
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FerriteRestoreStatus {
    /// Current phase
    #[serde(default)]
    pub phase: RestorePhase,
    /// Start time
    pub start_time: Option<String>,
    /// Completion time
    pub completion_time: Option<String>,
    /// Keys restored
    pub keys_restored: Option<i64>,
    /// Error message
    pub error: Option<String>,
    /// Conditions
    #[serde(default)]
    pub conditions: Vec<Condition>,
}

/// Restore phase
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum RestorePhase {
    /// Pending
    #[default]
    Pending,
    /// In progress
    InProgress,
    /// Completed
    Completed,
    /// Failed
    Failed,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ferrite_cluster_creation() {
        let spec = FerriteClusterSpec {
            replicas: 3,
            ..Default::default()
        };
        let cluster = FerriteCluster::new("test-cluster", spec).with_namespace("default");

        assert_eq!(cluster.metadata.name, "test-cluster");
        assert_eq!(cluster.spec.replicas, 3);
        assert_eq!(cluster.phase(), ClusterPhase::Pending);
    }

    #[test]
    fn test_cluster_spec_defaults() {
        let spec = FerriteClusterSpec::default();
        assert_eq!(spec.replicas, 1);
        assert!(spec.persistence.enabled);
        assert!(!spec.sentinel.enabled);
    }

    #[test]
    fn test_cluster_status_conditions() {
        let mut status = FerriteClusterStatus::default();

        status.set_condition(Condition::new("Ready", true));
        assert!(status.is_condition_true("Ready"));

        status.set_condition(Condition::new("Ready", false));
        assert!(!status.is_condition_true("Ready"));
    }

    #[test]
    fn test_backup_creation() {
        let spec = FerriteBackupSpec {
            cluster_name: "my-cluster".to_string(),
            storage: BackupStorage {
                storage_type: BackupStorageType::S3,
                s3: Some(S3Config {
                    bucket: "my-bucket".to_string(),
                    region: Some("us-east-1".to_string()),
                    endpoint: None,
                    prefix: None,
                    credentials_secret: "s3-creds".to_string(),
                }),
                gcs: None,
                azure: None,
                local: None,
            },
            schedule: Some("0 2 * * *".to_string()),
            retention: RetentionPolicy::default(),
            backup_type: BackupType::Full,
        };
        let backup = FerriteBackup::new("daily-backup", spec);

        assert_eq!(backup.metadata.name, "daily-backup");
        assert_eq!(backup.phase(), BackupPhase::Pending);
    }

    #[test]
    fn test_restore_creation() {
        let spec = FerriteRestoreSpec {
            cluster_name: "my-cluster".to_string(),
            backup_name: Some("daily-backup-20231215".to_string()),
            backup_location: None,
            storage: None,
            options: RestoreOptions::default(),
        };
        let restore = FerriteRestore::new("restore-1", spec);

        assert_eq!(restore.metadata.name, "restore-1");
        assert_eq!(restore.phase(), RestorePhase::Pending);
    }

    #[test]
    fn test_cluster_phase_display() {
        assert_eq!(ClusterPhase::Running.to_string(), "Running");
        assert_eq!(ClusterPhase::FailingOver.to_string(), "FailingOver");
    }

    #[test]
    fn test_cluster_serialization_roundtrip() {
        let spec = FerriteClusterSpec {
            replicas: 3,
            version: "1.0.0".to_string(),
            ..Default::default()
        };
        let cluster = FerriteCluster::new("test-cluster", spec).with_namespace("production");

        let json = serde_json::to_string(&cluster).unwrap();
        let deserialized: FerriteCluster = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.metadata.name, "test-cluster");
        assert_eq!(deserialized.spec.replicas, 3);
        assert_eq!(deserialized.spec.version, "1.0.0");
    }

    #[test]
    fn test_backup_serialization_roundtrip() {
        let spec = FerriteBackupSpec {
            cluster_name: "my-cluster".to_string(),
            storage: BackupStorage {
                storage_type: BackupStorageType::S3,
                s3: Some(S3Config {
                    bucket: "backups".to_string(),
                    region: Some("us-west-2".to_string()),
                    endpoint: None,
                    prefix: Some("ferrite/".to_string()),
                    credentials_secret: "aws-creds".to_string(),
                }),
                gcs: None,
                azure: None,
                local: None,
            },
            schedule: Some("0 */6 * * *".to_string()),
            retention: RetentionPolicy {
                keep_last: 10,
                keep_daily: Some(7),
                keep_weekly: Some(4),
                keep_monthly: Some(3),
            },
            backup_type: BackupType::Full,
        };
        let backup = FerriteBackup::new("scheduled-backup", spec);

        let json = serde_json::to_string(&backup).unwrap();
        let deserialized: FerriteBackup = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.metadata.name, "scheduled-backup");
        assert_eq!(deserialized.spec.cluster_name, "my-cluster");
        assert_eq!(deserialized.spec.retention.keep_last, 10);
    }

    #[test]
    fn test_backup_schedule_config() {
        let config = BackupScheduleConfig {
            enabled: true,
            schedule: Some("0 2 * * *".to_string()),
            destination: None,
            retention: RetentionPolicy::default(),
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: BackupScheduleConfig = serde_json::from_str(&json).unwrap();

        assert!(deserialized.enabled);
        assert_eq!(deserialized.schedule, Some("0 2 * * *".to_string()));
    }

    #[test]
    fn test_cluster_with_backup_config() {
        let spec = FerriteClusterSpec {
            replicas: 3,
            backup: Some(BackupScheduleConfig {
                enabled: true,
                schedule: Some("0 0 * * *".to_string()),
                destination: None,
                retention: RetentionPolicy::default(),
            }),
            ..Default::default()
        };
        let cluster = FerriteCluster::new("backed-up-cluster", spec);

        assert!(cluster.spec.backup.is_some());
        assert!(cluster.spec.backup.as_ref().unwrap().enabled);
    }

    #[test]
    fn test_sentinel_spec_defaults() {
        let sentinel = SentinelSpec::default();
        assert!(!sentinel.enabled);
        assert_eq!(sentinel.replicas, 3);
    }

    #[test]
    fn test_rolling_update_config_defaults() {
        let config = RollingUpdateConfig::default();
        assert_eq!(config.max_unavailable, "1");
        assert_eq!(config.max_surge, "0");
        assert_eq!(config.health_check_timeout_secs, 60);
        assert_eq!(config.max_consecutive_failures, 1);
        assert!(config.pre_update_health_check);
        assert!(config.post_update_verify);
    }

    #[test]
    fn test_max_unavailable_absolute() {
        let config = RollingUpdateConfig {
            max_unavailable: "2".to_string(),
            ..Default::default()
        };
        assert_eq!(config.max_unavailable_count(5), 2);
    }

    #[test]
    fn test_max_unavailable_percentage() {
        let config = RollingUpdateConfig {
            max_unavailable: "25%".to_string(),
            ..Default::default()
        };
        // 25% of 8 = 2
        assert_eq!(config.max_unavailable_count(8), 2);
        // 25% of 3 = 0.75, ceil = 1, max(1) = 1
        assert_eq!(config.max_unavailable_count(3), 1);
    }

    #[test]
    fn test_max_surge_percentage() {
        let config = RollingUpdateConfig {
            max_surge: "50%".to_string(),
            ..Default::default()
        };
        assert_eq!(config.max_surge_count(4), 2);
    }

    #[test]
    fn test_rolling_update_config_serialization() {
        let config = RollingUpdateConfig {
            max_unavailable: "25%".to_string(),
            max_surge: "1".to_string(),
            health_check_timeout_secs: 120,
            max_consecutive_failures: 3,
            pre_update_health_check: true,
            post_update_verify: false,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deser: RollingUpdateConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.max_unavailable, "25%");
        assert_eq!(deser.max_surge, "1");
        assert_eq!(deser.health_check_timeout_secs, 120);
        assert!(!deser.post_update_verify);
    }
}
