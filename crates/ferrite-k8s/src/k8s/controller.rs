//! Kubernetes Controllers for Ferrite
//!
//! Implements controllers for managing FerriteCluster, FerriteBackup, and FerriteRestore resources.

use super::crd::*;
use super::reconciler::{ReconcileAction, ReconcileContext, ReconcileResult, Reconciler};
use super::resources::ResourceManager;
use super::*;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Controller for FerriteCluster resources
pub struct ClusterController {
    /// Resource manager
    resource_manager: Arc<ResourceManager>,
    /// Cluster cache
    clusters: RwLock<HashMap<String, FerriteCluster>>,
    /// Reconciler
    reconciler: ClusterReconciler,
    /// Controller configuration
    config: ControllerConfig,
    /// Per-cluster retry contexts for exponential backoff
    retry_contexts: RwLock<HashMap<String, ReconcileContext>>,
}

impl ClusterController {
    /// Create a new cluster controller
    pub fn new(resource_manager: Arc<ResourceManager>, config: ControllerConfig) -> Self {
        Self {
            resource_manager: resource_manager.clone(),
            clusters: RwLock::new(HashMap::new()),
            reconciler: ClusterReconciler::new(resource_manager),
            config,
            retry_contexts: RwLock::new(HashMap::new()),
        }
    }

    /// Handle cluster add event
    pub async fn on_add(&self, cluster: FerriteCluster) -> Result<ReconcileResult, OperatorError> {
        let name = cluster.metadata.namespaced_name();
        tracing::info!(cluster = %name, "Cluster added");

        self.clusters.write().insert(name.clone(), cluster.clone());
        self.reconcile(&cluster).await
    }

    /// Handle cluster update event
    pub async fn on_update(
        &self,
        old: FerriteCluster,
        new: FerriteCluster,
    ) -> Result<ReconcileResult, OperatorError> {
        let name = new.metadata.namespaced_name();
        tracing::info!(cluster = %name, "Cluster updated");

        // Check if spec changed
        let spec_changed =
            serde_json::to_string(&old.spec).ok() != serde_json::to_string(&new.spec).ok();

        self.clusters.write().insert(name.clone(), new.clone());

        if spec_changed {
            self.reconcile(&new).await
        } else {
            Ok(ReconcileResult::done())
        }
    }

    /// Handle cluster delete event
    pub async fn on_delete(&self, cluster: FerriteCluster) -> Result<(), OperatorError> {
        let name = cluster.metadata.namespaced_name();
        tracing::info!(cluster = %name, "Cluster deleted");

        self.clusters.write().remove(&name);

        // Cleanup resources
        self.reconciler.cleanup(&cluster).await
    }

    /// Reconcile cluster state with retry context and exponential backoff.
    pub async fn reconcile(
        &self,
        cluster: &FerriteCluster,
    ) -> Result<ReconcileResult, OperatorError> {
        let key = cluster.metadata.namespaced_name();
        let result = self.reconciler.reconcile(cluster).await;

        match &result {
            Ok(_) => {
                let mut contexts = self.retry_contexts.write();
                if let Some(ctx) = contexts.get_mut(&key) {
                    ctx.on_success();
                }
            }
            Err(e) => {
                let mut contexts = self.retry_contexts.write();
                let ctx = contexts.entry(key.clone()).or_default();
                let retry_result = ctx.on_error(e.to_string());
                tracing::warn!(
                    cluster = %key,
                    attempt = ctx.attempt,
                    error = %e,
                    "Reconciliation failed, will retry"
                );
                return Ok(retry_result);
            }
        }

        result
    }

    /// Get cluster by name
    pub fn get(&self, namespace: &str, name: &str) -> Option<FerriteCluster> {
        let key = format!("{}/{}", namespace, name);
        self.clusters.read().get(&key).cloned()
    }

    /// List all clusters
    pub fn list(&self) -> Vec<FerriteCluster> {
        self.clusters.read().values().cloned().collect()
    }
}

/// Cluster reconciler
struct ClusterReconciler {
    resource_manager: Arc<ResourceManager>,
}

impl ClusterReconciler {
    fn new(resource_manager: Arc<ResourceManager>) -> Self {
        Self { resource_manager }
    }

    async fn reconcile(&self, cluster: &FerriteCluster) -> Result<ReconcileResult, OperatorError> {
        let name = cluster.metadata.namespaced_name();

        // Determine desired state based on spec
        let actions = self.calculate_actions(cluster).await?;

        if actions.is_empty() {
            tracing::debug!(cluster = %name, "Cluster is in desired state");
            return Ok(ReconcileResult::done());
        }

        // Execute actions
        for action in actions {
            match action {
                ReconcileAction::CreateStatefulSet(spec) => {
                    tracing::info!(cluster = %name, "Creating StatefulSet");
                    self.resource_manager.create_statefulset(&spec).await?;
                }
                ReconcileAction::UpdateStatefulSet(spec) => {
                    tracing::info!(cluster = %name, "Updating StatefulSet");
                    self.resource_manager.update_statefulset(&spec).await?;
                }
                ReconcileAction::CreateService(spec) => {
                    tracing::info!(cluster = %name, "Creating Service");
                    self.resource_manager.create_service(&spec).await?;
                }
                ReconcileAction::CreateConfigMap(spec) => {
                    tracing::info!(cluster = %name, "Creating ConfigMap");
                    self.resource_manager.create_configmap(&spec).await?;
                }
                ReconcileAction::CreatePVC(spec) => {
                    tracing::info!(cluster = %name, "Creating PVC");
                    self.resource_manager.create_pvc(&spec).await?;
                }
                ReconcileAction::ScaleStatefulSet { replicas } => {
                    tracing::info!(cluster = %name, replicas = replicas, "Scaling StatefulSet");
                    self.resource_manager
                        .scale_statefulset(cluster, replicas)
                        .await?;
                }
                ReconcileAction::UpdateStatus(status) => {
                    tracing::debug!(cluster = %name, "Updating status");
                    self.resource_manager
                        .update_cluster_status(cluster, status)
                        .await?;
                }
                ReconcileAction::DeletePod(pod_name) => {
                    tracing::info!(cluster = %name, pod = %pod_name, "Deleting pod");
                    self.resource_manager.delete_pod(&pod_name).await?;
                }
                ReconcileAction::CreatePDB(spec) => {
                    tracing::info!(cluster = %name, "Creating PodDisruptionBudget");
                    self.resource_manager.create_pdb(&spec).await?;
                }
                ReconcileAction::CreateServiceMonitor(spec) => {
                    tracing::info!(cluster = %name, "Creating ServiceMonitor");
                    self.resource_manager.create_service_monitor(&spec).await?;
                }
                ReconcileAction::CreateCronJob(spec) => {
                    tracing::info!(cluster = %name, "Creating CronJob");
                    self.resource_manager.create_cronjob(&spec).await?;
                }
                ReconcileAction::Requeue(duration) => {
                    return Ok(ReconcileResult::requeue(duration));
                }
            }
        }

        Ok(ReconcileResult::requeue(DEFAULT_REQUEUE_DURATION))
    }

    async fn calculate_actions(
        &self,
        cluster: &FerriteCluster,
    ) -> Result<Vec<ReconcileAction>, OperatorError> {
        let mut actions = Vec::new();
        let name = &cluster.metadata.name;
        let namespace = cluster.metadata.namespace.as_deref().unwrap_or("default");

        // Check if ConfigMap exists
        if !self
            .resource_manager
            .configmap_exists(namespace, &format!("{}-config", name))
            .await?
        {
            let configmap_spec = self.build_configmap_spec(cluster);
            actions.push(ReconcileAction::CreateConfigMap(configmap_spec));
        }

        // Check if headless service exists
        if !self
            .resource_manager
            .service_exists(namespace, &format!("{}-headless", name))
            .await?
        {
            let headless_svc = self.build_headless_service_spec(cluster);
            actions.push(ReconcileAction::CreateService(headless_svc));
        }

        // Check if main service exists
        if !self
            .resource_manager
            .service_exists(namespace, name)
            .await?
        {
            let service_spec = self.build_service_spec(cluster);
            actions.push(ReconcileAction::CreateService(service_spec));
        }

        // Check if StatefulSet exists
        let statefulset = self
            .resource_manager
            .get_statefulset(namespace, name)
            .await?;
        match statefulset {
            Some(current) => {
                // Check if scaling needed
                if current.replicas != cluster.spec.replicas {
                    actions.push(ReconcileAction::ScaleStatefulSet {
                        replicas: cluster.spec.replicas,
                    });

                    // Update status to Scaling
                    let mut status = cluster.status.clone().unwrap_or_default();
                    status.phase = ClusterPhase::Scaling;
                    actions.push(ReconcileAction::UpdateStatus(status));
                }

                // Check if update needed
                if self.needs_update(cluster, &current) {
                    let sts_spec = self.build_statefulset_spec(cluster);
                    actions.push(ReconcileAction::UpdateStatefulSet(sts_spec));
                }
            }
            None => {
                // Create StatefulSet
                let sts_spec = self.build_statefulset_spec(cluster);
                actions.push(ReconcileAction::CreateStatefulSet(sts_spec));

                // Update status to Creating
                let mut status = cluster.status.clone().unwrap_or_default();
                status.phase = ClusterPhase::Creating;
                actions.push(ReconcileAction::UpdateStatus(status));
            }
        }

        // Check cluster health and update status
        if actions.is_empty() {
            if let Some(status) = self.check_cluster_health(cluster).await? {
                // Always update status to reflect latest state
                actions.push(ReconcileAction::UpdateStatus(status));
            }
        }

        // Check if PodDisruptionBudget exists (only for multi-replica clusters)
        if cluster.spec.replicas > 1
            && !self
                .resource_manager
                .pdb_exists(namespace, &format!("{}-pdb", name))
                .await?
        {
            let pdb_spec = self.build_pdb_spec(cluster);
            actions.push(ReconcileAction::CreatePDB(pdb_spec));
        }

        // Check if ServiceMonitor should be created
        if cluster.spec.metrics.service_monitor.enabled
            && !self
                .resource_manager
                .service_monitor_exists(namespace, &format!("{}-metrics", name))
                .await?
        {
            let sm_spec = self.build_service_monitor_spec(cluster);
            actions.push(ReconcileAction::CreateServiceMonitor(sm_spec));
        }

        // Check if backup CronJob should be created
        if let Some(backup) = &cluster.spec.backup {
            if backup.enabled {
                if let Some(schedule) = &backup.schedule {
                    let cronjob_name = format!("{}-backup", name);
                    if !self
                        .resource_manager
                        .cronjob_exists(namespace, &cronjob_name)
                        .await?
                    {
                        let cronjob_spec = self.build_backup_cronjob_spec(cluster, schedule);
                        actions.push(ReconcileAction::CreateCronJob(cronjob_spec));
                    }
                }
            }
        }

        Ok(actions)
    }

    fn needs_update(&self, cluster: &FerriteCluster, current: &StatefulSetInfo) -> bool {
        // Check if image changed
        if current.image != cluster.spec.get_image() {
            return true;
        }

        // Check if resources changed
        // (simplified comparison)
        false
    }

    async fn check_cluster_health(
        &self,
        cluster: &FerriteCluster,
    ) -> Result<Option<FerriteClusterStatus>, OperatorError> {
        let namespace = cluster.metadata.namespace.as_deref().unwrap_or("default");
        let name = &cluster.metadata.name;

        let pods = self.resource_manager.list_pods(namespace, name).await?;

        let ready_count = pods.iter().filter(|p| p.ready).count() as i32;
        let total_count = pods.len() as i32;

        let phase = if ready_count == cluster.spec.replicas {
            ClusterPhase::Running
        } else if ready_count > 0 {
            ClusterPhase::Creating
        } else {
            ClusterPhase::Pending
        };

        let mut status = cluster.status.clone().unwrap_or_default();
        status.phase = phase.clone();
        status.replicas = total_count;
        status.ready_replicas = ready_count;
        status.last_update_time = Some(chrono::Utc::now().to_rfc3339());

        // Update members list
        status.members = pods
            .iter()
            .map(|p| ClusterMember {
                name: p.name.clone(),
                ip: p.ip.clone(),
                role: if p.is_primary {
                    MemberRole::Primary
                } else {
                    MemberRole::Replica
                },
                ready: p.ready,
                last_seen: Some(chrono::Utc::now().to_rfc3339()),
            })
            .collect();

        // Find leader
        status.leader = pods.iter().find(|p| p.is_primary).map(|p| p.name.clone());

        // Use typed conditions via StatusConditionManager
        {
            use super::conditions::StatusConditionManager;
            let mut mgr = StatusConditionManager::new(&mut status);

            // Ready condition
            mgr.set_ready(ready_count, cluster.spec.replicas);

            // Degraded condition
            let primary_healthy = pods.iter().any(|p| p.is_primary && p.ready);
            if !primary_healthy && !pods.is_empty() {
                if let Some(primary) = pods.iter().find(|p| p.is_primary) {
                    mgr.set_degraded_primary_unhealthy(&primary.name);
                }
            } else if ready_count < cluster.spec.replicas && ready_count > 0 {
                if cluster.spec.sentinel.enabled {
                    let quorum =
                        super::scaling::ScalingManager::minimum_quorum(cluster.spec.replicas);
                    if ready_count < quorum {
                        mgr.set_degraded_quorum_at_risk(ready_count, quorum);
                    } else {
                        mgr.set_degraded_partial_failure(ready_count, cluster.spec.replicas);
                    }
                } else {
                    mgr.set_degraded_partial_failure(ready_count, cluster.spec.replicas);
                }
            } else {
                mgr.clear_degraded();
            }

            // Scaling / Upgrading conditions are only cleared here;
            // they are set by the scaling and rolling-update managers.
            if phase == ClusterPhase::Running {
                mgr.clear_scaling();
                mgr.clear_upgrading();
            }
        }

        Ok(Some(status))
    }

    fn build_configmap_spec(&self, cluster: &FerriteCluster) -> ConfigMapSpec {
        let name = format!("{}-config", cluster.metadata.name);
        let namespace = cluster
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string());

        let mut data = HashMap::new();

        // Build ferrite.toml configuration
        let mut config = String::new();
        config.push_str("[server]\n");
        config.push_str(&format!("port = {}\n", cluster.spec.service.port));

        if cluster.spec.auth.enabled {
            config.push_str("\n[auth]\n");
            config.push_str("enabled = true\n");
        }

        if cluster.spec.persistence.enabled {
            config.push_str("\n[persistence]\n");
            config.push_str("enabled = true\n");
            config.push_str("dir = \"/data\"\n");
        }

        if cluster.spec.cluster_mode.enabled {
            config.push_str("\n[cluster]\n");
            config.push_str("enabled = true\n");
            config.push_str(&format!("shards = {}\n", cluster.spec.cluster_mode.shards));
        }

        // Add custom configuration
        for (key, value) in &cluster.spec.config {
            config.push_str(&format!("{} = \"{}\"\n", key, value));
        }

        data.insert("ferrite.toml".to_string(), config);

        ConfigMapSpec {
            name,
            namespace,
            data,
            labels: self.build_labels(cluster),
            owner_reference: Some(OwnerReference::from_cluster(cluster)),
        }
    }

    fn build_headless_service_spec(&self, cluster: &FerriteCluster) -> ServiceSpec {
        let name = format!("{}-headless", cluster.metadata.name);
        let namespace = cluster
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string());

        ServiceSpec {
            name,
            namespace,
            service_type: "ClusterIP".to_string(),
            cluster_ip: Some("None".to_string()),
            ports: vec![ServicePort {
                name: "ferrite".to_string(),
                port: cluster.spec.service.port,
                target_port: cluster.spec.service.port,
                protocol: "TCP".to_string(),
            }],
            selector: self.build_selector(cluster),
            labels: self.build_labels(cluster),
            annotations: HashMap::new(),
            owner_reference: Some(OwnerReference::from_cluster(cluster)),
        }
    }

    fn build_service_spec(&self, cluster: &FerriteCluster) -> ServiceSpec {
        let namespace = cluster
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string());

        ServiceSpec {
            name: cluster.metadata.name.clone(),
            namespace,
            service_type: cluster.spec.service.service_type.clone(),
            cluster_ip: None,
            ports: vec![ServicePort {
                name: "ferrite".to_string(),
                port: cluster.spec.service.port,
                target_port: cluster.spec.service.port,
                protocol: "TCP".to_string(),
            }],
            selector: self.build_selector(cluster),
            labels: self.build_labels(cluster),
            annotations: cluster.spec.service.annotations.clone(),
            owner_reference: Some(OwnerReference::from_cluster(cluster)),
        }
    }

    fn build_statefulset_spec(&self, cluster: &FerriteCluster) -> StatefulSetSpec {
        let namespace = cluster
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string());

        let mut env_vars = cluster.spec.env.clone();

        // Add pod name as env var
        env_vars.push(EnvVar {
            name: "POD_NAME".to_string(),
            value: None,
            value_from: Some(EnvVarSource {
                secret_key_ref: None,
                config_map_key_ref: None,
            }),
        });

        // Add password from secret if auth enabled
        if cluster.spec.auth.enabled {
            if let Some(secret) = &cluster.spec.auth.password_secret {
                env_vars.push(EnvVar {
                    name: "FERRITE_PASSWORD".to_string(),
                    value: None,
                    value_from: Some(EnvVarSource {
                        secret_key_ref: Some(KeyRef {
                            name: secret.clone(),
                            key: cluster.spec.auth.password_key.clone(),
                        }),
                        config_map_key_ref: None,
                    }),
                });
            }
        }

        let mut volume_mounts = vec![VolumeMount {
            name: "config".to_string(),
            mount_path: "/etc/ferrite".to_string(),
            read_only: true,
            sub_path: None,
        }];

        if cluster.spec.persistence.enabled {
            volume_mounts.push(VolumeMount {
                name: "data".to_string(),
                mount_path: "/data".to_string(),
                read_only: false,
                sub_path: None,
            });
        }

        StatefulSetSpec {
            name: cluster.metadata.name.clone(),
            namespace,
            replicas: cluster.spec.replicas,
            service_name: format!("{}-headless", cluster.metadata.name),
            labels: self.build_labels(cluster),
            selector: self.build_selector(cluster),
            pod_spec: PodSpec {
                containers: vec![ContainerSpec {
                    name: "ferrite".to_string(),
                    image: cluster.spec.get_image(),
                    command: vec![],
                    args: vec![
                        "--config".to_string(),
                        "/etc/ferrite/ferrite.toml".to_string(),
                    ],
                    env: env_vars,
                    resources: Some(cluster.spec.resources.clone()),
                    volume_mounts,
                }],
                init_containers: cluster.spec.init_containers.clone(),
                volumes: vec![Volume {
                    name: "config".to_string(),
                    config_map: Some(ConfigMapVolumeSource {
                        name: format!("{}-config", cluster.metadata.name),
                    }),
                    secret: None,
                    persistent_volume_claim: None,
                }],
                service_account_name: cluster.spec.service_account_name.clone(),
                security_context: cluster.spec.security_context.clone(),
                node_selector: cluster.spec.affinity.node_selector.clone(),
                tolerations: cluster.spec.affinity.tolerations.clone(),
                termination_grace_period_seconds: cluster.spec.termination_grace_period_seconds,
            },
            volume_claim_templates: if cluster.spec.persistence.enabled {
                vec![PersistentVolumeClaimSpec {
                    name: "data".to_string(),
                    storage_class: cluster.spec.persistence.storage_class.clone(),
                    access_modes: cluster.spec.persistence.access_modes.clone(),
                    size: cluster.spec.persistence.size.clone(),
                }]
            } else {
                vec![]
            },
            owner_reference: Some(OwnerReference::from_cluster(cluster)),
        }
    }

    fn build_labels(&self, cluster: &FerriteCluster) -> HashMap<String, String> {
        let mut labels = HashMap::new();
        labels.insert("app.kubernetes.io/name".to_string(), "ferrite".to_string());
        labels.insert(
            "app.kubernetes.io/instance".to_string(),
            cluster.metadata.name.clone(),
        );
        labels.insert(
            "app.kubernetes.io/managed-by".to_string(),
            "ferrite-operator".to_string(),
        );
        labels.insert(
            "app.kubernetes.io/version".to_string(),
            cluster.spec.version.clone(),
        );
        labels
    }

    fn build_selector(&self, cluster: &FerriteCluster) -> HashMap<String, String> {
        let mut selector = HashMap::new();
        selector.insert("app.kubernetes.io/name".to_string(), "ferrite".to_string());
        selector.insert(
            "app.kubernetes.io/instance".to_string(),
            cluster.metadata.name.clone(),
        );
        selector
    }

    fn build_pdb_spec(&self, cluster: &FerriteCluster) -> PodDisruptionBudgetSpec {
        let namespace = cluster
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string());

        PodDisruptionBudgetSpec {
            name: format!("{}-pdb", cluster.metadata.name),
            namespace,
            labels: self.build_labels(cluster),
            selector: self.build_selector(cluster),
            min_available: None,
            max_unavailable: Some("1".to_string()),
            owner_reference: Some(OwnerReference::from_cluster(cluster)),
        }
    }

    fn build_service_monitor_spec(&self, cluster: &FerriteCluster) -> ServiceMonitorSpec {
        let namespace = cluster
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string());

        let mut labels = self.build_labels(cluster);
        for (k, v) in &cluster.spec.metrics.service_monitor.labels {
            labels.insert(k.clone(), v.clone());
        }

        ServiceMonitorSpec {
            name: format!("{}-metrics", cluster.metadata.name),
            namespace,
            labels,
            endpoints: vec![MonitorEndpoint {
                port: "metrics".to_string(),
                interval: cluster.spec.metrics.service_monitor.interval.clone(),
                path: Some("/metrics".to_string()),
            }],
            selector: self.build_selector(cluster),
        }
    }

    fn build_backup_cronjob_spec(&self, cluster: &FerriteCluster, schedule: &str) -> CronJobSpec {
        let namespace = cluster
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string());

        CronJobSpec {
            name: format!("{}-backup", cluster.metadata.name),
            namespace,
            schedule: schedule.to_string(),
            labels: {
                let mut labels = self.build_labels(cluster);
                labels.insert(
                    "app.kubernetes.io/component".to_string(),
                    "backup".to_string(),
                );
                labels
            },
            container: ContainerSpec {
                name: "backup".to_string(),
                image: cluster.spec.get_image(),
                command: vec!["ferrite-backup".to_string()],
                args: vec!["--cluster".to_string(), cluster.metadata.name.clone()],
                env: vec![],
                resources: None,
                volume_mounts: vec![],
            },
            restart_policy: "OnFailure".to_string(),
            backoff_limit: 3,
            successful_jobs_history_limit: Some(3),
            failed_jobs_history_limit: Some(1),
            owner_reference: Some(OwnerReference::from_cluster(cluster)),
        }
    }

    async fn cleanup(&self, cluster: &FerriteCluster) -> Result<(), OperatorError> {
        let namespace = cluster.metadata.namespace.as_deref().unwrap_or("default");
        let name = &cluster.metadata.name;

        tracing::info!(cluster = %name, "Cleaning up cluster resources");

        // Delete StatefulSet (PVCs are handled by finalizers if needed)
        self.resource_manager
            .delete_statefulset(namespace, name)
            .await?;

        // Delete Services
        self.resource_manager
            .delete_service(namespace, name)
            .await?;
        self.resource_manager
            .delete_service(namespace, &format!("{}-headless", name))
            .await?;

        // Delete ConfigMap
        self.resource_manager
            .delete_configmap(namespace, &format!("{}-config", name))
            .await?;

        // Delete PDB
        self.resource_manager
            .delete_pdb(namespace, &format!("{}-pdb", name))
            .await?;

        // Delete ServiceMonitor
        self.resource_manager
            .delete_service_monitor(namespace, &format!("{}-metrics", name))
            .await?;

        // Delete backup CronJob
        self.resource_manager
            .delete_cronjob(namespace, &format!("{}-backup", name))
            .await?;

        Ok(())
    }
}

// ============================================================================
// Backup Controller
// ============================================================================

/// Controller for FerriteBackup resources
pub struct BackupController {
    /// Resource manager
    resource_manager: Arc<ResourceManager>,
    /// Backup cache
    backups: RwLock<HashMap<String, FerriteBackup>>,
    /// Controller configuration
    config: ControllerConfig,
}

impl BackupController {
    /// Create a new backup controller
    pub fn new(resource_manager: Arc<ResourceManager>, config: ControllerConfig) -> Self {
        Self {
            resource_manager,
            backups: RwLock::new(HashMap::new()),
            config,
        }
    }

    /// Handle backup add event
    pub async fn on_add(&self, backup: FerriteBackup) -> Result<ReconcileResult, OperatorError> {
        let name = backup.metadata.namespaced_name();
        tracing::info!(backup = %name, "Backup created");

        self.backups.write().insert(name.clone(), backup.clone());
        self.reconcile(&backup).await
    }

    /// Reconcile backup
    pub async fn reconcile(
        &self,
        backup: &FerriteBackup,
    ) -> Result<ReconcileResult, OperatorError> {
        let name = backup.metadata.namespaced_name();

        match backup.phase() {
            BackupPhase::Pending => {
                // Start backup job
                tracing::info!(backup = %name, "Starting backup");
                self.start_backup(backup).await?;
                Ok(ReconcileResult::requeue(Duration::from_secs(10)))
            }
            BackupPhase::InProgress => {
                // Check backup progress
                if self.check_backup_progress(backup).await? {
                    tracing::info!(backup = %name, "Backup completed");
                    Ok(ReconcileResult::done())
                } else {
                    Ok(ReconcileResult::requeue(Duration::from_secs(30)))
                }
            }
            BackupPhase::Completed | BackupPhase::Failed => Ok(ReconcileResult::done()),
        }
    }

    async fn start_backup(&self, backup: &FerriteBackup) -> Result<(), OperatorError> {
        let _namespace = backup.metadata.namespace.as_deref().unwrap_or("default");

        // Create backup job
        let job_spec = self.build_backup_job_spec(backup);
        self.resource_manager.create_job(&job_spec).await?;

        // Update status to InProgress
        let mut status = backup.status.clone().unwrap_or_default();
        status.phase = BackupPhase::InProgress;
        status.start_time = Some(chrono::Utc::now().to_rfc3339());
        self.resource_manager
            .update_backup_status(backup, status)
            .await?;

        Ok(())
    }

    async fn check_backup_progress(&self, backup: &FerriteBackup) -> Result<bool, OperatorError> {
        let namespace = backup.metadata.namespace.as_deref().unwrap_or("default");
        let job_name = format!("{}-backup", backup.metadata.name);

        let job_status = self
            .resource_manager
            .get_job_status(namespace, &job_name)
            .await?;

        match job_status {
            JobStatus::Succeeded => {
                let mut status = backup.status.clone().unwrap_or_default();
                status.phase = BackupPhase::Completed;
                status.completion_time = Some(chrono::Utc::now().to_rfc3339());
                self.resource_manager
                    .update_backup_status(backup, status)
                    .await?;
                Ok(true)
            }
            JobStatus::Failed(error) => {
                let mut status = backup.status.clone().unwrap_or_default();
                status.phase = BackupPhase::Failed;
                status.error = Some(error);
                status.completion_time = Some(chrono::Utc::now().to_rfc3339());
                self.resource_manager
                    .update_backup_status(backup, status)
                    .await?;
                Ok(true)
            }
            JobStatus::Running => Ok(false),
            JobStatus::Pending => Ok(false),
        }
    }

    fn build_backup_job_spec(&self, backup: &FerriteBackup) -> JobSpec {
        let namespace = backup
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string());

        JobSpec {
            name: format!("{}-backup", backup.metadata.name),
            namespace,
            labels: {
                let mut labels = HashMap::new();
                labels.insert(
                    "app.kubernetes.io/name".to_string(),
                    "ferrite-backup".to_string(),
                );
                labels.insert(
                    "ferrite.io/backup".to_string(),
                    backup.metadata.name.clone(),
                );
                labels
            },
            container: ContainerSpec {
                name: "backup".to_string(),
                image: DEFAULT_FERRITE_IMAGE.to_string(),
                command: vec!["ferrite-backup".to_string()],
                args: vec!["--cluster".to_string(), backup.spec.cluster_name.clone()],
                env: vec![],
                resources: None,
                volume_mounts: vec![],
            },
            restart_policy: "Never".to_string(),
            backoff_limit: 3,
        }
    }
}

// ============================================================================
// Restore Controller
// ============================================================================

/// Controller for FerriteRestore resources
pub struct RestoreController {
    /// Resource manager
    resource_manager: Arc<ResourceManager>,
    /// Restore cache
    restores: RwLock<HashMap<String, FerriteRestore>>,
    /// Controller configuration
    config: ControllerConfig,
}

impl RestoreController {
    /// Create a new restore controller
    pub fn new(resource_manager: Arc<ResourceManager>, config: ControllerConfig) -> Self {
        Self {
            resource_manager,
            restores: RwLock::new(HashMap::new()),
            config,
        }
    }

    /// Handle restore add event
    pub async fn on_add(&self, restore: FerriteRestore) -> Result<ReconcileResult, OperatorError> {
        let name = restore.metadata.namespaced_name();
        tracing::info!(restore = %name, "Restore created");

        self.restores.write().insert(name.clone(), restore.clone());
        self.reconcile(&restore).await
    }

    /// Reconcile restore
    pub async fn reconcile(
        &self,
        restore: &FerriteRestore,
    ) -> Result<ReconcileResult, OperatorError> {
        let name = restore.metadata.namespaced_name();

        match restore.phase() {
            RestorePhase::Pending => {
                tracing::info!(restore = %name, "Starting restore");
                self.start_restore(restore).await?;
                Ok(ReconcileResult::requeue(Duration::from_secs(10)))
            }
            RestorePhase::InProgress => {
                if self.check_restore_progress(restore).await? {
                    tracing::info!(restore = %name, "Restore completed");
                    Ok(ReconcileResult::done())
                } else {
                    Ok(ReconcileResult::requeue(Duration::from_secs(30)))
                }
            }
            RestorePhase::Completed | RestorePhase::Failed => Ok(ReconcileResult::done()),
        }
    }

    async fn start_restore(&self, restore: &FerriteRestore) -> Result<(), OperatorError> {
        // Create restore job
        let job_spec = self.build_restore_job_spec(restore);
        self.resource_manager.create_job(&job_spec).await?;

        // Update status
        let mut status = restore.status.clone().unwrap_or_default();
        status.phase = RestorePhase::InProgress;
        status.start_time = Some(chrono::Utc::now().to_rfc3339());
        self.resource_manager
            .update_restore_status(restore, status)
            .await?;

        Ok(())
    }

    async fn check_restore_progress(
        &self,
        restore: &FerriteRestore,
    ) -> Result<bool, OperatorError> {
        let namespace = restore.metadata.namespace.as_deref().unwrap_or("default");
        let job_name = format!("{}-restore", restore.metadata.name);

        let job_status = self
            .resource_manager
            .get_job_status(namespace, &job_name)
            .await?;

        match job_status {
            JobStatus::Succeeded => {
                let mut status = restore.status.clone().unwrap_or_default();
                status.phase = RestorePhase::Completed;
                status.completion_time = Some(chrono::Utc::now().to_rfc3339());
                self.resource_manager
                    .update_restore_status(restore, status)
                    .await?;
                Ok(true)
            }
            JobStatus::Failed(error) => {
                let mut status = restore.status.clone().unwrap_or_default();
                status.phase = RestorePhase::Failed;
                status.error = Some(error);
                status.completion_time = Some(chrono::Utc::now().to_rfc3339());
                self.resource_manager
                    .update_restore_status(restore, status)
                    .await?;
                Ok(true)
            }
            JobStatus::Running | JobStatus::Pending => Ok(false),
        }
    }

    fn build_restore_job_spec(&self, restore: &FerriteRestore) -> JobSpec {
        let namespace = restore
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string());

        let mut args = vec!["--cluster".to_string(), restore.spec.cluster_name.clone()];

        if let Some(backup_name) = &restore.spec.backup_name {
            args.push("--backup".to_string());
            args.push(backup_name.clone());
        }

        JobSpec {
            name: format!("{}-restore", restore.metadata.name),
            namespace,
            labels: {
                let mut labels = HashMap::new();
                labels.insert(
                    "app.kubernetes.io/name".to_string(),
                    "ferrite-restore".to_string(),
                );
                labels.insert(
                    "ferrite.io/restore".to_string(),
                    restore.metadata.name.clone(),
                );
                labels
            },
            container: ContainerSpec {
                name: "restore".to_string(),
                image: DEFAULT_FERRITE_IMAGE.to_string(),
                command: vec!["ferrite-restore".to_string()],
                args,
                env: vec![],
                resources: None,
                volume_mounts: vec![],
            },
            restart_policy: "Never".to_string(),
            backoff_limit: 3,
        }
    }
}

// ============================================================================
// Supporting Types
// ============================================================================

/// Controller configuration
#[derive(Debug, Clone)]
pub struct ControllerConfig {
    /// Reconciliation interval
    pub reconcile_interval: Duration,
    /// Maximum retries
    pub max_retries: u32,
    /// Leader election enabled
    pub leader_election: bool,
    /// Namespace to watch (None = all namespaces)
    pub namespace: Option<String>,
}

impl Default for ControllerConfig {
    fn default() -> Self {
        Self {
            reconcile_interval: DEFAULT_REQUEUE_DURATION,
            max_retries: MAX_RECONCILE_RETRIES,
            leader_election: true,
            namespace: None,
        }
    }
}

/// StatefulSet info for comparison
#[derive(Debug, Clone)]
pub struct StatefulSetInfo {
    /// Replicas
    pub replicas: i32,
    /// Container image
    pub image: String,
}

/// Job status
#[derive(Debug)]
pub enum JobStatus {
    /// Pending
    Pending,
    /// Running
    Running,
    /// Succeeded
    Succeeded,
    /// Failed with error message
    Failed(String),
}

/// StatefulSet specification
#[derive(Debug)]
pub struct StatefulSetSpec {
    /// Name
    pub name: String,
    /// Namespace
    pub namespace: String,
    /// Replicas
    pub replicas: i32,
    /// Service name
    pub service_name: String,
    /// Labels
    pub labels: HashMap<String, String>,
    /// Selector
    pub selector: HashMap<String, String>,
    /// Pod specification
    pub pod_spec: PodSpec,
    /// Volume claim templates
    pub volume_claim_templates: Vec<PersistentVolumeClaimSpec>,
    /// Owner reference
    pub owner_reference: Option<OwnerReference>,
}

/// Job specification
pub struct JobSpec {
    /// Name
    pub name: String,
    /// Namespace
    pub namespace: String,
    /// Labels
    pub labels: HashMap<String, String>,
    /// Container specification
    pub container: ContainerSpec,
    /// Restart policy
    pub restart_policy: String,
    /// Backoff limit
    pub backoff_limit: i32,
}

/// Persistent Volume Claim specification
#[derive(Debug, Clone)]
pub struct PersistentVolumeClaimSpec {
    /// Name
    pub name: String,
    /// Storage class
    pub storage_class: Option<String>,
    /// Access modes
    pub access_modes: Vec<String>,
    /// Size
    pub size: String,
}

/// Volume
#[derive(Debug, Clone)]
pub struct Volume {
    /// Name
    pub name: String,
    /// ConfigMap source
    pub config_map: Option<ConfigMapVolumeSource>,
    /// Secret source
    pub secret: Option<SecretVolumeSource>,
    /// PVC source
    pub persistent_volume_claim: Option<PvcVolumeSource>,
}

/// ConfigMap volume source
#[derive(Debug, Clone)]
pub struct ConfigMapVolumeSource {
    /// Name
    pub name: String,
}

/// Secret volume source
#[derive(Debug, Clone)]
pub struct SecretVolumeSource {
    /// Name
    pub secret_name: String,
}

/// PVC volume source
#[derive(Debug, Clone)]
pub struct PvcVolumeSource {
    /// Claim name
    pub claim_name: String,
}

/// PodDisruptionBudget specification
#[derive(Debug, Clone)]
pub struct PodDisruptionBudgetSpec {
    /// Resource name
    pub name: String,
    /// Namespace
    pub namespace: String,
    /// Labels applied to the PDB
    pub labels: HashMap<String, String>,
    /// Label selector matching pods
    pub selector: HashMap<String, String>,
    /// Minimum number of pods that must be available (e.g. "1" or "50%")
    pub min_available: Option<String>,
    /// Maximum number of pods that can be unavailable (e.g. "1" or "25%")
    pub max_unavailable: Option<String>,
    /// Owner reference
    pub owner_reference: Option<OwnerReference>,
}

/// ServiceMonitor specification for Prometheus Operator integration
#[derive(Debug, Clone)]
pub struct ServiceMonitorSpec {
    /// Resource name
    pub name: String,
    /// Namespace
    pub namespace: String,
    /// Labels applied to the ServiceMonitor
    pub labels: HashMap<String, String>,
    /// Endpoints to monitor
    pub endpoints: Vec<MonitorEndpoint>,
    /// Label selector for matching services
    pub selector: HashMap<String, String>,
}

/// Monitor endpoint configuration for ServiceMonitor
#[derive(Debug, Clone)]
pub struct MonitorEndpoint {
    /// Port name to scrape
    pub port: String,
    /// Scrape interval (e.g. "30s")
    pub interval: Option<String>,
    /// Metrics path (e.g. "/metrics")
    pub path: Option<String>,
}

/// CronJob specification for scheduled backups
#[derive(Debug, Clone)]
pub struct CronJobSpec {
    /// Resource name
    pub name: String,
    /// Namespace
    pub namespace: String,
    /// Cron schedule expression
    pub schedule: String,
    /// Labels
    pub labels: HashMap<String, String>,
    /// Job template container
    pub container: ContainerSpec,
    /// Restart policy
    pub restart_policy: String,
    /// Backoff limit per job
    pub backoff_limit: i32,
    /// Number of successful jobs to keep
    pub successful_jobs_history_limit: Option<i32>,
    /// Number of failed jobs to keep
    pub failed_jobs_history_limit: Option<i32>,
    /// Owner reference
    pub owner_reference: Option<OwnerReference>,
}

/// Service port
#[derive(Debug, Clone)]
pub struct ServicePort {
    /// Port name
    pub name: String,
    /// Port number
    pub port: i32,
    /// Target port
    pub target_port: i32,
    /// Protocol
    pub protocol: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_controller_config_default() {
        let config = ControllerConfig::default();
        assert_eq!(config.reconcile_interval, DEFAULT_REQUEUE_DURATION);
        assert_eq!(config.max_retries, MAX_RECONCILE_RETRIES);
    }

    #[test]
    fn test_cluster_reconciler_labels() {
        let resource_manager = Arc::new(ResourceManager::new());
        let reconciler = ClusterReconciler::new(resource_manager);

        let cluster = FerriteCluster::new("test", FerriteClusterSpec::default());
        let labels = reconciler.build_labels(&cluster);

        assert_eq!(
            labels.get("app.kubernetes.io/name"),
            Some(&"ferrite".to_string())
        );
        assert_eq!(
            labels.get("app.kubernetes.io/instance"),
            Some(&"test".to_string())
        );
    }
}
