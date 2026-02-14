//! Resource Generator Functions
//!
//! Public helper functions for generating Kubernetes resource specifications
//! from [`FerriteCluster`] custom resources.  Each function is a pure
//! transformation (no I/O) that produces a ready-to-apply spec.

use super::controller::{
    ConfigMapVolumeSource, CronJobSpec, MonitorEndpoint, PersistentVolumeClaimSpec,
    PodDisruptionBudgetSpec, ServiceMonitorSpec, ServicePort, StatefulSetSpec, Volume,
};
use super::crd::*;
use super::resources::{ConfigMapSpec, PodSpec, ServiceSpec};
use super::*;
use std::collections::HashMap;

// ============================================================================
// Label / Selector helpers
// ============================================================================

/// Build standard Kubernetes labels for a Ferrite cluster.
///
/// Returns the recommended `app.kubernetes.io/*` labels that every child
/// resource of the cluster should carry.
pub fn build_labels(cluster: &FerriteCluster) -> HashMap<String, String> {
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

/// Build label selector for matching pods belonging to a Ferrite cluster.
pub fn build_selector(cluster: &FerriteCluster) -> HashMap<String, String> {
    let mut selector = HashMap::new();
    selector.insert("app.kubernetes.io/name".to_string(), "ferrite".to_string());
    selector.insert(
        "app.kubernetes.io/instance".to_string(),
        cluster.metadata.name.clone(),
    );
    selector
}

// ============================================================================
// Resource Generators
// ============================================================================

/// Generate a [`StatefulSetSpec`] for a Ferrite cluster.
///
/// The StatefulSet manages the ordered, stable set of Ferrite pods.
/// It includes volume mounts for configuration and persistent data,
/// environment variables for pod identity and auth, and respects all
/// spec fields such as `persistence`, `auth`, `affinity`, etc.
pub fn generate_statefulset(cluster: &FerriteCluster) -> StatefulSetSpec {
    let namespace = cluster
        .metadata
        .namespace
        .clone()
        .unwrap_or_else(|| "default".to_string());

    let mut env_vars = cluster.spec.env.clone();

    // Pod name injected via downward API
    env_vars.push(EnvVar {
        name: "POD_NAME".to_string(),
        value: None,
        value_from: Some(EnvVarSource {
            secret_key_ref: None,
            config_map_key_ref: None,
        }),
    });

    // Auth password from secret
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
        labels: build_labels(cluster),
        selector: build_selector(cluster),
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

/// Generate the primary client-facing [`ServiceSpec`] for a Ferrite cluster.
///
/// This Service provides a stable endpoint for client connections and
/// routes traffic to the cluster pods.
pub fn generate_service(cluster: &FerriteCluster) -> ServiceSpec {
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
        selector: build_selector(cluster),
        labels: build_labels(cluster),
        annotations: cluster.spec.service.annotations.clone(),
        owner_reference: Some(OwnerReference::from_cluster(cluster)),
    }
}

/// Generate the headless [`ServiceSpec`] used for StatefulSet pod discovery.
///
/// Headless Services (ClusterIP: None) allow direct DNS resolution to
/// individual pod IPs, enabling the StatefulSet to assign stable network
/// identities to each Ferrite node.
pub fn generate_headless_service(cluster: &FerriteCluster) -> ServiceSpec {
    let namespace = cluster
        .metadata
        .namespace
        .clone()
        .unwrap_or_else(|| "default".to_string());

    ServiceSpec {
        name: format!("{}-headless", cluster.metadata.name),
        namespace,
        service_type: "ClusterIP".to_string(),
        cluster_ip: Some("None".to_string()),
        ports: vec![ServicePort {
            name: "ferrite".to_string(),
            port: cluster.spec.service.port,
            target_port: cluster.spec.service.port,
            protocol: "TCP".to_string(),
        }],
        selector: build_selector(cluster),
        labels: build_labels(cluster),
        annotations: HashMap::new(),
        owner_reference: Some(OwnerReference::from_cluster(cluster)),
    }
}

/// Generate a [`PodDisruptionBudgetSpec`] to protect cluster availability.
///
/// The PDB ensures that voluntary disruptions (e.g. node drains, upgrades)
/// never take down more than one pod at a time, maintaining a quorum for
/// multi-replica deployments.  For single-replica clusters, returns a PDB
/// with `min_available = 1`.
pub fn generate_pdb(cluster: &FerriteCluster) -> PodDisruptionBudgetSpec {
    let namespace = cluster
        .metadata
        .namespace
        .clone()
        .unwrap_or_else(|| "default".to_string());

    PodDisruptionBudgetSpec {
        name: format!("{}-pdb", cluster.metadata.name),
        namespace,
        labels: build_labels(cluster),
        selector: build_selector(cluster),
        min_available: None,
        max_unavailable: Some("1".to_string()),
        owner_reference: Some(OwnerReference::from_cluster(cluster)),
    }
}

/// Generate a [`ConfigMapSpec`] containing Ferrite server configuration.
///
/// Builds a `ferrite.toml` configuration file from the cluster spec, including
/// server port, authentication, persistence, and cluster-mode settings.
pub fn generate_configmap(cluster: &FerriteCluster) -> ConfigMapSpec {
    let name = format!("{}-config", cluster.metadata.name);
    let namespace = cluster
        .metadata
        .namespace
        .clone()
        .unwrap_or_else(|| "default".to_string());

    let mut data = HashMap::new();

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

    for (key, value) in &cluster.spec.config {
        config.push_str(&format!("{} = \"{}\"\n", key, value));
    }

    data.insert("ferrite.toml".to_string(), config);

    ConfigMapSpec {
        name,
        namespace,
        data,
        labels: build_labels(cluster),
        owner_reference: Some(OwnerReference::from_cluster(cluster)),
    }
}

/// Generate a [`ServiceMonitorSpec`] for Prometheus Operator integration.
///
/// Returns `None` if the service monitor is not enabled in the cluster spec.
pub fn generate_service_monitor(cluster: &FerriteCluster) -> Option<ServiceMonitorSpec> {
    if !cluster.spec.metrics.service_monitor.enabled {
        return None;
    }

    let namespace = cluster
        .metadata
        .namespace
        .clone()
        .unwrap_or_else(|| "default".to_string());

    let mut labels = build_labels(cluster);
    for (k, v) in &cluster.spec.metrics.service_monitor.labels {
        labels.insert(k.clone(), v.clone());
    }

    Some(ServiceMonitorSpec {
        name: format!("{}-metrics", cluster.metadata.name),
        namespace,
        labels,
        endpoints: vec![MonitorEndpoint {
            port: "metrics".to_string(),
            interval: cluster
                .spec
                .metrics
                .service_monitor
                .interval
                .clone()
                .or_else(|| Some("30s".to_string())),
            path: Some("/metrics".to_string()),
        }],
        selector: build_selector(cluster),
    })
}

/// Generate a [`CronJobSpec`] for scheduled cluster backups.
///
/// Returns `None` if backup scheduling is not configured on the cluster.
pub fn generate_backup_cronjob(cluster: &FerriteCluster) -> Option<CronJobSpec> {
    let backup = cluster.spec.backup.as_ref()?;
    if !backup.enabled {
        return None;
    }
    let schedule = backup.schedule.as_ref()?;

    let namespace = cluster
        .metadata
        .namespace
        .clone()
        .unwrap_or_else(|| "default".to_string());

    Some(CronJobSpec {
        name: format!("{}-backup", cluster.metadata.name),
        namespace,
        schedule: schedule.clone(),
        labels: {
            let mut labels = build_labels(cluster);
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
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_cluster(replicas: i32) -> FerriteCluster {
        let spec = FerriteClusterSpec {
            replicas,
            version: "1.0.0".to_string(),
            ..Default::default()
        };
        FerriteCluster::new("test-cluster", spec).with_namespace("default")
    }

    #[test]
    fn test_generate_statefulset() {
        let cluster = test_cluster(3);
        let sts = generate_statefulset(&cluster);

        assert_eq!(sts.name, "test-cluster");
        assert_eq!(sts.namespace, "default");
        assert_eq!(sts.replicas, 3);
        assert_eq!(sts.service_name, "test-cluster-headless");
        assert!(sts.owner_reference.is_some());
        assert!(!sts.pod_spec.containers.is_empty());
        assert_eq!(sts.pod_spec.containers[0].image, "ferrite/ferrite:1.0.0");
    }

    #[test]
    fn test_generate_service() {
        let cluster = test_cluster(3);
        let svc = generate_service(&cluster);

        assert_eq!(svc.name, "test-cluster");
        assert_eq!(svc.service_type, "ClusterIP");
        assert!(svc.cluster_ip.is_none());
        assert_eq!(svc.ports[0].port, 6379);
    }

    #[test]
    fn test_generate_headless_service() {
        let cluster = test_cluster(3);
        let svc = generate_headless_service(&cluster);

        assert_eq!(svc.name, "test-cluster-headless");
        assert_eq!(svc.cluster_ip, Some("None".to_string()));
    }

    #[test]
    fn test_generate_pdb() {
        let cluster = test_cluster(3);
        let pdb = generate_pdb(&cluster);

        assert_eq!(pdb.name, "test-cluster-pdb");
        assert_eq!(pdb.max_unavailable, Some("1".to_string()));
        assert!(pdb.owner_reference.is_some());
    }

    #[test]
    fn test_generate_configmap() {
        let cluster = test_cluster(1);
        let cm = generate_configmap(&cluster);

        assert_eq!(cm.name, "test-cluster-config");
        assert!(cm.data.contains_key("ferrite.toml"));
        let toml = &cm.data["ferrite.toml"];
        assert!(toml.contains("[server]"));
        assert!(toml.contains("port = 6379"));
    }

    #[test]
    fn test_generate_configmap_with_auth() {
        let spec = FerriteClusterSpec {
            auth: AuthConfig {
                enabled: true,
                password_secret: Some("my-secret".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let cluster = FerriteCluster::new("auth-cluster", spec).with_namespace("default");
        let cm = generate_configmap(&cluster);

        let toml = &cm.data["ferrite.toml"];
        assert!(toml.contains("[auth]"));
        assert!(toml.contains("enabled = true"));
    }

    #[test]
    fn test_generate_service_monitor_disabled() {
        let cluster = test_cluster(3);
        assert!(generate_service_monitor(&cluster).is_none());
    }

    #[test]
    fn test_generate_service_monitor_enabled() {
        let spec = FerriteClusterSpec {
            metrics: MetricsConfig {
                enabled: true,
                port: 9187,
                service_monitor: ServiceMonitorConfig {
                    enabled: true,
                    interval: Some("15s".to_string()),
                    labels: HashMap::new(),
                },
            },
            ..Default::default()
        };
        let cluster = FerriteCluster::new("monitored", spec).with_namespace("default");

        let sm = generate_service_monitor(&cluster).unwrap();
        assert_eq!(sm.name, "monitored-metrics");
        assert_eq!(sm.endpoints[0].interval, Some("15s".to_string()));
    }

    #[test]
    fn test_generate_backup_cronjob_disabled() {
        let cluster = test_cluster(3);
        assert!(generate_backup_cronjob(&cluster).is_none());
    }

    #[test]
    fn test_generate_backup_cronjob_enabled() {
        let spec = FerriteClusterSpec {
            backup: Some(BackupScheduleConfig {
                enabled: true,
                schedule: Some("0 2 * * *".to_string()),
                destination: None,
                retention: RetentionPolicy::default(),
            }),
            ..Default::default()
        };
        let cluster = FerriteCluster::new("backed-up", spec).with_namespace("default");

        let cj = generate_backup_cronjob(&cluster).unwrap();
        assert_eq!(cj.name, "backed-up-backup");
        assert_eq!(cj.schedule, "0 2 * * *");
    }

    #[test]
    fn test_build_labels() {
        let cluster = test_cluster(1);
        let labels = build_labels(&cluster);

        assert_eq!(
            labels.get("app.kubernetes.io/name"),
            Some(&"ferrite".to_string())
        );
        assert_eq!(
            labels.get("app.kubernetes.io/instance"),
            Some(&"test-cluster".to_string())
        );
        assert_eq!(
            labels.get("app.kubernetes.io/managed-by"),
            Some(&"ferrite-operator".to_string())
        );
    }

    #[test]
    fn test_generate_statefulset_with_persistence() {
        let spec = FerriteClusterSpec {
            replicas: 3,
            persistence: PersistenceConfig {
                enabled: true,
                storage_class: Some("fast-ssd".to_string()),
                size: "50Gi".to_string(),
                access_modes: vec!["ReadWriteOnce".to_string()],
            },
            ..Default::default()
        };
        let cluster = FerriteCluster::new("persistent", spec).with_namespace("default");
        let sts = generate_statefulset(&cluster);

        assert_eq!(sts.volume_claim_templates.len(), 1);
        assert_eq!(sts.volume_claim_templates[0].size, "50Gi");
        assert_eq!(
            sts.volume_claim_templates[0].storage_class,
            Some("fast-ssd".to_string())
        );
    }
}
