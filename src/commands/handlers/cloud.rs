//! Managed cloud service command handlers
//!
//! CLOUD.* commands for managing Ferrite Cloud instances.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::OnceLock;

use bytes::Bytes;

use crate::protocol::Frame;
use ferrite_cloud::managed::billing::{BillingPeriod, CostEstimate, PricingTable, UsageMeter};
use ferrite_cloud::managed::provisioner::{
    InstanceFilter, InstanceRecord, InstanceState, InstanceType, ProvisionRequest, Provisioner,
    ProvisionerConfig,
};
use ferrite_cloud::managed::scaling::{AutoScaler, ScalingConfig};

use super::err_frame;

/// Global provisioner instance.
static PROVISIONER: OnceLock<Provisioner> = OnceLock::new();

/// Global usage meter instance.
static USAGE_METER: OnceLock<UsageMeter> = OnceLock::new();

/// Global auto-scaler instance.
static AUTO_SCALER: OnceLock<AutoScaler> = OnceLock::new();

fn provisioner() -> &'static Provisioner {
    PROVISIONER.get_or_init(|| Provisioner::new(ProvisionerConfig::default()))
}

fn usage_meter() -> &'static UsageMeter {
    USAGE_METER.get_or_init(|| UsageMeter::new(PricingTable::default()))
}

fn auto_scaler() -> &'static AutoScaler {
    AUTO_SCALER.get_or_init(|| AutoScaler::new(ScalingConfig::default()))
}

/// Dispatch a `CLOUD` subcommand.
pub fn cloud(subcommand: &str, args: &[String]) -> Frame {
    match subcommand.to_uppercase().as_str() {
        "PROVISION" => cloud_provision(args),
        "LIST" => cloud_list(args),
        "INFO" => cloud_info(args),
        "TERMINATE" => cloud_terminate(args),
        "SCALE" => cloud_scale(args),
        "COST" => cloud_cost(args),
        "USAGE" => cloud_usage(args),
        "HEALTH" => cloud_health(args),
        "HELP" => cloud_help(),
        _ => err_frame(&format!("unknown CLOUD subcommand '{}'", subcommand)),
    }
}

/// CLOUD.PROVISION name [TYPE dev|small|medium|large] [REPLICAS n] [STORAGE gb] [TLS] [PERSIST]
fn cloud_provision(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("CLOUD PROVISION requires at least a name");
    }

    let name = args[0].clone();
    let mut instance_type = InstanceType::Small;
    let mut replicas: u8 = 1;
    let mut storage_gb: u32 = 10;
    let mut enable_tls = false;
    let mut enable_persistence = false;

    let mut i = 1;
    while i < args.len() {
        match args[i].to_uppercase().as_str() {
            "TYPE" => {
                i += 1;
                if i < args.len() {
                    instance_type = match args[i].to_lowercase().as_str() {
                        "dev" => InstanceType::Dev,
                        "small" => InstanceType::Small,
                        "medium" => InstanceType::Medium,
                        "large" => InstanceType::Large,
                        _ => InstanceType::Small,
                    };
                }
            }
            "REPLICAS" => {
                i += 1;
                if i < args.len() {
                    replicas = args[i].parse().unwrap_or(1);
                }
            }
            "STORAGE" => {
                i += 1;
                if i < args.len() {
                    storage_gb = args[i].parse().unwrap_or(10);
                }
            }
            "TLS" => enable_tls = true,
            "PERSIST" => enable_persistence = true,
            _ => {}
        }
        i += 1;
    }

    let request = ProvisionRequest {
        instance_id: None,
        name,
        instance_type,
        replicas,
        storage_gb,
        enable_tls,
        enable_persistence,
        cloud_tiering: false,
        tags: HashMap::new(),
    };

    match provisioner().provision(request) {
        Ok(record) => instance_to_frame(&record),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// CLOUD.LIST [STATE running|stopped]
fn cloud_list(args: &[String]) -> Frame {
    let mut filter = InstanceFilter::default();

    let mut i = 0;
    while i < args.len() {
        if args[i].to_uppercase() == "STATE" {
            i += 1;
            if i < args.len() {
                filter.state = match args[i].to_lowercase().as_str() {
                    "running" => Some(InstanceState::Running),
                    "stopped" => Some(InstanceState::Stopped),
                    "provisioning" => Some(InstanceState::Provisioning),
                    "terminated" => Some(InstanceState::Terminated),
                    _ => None,
                };
            }
        }
        i += 1;
    }

    let instances = provisioner().list_instances(filter);
    let frames: Vec<Frame> = instances.iter().map(|r| instance_to_frame(r)).collect();
    Frame::array(frames)
}

/// CLOUD.INFO instance_id
fn cloud_info(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("CLOUD INFO requires an instance_id");
    }

    match provisioner().get_instance(&args[0]) {
        Some(record) => instance_to_frame(&record),
        None => err_frame(&format!("instance not found: {}", args[0])),
    }
}

/// CLOUD.TERMINATE instance_id
fn cloud_terminate(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("CLOUD TERMINATE requires an instance_id");
    }

    match provisioner().terminate(&args[0]) {
        Ok(()) => Frame::simple("OK"),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// CLOUD.SCALE instance_id [TYPE new_type] [REPLICAS n]
fn cloud_scale(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("CLOUD SCALE requires an instance_id");
    }

    let instance_id = &args[0];

    // Get current instance to use as defaults
    let current = match provisioner().get_instance(instance_id) {
        Some(r) => r,
        None => return err_frame(&format!("instance not found: {}", instance_id)),
    };

    let mut new_type = current.instance_type.clone();
    let mut replicas = current.replicas;

    let mut i = 1;
    while i < args.len() {
        match args[i].to_uppercase().as_str() {
            "TYPE" => {
                i += 1;
                if i < args.len() {
                    new_type = match args[i].to_lowercase().as_str() {
                        "dev" => InstanceType::Dev,
                        "small" => InstanceType::Small,
                        "medium" => InstanceType::Medium,
                        "large" => InstanceType::Large,
                        _ => new_type,
                    };
                }
            }
            "REPLICAS" => {
                i += 1;
                if i < args.len() {
                    replicas = args[i].parse().unwrap_or(replicas);
                }
            }
            _ => {}
        }
        i += 1;
    }

    match provisioner().scale(instance_id, new_type, replicas) {
        Ok(()) => Frame::simple("OK"),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// CLOUD.COST [instance_id]
fn cloud_cost(args: &[String]) -> Frame {
    if args.is_empty() {
        // Return a generic estimate for a Small instance
        let dummy = InstanceRecord {
            id: String::new(),
            name: String::new(),
            state: InstanceState::Running,
            instance_type: InstanceType::Small,
            endpoint: String::new(),
            replicas: 1,
            created_at: chrono::Utc::now(),
            region: String::new(),
            health: Default::default(),
        };
        let estimate = usage_meter().estimate_monthly_cost(&dummy);
        return cost_to_frame(&estimate);
    }

    match provisioner().get_instance(&args[0]) {
        Some(record) => {
            let estimate = usage_meter().estimate_monthly_cost(&record);
            cost_to_frame(&estimate)
        }
        None => err_frame(&format!("instance not found: {}", args[0])),
    }
}

/// CLOUD.USAGE instance_id [PERIOD start end]
fn cloud_usage(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("CLOUD USAGE requires an instance_id");
    }

    let instance_id = &args[0];

    let period = BillingPeriod {
        start: chrono::Utc::now() - chrono::Duration::days(30),
        end: chrono::Utc::now(),
    };

    let report = usage_meter().get_usage(instance_id, period);

    let mut map = HashMap::new();
    map.insert(
        Bytes::from_static(b"instance_id"),
        Frame::bulk(Bytes::from(report.instance_id)),
    );
    map.insert(
        Bytes::from_static(b"compute_hours"),
        Frame::Double(report.compute_hours),
    );
    map.insert(
        Bytes::from_static(b"memory_gb_hours"),
        Frame::Double(report.memory_gb_hours),
    );
    map.insert(
        Bytes::from_static(b"storage_gb"),
        Frame::Double(report.storage_gb),
    );
    map.insert(
        Bytes::from_static(b"operations"),
        Frame::Integer(report.operations as i64),
    );
    map.insert(
        Bytes::from_static(b"network_egress_gb"),
        Frame::Double(report.network_egress_gb),
    );
    map.insert(
        Bytes::from_static(b"total_cost"),
        Frame::Double(report.total_cost),
    );
    Frame::Map(map)
}

/// CLOUD.HEALTH instance_id
fn cloud_health(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("CLOUD HEALTH requires an instance_id");
    }

    let health = provisioner().health_check(&args[0]);

    let mut map = HashMap::new();
    map.insert(
        Bytes::from_static(b"status"),
        Frame::bulk(Bytes::from(format!("{:?}", health.status))),
    );
    map.insert(
        Bytes::from_static(b"latency_ms"),
        Frame::Double(health.latency_ms),
    );
    map.insert(
        Bytes::from_static(b"memory_usage_pct"),
        Frame::Double(health.memory_usage_pct),
    );
    map.insert(
        Bytes::from_static(b"connections"),
        Frame::Integer(health.connections as i64),
    );
    map.insert(
        Bytes::from_static(b"uptime_seconds"),
        Frame::Integer(health.uptime_seconds as i64),
    );
    Frame::Map(map)
}

/// CLOUD.HELP
fn cloud_help() -> Frame {
    Frame::array(vec![
        Frame::bulk("CLOUD.PROVISION name [TYPE dev|small|medium|large] [REPLICAS n] [STORAGE gb] [TLS] [PERSIST]"),
        Frame::bulk("  Provision a new managed Ferrite instance."),
        Frame::bulk("CLOUD.LIST [STATE running|stopped]"),
        Frame::bulk("  List all managed instances, optionally filtered by state."),
        Frame::bulk("CLOUD.INFO instance_id"),
        Frame::bulk("  Return details for a specific instance."),
        Frame::bulk("CLOUD.TERMINATE instance_id"),
        Frame::bulk("  Terminate a managed instance."),
        Frame::bulk("CLOUD.SCALE instance_id [TYPE new_type] [REPLICAS n]"),
        Frame::bulk("  Scale an instance's type or replica count."),
        Frame::bulk("CLOUD.COST [instance_id]"),
        Frame::bulk("  Return estimated monthly cost for an instance."),
        Frame::bulk("CLOUD.USAGE instance_id [PERIOD start end]"),
        Frame::bulk("  Return usage report for an instance."),
        Frame::bulk("CLOUD.HEALTH instance_id"),
        Frame::bulk("  Return health check for an instance."),
    ])
}

/// Convert an InstanceRecord to a Frame::Map.
fn instance_to_frame(record: &InstanceRecord) -> Frame {
    let mut map = HashMap::new();
    map.insert(
        Bytes::from_static(b"id"),
        Frame::bulk(Bytes::from(record.id.clone())),
    );
    map.insert(
        Bytes::from_static(b"name"),
        Frame::bulk(Bytes::from(record.name.clone())),
    );
    map.insert(
        Bytes::from_static(b"state"),
        Frame::bulk(Bytes::from(format!("{:?}", record.state))),
    );
    map.insert(
        Bytes::from_static(b"type"),
        Frame::bulk(Bytes::from(format!("{:?}", record.instance_type))),
    );
    map.insert(
        Bytes::from_static(b"endpoint"),
        Frame::bulk(Bytes::from(record.endpoint.clone())),
    );
    map.insert(
        Bytes::from_static(b"replicas"),
        Frame::Integer(record.replicas as i64),
    );
    map.insert(
        Bytes::from_static(b"region"),
        Frame::bulk(Bytes::from(record.region.clone())),
    );
    map.insert(
        Bytes::from_static(b"health"),
        Frame::bulk(Bytes::from(format!("{:?}", record.health.status))),
    );
    Frame::Map(map)
}

/// Convert a CostEstimate to a Frame::Map.
fn cost_to_frame(est: &CostEstimate) -> Frame {
    let mut map = HashMap::new();
    map.insert(
        Bytes::from_static(b"compute"),
        Frame::Double(est.compute_cost),
    );
    map.insert(
        Bytes::from_static(b"memory"),
        Frame::Double(est.memory_cost),
    );
    map.insert(
        Bytes::from_static(b"storage"),
        Frame::Double(est.storage_cost),
    );
    map.insert(
        Bytes::from_static(b"operations"),
        Frame::Double(est.operations_cost),
    );
    map.insert(
        Bytes::from_static(b"network"),
        Frame::Double(est.network_cost),
    );
    map.insert(
        Bytes::from_static(b"total_monthly"),
        Frame::Double(est.total_monthly),
    );
    map.insert(
        Bytes::from_static(b"currency"),
        Frame::bulk(Bytes::from(est.currency.clone())),
    );
    Frame::Map(map)
}
