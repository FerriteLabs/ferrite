//! Tenant management command handlers
//!
//! Wires TENANT.* commands to the ferrite-enterprise tenancy module.

#![allow(dead_code)]

use crate::commands::executor::CommandExecutor;
use crate::protocol::Frame;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::OnceLock;

use ferrite_enterprise::tenancy::quota_resource::QuotaConfig;
use ferrite_enterprise::tenancy::{
    CreateTenantRequest, LifecycleState, TenantLifecycleManager, TenantTier,
};

/// Global tenant lifecycle manager instance (lazily initialized).
fn tenant_manager() -> &'static TenantLifecycleManager {
    static MGR: OnceLock<TenantLifecycleManager> = OnceLock::new();
    MGR.get_or_init(TenantLifecycleManager::new)
}

/// Parse a tier string into a TenantTier
fn parse_tier(s: &str) -> Option<TenantTier> {
    match s.to_lowercase().as_str() {
        "free" => Some(TenantTier::Free),
        "basic" => Some(TenantTier::Basic),
        "pro" => Some(TenantTier::Pro),
        "enterprise" => Some(TenantTier::Enterprise),
        other => Some(TenantTier::Custom(other.to_string())),
    }
}

/// Format a TenantTier for display
fn tier_name(tier: &TenantTier) -> &str {
    match tier {
        TenantTier::Free => "free",
        TenantTier::Basic => "basic",
        TenantTier::Pro => "pro",
        TenantTier::Enterprise => "enterprise",
        TenantTier::Custom(s) => s.as_str(),
    }
}

impl CommandExecutor {
    /// Main dispatcher for TENANT subcommands
    pub(super) fn tenant_command(&self, subcommand: &str, args: &[String]) -> Frame {
        match subcommand.to_uppercase().as_str() {
            "CREATE" => self.tenant_create(args),
            "LIST" => self.tenant_list(args),
            "DELETE" => self.tenant_delete(args),
            "INFO" => self.tenant_info(args),
            "SUSPEND" => self.tenant_suspend(args),
            "RESUME" => self.tenant_resume(args),
            "QUOTA" => self.tenant_quota(args),
            "HELP" => self.tenant_help(),
            _ => Frame::error(format!(
                "ERR Unknown subcommand or wrong number of arguments for 'tenant|{}'",
                subcommand.to_lowercase()
            )),
        }
    }

    /// TENANT CREATE name [TIER free|basic|pro|enterprise] [QUOTA memory_mb N] [QUOTA max_keys N]
    fn tenant_create(&self, args: &[String]) -> Frame {
        if args.is_empty() {
            return Frame::error("ERR TENANT CREATE requires a tenant name");
        }

        let name = args[0].clone();
        let mut tier = TenantTier::Free;
        let mut quota_override: Option<QuotaConfig> = None;

        let mut i = 1;
        while i < args.len() {
            match args[i].to_uppercase().as_str() {
                "TIER" => {
                    if i + 1 >= args.len() {
                        return Frame::error("ERR TIER requires a value");
                    }
                    tier = match parse_tier(&args[i + 1]) {
                        Some(t) => t,
                        None => {
                            return Frame::error(format!("ERR Unknown tier '{}'", args[i + 1]));
                        }
                    };
                    i += 2;
                }
                "QUOTA" => {
                    if i + 2 >= args.len() {
                        return Frame::error("ERR QUOTA requires <field> <value>");
                    }
                    let field = args[i + 1].to_lowercase();
                    let value = match args[i + 2].parse::<u64>() {
                        Ok(v) => v,
                        Err(_) => {
                            return Frame::error(format!(
                                "ERR Invalid quota value '{}'",
                                args[i + 2]
                            ));
                        }
                    };
                    let q = quota_override.get_or_insert_with(QuotaConfig::default);
                    match field.as_str() {
                        "memory_mb" => q.max_memory_bytes = value * 1024 * 1024,
                        "max_keys" => q.max_keys = value,
                        "max_connections" => q.max_connections = value as u32,
                        "max_ops_per_second" => q.max_ops_per_second = value,
                        _ => {
                            return Frame::error(format!("ERR Unknown quota field '{}'", field));
                        }
                    }
                    i += 3;
                }
                _ => {
                    i += 1;
                }
            }
        }

        let mgr = tenant_manager();
        let request = CreateTenantRequest {
            id: None,
            name,
            tier,
            quota_override,
            metadata: HashMap::new(),
        };

        match mgr.create_tenant(request) {
            Ok(record) => {
                let mut map = HashMap::new();
                map.insert(Bytes::from_static(b"id"), Frame::bulk(record.id.as_str()));
                map.insert(
                    Bytes::from_static(b"name"),
                    Frame::bulk(record.name.as_str()),
                );
                map.insert(
                    Bytes::from_static(b"state"),
                    Frame::bulk(record.state.to_string()),
                );
                map.insert(
                    Bytes::from_static(b"tier"),
                    Frame::bulk(tier_name(&record.tier)),
                );
                Frame::Map(map)
            }
            Err(e) => Frame::error(format!("ERR {}", e)),
        }
    }

    /// TENANT LIST [STATE active|suspended] [TIER tier]
    fn tenant_list(&self, args: &[String]) -> Frame {
        use ferrite_enterprise::tenancy::TenantFilter;

        let mut filter = TenantFilter::default();

        let mut i = 0;
        while i < args.len() {
            match args[i].to_uppercase().as_str() {
                "STATE" => {
                    if i + 1 < args.len() {
                        filter.state = match args[i + 1].to_lowercase().as_str() {
                            "active" => Some(LifecycleState::Active),
                            "suspended" => Some(LifecycleState::Suspended),
                            "deleting" => Some(LifecycleState::Deleting),
                            "archived" => Some(LifecycleState::Archived),
                            _ => None,
                        };
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "TIER" => {
                    if i + 1 < args.len() {
                        filter.tier = parse_tier(&args[i + 1]);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                _ => {
                    i += 1;
                }
            }
        }

        let mgr = tenant_manager();
        let records = mgr.list_tenants(filter);

        let items: Vec<Frame> = records
            .iter()
            .map(|r| {
                let mut map = HashMap::new();
                map.insert(Bytes::from_static(b"id"), Frame::bulk(r.id.as_str()));
                map.insert(Bytes::from_static(b"name"), Frame::bulk(r.name.as_str()));
                map.insert(
                    Bytes::from_static(b"state"),
                    Frame::bulk(r.state.to_string()),
                );
                map.insert(Bytes::from_static(b"tier"), Frame::bulk(tier_name(&r.tier)));
                Frame::Map(map)
            })
            .collect();

        Frame::array(items)
    }

    /// TENANT DELETE tenant_id [FORCE]
    fn tenant_delete(&self, args: &[String]) -> Frame {
        if args.is_empty() {
            return Frame::error("ERR TENANT DELETE requires a tenant_id");
        }
        let tenant_id = &args[0];
        let force = args.get(1).map_or(false, |s| s.to_uppercase() == "FORCE");

        let mgr = tenant_manager();
        match mgr.delete_tenant(tenant_id, force) {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::error(format!("ERR {}", e)),
        }
    }

    /// TENANT INFO tenant_id — returns Map with id, name, state, tier, quota usage
    fn tenant_info(&self, args: &[String]) -> Frame {
        if args.is_empty() {
            return Frame::error("ERR TENANT INFO requires a tenant_id");
        }
        let tenant_id = &args[0];
        let mgr = tenant_manager();

        match mgr.get_tenant(tenant_id) {
            Some(record) => {
                let mut map = HashMap::new();
                map.insert(Bytes::from_static(b"id"), Frame::bulk(record.id.as_str()));
                map.insert(
                    Bytes::from_static(b"name"),
                    Frame::bulk(record.name.as_str()),
                );
                map.insert(
                    Bytes::from_static(b"state"),
                    Frame::bulk(record.state.to_string()),
                );
                map.insert(
                    Bytes::from_static(b"tier"),
                    Frame::bulk(tier_name(&record.tier)),
                );
                map.insert(
                    Bytes::from_static(b"max_memory_bytes"),
                    Frame::Integer(record.quota.max_memory_bytes as i64),
                );
                map.insert(
                    Bytes::from_static(b"max_keys"),
                    Frame::Integer(record.quota.max_keys as i64),
                );
                map.insert(
                    Bytes::from_static(b"max_connections"),
                    Frame::Integer(record.quota.max_connections as i64),
                );
                map.insert(
                    Bytes::from_static(b"max_ops_per_second"),
                    Frame::Integer(record.quota.max_ops_per_second as i64),
                );
                map.insert(
                    Bytes::from_static(b"created_at"),
                    Frame::Integer(record.created_at as i64),
                );
                map.insert(
                    Bytes::from_static(b"updated_at"),
                    Frame::Integer(record.updated_at as i64),
                );

                // Include stats if available
                if let Some(stats) = mgr.tenant_stats(tenant_id) {
                    map.insert(
                        Bytes::from_static(b"memory_used"),
                        Frame::Integer(stats.memory_used as i64),
                    );
                    map.insert(
                        Bytes::from_static(b"keys_count"),
                        Frame::Integer(stats.keys_count as i64),
                    );
                    map.insert(
                        Bytes::from_static(b"connections_active"),
                        Frame::Integer(stats.connections_active as i64),
                    );
                    map.insert(
                        Bytes::from_static(b"uptime_secs"),
                        Frame::Integer(stats.uptime_secs as i64),
                    );
                }

                Frame::Map(map)
            }
            None => Frame::error(format!("ERR tenant not found: {}", tenant_id)),
        }
    }

    /// TENANT SUSPEND tenant_id reason
    fn tenant_suspend(&self, args: &[String]) -> Frame {
        if args.len() < 2 {
            return Frame::error("ERR TENANT SUSPEND requires <tenant_id> <reason>");
        }
        let tenant_id = &args[0];
        let reason = args[1..].join(" ");

        let mgr = tenant_manager();
        match mgr.suspend_tenant(tenant_id, &reason) {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::error(format!("ERR {}", e)),
        }
    }

    /// TENANT RESUME tenant_id
    fn tenant_resume(&self, args: &[String]) -> Frame {
        if args.is_empty() {
            return Frame::error("ERR TENANT RESUME requires a tenant_id");
        }
        let tenant_id = &args[0];

        let mgr = tenant_manager();
        match mgr.resume_tenant(tenant_id) {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::error(format!("ERR {}", e)),
        }
    }

    /// TENANT QUOTA tenant_id [GET|SET field value]
    fn tenant_quota(&self, args: &[String]) -> Frame {
        if args.is_empty() {
            return Frame::error("ERR TENANT QUOTA requires a tenant_id");
        }
        let tenant_id = &args[0];
        let mgr = tenant_manager();

        let subcmd = args.get(1).map(|s| s.to_uppercase());
        match subcmd.as_deref() {
            None | Some("GET") => {
                // Return current quota
                match mgr.get_tenant(tenant_id) {
                    Some(record) => {
                        let mut map = HashMap::new();
                        map.insert(
                            Bytes::from_static(b"max_memory_bytes"),
                            Frame::Integer(record.quota.max_memory_bytes as i64),
                        );
                        map.insert(
                            Bytes::from_static(b"max_keys"),
                            Frame::Integer(record.quota.max_keys as i64),
                        );
                        map.insert(
                            Bytes::from_static(b"max_connections"),
                            Frame::Integer(record.quota.max_connections as i64),
                        );
                        map.insert(
                            Bytes::from_static(b"max_ops_per_second"),
                            Frame::Integer(record.quota.max_ops_per_second as i64),
                        );
                        map.insert(
                            Bytes::from_static(b"max_bandwidth_bytes_per_sec"),
                            Frame::Integer(record.quota.max_bandwidth_bytes_per_sec as i64),
                        );
                        Frame::Map(map)
                    }
                    None => Frame::error(format!("ERR tenant not found: {}", tenant_id)),
                }
            }
            Some("SET") => {
                if args.len() < 4 {
                    return Frame::error("ERR TENANT QUOTA SET requires <field> <value>");
                }
                let field = args[2].to_lowercase();
                let value = match args[3].parse::<u64>() {
                    Ok(v) => v,
                    Err(_) => {
                        return Frame::error(format!("ERR Invalid quota value '{}'", args[3]));
                    }
                };

                // Get current record, modify quota, then update
                match mgr.get_tenant(tenant_id) {
                    Some(record) => {
                        let mut quota = record.quota.clone();
                        match field.as_str() {
                            "max_memory_bytes" | "memory_mb" => {
                                quota.max_memory_bytes = if field == "memory_mb" {
                                    value * 1024 * 1024
                                } else {
                                    value
                                };
                            }
                            "max_keys" => quota.max_keys = value,
                            "max_connections" => quota.max_connections = value as u32,
                            "max_ops_per_second" => quota.max_ops_per_second = value,
                            "max_bandwidth_bytes_per_sec" => {
                                quota.max_bandwidth_bytes_per_sec = value;
                            }
                            _ => {
                                return Frame::error(format!(
                                    "ERR Unknown quota field '{}'",
                                    field
                                ));
                            }
                        }
                        match mgr.update_quota(tenant_id, quota) {
                            Ok(()) => Frame::simple("OK"),
                            Err(e) => Frame::error(format!("ERR {}", e)),
                        }
                    }
                    None => Frame::error(format!("ERR tenant not found: {}", tenant_id)),
                }
            }
            Some(other) => Frame::error(format!(
                "ERR Unknown TENANT QUOTA subcommand '{}'. Try GET or SET.",
                other
            )),
        }
    }

    /// Return help text for TENANT commands
    fn tenant_help(&self) -> Frame {
        Frame::array(vec![
            Frame::bulk("TENANT <subcommand> [<arg> [value] ...]"),
            Frame::bulk("CREATE <name> [TIER free|basic|pro|enterprise] [QUOTA <field> <value>] -- Create a new tenant."),
            Frame::bulk("LIST [STATE active|suspended] [TIER tier] -- List tenants."),
            Frame::bulk("DELETE <tenant_id> [FORCE] -- Delete a tenant (soft or force)."),
            Frame::bulk("INFO <tenant_id> -- Get tenant details and quota usage."),
            Frame::bulk("SUSPEND <tenant_id> <reason> -- Suspend a tenant."),
            Frame::bulk("RESUME <tenant_id> -- Resume a suspended tenant."),
            Frame::bulk("QUOTA <tenant_id> [GET|SET <field> <value>] -- Get or set tenant quota."),
            Frame::bulk("HELP -- Show this help."),
        ])
    }
}
