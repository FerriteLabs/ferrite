//! Multi-cloud command handlers
//!
//! Implements multi-cloud active-active replication commands.

use std::sync::OnceLock;

use bytes::Bytes;

use ferrite_cloud::multicloud::{
    CloudProvider, CloudRegion, ConflictStrategy, MultiCloudConfig, MultiCloudManager,
    ProviderConfig, ProviderType, RegionConfig,
};
use crate::protocol::Frame;

use super::{err_frame, ok_frame, HandlerContext};

/// Global multi-cloud manager instance
static MULTICLOUD_MANAGER: OnceLock<MultiCloudManager> = OnceLock::new();

/// Get the global multi-cloud manager
fn multicloud_manager() -> &'static MultiCloudManager {
    MULTICLOUD_MANAGER.get_or_init(|| MultiCloudManager::new(MultiCloudConfig::default()))
}

/// Handle CLOUD.PROVIDER.ADD command
///
/// CLOUD.PROVIDER.ADD name type [ENDPOINT endpoint] [KEY access_key secret_key]
pub fn handle_cloud_provider_add(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("CLOUD.PROVIDER.ADD requires name and type");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();
    let type_str = String::from_utf8_lossy(&args[1]).to_lowercase();

    let provider_type = match ProviderType::from_str(&type_str) {
        Some(pt) => pt,
        None => return err_frame("invalid provider type (aws, gcp, azure, custom)"),
    };

    let mut config = ProviderConfig::new(provider_type);

    let mut i = 2;
    while i < args.len() {
        let keyword = String::from_utf8_lossy(&args[i]).to_uppercase();
        match keyword.as_str() {
            "ENDPOINT" if i + 1 < args.len() => {
                i += 1;
                config = config.with_endpoint(String::from_utf8_lossy(&args[i]).to_string());
            }
            "KEY" if i + 2 < args.len() => {
                i += 1;
                let access_key = String::from_utf8_lossy(&args[i]).to_string();
                i += 1;
                let secret_key = String::from_utf8_lossy(&args[i]).to_string();
                config = config.with_credentials(access_key, secret_key);
            }
            _ => {}
        }
        i += 1;
    }

    let provider = CloudProvider::new(&name, config);

    match multicloud_manager().register_provider(provider) {
        Ok(()) => ok_frame(),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle CLOUD.PROVIDER.LIST command
///
/// CLOUD.PROVIDER.LIST
pub fn handle_cloud_provider_list(_ctx: &HandlerContext<'_>, _args: &[Bytes]) -> Frame {
    // In a real implementation, we'd list providers
    // For now, return empty list as providers are stored in RwLock
    Frame::array(vec![])
}

/// Handle CLOUD.REGION.ADD command
///
/// CLOUD.REGION.ADD name provider [ENDPOINT endpoint] [PRIORITY n] [WEIGHT n]
pub fn handle_cloud_region_add(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("CLOUD.REGION.ADD requires name and provider");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();
    let provider = String::from_utf8_lossy(&args[1]).to_string();

    let mut config = RegionConfig::new(&name, &provider);

    let mut i = 2;
    while i < args.len() {
        let keyword = String::from_utf8_lossy(&args[i]).to_uppercase();
        match keyword.as_str() {
            "ENDPOINT" if i + 1 < args.len() => {
                i += 1;
                config = config.with_endpoint(String::from_utf8_lossy(&args[i]).to_string());
            }
            "PRIORITY" if i + 1 < args.len() => {
                i += 1;
                if let Ok(p) = String::from_utf8_lossy(&args[i]).parse::<u32>() {
                    config = config.with_priority(p);
                }
            }
            "WEIGHT" if i + 1 < args.len() => {
                i += 1;
                if let Ok(w) = String::from_utf8_lossy(&args[i]).parse::<u32>() {
                    config = config.with_weight(w);
                }
            }
            _ => {}
        }
        i += 1;
    }

    let region = CloudRegion::new(&name, config);

    match multicloud_manager().register_region(region) {
        Ok(()) => ok_frame(),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle CLOUD.REGION.GET command
///
/// CLOUD.REGION.GET name
pub fn handle_cloud_region_get(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("CLOUD.REGION.GET requires region name");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();

    match multicloud_manager().get_region(&name) {
        Some(region) => {
            let region_name = region.name().to_string();
            let provider = region.provider().to_string();
            let status = region.status().as_str().to_string();
            let priority = region.priority() as i64;
            let weight = region.weight() as i64;
            let endpoint = region.endpoint().to_string();
            Frame::array(vec![
                Frame::bulk("name"),
                Frame::bulk(region_name),
                Frame::bulk("provider"),
                Frame::bulk(provider),
                Frame::bulk("status"),
                Frame::bulk(status),
                Frame::bulk("priority"),
                Frame::Integer(priority),
                Frame::bulk("weight"),
                Frame::Integer(weight),
                Frame::bulk("endpoint"),
                Frame::bulk(endpoint),
            ])
        }
        None => Frame::Null,
    }
}

/// Handle CLOUD.REGION.LIST command
///
/// CLOUD.REGION.LIST [HEALTHY]
pub fn handle_cloud_region_list(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    let healthy_only = args
        .first()
        .map(|a| String::from_utf8_lossy(a).to_uppercase() == "HEALTHY")
        .unwrap_or(false);

    let regions = if healthy_only {
        multicloud_manager().healthy_regions()
    } else {
        multicloud_manager().list_regions()
    };

    let items: Vec<Frame> = regions
        .into_iter()
        .map(|r| {
            Frame::array(vec![
                Frame::bulk(r.name().to_string()),
                Frame::bulk(r.provider().to_string()),
                Frame::bulk(r.status().as_str().to_string()),
                Frame::Integer(r.priority() as i64),
            ])
        })
        .collect();

    Frame::array(items)
}

/// Handle CLOUD.ROUTE command
///
/// CLOUD.ROUTE [FROM client_region]
pub fn handle_cloud_route(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    let client_region = if args.len() >= 2 {
        let keyword = String::from_utf8_lossy(&args[0]).to_uppercase();
        if keyword == "FROM" {
            Some(String::from_utf8_lossy(&args[1]).to_string())
        } else {
            None
        }
    } else {
        None
    };

    match multicloud_manager().route_request(client_region.as_deref()) {
        Some(region) => {
            let region_name = region.name().to_string();
            let provider = region.provider().to_string();
            let endpoint = region.endpoint().to_string();
            Frame::array(vec![
                Frame::bulk("region"),
                Frame::bulk(region_name),
                Frame::bulk("provider"),
                Frame::bulk(provider),
                Frame::bulk("endpoint"),
                Frame::bulk(endpoint),
            ])
        }
        None => Frame::Null,
    }
}

/// Handle CLOUD.SYNC command
///
/// CLOUD.SYNC key value [TO region|ALL] [TIMESTAMP ts]
pub fn handle_cloud_sync(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("CLOUD.SYNC requires key and value");
    }

    let key = String::from_utf8_lossy(&args[0]).to_string();
    let value = args[1].to_vec();

    let mut target_region: Option<String> = None;
    let mut sync_all = false;
    let mut timestamp = current_timestamp();

    let mut i = 2;
    while i < args.len() {
        let keyword = String::from_utf8_lossy(&args[i]).to_uppercase();
        match keyword.as_str() {
            "TO" if i + 1 < args.len() => {
                i += 1;
                let target = String::from_utf8_lossy(&args[i]).to_string();
                if target.to_uppercase() == "ALL" {
                    sync_all = true;
                } else {
                    target_region = Some(target);
                }
            }
            "TIMESTAMP" if i + 1 < args.len() => {
                i += 1;
                if let Ok(ts) = String::from_utf8_lossy(&args[i]).parse::<u64>() {
                    timestamp = ts;
                }
            }
            _ => {}
        }
        i += 1;
    }

    if sync_all {
        match multicloud_manager().sync_to_all(&key, &value, timestamp) {
            Ok(count) => Frame::Integer(count as i64),
            Err(e) => err_frame(&e.to_string()),
        }
    } else if let Some(region) = target_region {
        match multicloud_manager().sync_to_region(&region, &key, &value, timestamp) {
            Ok(()) => ok_frame(),
            Err(e) => err_frame(&e.to_string()),
        }
    } else {
        err_frame("CLOUD.SYNC requires TO region or TO ALL")
    }
}

/// Handle CLOUD.CONFLICT.RESOLVE command
///
/// CLOUD.CONFLICT.RESOLVE local_value local_ts remote_value remote_ts
pub fn handle_cloud_conflict_resolve(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 4 {
        return err_frame(
            "CLOUD.CONFLICT.RESOLVE requires local_value, local_ts, remote_value, remote_ts",
        );
    }

    let local_value = args[0].to_vec();
    let local_ts = match String::from_utf8_lossy(&args[1]).parse::<u64>() {
        Ok(ts) => ts,
        Err(_) => return err_frame("invalid local timestamp"),
    };
    let remote_value = args[2].to_vec();
    let remote_ts = match String::from_utf8_lossy(&args[3]).parse::<u64>() {
        Ok(ts) => ts,
        Err(_) => return err_frame("invalid remote timestamp"),
    };

    let resolved =
        multicloud_manager().resolve_conflict(&local_value, local_ts, &remote_value, remote_ts);

    Frame::Bulk(Some(Bytes::from(resolved)))
}

/// Handle CLOUD.HEALTH command
///
/// CLOUD.HEALTH [region]
pub fn handle_cloud_health(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    let health_status = multicloud_manager().health_status();

    if let Some(region_arg) = args.first() {
        let region = String::from_utf8_lossy(region_arg).to_string();
        if let Some(status) = health_status.get(&region) {
            let status_str = status.as_str().to_string();
            return Frame::array(vec![
                Frame::bulk("region"),
                Frame::bulk(region),
                Frame::bulk("status"),
                Frame::bulk(status_str),
            ]);
        }
        return Frame::Null;
    }

    let items: Vec<Frame> = health_status
        .into_iter()
        .map(|(region, status)| {
            Frame::array(vec![
                Frame::bulk(region),
                Frame::bulk(status.as_str().to_string()),
            ])
        })
        .collect();

    Frame::array(items)
}

/// Handle CLOUD.STATUS command
///
/// CLOUD.STATUS
pub fn handle_cloud_status(_ctx: &HandlerContext<'_>, _args: &[Bytes]) -> Frame {
    let status = multicloud_manager().status();

    Frame::array(vec![
        Frame::bulk("local_region"),
        Frame::bulk(status.local_region),
        Frame::bulk("total_regions"),
        Frame::Integer(status.total_regions as i64),
        Frame::bulk("healthy_regions"),
        Frame::Integer(status.healthy_regions as i64),
        Frame::bulk("total_providers"),
        Frame::Integer(status.total_providers as i64),
        Frame::bulk("conflict_strategy"),
        Frame::bulk(match status.conflict_strategy {
            ConflictStrategy::LastWriteWins => "last_write_wins",
            ConflictStrategy::FirstWriteWins => "first_write_wins",
            ConflictStrategy::CrdtMerge => "crdt_merge",
            ConflictStrategy::Custom => "custom",
        }),
        Frame::bulk("routing_strategy"),
        Frame::bulk(status.routing_strategy.as_str()),
        Frame::bulk("sync_items_pending"),
        Frame::Integer(status.sync_status.items_pending as i64),
        Frame::bulk("sync_items_synced"),
        Frame::Integer(status.sync_status.items_synced as i64),
    ])
}

/// Handle CLOUD.SYNC.STATS command
///
/// CLOUD.SYNC.STATS
pub fn handle_cloud_sync_stats(_ctx: &HandlerContext<'_>, _args: &[Bytes]) -> Frame {
    let stats = multicloud_manager().sync_stats();

    Frame::array(vec![
        Frame::bulk("items_synced"),
        Frame::Integer(stats.items_synced as i64),
        Frame::bulk("items_pending"),
        Frame::Integer(stats.items_pending as i64),
        Frame::bulk("last_sync"),
        Frame::Integer(stats.last_sync as i64),
        Frame::bulk("errors"),
        Frame::Integer(stats.errors as i64),
        Frame::bulk("bytes_transferred"),
        Frame::Integer(stats.bytes_transferred as i64),
    ])
}

/// Get current timestamp
fn current_timestamp() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multicloud_manager_singleton() {
        let m1 = multicloud_manager();
        let m2 = multicloud_manager();
        assert!(std::ptr::eq(m1, m2));
    }
}
