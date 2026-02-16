//! Cluster, tiering, and CDC command handlers
//!
//! This module contains handlers for:
//! - CLUSTER commands (cluster management, slots, failover)
//! - TIERING commands (storage tiering, cost optimization)
//! - CDC commands (change data capture)

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::Store;

use super::err_frame;

/// Handle CLUSTER command
pub fn cluster(subcommand: &str, args: &[String]) -> Frame {
    use crate::cluster::{HashSlot, CLUSTER_SLOTS};

    match subcommand.to_uppercase().as_str() {
        "INFO" => {
            let info = "\
cluster_state:ok\r\n\
cluster_slots_assigned:16384\r\n\
cluster_slots_ok:16384\r\n\
cluster_slots_pfail:0\r\n\
cluster_slots_fail:0\r\n\
cluster_known_nodes:1\r\n\
cluster_size:1\r\n\
cluster_current_epoch:1\r\n\
cluster_my_epoch:1\r\n\
cluster_stats_messages_ping_sent:0\r\n\
cluster_stats_messages_pong_sent:0\r\n\
cluster_stats_messages_sent:0\r\n\
cluster_stats_messages_ping_received:0\r\n\
cluster_stats_messages_pong_received:0\r\n\
cluster_stats_messages_received:0";
            Frame::bulk(info)
        }
        "NODES" => {
            let node_id = "0000000000000000000000000000000000000001";
            let info = format!(
                "{} 127.0.0.1:6379@16379 myself,master - 0 0 1 connected 0-16383",
                node_id
            );
            Frame::bulk(info)
        }
        "SLOTS" => {
            let slot_info = Frame::array(vec![
                Frame::Integer(0),
                Frame::Integer(16383),
                Frame::array(vec![
                    Frame::bulk("127.0.0.1"),
                    Frame::Integer(6379),
                    Frame::bulk("0000000000000000000000000000000000000001"),
                ]),
            ]);
            Frame::array(vec![slot_info])
        }
        "KEYSLOT" => {
            if args.is_empty() {
                return Frame::error("ERR wrong number of arguments for 'cluster|keyslot' command");
            }
            let key = args[0].as_bytes();
            let slot = HashSlot::for_key(key);
            Frame::Integer(slot as i64)
        }
        "MYID" => Frame::bulk("0000000000000000000000000000000000000001"),
        "COUNTKEYSINSLOT" => {
            if args.is_empty() {
                return Frame::error(
                    "ERR wrong number of arguments for 'cluster|countkeysinslot' command",
                );
            }
            let slot: u16 = match args[0].parse() {
                Ok(s) if s < CLUSTER_SLOTS => s,
                _ => return Frame::error("ERR Invalid or out of range slot"),
            };
            let _ = slot;
            Frame::Integer(0)
        }
        "GETKEYSINSLOT" => {
            if args.len() < 2 {
                return Frame::error(
                    "ERR wrong number of arguments for 'cluster|getkeysinslot' command",
                );
            }
            let slot: u16 = match args[0].parse() {
                Ok(s) if s < CLUSTER_SLOTS => s,
                _ => return Frame::error("ERR Invalid or out of range slot"),
            };
            let _count: u32 = match args[1].parse() {
                Ok(c) => c,
                _ => return Frame::error("ERR Invalid count"),
            };
            let _ = slot;
            Frame::array(vec![])
        }
        "MEET" => {
            if args.len() < 2 {
                return Frame::error("ERR wrong number of arguments for 'cluster|meet' command");
            }
            let ip: std::net::IpAddr = match args[0].parse() {
                Ok(ip) => ip,
                Err(_) => return Frame::error("ERR Invalid IP address"),
            };
            let port: u16 = match args[1].parse() {
                Ok(p) => p,
                Err(_) => return Frame::error("ERR Invalid port number"),
            };
            let addr = std::net::SocketAddr::new(ip, port);
            tracing::info!("CLUSTER MEET request for {}", addr);
            Frame::simple("OK")
        }
        "ADDSLOTS" => {
            if args.is_empty() {
                return Frame::error(
                    "ERR wrong number of arguments for 'cluster|addslots' command",
                );
            }
            for arg in args {
                let slot: u16 = match arg.parse() {
                    Ok(s) if s < CLUSTER_SLOTS => s,
                    _ => return Frame::error("ERR Invalid or out of range slot"),
                };
                let _ = slot;
            }
            Frame::simple("OK")
        }
        "DELSLOTS" => {
            if args.is_empty() {
                return Frame::error(
                    "ERR wrong number of arguments for 'cluster|delslots' command",
                );
            }
            for arg in args {
                let slot: u16 = match arg.parse() {
                    Ok(s) if s < CLUSTER_SLOTS => s,
                    _ => return Frame::error("ERR Invalid or out of range slot"),
                };
                let _ = slot;
            }
            Frame::simple("OK")
        }
        "SETSLOT" => {
            if args.len() < 2 {
                return Frame::error("ERR wrong number of arguments for 'cluster|setslot' command");
            }
            let slot: u16 = match args[0].parse() {
                Ok(s) if s < CLUSTER_SLOTS => s,
                _ => return Frame::error("ERR Invalid or out of range slot"),
            };
            let sub = args[1].to_uppercase();
            match sub.as_str() {
                "IMPORTING" => {
                    if args.len() < 3 {
                        return Frame::error("ERR Please specify the source node ID");
                    }
                    let source_node_id = &args[2];
                    tracing::info!("CLUSTER SETSLOT {} IMPORTING from {}", slot, source_node_id);
                    Frame::simple("OK")
                }
                "MIGRATING" => {
                    if args.len() < 3 {
                        return Frame::error("ERR Please specify the target node ID");
                    }
                    let target_node_id = &args[2];
                    tracing::info!("CLUSTER SETSLOT {} MIGRATING to {}", slot, target_node_id);
                    Frame::simple("OK")
                }
                "STABLE" => {
                    tracing::info!("CLUSTER SETSLOT {} STABLE", slot);
                    Frame::simple("OK")
                }
                "NODE" => {
                    if args.len() < 3 {
                        return Frame::error("ERR Please specify the node ID");
                    }
                    let node_id = &args[2];
                    tracing::info!("CLUSTER SETSLOT {} NODE {}", slot, node_id);
                    Frame::simple("OK")
                }
                _ => Frame::error("ERR Invalid CLUSTER SETSLOT action"),
            }
        }
        "FAILOVER" => {
            let failover_type = if !args.is_empty() {
                match args[0].to_uppercase().as_str() {
                    "FORCE" => {
                        tracing::info!("CLUSTER FAILOVER FORCE requested");
                        "FORCE"
                    }
                    "TAKEOVER" => {
                        tracing::info!("CLUSTER FAILOVER TAKEOVER requested");
                        "TAKEOVER"
                    }
                    _ => {
                        return Frame::error(
                            "ERR Invalid CLUSTER FAILOVER option. Valid: FORCE, TAKEOVER",
                        )
                    }
                }
            } else {
                tracing::info!("CLUSTER FAILOVER (normal) requested");
                "NORMAL"
            };
            let _ = failover_type;
            Frame::simple("OK")
        }
        "REPLICATE" => {
            if args.is_empty() {
                return Frame::error(
                    "ERR wrong number of arguments for 'cluster|replicate' command",
                );
            }
            Frame::simple("OK")
        }
        "RESET" => Frame::simple("OK"),
        "SAVECONFIG" => Frame::simple("OK"),
        "SHARDS" => {
            let shard = Frame::Map(HashMap::from([
                (
                    Bytes::from_static(b"slots"),
                    Frame::array(vec![Frame::Integer(0), Frame::Integer(16383)]),
                ),
                (
                    Bytes::from_static(b"nodes"),
                    Frame::array(vec![Frame::Map(HashMap::from([
                        (
                            Bytes::from_static(b"id"),
                            Frame::bulk("0000000000000000000000000000000000000001"),
                        ),
                        (Bytes::from_static(b"port"), Frame::Integer(6379)),
                        (Bytes::from_static(b"ip"), Frame::bulk("127.0.0.1")),
                        (Bytes::from_static(b"role"), Frame::bulk("master")),
                        (Bytes::from_static(b"health"), Frame::bulk("online")),
                    ]))]),
                ),
            ]));
            Frame::array(vec![shard])
        }
        "FLUSHSLOTS" => {
            tracing::info!("CLUSTER FLUSHSLOTS requested");
            Frame::simple("OK")
        }
        "COUNT-FAILURE-REPORTS" => {
            if args.is_empty() {
                return Frame::error(
                    "ERR wrong number of arguments for 'cluster|count-failure-reports' command",
                );
            }
            // In a full implementation, this would query the failure detector
            Frame::Integer(0)
        }
        "SET-CONFIG-EPOCH" => {
            if args.is_empty() {
                return Frame::error(
                    "ERR wrong number of arguments for 'cluster|set-config-epoch' command",
                );
            }
            match args[0].parse::<u64>() {
                Ok(epoch) => {
                    tracing::info!("CLUSTER SET-CONFIG-EPOCH {}", epoch);
                    Frame::simple("OK")
                }
                Err(_) => Frame::error("ERR Invalid config epoch specified"),
            }
        }
        "LINKS" => {
            // Return empty array for standalone mode
            Frame::array(vec![])
        }
        "HELP" => Frame::array(vec![
            Frame::bulk("CLUSTER <subcommand> [<arg> [value] [opt] ...]"),
            Frame::bulk("INFO -- Return cluster information."),
            Frame::bulk("NODES -- Return cluster nodes information."),
            Frame::bulk("SLOTS -- Return information about slots."),
            Frame::bulk("KEYSLOT <key> -- Return the hash slot for a key."),
            Frame::bulk("MYID -- Return this node's ID."),
            Frame::bulk("COUNTKEYSINSLOT <slot> -- Count keys in a slot."),
            Frame::bulk("GETKEYSINSLOT <slot> <count> -- Get keys in a slot."),
            Frame::bulk("MEET <ip> <port> -- Connect to another node."),
            Frame::bulk("ADDSLOTS <slot> [...] -- Assign slots to this node."),
            Frame::bulk("DELSLOTS <slot> [...] -- Remove slot assignments."),
            Frame::bulk("SETSLOT <slot> <action> [node-id] -- Set slot state."),
            Frame::bulk("FAILOVER [FORCE|TAKEOVER] -- Trigger manual failover."),
            Frame::bulk("REPLICATE <node-id> -- Configure as replica."),
            Frame::bulk("RESET [HARD|SOFT] -- Reset cluster state."),
            Frame::bulk("SAVECONFIG -- Save cluster config to disk."),
            Frame::bulk("SHARDS -- Return shard information."),
            Frame::bulk("FLUSHSLOTS -- Remove all slot assignments."),
            Frame::bulk("COUNT-FAILURE-REPORTS <node-id> -- Failure report count."),
            Frame::bulk("SET-CONFIG-EPOCH <epoch> -- Set the config epoch."),
            Frame::bulk("LINKS -- Return cluster bus connections info."),
        ]),
        _ => Frame::error(format!(
            "ERR Unknown subcommand or wrong number of arguments for '{}'",
            subcommand
        )),
    }
}

/// Handle TIERING command
pub async fn tiering(subcommand: &str, args: &[String], key: Option<&Bytes>) -> Frame {
    use crate::tiering::{Priority, StorageTier, TieringEngine};

    let engine = TieringEngine::new_default();

    match subcommand.to_uppercase().as_str() {
        "INFO" => {
            let info = engine.info().await;
            let mut map = HashMap::new();
            map.insert(
                Bytes::from_static(b"total_keys"),
                Frame::Integer(info.total_keys as i64),
            );
            map.insert(
                Bytes::from_static(b"total_size_bytes"),
                Frame::Integer(info.total_size_bytes as i64),
            );
            map.insert(
                Bytes::from_static(b"total_size_gb"),
                Frame::Double(info.total_size_gb()),
            );
            map.insert(
                Bytes::from_static(b"monthly_cost_current"),
                Frame::Double(info.monthly_cost_current),
            );
            map.insert(
                Bytes::from_static(b"monthly_cost_optimal"),
                Frame::Double(info.monthly_cost_optimal),
            );
            map.insert(
                Bytes::from_static(b"potential_savings_pct"),
                Frame::Double(info.potential_savings_pct),
            );
            map.insert(
                Bytes::from_static(b"migrations_pending"),
                Frame::Integer(info.migrations_pending as i64),
            );
            map.insert(
                Bytes::from_static(b"migrations_rate_per_sec"),
                Frame::Integer(info.migrations_rate_per_sec as i64),
            );
            Frame::Map(map)
        }
        "COSTS" => {
            if args.is_empty() {
                let config = engine.config();
                let mut map = HashMap::new();
                for tier in StorageTier::all() {
                    let cost = config.costs.cost_for_tier(*tier);
                    let mut tier_map = HashMap::new();
                    tier_map.insert(
                        Bytes::from_static(b"storage_per_gb_month"),
                        Frame::Double(cost.storage_per_gb_month),
                    );
                    tier_map.insert(
                        Bytes::from_static(b"read_per_1k"),
                        Frame::Double(cost.read_per_1k),
                    );
                    tier_map.insert(
                        Bytes::from_static(b"write_per_1k"),
                        Frame::Double(cost.write_per_1k),
                    );
                    tier_map.insert(
                        Bytes::from_static(b"egress_per_gb"),
                        Frame::Double(cost.egress_per_gb),
                    );
                    tier_map.insert(
                        Bytes::from_static(b"read_latency_ms"),
                        Frame::Double(cost.read_latency_ms),
                    );
                    tier_map.insert(
                        Bytes::from_static(b"write_latency_ms"),
                        Frame::Double(cost.write_latency_ms),
                    );
                    map.insert(Bytes::from(tier.name()), Frame::Map(tier_map));
                }
                Frame::Map(map)
            } else {
                Frame::simple("OK")
            }
        }
        "POLICY" => {
            if args.is_empty() {
                let config = engine.config();
                let mut map = HashMap::new();
                map.insert(
                    Bytes::from_static(b"enabled"),
                    Frame::Boolean(config.enabled),
                );
                map.insert(
                    Bytes::from_static(b"optimize_for"),
                    Frame::bulk(config.optimize_for.name()),
                );
                map.insert(
                    Bytes::from_static(b"max_latency_ms"),
                    Frame::Double(config.max_latency_ms),
                );
                map.insert(
                    Bytes::from_static(b"memory_budget"),
                    Frame::Integer(config.memory_budget as i64),
                );
                Frame::Map(map)
            } else {
                Frame::simple("OK")
            }
        }
        "COST" => {
            if let Some(key_bytes) = key {
                if let Some(analysis) = engine.key_cost(key_bytes) {
                    let mut map = HashMap::new();
                    map.insert(
                        Bytes::from_static(b"key"),
                        Frame::bulk(analysis.key.clone()),
                    );
                    map.insert(
                        Bytes::from_static(b"size_bytes"),
                        Frame::Integer(analysis.size_bytes as i64),
                    );
                    map.insert(
                        Bytes::from_static(b"current_tier"),
                        Frame::bulk(analysis.current_tier.name()),
                    );
                    map.insert(
                        Bytes::from_static(b"current_cost_monthly"),
                        Frame::Double(analysis.current_cost_monthly),
                    );
                    map.insert(
                        Bytes::from_static(b"access_pattern"),
                        Frame::bulk(analysis.access_pattern.clone()),
                    );
                    map.insert(
                        Bytes::from_static(b"reads_per_day"),
                        Frame::Integer(analysis.reads_per_day as i64),
                    );
                    map.insert(
                        Bytes::from_static(b"writes_per_day"),
                        Frame::Integer(analysis.writes_per_day as i64),
                    );
                    map.insert(
                        Bytes::from_static(b"optimal_tier"),
                        Frame::bulk(analysis.optimal_tier.name()),
                    );
                    map.insert(
                        Bytes::from_static(b"optimal_cost_monthly"),
                        Frame::Double(analysis.optimal_cost_monthly),
                    );
                    map.insert(
                        Bytes::from_static(b"potential_savings_pct"),
                        Frame::Double(analysis.potential_savings_pct),
                    );
                    Frame::Map(map)
                } else {
                    Frame::null()
                }
            } else if !args.is_empty() && args[0].to_uppercase() == "TOTAL" {
                let summary = engine.cost_summary();
                let mut map = HashMap::new();
                map.insert(
                    Bytes::from_static(b"total_keys"),
                    Frame::Integer(summary.total_keys as i64),
                );
                map.insert(
                    Bytes::from_static(b"total_size_gb"),
                    Frame::Double(summary.total_size_gb()),
                );
                map.insert(
                    Bytes::from_static(b"monthly_cost_current"),
                    Frame::Double(summary.monthly_cost_current),
                );
                map.insert(
                    Bytes::from_static(b"monthly_cost_optimal"),
                    Frame::Double(summary.monthly_cost_optimal),
                );
                map.insert(
                    Bytes::from_static(b"potential_savings"),
                    Frame::Double(summary.potential_savings()),
                );
                map.insert(
                    Bytes::from_static(b"potential_savings_pct"),
                    Frame::Double(summary.potential_savings_pct()),
                );
                Frame::Map(map)
            } else {
                Frame::error("ERR wrong number of arguments for 'tiering|cost' command")
            }
        }
        "TIER" => {
            if let Some(key_bytes) = key {
                if let Some(tier) = engine.get_tier(key_bytes) {
                    Frame::bulk(tier.name())
                } else {
                    Frame::null()
                }
            } else {
                Frame::error("ERR wrong number of arguments for 'tiering|tier' command")
            }
        }
        "PIN" => {
            if let Some(key_bytes) = key {
                if args.is_empty() {
                    return Frame::error("ERR missing tier argument for 'tiering|pin' command");
                }
                if let Some(tier) = StorageTier::parse_str(&args[0]) {
                    engine.pin_to_tier(key_bytes, tier);
                    Frame::simple("OK")
                } else {
                    Frame::error("ERR invalid tier name")
                }
            } else {
                Frame::error("ERR wrong number of arguments for 'tiering|pin' command")
            }
        }
        "UNPIN" => {
            if let Some(key_bytes) = key {
                engine.unpin(key_bytes);
                Frame::simple("OK")
            } else {
                Frame::error("ERR wrong number of arguments for 'tiering|unpin' command")
            }
        }
        "MIGRATE" => {
            if let Some(key_bytes) = key {
                if args.is_empty() {
                    return Frame::error("ERR missing tier argument for 'tiering|migrate' command");
                }
                if let Some(tier) = StorageTier::parse_str(&args[0]) {
                    if let Some(id) = engine.queue_migration(key_bytes, tier).await {
                        Frame::Integer(id as i64)
                    } else {
                        Frame::error("ERR key not found")
                    }
                } else {
                    Frame::error("ERR invalid tier name")
                }
            } else {
                Frame::error("ERR wrong number of arguments for 'tiering|migrate' command")
            }
        }
        "PRIORITY" => {
            if let Some(key_bytes) = key {
                if args.is_empty() {
                    return Frame::error(
                        "ERR missing priority argument for 'tiering|priority' command",
                    );
                }
                if let Some(priority) = Priority::parse_str(&args[0]) {
                    engine.set_priority(key_bytes, priority);
                    Frame::simple("OK")
                } else {
                    Frame::error(
                        "ERR invalid priority. Valid: critical, high, normal, low, archive",
                    )
                }
            } else {
                Frame::error("ERR wrong number of arguments for 'tiering|priority' command")
            }
        }
        "STATS" => {
            if let Some(key_bytes) = key {
                if let Some(stats) = engine.get_stats(key_bytes) {
                    let mut map = HashMap::new();
                    map.insert(
                        Bytes::from_static(b"size"),
                        Frame::Integer(stats.size as i64),
                    );
                    map.insert(Bytes::from_static(b"tier"), Frame::bulk(stats.tier.name()));
                    map.insert(
                        Bytes::from_static(b"reads_1m"),
                        Frame::Integer(stats.access_counts.reads_1m as i64),
                    );
                    map.insert(
                        Bytes::from_static(b"reads_1h"),
                        Frame::Integer(stats.access_counts.reads_1h as i64),
                    );
                    map.insert(
                        Bytes::from_static(b"reads_1d"),
                        Frame::Integer(stats.access_counts.reads_1d as i64),
                    );
                    map.insert(
                        Bytes::from_static(b"writes_1m"),
                        Frame::Integer(stats.access_counts.writes_1m as i64),
                    );
                    map.insert(
                        Bytes::from_static(b"writes_1h"),
                        Frame::Integer(stats.access_counts.writes_1h as i64),
                    );
                    map.insert(
                        Bytes::from_static(b"writes_1d"),
                        Frame::Integer(stats.access_counts.writes_1d as i64),
                    );
                    map.insert(
                        Bytes::from_static(b"priority"),
                        Frame::bulk(stats.priority.name()),
                    );
                    map.insert(Bytes::from_static(b"pinned"), Frame::Boolean(stats.pinned));
                    Frame::Map(map)
                } else {
                    Frame::null()
                }
            } else {
                Frame::error("ERR wrong number of arguments for 'tiering|stats' command")
            }
        }
        "TOP" => {
            if args.is_empty() {
                return Frame::error("ERR missing subcommand for 'tiering|top'");
            }
            let limit = if args.len() >= 3 && args[1].to_uppercase() == "LIMIT" {
                args[2].parse().unwrap_or(10)
            } else {
                10
            };

            match args[0].to_uppercase().as_str() {
                "COST" => {
                    let top = engine.top_by_cost(limit);
                    Frame::array(
                        top.into_iter()
                            .map(|(key, _, cost)| {
                                Frame::array(vec![Frame::bulk(key), Frame::Double(cost)])
                            })
                            .collect(),
                    )
                }
                "SAVINGS" => {
                    let top = engine.top_savings(limit);
                    Frame::array(
                        top.into_iter()
                            .map(|(key, _, savings)| {
                                Frame::array(vec![Frame::bulk(key), Frame::Double(savings)])
                            })
                            .collect(),
                    )
                }
                _ => Frame::error("ERR invalid TOP subcommand. Valid: COST, SAVINGS"),
            }
        }
        "SUBOPTIMAL" => {
            let limit = if args.len() >= 2 && args[0].to_uppercase() == "LIMIT" {
                args[1].parse().unwrap_or(10)
            } else {
                10
            };

            let suboptimal = engine.suboptimal_keys(limit);
            Frame::array(
                suboptimal
                    .into_iter()
                    .map(|(key, stats, decision)| {
                        let mut map = HashMap::new();
                        map.insert(Bytes::from_static(b"key"), Frame::bulk(key));
                        map.insert(
                            Bytes::from_static(b"current_tier"),
                            Frame::bulk(stats.tier.name()),
                        );
                        map.insert(
                            Bytes::from_static(b"optimal_tier"),
                            Frame::bulk(decision.tier.name()),
                        );
                        map.insert(
                            Bytes::from_static(b"reason"),
                            Frame::bulk(decision.reason.description()),
                        );
                        Frame::Map(map)
                    })
                    .collect(),
            )
        }
        "HELP" => Frame::array(vec![
            Frame::bulk("TIERING <subcommand> [<arg> [value] ...]"),
            Frame::bulk("INFO -- Return tiering information."),
            Frame::bulk("COSTS -- Return or set tier cost configuration."),
            Frame::bulk("POLICY -- Return or set tiering policy."),
            Frame::bulk("COST <key> -- Get cost analysis for a key."),
            Frame::bulk("COST TOTAL -- Get total cost summary."),
            Frame::bulk("TIER <key> -- Get current tier for a key."),
            Frame::bulk("PIN <key> <tier> -- Pin key to specific tier."),
            Frame::bulk("UNPIN <key> -- Remove tier pin from key."),
            Frame::bulk("MIGRATE <key> <tier> -- Force immediate migration."),
            Frame::bulk("PRIORITY <key> <priority> -- Set key priority."),
            Frame::bulk("STATS <key> -- Get access statistics for a key."),
            Frame::bulk("TOP COST [LIMIT n] -- Get top keys by cost."),
            Frame::bulk("TOP SAVINGS [LIMIT n] -- Get top savings opportunities."),
            Frame::bulk("SUBOPTIMAL [LIMIT n] -- Get keys not in optimal tier."),
        ]),
        _ => Frame::error(format!(
            "ERR Unknown subcommand or wrong number of arguments for 'tiering|{}'",
            subcommand.to_lowercase()
        )),
    }
}

/// Handle CDC command
pub async fn cdc(subcommand: &str, args: &[String]) -> Frame {
    use ferrite_streaming::cdc::{CdcEngine, HttpSinkConfig, OutputFormat, SinkConfig};

    let engine = CdcEngine::default();

    match subcommand.to_uppercase().as_str() {
        "INFO" => {
            let info = engine.info().await;
            let mut map = HashMap::new();
            map.insert(
                Bytes::from_static(b"enabled"),
                Frame::Integer(if info.enabled { 1 } else { 0 }),
            );
            map.insert(
                Bytes::from_static(b"capture_old_values"),
                Frame::Integer(if info.capture_old_values { 1 } else { 0 }),
            );
            map.insert(
                Bytes::from_static(b"subscriptions"),
                Frame::Integer(info.subscriptions as i64),
            );
            map.insert(Bytes::from_static(b"sinks"), Frame::Integer(info.sinks as i64));
            map.insert(
                Bytes::from_static(b"events_captured"),
                Frame::Integer(info.events_captured as i64),
            );
            map.insert(
                Bytes::from_static(b"events_delivered"),
                Frame::Integer(info.events_delivered as i64),
            );
            map.insert(
                Bytes::from_static(b"log_segments"),
                Frame::Integer(info.log_segments as i64),
            );
            map.insert(Bytes::from_static(b"log_size"), Frame::Integer(info.log_size as i64));
            map.insert(
                Bytes::from_static(b"log_events"),
                Frame::Integer(info.log_events as i64),
            );
            map.insert(
                Bytes::from_static(b"oldest_event"),
                Frame::Integer(info.oldest_event as i64),
            );
            map.insert(
                Bytes::from_static(b"newest_event"),
                Frame::Integer(info.newest_event as i64),
            );
            Frame::Map(map)
        }
        "SUBSCRIBE" => {
            if args.is_empty() {
                return Frame::error("ERR wrong number of arguments for 'cdc|subscribe'");
            }

            let name = &args[0];
            let mut patterns = Vec::new();
            let mut operations = HashSet::new();
            let mut databases = HashSet::new();
            let mut format = None;
            let mut from_position = None;
            let mut from_earliest = false;
            let mut with_old_values = false;

            let mut i = 1;
            while i < args.len() {
                match args[i].to_uppercase().as_str() {
                    "PATTERN" => {
                        i += 1;
                        while i < args.len()
                            && !["OPERATIONS", "DB", "FORMAT", "FROM", "WITH_OLD_VALUES"]
                                .contains(&args[i].to_uppercase().as_str())
                        {
                            patterns.push(args[i].clone());
                            i += 1;
                        }
                    }
                    "OPERATIONS" => {
                        i += 1;
                        while i < args.len()
                            && !["PATTERN", "DB", "FORMAT", "FROM", "WITH_OLD_VALUES"]
                                .contains(&args[i].to_uppercase().as_str())
                        {
                            operations.insert(args[i].to_uppercase());
                            i += 1;
                        }
                    }
                    "DB" => {
                        i += 1;
                        while i < args.len()
                            && !["PATTERN", "OPERATIONS", "FORMAT", "FROM", "WITH_OLD_VALUES"]
                                .contains(&args[i].to_uppercase().as_str())
                        {
                            if let Ok(db) = args[i].parse::<u8>() {
                                databases.insert(db);
                            }
                            i += 1;
                        }
                    }
                    "FORMAT" => {
                        i += 1;
                        if i < args.len() {
                            format = OutputFormat::from_str(&args[i]);
                            i += 1;
                        }
                    }
                    "FROM" => {
                        i += 1;
                        if i < args.len() {
                            match args[i].to_uppercase().as_str() {
                                "EARLIEST" => from_earliest = true,
                                "LATEST" => {}
                                pos => {
                                    from_position = pos.parse().ok();
                                }
                            }
                            i += 1;
                        }
                    }
                    "WITH_OLD_VALUES" => {
                        with_old_values = true;
                        i += 1;
                    }
                    _ => i += 1,
                }
            }

            if patterns.is_empty() {
                patterns.push("*".to_string());
            }

            let options = ferrite_streaming::cdc::SubscribeOptions {
                operations,
                databases,
                format,
                from_position,
                from_earliest,
                with_old_values,
                ..Default::default()
            };

            match engine.subscribe(name, patterns, options).await {
                Ok(id) => Frame::Integer(id as i64),
                Err(e) => Frame::error(format!("ERR {}", e)),
            }
        }
        "SUBSCRIPTIONS" => {
            let subs = engine.list_subscriptions();
            Frame::array(
                subs.into_iter()
                    .map(|info| {
                        let mut map = HashMap::new();
                        map.insert(Bytes::from_static(b"id"), Frame::Integer(info.id as i64));
                        map.insert(Bytes::from_static(b"name"), Frame::bulk(info.name));
                        map.insert(
                            Bytes::from_static(b"patterns"),
                            Frame::array(info.patterns.into_iter().map(Frame::bulk).collect()),
                        );
                        map.insert(
                            Bytes::from_static(b"position"),
                            Frame::Integer(info.position as i64),
                        );
                        map.insert(Bytes::from_static(b"state"), Frame::bulk(info.state.name()));
                        Frame::Map(map)
                    })
                    .collect(),
            )
        }
        "UNSUBSCRIBE" => {
            if args.is_empty() {
                return Frame::error("ERR wrong number of arguments for 'cdc|unsubscribe'");
            }
            if engine.unsubscribe(&args[0]) {
                Frame::simple("OK")
            } else {
                Frame::error("ERR subscription not found")
            }
        }
        "READ" => {
            if args.is_empty() {
                return Frame::error("ERR wrong number of arguments for 'cdc|read'");
            }

            let name = &args[0];
            let mut count = 100;

            let mut i = 1;
            while i < args.len() {
                if args[i].to_uppercase() == "COUNT" && i + 1 < args.len() {
                    count = args[i + 1].parse().unwrap_or(100);
                    i += 2;
                } else {
                    i += 1;
                }
            }

            match engine.read(name, count).await {
                Ok(events) => Frame::array(
                    events
                        .into_iter()
                        .map(|event| {
                            let mut map = HashMap::new();
                            map.insert(Bytes::from_static(b"id"), Frame::Integer(event.id as i64));
                            map.insert(
                                Bytes::from_static(b"timestamp"),
                                Frame::Integer(event.timestamp_ms() as i64),
                            );
                            map.insert(Bytes::from_static(b"db"), Frame::Integer(event.db as i64));
                            map.insert(
                                Bytes::from_static(b"op"),
                                Frame::bulk(event.operation.name()),
                            );
                            map.insert(Bytes::from_static(b"key"), Frame::Bulk(Some(event.key)));
                            if let Some(value) = event.value {
                                map.insert(Bytes::from_static(b"value"), Frame::Bulk(Some(value)));
                            }
                            if let Some(old_value) = event.old_value {
                                map.insert(
                                    Bytes::from_static(b"old_value"),
                                    Frame::Bulk(Some(old_value)),
                                );
                            }
                            Frame::Map(map)
                        })
                        .collect(),
                ),
                Err(e) => Frame::error(format!("ERR {}", e)),
            }
        }
        "ACK" => {
            if args.len() < 2 {
                return Frame::error("ERR wrong number of arguments for 'cdc|ack'");
            }

            let name = &args[0];
            for id_str in &args[1..] {
                if let Ok(id) = id_str.parse::<u64>() {
                    let _ = engine.acknowledge(name, id);
                }
            }
            Frame::simple("OK")
        }
        "POSITION" => {
            if args.is_empty() {
                return Frame::error("ERR wrong number of arguments for 'cdc|position'");
            }
            match engine.position(&args[0]) {
                Ok(pos) => Frame::Integer(pos as i64),
                Err(e) => Frame::error(format!("ERR {}", e)),
            }
        }
        "SEEK" => {
            if args.len() < 2 {
                return Frame::error("ERR wrong number of arguments for 'cdc|seek'");
            }

            let name = &args[0];
            let position = match args[1].to_uppercase().as_str() {
                "LATEST" => engine.log_stats().await.newest_event,
                "EARLIEST" => engine.log_stats().await.oldest_event,
                pos => pos.parse().unwrap_or(0),
            };

            match engine.seek(name, position) {
                Ok(()) => Frame::simple("OK"),
                Err(e) => Frame::error(format!("ERR {}", e)),
            }
        }
        "SINK" => {
            if args.is_empty() {
                return Frame::error("ERR wrong number of arguments for 'cdc|sink'");
            }

            match args[0].to_uppercase().as_str() {
                "CREATE" => {
                    if args.len() < 4 {
                        return Frame::error(
                            "ERR wrong number of arguments for 'cdc|sink|create'",
                        );
                    }
                    let name = &args[1];
                    let sink_type = &args[2];
                    let config_json = &args[3];

                    let config = match sink_type.to_lowercase().as_str() {
                        "http" | "webhook" => {
                            match serde_json::from_str::<HttpSinkConfig>(config_json) {
                                Ok(cfg) => SinkConfig::Http(cfg),
                                Err(e) => {
                                    return Frame::error(format!("ERR invalid config: {}", e))
                                }
                            }
                        }
                        "kafka" => {
                            match serde_json::from_str::<ferrite_streaming::cdc::KafkaSinkConfig>(config_json) {
                                Ok(cfg) => SinkConfig::Kafka(cfg),
                                Err(e) => {
                                    return Frame::error(format!("ERR invalid config: {}", e))
                                }
                            }
                        }
                        _ => return Frame::error(format!("ERR unknown sink type: {}", sink_type)),
                    };

                    match engine.create_sink(name, config) {
                        Ok(id) => Frame::Integer(id as i64),
                        Err(e) => Frame::error(format!("ERR {}", e)),
                    }
                }
                "DELETE" => {
                    if args.len() < 2 {
                        return Frame::error(
                            "ERR wrong number of arguments for 'cdc|sink|delete'",
                        );
                    }
                    if engine.delete_sink(&args[1]).await {
                        Frame::simple("OK")
                    } else {
                        Frame::error("ERR sink not found")
                    }
                }
                "ATTACH" => {
                    if args.len() < 3 {
                        return Frame::error(
                            "ERR wrong number of arguments for 'cdc|sink|attach'",
                        );
                    }
                    match engine.attach_sink(&args[1], &args[2]) {
                        Ok(()) => Frame::simple("OK"),
                        Err(e) => Frame::error(format!("ERR {}", e)),
                    }
                }
                "DETACH" => {
                    if args.len() < 3 {
                        return Frame::error(
                            "ERR wrong number of arguments for 'cdc|sink|detach'",
                        );
                    }
                    match engine.detach_sink(&args[1], &args[2]) {
                        Ok(()) => Frame::simple("OK"),
                        Err(e) => Frame::error(format!("ERR {}", e)),
                    }
                }
                "STATUS" => {
                    if args.len() < 2 {
                        return Frame::error(
                            "ERR wrong number of arguments for 'cdc|sink|status'",
                        );
                    }
                    match engine.sink_status(&args[1]).await {
                        Some(status) => {
                            let mut map = HashMap::new();
                            map.insert(Bytes::from_static(b"name"), Frame::bulk(status.name));
                            map.insert(
                                Bytes::from_static(b"type"),
                                Frame::bulk(status.sink_type.name()),
                            );
                            map.insert(
                                Bytes::from_static(b"state"),
                                Frame::bulk(status.state.name()),
                            );
                            map.insert(
                                Bytes::from_static(b"events_sent"),
                                Frame::Integer(status.events_sent as i64),
                            );
                            map.insert(
                                Bytes::from_static(b"events_failed"),
                                Frame::Integer(status.events_failed as i64),
                            );
                            map.insert(Bytes::from_static(b"lag"), Frame::Integer(status.lag as i64));
                            Frame::Map(map)
                        }
                        None => Frame::null(),
                    }
                }
                _ => Frame::error("ERR invalid SINK subcommand"),
            }
        }
        "SINKS" => {
            let sinks = engine.list_sinks().await;
            Frame::array(
                sinks
                    .into_iter()
                    .map(|status| {
                        let mut map = HashMap::new();
                        map.insert(Bytes::from_static(b"name"), Frame::bulk(status.name));
                        map.insert(
                            Bytes::from_static(b"type"),
                            Frame::bulk(status.sink_type.name()),
                        );
                        map.insert(
                            Bytes::from_static(b"state"),
                            Frame::bulk(status.state.name()),
                        );
                        map.insert(
                            Bytes::from_static(b"events_sent"),
                            Frame::Integer(status.events_sent as i64),
                        );
                        Frame::Map(map)
                    })
                    .collect(),
            )
        }
        "LOG" => {
            if args.is_empty() {
                return Frame::error("ERR wrong number of arguments for 'cdc|log'");
            }

            match args[0].to_uppercase().as_str() {
                "INFO" => {
                    let stats = engine.log_stats().await;
                    let mut map = HashMap::new();
                    map.insert(
                        Bytes::from_static(b"segments"),
                        Frame::Integer(stats.total_segments as i64),
                    );
                    map.insert(
                        Bytes::from_static(b"events"),
                        Frame::Integer(stats.total_events as i64),
                    );
                    map.insert(Bytes::from_static(b"size"), Frame::Integer(stats.total_size as i64));
                    map.insert(
                        Bytes::from_static(b"size_human"),
                        Frame::bulk(stats.total_size_human()),
                    );
                    map.insert(
                        Bytes::from_static(b"oldest_event"),
                        Frame::Integer(stats.oldest_event as i64),
                    );
                    map.insert(
                        Bytes::from_static(b"newest_event"),
                        Frame::Integer(stats.newest_event as i64),
                    );
                    map.insert(
                        Bytes::from_static(b"compression"),
                        Frame::bulk(stats.compression.name()),
                    );
                    map.insert(
                        Bytes::from_static(b"retention"),
                        Frame::bulk(stats.retention_human()),
                    );
                    Frame::Map(map)
                }
                "RANGE" => {
                    if args.len() < 3 {
                        return Frame::error(
                            "ERR wrong number of arguments for 'cdc|log|range'",
                        );
                    }
                    let from: u64 = args[1].parse().unwrap_or(0);
                    let to: u64 = args[2].parse().unwrap_or(u64::MAX);
                    let count = if args.len() >= 5 && args[3].to_uppercase() == "COUNT" {
                        args[4].parse().unwrap_or(100)
                    } else {
                        100
                    };

                    let events = engine.log_range(from, to, count).await;
                    Frame::array(
                        events
                            .into_iter()
                            .map(|e| {
                            let mut map = HashMap::new();
                            map.insert(Bytes::from_static(b"id"), Frame::Integer(e.id as i64));
                            map.insert(
                                Bytes::from_static(b"op"),
                                Frame::bulk(e.operation.name()),
                            );
                            map.insert(Bytes::from_static(b"key"), Frame::Bulk(Some(e.key)));
                            Frame::Map(map)
                            })
                            .collect(),
                    )
                }
                "COMPACT" => match engine.compact().await {
                    Ok(()) => Frame::simple("OK"),
                    Err(e) => Frame::error(format!("ERR {}", e)),
                },
                "STATS" => {
                    let stats = engine.log_stats().await;
                    let mut map = HashMap::new();
                    map.insert(
                        Bytes::from_static(b"segments"),
                        Frame::Integer(stats.total_segments as i64),
                    );
                    map.insert(
                        Bytes::from_static(b"events"),
                        Frame::Integer(stats.total_events as i64),
                    );
                    map.insert(Bytes::from_static(b"size"), Frame::Integer(stats.total_size as i64));
                    Frame::Map(map)
                }
                _ => Frame::error("ERR invalid LOG subcommand"),
            }
        }
        "HELP" => Frame::array(vec![
            Frame::bulk("CDC <subcommand> [<arg> [value] ...]"),
            Frame::bulk("INFO -- Return CDC information."),
            Frame::bulk("SUBSCRIBE <name> [PATTERN <p>...] [OPERATIONS <op>...] [DB <db>...] [FORMAT <f>] [FROM <pos>|LATEST|EARLIEST] [WITH_OLD_VALUES] -- Create subscription."),
            Frame::bulk("SUBSCRIPTIONS -- List all subscriptions."),
            Frame::bulk("UNSUBSCRIBE <name> -- Delete subscription."),
            Frame::bulk("READ <name> [COUNT <n>] [BLOCK <ms>] -- Read events."),
            Frame::bulk("ACK <name> <id> [<id>...] -- Acknowledge events."),
            Frame::bulk("POSITION <name> -- Get current position."),
            Frame::bulk("SEEK <name> <pos>|LATEST|EARLIEST -- Seek to position."),
            Frame::bulk("SINK CREATE <name> <type> <config_json> -- Create sink."),
            Frame::bulk("SINK DELETE <name> -- Delete sink."),
            Frame::bulk("SINK ATTACH <subscription> <sink> -- Attach subscription to sink."),
            Frame::bulk("SINK DETACH <subscription> <sink> -- Detach subscription from sink."),
            Frame::bulk("SINK STATUS <name> -- Get sink status."),
            Frame::bulk("SINKS -- List all sinks."),
            Frame::bulk("LOG INFO -- Get change log info."),
            Frame::bulk("LOG RANGE <from> <to> [COUNT <n>] -- Read events by ID range."),
            Frame::bulk("LOG COMPACT -- Trigger log compaction."),
            Frame::bulk("LOG STATS -- Get log statistics."),
        ]),
        _ => Frame::error(format!(
            "ERR Unknown subcommand or wrong number of arguments for 'cdc|{}'",
            subcommand.to_lowercase()
        )),
    }
}

/// Handle CLUSTER MYID - return the node's unique ID
pub fn cluster_myid() -> Frame {
    // Generate a deterministic node ID based on process
    let node_id = format!("{:040x}", std::process::id() as u64);
    Frame::bulk(node_id)
}

/// Handle CLUSTER RESET [HARD|SOFT] - reset cluster state
pub fn cluster_reset(args: &[Bytes], _store: &Arc<Store>) -> Frame {
    let mode = args
        .first()
        .and_then(|a| std::str::from_utf8(a).ok())
        .unwrap_or("SOFT")
        .to_uppercase();

    match mode.as_str() {
        "SOFT" | "HARD" => {
            // In a full implementation, SOFT resets slot assignments and epochs,
            // HARD additionally generates a new node ID
            Frame::simple("OK")
        }
        _ => err_frame("Invalid CLUSTER RESET mode. Use SOFT or HARD."),
    }
}

/// Handle CLUSTER REPLICATE <node-id> - configure node as replica
pub fn cluster_replicate(args: &[Bytes]) -> Frame {
    match args.first() {
        Some(node_id) => {
            let _node_id = String::from_utf8_lossy(node_id);
            // In a full implementation, this would configure replication to the target node
            Frame::simple("OK")
        }
        None => err_frame("wrong number of arguments for 'CLUSTER REPLICATE'"),
    }
}

/// Handle CLUSTER SAVECONFIG - save cluster config to disk
pub fn cluster_saveconfig() -> Frame {
    Frame::simple("OK")
}

/// Handle CLUSTER FLUSHSLOTS - delete all slots from the node
pub fn cluster_flushslots() -> Frame {
    Frame::simple("OK")
}

/// Handle CLUSTER COUNT-FAILURE-REPORTS <node-id>
pub fn cluster_count_failure_reports(args: &[Bytes]) -> Frame {
    match args.first() {
        Some(_node_id) => Frame::Integer(0),
        None => err_frame("wrong number of arguments for 'CLUSTER COUNT-FAILURE-REPORTS'"),
    }
}

/// Handle CLUSTER SET-CONFIG-EPOCH <epoch>
pub fn cluster_set_config_epoch(args: &[Bytes]) -> Frame {
    match args.first() {
        Some(epoch_bytes) => {
            if let Ok(epoch_str) = std::str::from_utf8(epoch_bytes) {
                if epoch_str.parse::<u64>().is_ok() {
                    return Frame::simple("OK");
                }
            }
            err_frame("Invalid config epoch")
        }
        None => err_frame("wrong number of arguments for 'CLUSTER SET-CONFIG-EPOCH'"),
    }
}

/// Handle CLUSTER LINKS - return information about cluster bus connections
pub fn cluster_links() -> Frame {
    // Return empty array for standalone mode
    Frame::array(vec![])
}

/// Handle CLUSTER SHARDS - return information about cluster shards (Redis 7.0+)
pub fn cluster_shards() -> Frame {
    // Return empty array for standalone mode
    Frame::array(vec![])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_info() {
        let result = cluster("INFO", &[]);
        assert!(matches!(result, Frame::Bulk(_)));
    }

    #[test]
    fn test_cluster_myid() {
        let result = cluster("MYID", &[]);
        assert!(matches!(result, Frame::Bulk(_)));
    }

    #[test]
    fn test_cluster_keyslot() {
        let result = cluster("KEYSLOT", &["test".to_string()]);
        assert!(matches!(result, Frame::Integer(_)));
    }

    #[test]
    fn test_cluster_keyslot_missing_arg() {
        let result = cluster("KEYSLOT", &[]);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_cluster_unknown_subcommand() {
        let result = cluster("UNKNOWN", &[]);
        assert!(matches!(result, Frame::Error(_)));
    }
}
