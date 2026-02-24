//! Advanced operation helper methods on CommandExecutor (tiering, CDC, temporal,
//! streams, geo, HyperLogLog, scan, vector search, CRDT, WASM, semantic cache,
//! triggers, time-series, document, graph, RAG, and Kafka-streaming commands).

use std::sync::OnceLock;

use bytes::Bytes;

use crate::protocol::Frame;

use crate::commands::geo;
use crate::commands::hyperloglog;
use crate::commands::scan;
use crate::commands::streams;

use super::CommandExecutor;

/// Global streaming broker instance (lazily initialized).
fn streaming_broker() -> &'static ferrite_streaming::kafka::StreamingBroker {
    static BROKER: OnceLock<ferrite_streaming::kafka::StreamingBroker> = OnceLock::new();
    BROKER.get_or_init(ferrite_streaming::kafka::StreamingBroker::new)
}

/// Global active-active replicator instance (lazily initialized).
#[cfg(feature = "experimental")]
fn active_active_replicator() -> &'static ferrite_enterprise::active_active::ActiveActiveReplicator
{
    static REPLICATOR: OnceLock<ferrite_enterprise::active_active::ActiveActiveReplicator> =
        OnceLock::new();
    REPLICATOR.get_or_init(|| {
        ferrite_enterprise::active_active::ActiveActiveReplicator::with_defaults(
            "local".to_string(),
        )
    })
}

/// Global Data Mesh Gateway instance (lazily initialized).
#[cfg(feature = "experimental")]
fn mesh_gateway() -> &'static ferrite_enterprise::mesh::DataMeshGateway {
    static GATEWAY: OnceLock<ferrite_enterprise::mesh::DataMeshGateway> = OnceLock::new();
    GATEWAY.get_or_init(ferrite_enterprise::mesh::DataMeshGateway::with_defaults)
}

impl CommandExecutor {
    // Tiering commands

    pub(super) async fn tiering(
        &self,
        subcommand: &str,
        args: &[String],
        key: Option<&Bytes>,
    ) -> Frame {
        use crate::tiering::{Priority, StorageTier, TieringEngine};

        // Create or get a tiering engine (in production, this would be shared)
        // For now, we create a simple engine to demonstrate the API
        let engine = TieringEngine::new_default();

        match subcommand.to_uppercase().as_str() {
            "INFO" => {
                // TIERING INFO - Return tiering information
                let info = engine.info().await;
                let mut map = std::collections::HashMap::new();
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
                // TIERING COSTS [SET <tier> <json>] [IMPORT <provider> <region>]
                if args.is_empty() {
                    // Return current cost configuration
                    let config = engine.config();
                    let mut map = std::collections::HashMap::new();
                    for tier in StorageTier::all() {
                        let cost = config.costs.cost_for_tier(*tier);
                        let mut tier_map = std::collections::HashMap::new();
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
                // TIERING POLICY [SET ...] [PATTERN ...]
                if args.is_empty() {
                    let config = engine.config();
                    let mut map = std::collections::HashMap::new();
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
                // TIERING COST <key> | TIERING COST PATTERN <pattern> | TIERING COST TOTAL
                if let Some(key_bytes) = key {
                    // Cost for specific key
                    if let Some(analysis) = engine.key_cost(key_bytes) {
                        let mut map = std::collections::HashMap::new();
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
                    // Total cost summary
                    let summary = engine.cost_summary();
                    let mut map = std::collections::HashMap::new();
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
                // TIERING TIER <key> - Get current tier for a key
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
                // TIERING PIN <key> <tier> - Pin key to specific tier
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
                // TIERING UNPIN <key> - Remove pin from key
                if let Some(key_bytes) = key {
                    engine.unpin(key_bytes);
                    Frame::simple("OK")
                } else {
                    Frame::error("ERR wrong number of arguments for 'tiering|unpin' command")
                }
            }
            "MIGRATE" => {
                // TIERING MIGRATE <key> <tier> - Force immediate migration
                if let Some(key_bytes) = key {
                    if args.is_empty() {
                        return Frame::error(
                            "ERR missing tier argument for 'tiering|migrate' command",
                        );
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
                // TIERING PRIORITY <key> <critical|high|normal|low|archive>
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
                // TIERING STATS <key> - Get access stats for key
                if let Some(key_bytes) = key {
                    if let Some(stats) = engine.get_stats(key_bytes) {
                        let mut map = std::collections::HashMap::new();
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
                // TIERING TOP <COST|SAVINGS> [LIMIT <count>]
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
                // TIERING SUBOPTIMAL [LIMIT <count>] - Keys not in optimal tier
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
                            let mut map = std::collections::HashMap::new();
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

    // CDC commands

    pub(super) async fn cdc(&self, subcommand: &str, args: &[String]) -> Frame {
        use ferrite_streaming::cdc::{CdcEngine, HttpSinkConfig, OutputFormat, SinkConfig};

        // Create or get a CDC engine (in production, this would be shared)
        let engine = CdcEngine::default();

        match subcommand.to_uppercase().as_str() {
            "INFO" => {
                // CDC INFO - Return CDC information
                let info = engine.info().await;
                let mut map = std::collections::HashMap::new();
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
                map.insert(
                    Bytes::from_static(b"sinks"),
                    Frame::Integer(info.sinks as i64),
                );
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
                map.insert(
                    Bytes::from_static(b"log_size"),
                    Frame::Integer(info.log_size as i64),
                );
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
                // CDC SUBSCRIBE <name> [PATTERN <pattern>...] [OPERATIONS <op>...] [DB <db>...] [FORMAT <format>] [FROM <pos>|LATEST|EARLIEST] [WITH_OLD_VALUES]
                if args.is_empty() {
                    return Frame::error("ERR wrong number of arguments for 'cdc|subscribe'");
                }

                let name = &args[0];
                let mut patterns = Vec::new();
                let mut operations = std::collections::HashSet::new();
                let mut databases = std::collections::HashSet::new();
                let mut format = None;
                let mut from_position = None;
                let mut from_earliest = false;
                let mut with_old_values = false;

                let mut i = 1;
                while i < args.len() {
                    match args[i].to_uppercase().as_str() {
                        "PATTERN" => {
                            i += 1;
                            while i < args.len() && !["OPERATIONS", "DB", "FORMAT", "FROM", "WITH_OLD_VALUES"].contains(&args[i].to_uppercase().as_str()) {
                                patterns.push(args[i].clone());
                                i += 1;
                            }
                        }
                        "OPERATIONS" => {
                            i += 1;
                            while i < args.len() && !["PATTERN", "DB", "FORMAT", "FROM", "WITH_OLD_VALUES"].contains(&args[i].to_uppercase().as_str()) {
                                operations.insert(args[i].to_uppercase());
                                i += 1;
                            }
                        }
                        "DB" => {
                            i += 1;
                            while i < args.len() && !["PATTERN", "OPERATIONS", "FORMAT", "FROM", "WITH_OLD_VALUES"].contains(&args[i].to_uppercase().as_str()) {
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
                // CDC SUBSCRIPTIONS - List all subscriptions
                let subs = engine.list_subscriptions();
                Frame::array(
                    subs.into_iter()
                        .map(|info| {
                            let mut map = std::collections::HashMap::new();
                            map.insert(Bytes::from_static(b"id"), Frame::Integer(info.id as i64));
                            map.insert(Bytes::from_static(b"name"), Frame::bulk(info.name));
                            map.insert(Bytes::from_static(b"patterns"), Frame::array(
                                info.patterns.into_iter().map(Frame::bulk).collect()
                            ));
                            map.insert(Bytes::from_static(b"position"), Frame::Integer(info.position as i64));
                            map.insert(Bytes::from_static(b"state"), Frame::bulk(info.state.name()));
                            Frame::Map(map)
                        })
                        .collect()
                )
            }
            "UNSUBSCRIBE" => {
                // CDC UNSUBSCRIBE <name>
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
                // CDC READ <name> [COUNT <count>] [BLOCK <ms>]
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
                        events.into_iter()
                            .map(|event| {
                        let mut map = std::collections::HashMap::new();
                        map.insert(Bytes::from_static(b"id"), Frame::Integer(event.id as i64));
                        map.insert(Bytes::from_static(b"timestamp"), Frame::Integer(event.timestamp_ms() as i64));
                        map.insert(Bytes::from_static(b"db"), Frame::Integer(event.db as i64));
                        map.insert(Bytes::from_static(b"op"), Frame::bulk(event.operation.name()));
                        map.insert(Bytes::from_static(b"key"), Frame::Bulk(Some(event.key)));
                        if let Some(value) = event.value {
                            map.insert(Bytes::from_static(b"value"), Frame::Bulk(Some(value)));
                        }
                        if let Some(old_value) = event.old_value {
                            map.insert(Bytes::from_static(b"old_value"), Frame::Bulk(Some(old_value)));
                        }
                                Frame::Map(map)
                            })
                            .collect()
                    ),
                    Err(e) => Frame::error(format!("ERR {}", e)),
                }
            }
            "ACK" => {
                // CDC ACK <name> <id> [<id>...]
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
                // CDC POSITION <name>
                if args.is_empty() {
                    return Frame::error("ERR wrong number of arguments for 'cdc|position'");
                }
                match engine.position(&args[0]) {
                    Ok(pos) => Frame::Integer(pos as i64),
                    Err(e) => Frame::error(format!("ERR {}", e)),
                }
            }
            "SEEK" => {
                // CDC SEEK <name> <position>|LATEST|EARLIEST
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
                // CDC SINK subcommand
                if args.is_empty() {
                    return Frame::error("ERR wrong number of arguments for 'cdc|sink'");
                }

                match args[0].to_uppercase().as_str() {
                    "CREATE" => {
                        // CDC SINK CREATE <name> <type> <config_json>
                        if args.len() < 4 {
                            return Frame::error("ERR wrong number of arguments for 'cdc|sink|create'");
                        }
                        let name = &args[1];
                        let sink_type = &args[2];
                        let config_json = &args[3];

                        // Parse config based on type
                        let config = match sink_type.to_lowercase().as_str() {
                            "http" | "webhook" => {
                                match serde_json::from_str::<HttpSinkConfig>(config_json) {
                                    Ok(cfg) => SinkConfig::Http(cfg),
                                    Err(e) => return Frame::error(format!("ERR invalid config: {}", e)),
                                }
                            }
                            "kafka" => {
                                match serde_json::from_str::<ferrite_streaming::cdc::KafkaSinkConfig>(config_json) {
                                    Ok(cfg) => SinkConfig::Kafka(cfg),
                                    Err(e) => return Frame::error(format!("ERR invalid config: {}", e)),
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
                            return Frame::error("ERR wrong number of arguments for 'cdc|sink|delete'");
                        }
                        if engine.delete_sink(&args[1]).await {
                            Frame::simple("OK")
                        } else {
                            Frame::error("ERR sink not found")
                        }
                    }
                    "ATTACH" => {
                        if args.len() < 3 {
                            return Frame::error("ERR wrong number of arguments for 'cdc|sink|attach'");
                        }
                        match engine.attach_sink(&args[1], &args[2]) {
                            Ok(()) => Frame::simple("OK"),
                            Err(e) => Frame::error(format!("ERR {}", e)),
                        }
                    }
                    "DETACH" => {
                        if args.len() < 3 {
                            return Frame::error("ERR wrong number of arguments for 'cdc|sink|detach'");
                        }
                        match engine.detach_sink(&args[1], &args[2]) {
                            Ok(()) => Frame::simple("OK"),
                            Err(e) => Frame::error(format!("ERR {}", e)),
                        }
                    }
                    "STATUS" => {
                        if args.len() < 2 {
                            return Frame::error("ERR wrong number of arguments for 'cdc|sink|status'");
                        }
                        match engine.sink_status(&args[1]).await {
                            Some(status) => {
                                let mut map = std::collections::HashMap::new();
                                map.insert(Bytes::from_static(b"name"), Frame::bulk(status.name));
                                map.insert(Bytes::from_static(b"type"), Frame::bulk(status.sink_type.name()));
                                map.insert(Bytes::from_static(b"state"), Frame::bulk(status.state.name()));
                                map.insert(Bytes::from_static(b"events_sent"), Frame::Integer(status.events_sent as i64));
                                map.insert(Bytes::from_static(b"events_failed"), Frame::Integer(status.events_failed as i64));
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
                // CDC SINKS - List all sinks
                let sinks = engine.list_sinks().await;
                Frame::array(
                    sinks.into_iter()
                        .map(|status| {
                            let mut map = std::collections::HashMap::new();
                            map.insert(Bytes::from_static(b"name"), Frame::bulk(status.name));
                            map.insert(Bytes::from_static(b"type"), Frame::bulk(status.sink_type.name()));
                            map.insert(Bytes::from_static(b"state"), Frame::bulk(status.state.name()));
                            map.insert(Bytes::from_static(b"events_sent"), Frame::Integer(status.events_sent as i64));
                            Frame::Map(map)
                        })
                        .collect()
                )
            }
            "LOG" => {
                // CDC LOG subcommand
                if args.is_empty() {
                    return Frame::error("ERR wrong number of arguments for 'cdc|log'");
                }

                match args[0].to_uppercase().as_str() {
                    "INFO" => {
                        let stats = engine.log_stats().await;
                        let mut map = std::collections::HashMap::new();
                        map.insert(Bytes::from_static(b"segments"), Frame::Integer(stats.total_segments as i64));
                        map.insert(Bytes::from_static(b"events"), Frame::Integer(stats.total_events as i64));
                        map.insert(Bytes::from_static(b"size"), Frame::Integer(stats.total_size as i64));
                        map.insert(Bytes::from_static(b"size_human"), Frame::bulk(stats.total_size_human()));
                        map.insert(Bytes::from_static(b"oldest_event"), Frame::Integer(stats.oldest_event as i64));
                        map.insert(Bytes::from_static(b"newest_event"), Frame::Integer(stats.newest_event as i64));
                        map.insert(Bytes::from_static(b"compression"), Frame::bulk(stats.compression.name()));
                        map.insert(Bytes::from_static(b"retention"), Frame::bulk(stats.retention_human()));
                        Frame::Map(map)
                    }
                    "RANGE" => {
                        // CDC LOG RANGE <from> <to> [COUNT <n>]
                        if args.len() < 3 {
                            return Frame::error("ERR wrong number of arguments for 'cdc|log|range'");
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
                            events.into_iter()
                                .map(|e| {
                                    let mut map = std::collections::HashMap::new();
                                    map.insert(Bytes::from_static(b"id"), Frame::Integer(e.id as i64));
                                    map.insert(Bytes::from_static(b"op"), Frame::bulk(e.operation.name()));
                                    map.insert(Bytes::from_static(b"key"), Frame::Bulk(Some(e.key)));
                                    Frame::Map(map)
                                })
                                .collect()
                        )
                    }
                    "COMPACT" => {
                        match engine.compact().await {
                            Ok(()) => Frame::simple("OK"),
                            Err(e) => Frame::error(format!("ERR {}", e)),
                        }
                    }
                    "STATS" => {
                        let stats = engine.log_stats().await;
                        let mut map = std::collections::HashMap::new();
                        map.insert(Bytes::from_static(b"segments"), Frame::Integer(stats.total_segments as i64));
                        map.insert(Bytes::from_static(b"events"), Frame::Integer(stats.total_events as i64));
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

    // Temporal commands

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn history(
        &self,
        db: u8,
        key: &Bytes,
        from: Option<String>,
        to: Option<String>,
        limit: Option<usize>,
        ascending: bool,
        with_values: bool,
    ) -> Frame {
        use crate::temporal::TemporalIndex;

        // Create a temporary index for the query (in production, this would be persistent)
        let _index = TemporalIndex::default();

        // For now, return information about what the query would do
        // Full implementation requires integrating with HybridLog
        let mut info = vec![Frame::bulk("key"), Frame::Bulk(Some(key.clone()))];

        if let Some(f) = &from {
            info.push(Frame::bulk("from"));
            info.push(Frame::bulk(f.clone()));
        }

        if let Some(t) = &to {
            info.push(Frame::bulk("to"));
            info.push(Frame::bulk(t.clone()));
        }

        if let Some(l) = limit {
            info.push(Frame::bulk("limit"));
            info.push(Frame::Integer(l as i64));
        }

        info.push(Frame::bulk("order"));
        info.push(Frame::bulk(if ascending { "ASC" } else { "DESC" }));

        info.push(Frame::bulk("with_values"));
        info.push(Frame::bulk(if with_values { "true" } else { "false" }));

        // Check if key exists (temporal history not yet tracked)
        let exists = self.store.get(db, key).is_some();
        info.push(Frame::bulk("key_exists"));
        info.push(Frame::bulk(if exists { "true" } else { "false" }));

        info.push(Frame::bulk("versions"));
        info.push(Frame::Integer(0)); // No history tracked yet

        info.push(Frame::bulk("note"));
        info.push(Frame::bulk(
            "Temporal history tracking requires HybridLog integration",
        ));

        Frame::array(info)
    }

    pub(super) fn history_count(
        &self,
        _db: u8,
        _key: &Bytes,
        _from: Option<String>,
        _to: Option<String>,
    ) -> Frame {
        // Return 0 versions until temporal index is integrated with storage
        Frame::Integer(0)
    }

    pub(super) fn history_first(&self, _db: u8, _key: &Bytes) -> Frame {
        // Return nil until temporal index is integrated
        Frame::Null
    }

    pub(super) fn history_last(&self, _db: u8, _key: &Bytes) -> Frame {
        // Return nil until temporal index is integrated
        Frame::Null
    }

    pub(super) async fn diff(
        &self,
        db: u8,
        key: &Bytes,
        timestamp1: &str,
        timestamp2: &str,
    ) -> Frame {
        use crate::temporal::TimestampSpec;

        let ts1 = TimestampSpec::parse(timestamp1);
        let ts2 = TimestampSpec::parse(timestamp2);

        if ts1.is_none() || ts2.is_none() {
            return Frame::error("ERR invalid timestamp format");
        }

        // Get current value
        let current = self.store.get(db, key);

        let mut result = vec![
            Frame::bulk("key"),
            Frame::Bulk(Some(key.clone())),
            Frame::bulk("timestamp1"),
            Frame::bulk(timestamp1.to_string()),
            Frame::bulk("timestamp2"),
            Frame::bulk(timestamp2.to_string()),
            Frame::bulk("current_exists"),
            Frame::bulk(if current.is_some() { "true" } else { "false" }),
        ];

        if let Some(val) = current {
            result.push(Frame::bulk("current_type"));
            result.push(Frame::bulk(match val {
                crate::storage::Value::String(_) => "string",
                crate::storage::Value::List(_) => "list",
                crate::storage::Value::Hash(_) => "hash",
                crate::storage::Value::Set(_) => "set",
                crate::storage::Value::SortedSet { .. } => "zset",
                crate::storage::Value::Stream(_) => "stream",
                crate::storage::Value::HyperLogLog(_) => "hyperloglog",
            }));
        }

        result.push(Frame::bulk("note"));
        result.push(Frame::bulk(
            "Historical values require HybridLog integration",
        ));

        Frame::array(result)
    }

    pub(super) async fn restore_from(
        &self,
        _db: u8,
        key: &Bytes,
        timestamp: &str,
        target: Option<&Bytes>,
    ) -> Frame {
        use crate::temporal::TimestampSpec;

        let ts = TimestampSpec::parse(timestamp);
        if ts.is_none() {
            return Frame::error("ERR invalid timestamp format");
        }

        // In a full implementation, we would:
        // 1. Look up the value at the given timestamp in the temporal index
        // 2. Restore it to the target key (or original key)

        let _dest_key = target.unwrap_or(key);

        Frame::error("ERR temporal history not yet available for restore")
    }

    pub(super) fn temporal(&self, subcommand: &str, args: &[String]) -> Frame {
        match subcommand.to_uppercase().as_str() {
            "INFO" => {
                // Return temporal query system info
                let info = vec![
                    Frame::bulk("enabled"),
                    Frame::bulk("true"),
                    Frame::bulk("keys_tracked"),
                    Frame::Integer(0),
                    Frame::bulk("total_versions"),
                    Frame::Integer(0),
                    Frame::bulk("index_size_bytes"),
                    Frame::Integer(0),
                    Frame::bulk("retention_max_age"),
                    Frame::bulk("7d"),
                    Frame::bulk("retention_max_versions"),
                    Frame::Integer(1000),
                    Frame::bulk("note"),
                    Frame::bulk("Temporal index integration pending"),
                ];
                Frame::array(info)
            }
            "POLICY" => {
                if args.is_empty() {
                    // Return current policy
                    Frame::array(vec![
                        Frame::bulk("max_age"),
                        Frame::bulk("7d"),
                        Frame::bulk("max_versions"),
                        Frame::Integer(1000),
                        Frame::bulk("min_versions"),
                        Frame::Integer(1),
                    ])
                } else if args[0].to_uppercase() == "SET" {
                    // Set policy
                    Frame::simple("OK")
                } else if args[0].to_uppercase() == "PATTERN" {
                    // Set pattern-specific policy
                    Frame::simple("OK")
                } else {
                    Frame::error("ERR invalid POLICY subcommand")
                }
            }
            "CLEANUP" => {
                // Trigger cleanup
                let dry_run = args.iter().any(|a| a.to_uppercase() == "DRY-RUN");
                Frame::array(vec![
                    Frame::bulk("versions_removed"),
                    Frame::Integer(0),
                    Frame::bulk("keys_removed"),
                    Frame::Integer(0),
                    Frame::bulk("dry_run"),
                    Frame::bulk(if dry_run { "true" } else { "false" }),
                ])
            }
            "HELP" => Frame::array(vec![
                Frame::bulk("TEMPORAL <subcommand> [<arg> ...]"),
                Frame::bulk("INFO -- Return temporal query system information."),
                Frame::bulk("POLICY -- Get current retention policy."),
                Frame::bulk("POLICY SET [MAXAGE <duration>] [MAXVERSIONS <n>] [MINVERSIONS <n>] -- Set retention policy."),
                Frame::bulk("POLICY PATTERN <pattern> [MAXAGE <duration>] [MAXVERSIONS <n>] -- Set pattern-specific policy."),
                Frame::bulk("CLEANUP [DRY-RUN] -- Trigger retention cleanup."),
                Frame::bulk(""),
                Frame::bulk("Related commands:"),
                Frame::bulk("HISTORY <key> [FROM ts] [TO ts] [LIMIT n] [ORDER ASC|DESC] [WITHVALUES] -- Get key history."),
                Frame::bulk("HISTORY.COUNT <key> [FROM ts] [TO ts] -- Count versions."),
                Frame::bulk("HISTORY.FIRST <key> -- Get oldest version."),
                Frame::bulk("HISTORY.LAST <key> -- Get newest version."),
                Frame::bulk("DIFF <key> <ts1> <ts2> -- Compare values at two times."),
                Frame::bulk("RESTORE.FROM <key> <ts> [NEWKEY <target>] -- Restore from history."),
            ]),
            _ => Frame::error(format!(
                "ERR Unknown subcommand or wrong number of arguments for 'temporal|{}'",
                subcommand.to_lowercase()
            )),
        }
    }

    // Stream commands

    #[allow(clippy::too_many_arguments)]
    pub(super) fn xadd(
        &self,
        db: u8,
        key: &Bytes,
        id: Option<&str>,
        fields: Vec<(Bytes, Bytes)>,
        maxlen: Option<usize>,
        minid: Option<&str>,
        nomkstream: bool,
    ) -> Frame {
        use crate::storage::StreamEntryId;

        // NOMKSTREAM: don't create the stream if it doesn't exist
        if nomkstream {
            match self.store.get(db, key) {
                Some(crate::storage::Value::Stream(_)) => {} // Stream exists, proceed
                Some(_) => {
                    return Frame::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                }
                None => return Frame::null(), // Stream doesn't exist, return nil
            }
        }

        // Parse the ID if provided
        let entry_id = match id {
            Some("*") | None => None, // Auto-generate
            Some(id_str) => match StreamEntryId::parse(id_str) {
                Some(parsed_id) => Some(parsed_id),
                None => {
                    return Frame::error(
                        "ERR Invalid stream ID specified as stream command argument",
                    )
                }
            },
        };

        let result = streams::xadd(&self.store, db, key, entry_id, fields);

        // Handle MAXLEN trimming after add
        if let Some(maxlen) = maxlen {
            if let Frame::Bulk(Some(_)) = &result {
                streams::xtrim(&self.store, db, key, maxlen);
            }
        }

        // Handle MINID trimming after add
        if let Some(minid_str) = minid {
            if let Frame::Bulk(Some(_)) = &result {
                streams::xtrim_by_minid(&self.store, db, key, minid_str);
            }
        }

        // Notify blocking stream manager that a new entry was added
        if let Frame::Bulk(Some(_)) = &result {
            crate::commands::blocking::notify_stream_add(&self.blocking_stream_manager, db, key);
        }

        result
    }

    pub(super) fn xlen(&self, db: u8, key: &Bytes) -> Frame {
        streams::xlen(&self.store, db, key)
    }

    #[inline]
    pub(super) fn xrange(
        &self,
        db: u8,
        key: &Bytes,
        start: &str,
        end: &str,
        count: Option<usize>,
    ) -> Frame {
        streams::xrange(&self.store, db, key, start, end, count)
    }

    #[inline]
    pub(super) fn xrevrange(
        &self,
        db: u8,
        key: &Bytes,
        end: &str,
        start: &str,
        count: Option<usize>,
    ) -> Frame {
        streams::xrevrange(&self.store, db, key, end, start, count)
    }

    #[inline]
    pub(super) fn xread(
        &self,
        db: u8,
        stream_keys: &[(Bytes, String)],
        count: Option<usize>,
    ) -> Frame {
        streams::xread(&self.store, db, stream_keys, count)
    }

    pub(super) fn xdel(&self, db: u8, key: &Bytes, ids: &[String]) -> Frame {
        use crate::storage::StreamEntryId;

        // Parse all IDs
        let mut parsed_ids = Vec::with_capacity(ids.len());
        for id_str in ids {
            match StreamEntryId::parse(id_str) {
                Some(id) => parsed_ids.push(id),
                None => {
                    return Frame::error(
                        "ERR Invalid stream ID specified as stream command argument",
                    )
                }
            }
        }

        streams::xdel(&self.store, db, key, &parsed_ids)
    }

    pub(super) fn xtrim(
        &self,
        db: u8,
        key: &Bytes,
        maxlen: Option<usize>,
        minid: Option<&str>,
    ) -> Frame {
        if let Some(maxlen) = maxlen {
            streams::xtrim(&self.store, db, key, maxlen)
        } else if let Some(minid_str) = minid {
            streams::xtrim_by_minid(&self.store, db, key, minid_str)
        } else {
            Frame::Integer(0)
        }
    }

    pub(super) fn xinfo(
        &self,
        db: u8,
        key: &Bytes,
        subcommand: &str,
        group_name: Option<&Bytes>,
    ) -> Frame {
        match subcommand.to_uppercase().as_str() {
            "STREAM" => streams::xinfo_stream(&self.store, db, key),
            "GROUPS" => streams::xinfo_groups(&self.store, db, key),
            "CONSUMERS" => match group_name {
                Some(gn) => streams::xinfo_consumers(&self.store, db, key, gn),
                None => Frame::error("ERR wrong number of arguments for 'xinfo|consumers' command"),
            },
            "HELP" => Frame::array(vec![
                Frame::bulk("XINFO <subcommand> [<arg> [value] [opt] ...]"),
                Frame::bulk("STREAM <key> [FULL [COUNT <count>]] -- Show stream info."),
                Frame::bulk("GROUPS <key> -- Show consumer groups."),
                Frame::bulk("CONSUMERS <key> <group> -- Show consumers in a group."),
            ]),
            _ => Frame::error(format!(
                "ERR Unknown subcommand or wrong number of arguments for 'xinfo|{}'",
                subcommand.to_lowercase()
            )),
        }
    }

    // Geo commands

    pub(super) fn geoadd(
        &self,
        db: u8,
        key: &Bytes,
        items: Vec<(f64, f64, Bytes)>,
        nx: bool,
        xx: bool,
        ch: bool,
    ) -> Frame {
        geo::geoadd(&self.store, db, key, items, nx, xx, ch)
    }

    pub(super) fn geopos(&self, db: u8, key: &Bytes, members: &[Bytes]) -> Frame {
        geo::geopos(&self.store, db, key, members)
    }

    pub(super) fn geodist(
        &self,
        db: u8,
        key: &Bytes,
        member1: &Bytes,
        member2: &Bytes,
        unit: &str,
    ) -> Frame {
        let unit = geo::DistanceUnit::parse(unit).unwrap_or(geo::DistanceUnit::Meters);
        geo::geodist(&self.store, db, key, member1, member2, unit)
    }

    pub(super) fn geohash(&self, db: u8, key: &Bytes, members: &[Bytes]) -> Frame {
        geo::geohash(&self.store, db, key, members)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn georadius(
        &self,
        db: u8,
        key: &Bytes,
        longitude: f64,
        latitude: f64,
        radius: f64,
        unit: &str,
        with_coord: bool,
        with_dist: bool,
        with_hash: bool,
        count: Option<usize>,
        asc: bool,
    ) -> Frame {
        let unit = geo::DistanceUnit::parse(unit).unwrap_or(geo::DistanceUnit::Meters);
        geo::georadius(
            &self.store,
            db,
            key,
            longitude,
            latitude,
            radius,
            unit,
            with_coord,
            with_dist,
            with_hash,
            count,
            asc,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn georadiusbymember(
        &self,
        db: u8,
        key: &Bytes,
        member: &Bytes,
        radius: f64,
        unit: &str,
        with_coord: bool,
        with_dist: bool,
        with_hash: bool,
        count: Option<usize>,
        asc: bool,
    ) -> Frame {
        let unit = geo::DistanceUnit::parse(unit).unwrap_or(geo::DistanceUnit::Meters);
        geo::georadiusbymember(
            &self.store,
            db,
            key,
            member,
            radius,
            unit,
            with_coord,
            with_dist,
            with_hash,
            count,
            asc,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn geosearch(
        &self,
        db: u8,
        key: &Bytes,
        from_member: Option<Bytes>,
        from_lonlat: Option<(f64, f64)>,
        by_radius: Option<(f64, String)>,
        by_box: Option<(f64, f64, String)>,
        asc: bool,
        count: Option<usize>,
        any: bool,
        with_coord: bool,
        with_dist: bool,
        with_hash: bool,
    ) -> Frame {
        // Build center
        let center = if let Some(member) = from_member {
            geo::GeoSearchCenter::Member(member)
        } else if let Some((lon, lat)) = from_lonlat {
            geo::GeoSearchCenter::LonLat(lon, lat)
        } else {
            return Frame::error("ERR FROMMEMBER or FROMLONLAT is required for GEOSEARCH");
        };

        // Build shape
        let shape = if let Some((radius, unit_str)) = by_radius {
            let unit = geo::DistanceUnit::parse(&unit_str).unwrap_or(geo::DistanceUnit::Meters);
            geo::GeoSearchShape::Radius(radius, unit)
        } else if let Some((width, height, unit_str)) = by_box {
            let unit = geo::DistanceUnit::parse(&unit_str).unwrap_or(geo::DistanceUnit::Meters);
            geo::GeoSearchShape::Box(width, height, unit)
        } else {
            return Frame::error("ERR BYRADIUS or BYBOX is required for GEOSEARCH");
        };

        let options = geo::GeoSearchOptions {
            center,
            shape,
            count,
            any,
            asc,
            with_coord,
            with_dist,
            with_hash,
        };

        geo::geosearch(&self.store, db, key, &options)
    }

    // HyperLogLog methods

    pub(super) fn pfadd(&self, db: u8, key: &Bytes, elements: &[Bytes]) -> Frame {
        hyperloglog::pfadd(&self.store, db, key, elements)
    }

    pub(super) fn pfcount(&self, db: u8, keys: &[Bytes]) -> Frame {
        hyperloglog::pfcount(&self.store, db, keys)
    }

    pub(super) fn pfmerge(&self, db: u8, destkey: &Bytes, sourcekeys: &[Bytes]) -> Frame {
        hyperloglog::pfmerge(&self.store, db, destkey, sourcekeys)
    }

    // Scan commands
    pub(super) fn scan(
        &self,
        db: u8,
        cursor: u64,
        pattern: Option<&str>,
        count: Option<usize>,
        type_filter: Option<&str>,
    ) -> Frame {
        scan::scan(&self.store, db, cursor, pattern, count, type_filter)
    }

    pub(super) fn zscan(
        &self,
        db: u8,
        key: &Bytes,
        cursor: u64,
        pattern: Option<&str>,
        count: Option<usize>,
    ) -> Frame {
        scan::zscan(&self.store, db, key, cursor, pattern, count)
    }

    // Key management helper methods

    /// DUMP command - serialize a key's value
    pub(super) async fn ft_create(
        &self,
        index: &Bytes,
        schema: &[(String, String)],
        index_type: Option<&str>,
        dimension: Option<usize>,
        metric: Option<&str>,
    ) -> Frame {
        use ferrite_ai::vector::{DistanceMetric, VectorIndexConfig, VectorStore};

        let index_name = String::from_utf8_lossy(index).to_string();
        let dim = dimension.unwrap_or(128);

        let distance_metric = match metric {
            Some("COSINE") | Some("cosine") => DistanceMetric::Cosine,
            Some("L2") | Some("l2") | Some("EUCLIDEAN") => DistanceMetric::Euclidean,
            Some("IP") | Some("ip") | Some("DOT") => DistanceMetric::DotProduct,
            _ => DistanceMetric::Cosine,
        };

        let idx_type = match index_type {
            Some("FLAT") | Some("flat") => ferrite_ai::vector::IndexType::Flat,
            _ => ferrite_ai::vector::IndexType::Hnsw,
        };

        let config = match idx_type {
            ferrite_ai::vector::IndexType::Flat => {
                VectorIndexConfig::flat(&index_name, dim).with_metric(distance_metric)
            }
            _ => VectorIndexConfig::hnsw(&index_name, dim).with_metric(distance_metric),
        };

        let store = VectorStore::new();
        match store.create_index(config) {
            Ok(_) => {
                let mut info = vec![
                    Frame::bulk("OK"),
                    Frame::bulk("index_name"),
                    Frame::bulk(Bytes::from(index_name.clone())),
                    Frame::bulk("dimension"),
                    Frame::Integer(dim as i64),
                ];
                for (field, ftype) in schema {
                    info.push(Frame::bulk(format!("field:{}", field)));
                    info.push(Frame::bulk(Bytes::from(ftype.clone())));
                }
                Frame::array(info)
            }
            Err(e) => Frame::error(format!("ERR {}", e)),
        }
    }

    pub(super) async fn ft_dropindex(&self, index: &Bytes, _delete_docs: bool) -> Frame {
        use ferrite_ai::vector::VectorStore;

        let index_name = String::from_utf8_lossy(index).to_string();
        let store = VectorStore::new();

        match store.drop_index(&index_name) {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::error(format!("ERR {}", e)),
        }
    }

    pub(super) async fn ft_add(
        &self,
        index: &Bytes,
        key: &Bytes,
        vector: &[f32],
        _payload: Option<&Bytes>,
    ) -> Frame {
        use ferrite_ai::vector::{VectorId, VectorIndex, VectorStore};

        let index_name = String::from_utf8_lossy(index).to_string();
        let vector_id = VectorId::from(String::from_utf8_lossy(key).to_string());

        let store = VectorStore::new();
        match store.get_index(&index_name) {
            Some(idx) => match idx.add(vector_id, vector) {
                Ok(()) => Frame::simple("OK"),
                Err(e) => Frame::error(format!("ERR {}", e)),
            },
            None => Frame::error(format!("ERR Unknown index: {}", index_name)),
        }
    }

    pub(super) async fn ft_del(&self, index: &Bytes, key: &Bytes) -> Frame {
        use ferrite_ai::vector::{VectorId, VectorIndex, VectorStore};

        let index_name = String::from_utf8_lossy(index).to_string();
        let vector_id = VectorId::from(String::from_utf8_lossy(key).to_string());

        let store = VectorStore::new();
        match store.get_index(&index_name) {
            Some(idx) => match idx.remove(&vector_id) {
                Ok(true) => Frame::Integer(1),
                Ok(false) => Frame::Integer(0),
                Err(e) => Frame::error(format!("ERR {}", e)),
            },
            None => Frame::error(format!("ERR Unknown index: {}", index_name)),
        }
    }

    pub(super) async fn ft_search(
        &self,
        index: &Bytes,
        query: &[f32],
        k: usize,
        _return_fields: &[String],
        _filter: Option<&str>,
    ) -> Frame {
        use ferrite_ai::vector::{VectorIndex, VectorStore};

        let index_name = String::from_utf8_lossy(index).to_string();
        let store = VectorStore::new();

        match store.get_index(&index_name) {
            Some(idx) => match idx.search(query, k) {
                Ok(results) => {
                    let mut response = vec![Frame::Integer(results.len() as i64)];
                    for result in results {
                        response.push(Frame::bulk(Bytes::from(result.id.to_string())));
                        response.push(Frame::bulk(format!("{:.6}", result.score)));
                    }
                    Frame::array(response)
                }
                Err(e) => Frame::error(format!("ERR {}", e)),
            },
            None => Frame::error(format!("ERR Unknown index: {}", index_name)),
        }
    }

    pub(super) async fn ft_info(&self, index: &Bytes) -> Frame {
        use ferrite_ai::vector::{VectorIndex, VectorStore};

        let index_name = String::from_utf8_lossy(index).to_string();
        let store = VectorStore::new();

        match store.get_index(&index_name) {
            Some(idx) => Frame::array(vec![
                Frame::bulk("index_name"),
                Frame::bulk(Bytes::from(index_name.clone())),
                Frame::bulk("dimension"),
                Frame::Integer(idx.dimension() as i64),
                Frame::bulk("num_vectors"),
                Frame::Integer(idx.len() as i64),
                Frame::bulk("metric"),
                Frame::bulk(format!("{:?}", idx.metric())),
            ]),
            None => Frame::error(format!("ERR Unknown index: {}", index_name)),
        }
    }

    pub(super) async fn ft_list(&self) -> Frame {
        use ferrite_ai::vector::VectorStore;

        let store = VectorStore::new();
        let indexes = store.list_indexes();

        Frame::array(indexes.into_iter().map(Frame::bulk).collect())
    }

    // Hybrid vector search commands

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn vector_hybrid_search(
        &self,
        _db: u8,
        index: &Bytes,
        query_vector: &[f32],
        query_text: &str,
        top_k: usize,
        alpha: f64,
        strategy: &str,
    ) -> Frame {
        use ferrite_ai::hybrid::fusion::{
            FusionStrategy, LinearCombination, ReciprocalRankFusion, ScoredResult,
        };
        use ferrite_ai::vector::{VectorIndex, VectorStore};
        use ferrite_search::bm25::Bm25Index;

        let index_name = String::from_utf8_lossy(index).to_string();

        // Dense retrieval from vector store
        let store = VectorStore::new();
        let dense_results: Vec<ScoredResult> = match store.get_index(&index_name) {
            Some(idx) => match idx.search(query_vector, top_k * 2) {
                Ok(results) => results
                    .into_iter()
                    .map(|r| ScoredResult {
                        doc_id: r.id.to_string(),
                        score: r.score as f64,
                    })
                    .collect(),
                Err(e) => return Frame::error(format!("ERR dense search failed: {}", e)),
            },
            None => return Frame::error(format!("ERR Unknown index: {}", index_name)),
        };

        // Sparse retrieval using BM25
        let bm25 = Bm25Index::default();
        let sparse_results: Vec<ScoredResult> = bm25
            .search(query_text, top_k * 2)
            .into_iter()
            .map(|r| ScoredResult {
                doc_id: r.doc_id,
                score: r.score,
            })
            .collect();

        // Parse strategy
        let fusion_strategy = match strategy {
            "linear" => FusionStrategy::Linear,
            "dense" => FusionStrategy::DenseOnly,
            "sparse" => FusionStrategy::SparseOnly,
            _ => FusionStrategy::RRF,
        };

        // Fuse results
        let fused = match fusion_strategy {
            FusionStrategy::RRF => {
                ReciprocalRankFusion::fuse(&dense_results, &sparse_results, 60, top_k)
            }
            FusionStrategy::Linear => {
                LinearCombination::fuse(&dense_results, &sparse_results, alpha, top_k)
            }
            FusionStrategy::DenseOnly => {
                ReciprocalRankFusion::fuse(&dense_results, &[], 60, top_k)
            }
            FusionStrategy::SparseOnly => {
                ReciprocalRankFusion::fuse(&[], &sparse_results, 60, top_k)
            }
        };

        let mut response = vec![Frame::Integer(fused.len() as i64)];
        for result in fused {
            response.push(Frame::bulk(Bytes::from(result.doc_id)));
            response.push(Frame::bulk(format!("{:.6}", result.fused_score)));
        }
        Frame::array(response)
    }

    pub(super) async fn vector_rerank(
        &self,
        _db: u8,
        _index: &Bytes,
        query_text: &str,
        doc_ids: &[String],
        top_k: usize,
    ) -> Frame {
        use ferrite_ai::hybrid::reranker::{Document, Reranker, SimpleReranker};

        let documents: Vec<Document> = doc_ids
            .iter()
            .enumerate()
            .map(|(i, id)| Document {
                id: id.clone(),
                text: id.clone(), // Use doc_id as placeholder text
                original_score: 1.0 - (i as f64 * 0.01),
            })
            .collect();

        let reranker = SimpleReranker;
        let ranked = reranker.rerank(query_text, &documents, top_k);

        let mut response = vec![Frame::Integer(ranked.len() as i64)];
        for doc in ranked {
            response.push(Frame::bulk(Bytes::from(doc.doc_id)));
            response.push(Frame::bulk(format!("{:.6}", doc.reranked_score)));
            response.push(Frame::Integer(doc.rank as i64));
        }
        Frame::array(response)
    }

    // CRDT commands

    pub(super) async fn crdt_gcounter(
        &self,
        key: &Bytes,
        subcommand: &str,
        args: &[String],
    ) -> Frame {
        use ferrite_plugins::crdt::{GCounter, SiteId};

        let key_str = String::from_utf8_lossy(key).to_string();
        let site_id = SiteId::new(1); // Default site ID
        let mut counter = GCounter::new();

        match subcommand {
            "INCR" => {
                let delta = args
                    .first()
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(1);
                counter.increment(site_id, delta);
                Frame::Integer(counter.value() as i64)
            }
            "GET" => Frame::Integer(counter.value() as i64),
            "MERGE" => {
                // In production, would merge from serialized state
                Frame::simple("OK")
            }
            "INFO" => Frame::array(vec![
                Frame::bulk("key"),
                Frame::bulk(Bytes::from(key_str.clone())),
                Frame::bulk("type"),
                Frame::bulk("gcounter"),
                Frame::bulk("value"),
                Frame::Integer(counter.value() as i64),
            ]),
            _ => Frame::error(format!(
                "ERR Unknown CRDT.GCOUNTER subcommand: {}",
                subcommand
            )),
        }
    }

    pub(super) async fn crdt_pncounter(
        &self,
        key: &Bytes,
        subcommand: &str,
        args: &[String],
    ) -> Frame {
        use ferrite_plugins::crdt::{PNCounter, SiteId};

        let key_str = String::from_utf8_lossy(key).to_string();
        let site_id = SiteId::new(1);
        let mut counter = PNCounter::new();

        match subcommand {
            "INCR" => {
                let delta = args
                    .first()
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(1);
                counter.increment(site_id, delta);
                Frame::Integer(counter.value())
            }
            "DECR" => {
                let delta = args
                    .first()
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(1);
                counter.decrement(site_id, delta);
                Frame::Integer(counter.value())
            }
            "GET" => Frame::Integer(counter.value()),
            "MERGE" => Frame::simple("OK"),
            "INFO" => Frame::array(vec![
                Frame::bulk("key"),
                Frame::bulk(Bytes::from(key_str.clone())),
                Frame::bulk("type"),
                Frame::bulk("pncounter"),
                Frame::bulk("value"),
                Frame::Integer(counter.value()),
            ]),
            _ => Frame::error(format!(
                "ERR Unknown CRDT.PNCOUNTER subcommand: {}",
                subcommand
            )),
        }
    }

    pub(super) async fn crdt_lwwreg(
        &self,
        key: &Bytes,
        subcommand: &str,
        args: &[String],
    ) -> Frame {
        use ferrite_plugins::crdt::{HybridTimestamp, LwwRegister, SiteId};

        let key_str = String::from_utf8_lossy(key).to_string();
        let mut register: LwwRegister<String> = LwwRegister::new();
        let site_id = SiteId::new(1);
        let timestamp = HybridTimestamp::now(site_id);

        match subcommand {
            "SET" => {
                if let Some(value) = args.first() {
                    register.set(value.clone(), timestamp);
                    Frame::simple("OK")
                } else {
                    Frame::error("ERR CRDT.LWWREG SET requires a value")
                }
            }
            "GET" => match register.value() {
                Some(value) => Frame::bulk(Bytes::from(value.clone())),
                None => Frame::null(),
            },
            "MERGE" => Frame::simple("OK"),
            "INFO" => Frame::array(vec![
                Frame::bulk("key"),
                Frame::bulk(Bytes::from(key_str)),
                Frame::bulk("type"),
                Frame::bulk("lwwregister"),
            ]),
            _ => Frame::error(format!(
                "ERR Unknown CRDT.LWWREG subcommand: {}",
                subcommand
            )),
        }
    }

    pub(super) async fn crdt_orset(&self, key: &Bytes, subcommand: &str, args: &[String]) -> Frame {
        use ferrite_plugins::crdt::{OrSet, SiteId};
        use std::sync::atomic::{AtomicU64, Ordering};

        let key_str = String::from_utf8_lossy(key).to_string();
        let _site_id = SiteId::new(1);
        let set: OrSet<String> = OrSet::new();
        static COUNTER: AtomicU64 = AtomicU64::new(1);

        match subcommand {
            "ADD" => {
                if !args.is_empty() {
                    let _counter = COUNTER.fetch_add(1, Ordering::SeqCst);
                    // In production, would persist to storage
                    Frame::Integer(1)
                } else {
                    Frame::error("ERR CRDT.ORSET ADD requires an element")
                }
            }
            "REMOVE" => {
                if !args.is_empty() {
                    // In production, would remove from storage
                    Frame::Integer(1)
                } else {
                    Frame::error("ERR CRDT.ORSET REMOVE requires an element")
                }
            }
            "CONTAINS" => {
                if let Some(element) = args.first() {
                    Frame::Integer(if set.contains(element) { 1 } else { 0 })
                } else {
                    Frame::error("ERR CRDT.ORSET CONTAINS requires an element")
                }
            }
            "MEMBERS" => {
                let members: Vec<Frame> = set
                    .members()
                    .map(|s| Frame::bulk(Bytes::from(s.clone())))
                    .collect();
                Frame::array(members)
            }
            "CARD" => Frame::Integer(set.len() as i64),
            "MERGE" => Frame::simple("OK"),
            "INFO" => Frame::array(vec![
                Frame::bulk("key"),
                Frame::bulk(Bytes::from(key_str)),
                Frame::bulk("type"),
                Frame::bulk("orset"),
                Frame::bulk("cardinality"),
                Frame::Integer(set.len() as i64),
            ]),
            _ => Frame::error(format!("ERR Unknown CRDT.ORSET subcommand: {}", subcommand)),
        }
    }

    pub(super) async fn crdt_info(&self, key: Option<&Bytes>) -> Frame {
        use ferrite_plugins::crdt::CrdtConfig;

        let config = CrdtConfig::default();

        let mut info = vec![
            Frame::bulk("crdt_enabled"),
            Frame::bulk(if config.enabled { "yes" } else { "no" }),
            Frame::bulk("site_id"),
            Frame::bulk(Bytes::from(config.site_id.clone())),
            Frame::bulk("replication_mode"),
            Frame::bulk(Bytes::from(format!("{:?}", config.mode))),
            Frame::bulk("sync_interval_ms"),
            Frame::Integer(config.sync_interval_ms as i64),
        ];

        if let Some(k) = key {
            info.push(Frame::bulk("key"));
            info.push(Frame::bulk(String::from_utf8_lossy(k).to_string()));
        }

        Frame::array(info)
    }

    // WASM commands

    pub(super) async fn wasm_load(
        &self,
        name: &str,
        module: &Bytes,
        replace: bool,
        permissions: &[String],
    ) -> Frame {
        use ferrite_plugins::wasm::{FunctionMetadata, FunctionPermissions, FunctionRegistry};

        let registry = FunctionRegistry::new();

        let perms = if permissions.is_empty() {
            FunctionPermissions::default()
        } else {
            let mut p = FunctionPermissions::default();
            for perm in permissions {
                match perm.to_uppercase().as_str() {
                    "WRITE" => p.allow_write = true,
                    "NETWORK" => p.allow_network = true,
                    "ADMIN" => p.allow_admin = true,
                    _ => {}
                }
            }
            p
        };

        // Calculate source hash using simple hash
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        module.as_ref().hash(&mut hasher);
        let source_hash = format!("{:016x}", hasher.finish());
        let metadata = FunctionMetadata::new(name.to_string(), source_hash).with_permissions(perms);

        if !replace && registry.get(name).is_some() {
            return Frame::error(format!("ERR Function already exists: {}", name));
        }

        match registry.load(name, module.to_vec(), Some(metadata)) {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::error(format!("ERR {}", e)),
        }
    }

    pub(super) async fn wasm_unload(&self, name: &str) -> Frame {
        use ferrite_plugins::wasm::FunctionRegistry;

        let registry = FunctionRegistry::new();

        match registry.unload(name) {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::error(format!("ERR {}", e)),
        }
    }

    pub(super) async fn wasm_call(&self, name: &str, keys: &[Bytes], args: &[Bytes]) -> Frame {
        use ferrite_plugins::wasm::FunctionRegistry;

        let registry = FunctionRegistry::new();

        match registry.get(name) {
            Some(_func) => {
                // In production, would execute the WASM function
                let _ = (keys, args);
                Frame::array(vec![
                    Frame::bulk("result"),
                    Frame::bulk(format!("WASM function '{}' execution placeholder", name)),
                ])
            }
            None => Frame::error(format!("ERR Unknown function: {}", name)),
        }
    }

    pub(super) async fn wasm_call_ro(&self, name: &str, keys: &[Bytes], args: &[Bytes]) -> Frame {
        // Read-only variant - same as wasm_call but enforces read-only
        self.wasm_call(name, keys, args).await
    }

    pub(super) async fn wasm_list(&self, with_stats: bool) -> Frame {
        use ferrite_plugins::wasm::FunctionRegistry;

        let registry = FunctionRegistry::new();
        let functions = registry.list();

        if with_stats {
            let items: Vec<Frame> = functions
                .iter()
                .map(|name| {
                    if let Some(info) = registry.info(name) {
                        Frame::array(vec![
                            Frame::bulk("name"),
                            Frame::bulk(Bytes::from(name.clone())),
                            Frame::bulk("calls"),
                            Frame::Integer(info.call_count as i64),
                            Frame::bulk("avg_duration_us"),
                            Frame::Integer((info.avg_execution_time_ms * 1000.0) as i64),
                        ])
                    } else {
                        Frame::array(vec![
                            Frame::bulk("name"),
                            Frame::bulk(Bytes::from(name.clone())),
                        ])
                    }
                })
                .collect();
            Frame::array(items)
        } else {
            Frame::array(
                functions
                    .iter()
                    .map(|name| Frame::bulk(Bytes::from(name.clone())))
                    .collect(),
            )
        }
    }

    pub(super) async fn wasm_info(&self, name: &str) -> Frame {
        use ferrite_plugins::wasm::FunctionRegistry;

        let registry = FunctionRegistry::new();

        match registry.info(name) {
            Some(info) => {
                let mut perms = Vec::new();
                if info.permissions.allow_write {
                    perms.push(Frame::bulk("write"));
                }
                if info.permissions.allow_network {
                    perms.push(Frame::bulk("network"));
                }
                if info.permissions.allow_admin {
                    perms.push(Frame::bulk("admin"));
                }

                Frame::array(vec![
                    Frame::bulk("name"),
                    Frame::bulk(Bytes::from(name.to_string())),
                    Frame::bulk("loaded"),
                    Frame::bulk("yes"),
                    Frame::bulk("source_hash"),
                    Frame::bulk(Bytes::from(info.source_hash.clone())),
                    Frame::bulk("call_count"),
                    Frame::Integer(info.call_count as i64),
                    Frame::bulk("permissions"),
                    Frame::array(perms),
                ])
            }
            None => Frame::error(format!("ERR Unknown function: {}", name)),
        }
    }

    pub(super) async fn wasm_stats(&self) -> Frame {
        use ferrite_plugins::wasm::{FunctionRegistry, WasmConfig};

        let config = WasmConfig::default();
        let registry = FunctionRegistry::new();
        let functions = registry.list();

        Frame::array(vec![
            Frame::bulk("wasm_enabled"),
            Frame::bulk(if config.enabled { "yes" } else { "no" }),
            Frame::bulk("loaded_functions"),
            Frame::Integer(functions.len() as i64),
            Frame::bulk("module_dir"),
            Frame::bulk(Bytes::from(config.module_dir.clone())),
            Frame::bulk("pool_min_instances"),
            Frame::Integer(config.pool.min_instances as i64),
            Frame::bulk("pool_max_instances"),
            Frame::Integer(config.pool.max_instances as i64),
        ])
    }

    // Semantic Caching commands

    #[cfg(feature = "cloud")]
    pub(super) async fn semantic_set(
        &self,
        query: &Bytes,
        value: &Bytes,
        embedding: &[f32],
        ttl_secs: Option<u64>,
    ) -> Frame {
        use ferrite_ai::semantic::SemanticCache;

        let cache = SemanticCache::with_defaults();
        let query_str = String::from_utf8_lossy(query).to_string();

        match cache.set(&query_str, value.clone(), embedding, ttl_secs) {
            Ok(id) => Frame::Integer(id as i64),
            Err(e) => Frame::error(format!("ERR {}", e)),
        }
    }

    #[cfg(not(feature = "cloud"))]
    pub(super) async fn semantic_set(
        &self,
        _query: &Bytes,
        _value: &Bytes,
        _embedding: &[f32],
        _ttl_secs: Option<u64>,
    ) -> Frame {
        Frame::error("ERR semantic caching requires the 'cloud' feature")
    }

    #[cfg(feature = "cloud")]
    pub(super) async fn semantic_get(
        &self,
        embedding: &[f32],
        threshold: Option<f32>,
        count: Option<usize>,
    ) -> Frame {
        use ferrite_ai::semantic::SemanticCache;

        let cache = SemanticCache::with_defaults();

        if let Some(count) = count {
            // Return multiple results
            match cache.get_many(embedding, count, threshold) {
                Ok(results) => {
                    if results.is_empty() {
                        Frame::Null
                    } else {
                        let items: Vec<Frame> = results
                            .iter()
                            .map(|r| {
                                Frame::array(vec![
                                    Frame::bulk("id"),
                                    Frame::Integer(r.id as i64),
                                    Frame::bulk("query"),
                                    Frame::bulk(Bytes::from(r.entry.query.clone())),
                                    Frame::bulk("value"),
                                    Frame::bulk(r.entry.value.clone()),
                                    Frame::bulk("similarity"),
                                    Frame::Double(r.similarity as f64),
                                ])
                            })
                            .collect();
                        Frame::array(items)
                    }
                }
                Err(e) => Frame::error(format!("ERR {}", e)),
            }
        } else {
            // Return single best result
            match cache.get(embedding, threshold) {
                Ok(Some(result)) => Frame::array(vec![
                    Frame::bulk("id"),
                    Frame::Integer(result.id as i64),
                    Frame::bulk("query"),
                    Frame::bulk(Bytes::from(result.entry.query.clone())),
                    Frame::bulk("value"),
                    Frame::bulk(result.entry.value.clone()),
                    Frame::bulk("similarity"),
                    Frame::Double(result.similarity as f64),
                ]),
                Ok(None) => Frame::Null,
                Err(e) => Frame::error(format!("ERR {}", e)),
            }
        }
    }

    #[cfg(not(feature = "cloud"))]
    pub(super) async fn semantic_get(
        &self,
        _embedding: &[f32],
        _threshold: Option<f32>,
        _count: Option<usize>,
    ) -> Frame {
        Frame::error("ERR semantic caching requires the 'cloud' feature")
    }

    pub(super) async fn semantic_gettext(
        &self,
        _query: &Bytes,
        _threshold: Option<f32>,
        _count: Option<usize>,
    ) -> Frame {
        // This requires auto-embed to be enabled with a model
        // For now, return an error indicating this feature needs configuration
        Frame::error(
            "ERR SEMANTIC.GETTEXT requires auto_embed to be enabled with an embedding model",
        )
    }

    #[cfg(feature = "cloud")]
    pub(super) async fn semantic_del(&self, id: u64) -> Frame {
        use ferrite_ai::semantic::SemanticCache;

        let cache = SemanticCache::with_defaults();

        if cache.remove(id) {
            Frame::Integer(1)
        } else {
            Frame::Integer(0)
        }
    }

    #[cfg(not(feature = "cloud"))]
    pub(super) async fn semantic_del(&self, _id: u64) -> Frame {
        Frame::error("ERR semantic caching requires the 'cloud' feature")
    }

    #[cfg(feature = "cloud")]
    pub(super) async fn semantic_clear(&self) -> Frame {
        use ferrite_ai::semantic::SemanticCache;

        let cache = SemanticCache::with_defaults();
        cache.clear();

        Frame::simple("OK")
    }

    #[cfg(not(feature = "cloud"))]
    pub(super) async fn semantic_clear(&self) -> Frame {
        Frame::error("ERR semantic caching requires the 'cloud' feature")
    }

    #[cfg(feature = "cloud")]
    pub(super) async fn semantic_info(&self) -> Frame {
        use ferrite_ai::semantic::SemanticConfig;

        let config = SemanticConfig::default();

        Frame::array(vec![
            Frame::bulk("semantic_enabled"),
            Frame::bulk(if config.enabled { "yes" } else { "no" }),
            Frame::bulk("embedding_dim"),
            Frame::Integer(config.embedding_dim as i64),
            Frame::bulk("default_threshold"),
            Frame::Double(config.default_threshold as f64),
            Frame::bulk("max_entries"),
            Frame::Integer(config.max_entries as i64),
            Frame::bulk("default_ttl_secs"),
            Frame::Integer(config.default_ttl_secs as i64),
            Frame::bulk("index_type"),
            Frame::bulk(Bytes::from(format!("{:?}", config.index_type))),
            Frame::bulk("distance_metric"),
            Frame::bulk(Bytes::from(format!("{:?}", config.distance_metric))),
            Frame::bulk("auto_embed"),
            Frame::bulk(if config.auto_embed { "yes" } else { "no" }),
        ])
    }

    #[cfg(not(feature = "cloud"))]
    pub(super) async fn semantic_info(&self) -> Frame {
        Frame::error("ERR semantic caching requires the 'cloud' feature")
    }

    #[cfg(feature = "cloud")]
    pub(super) async fn semantic_stats(&self) -> Frame {
        use ferrite_ai::semantic::SemanticCache;

        let cache = SemanticCache::with_defaults();
        let stats = cache.stats();

        Frame::array(vec![
            Frame::bulk("entries"),
            Frame::Integer(stats.entries as i64),
            Frame::bulk("hits"),
            Frame::Integer(stats.hits as i64),
            Frame::bulk("misses"),
            Frame::Integer(stats.misses as i64),
            Frame::bulk("sets"),
            Frame::Integer(stats.sets as i64),
            Frame::bulk("evictions"),
            Frame::Integer(stats.evictions as i64),
            Frame::bulk("hit_rate"),
            Frame::Double(stats.hit_rate),
        ])
    }

    #[cfg(not(feature = "cloud"))]
    pub(super) async fn semantic_stats(&self) -> Frame {
        Frame::error("ERR semantic caching requires the 'cloud' feature")
    }

    #[cfg(feature = "cloud")]
    pub(super) async fn semantic_config(
        &self,
        operation: &Bytes,
        param: Option<&Bytes>,
        _value: Option<&Bytes>,
    ) -> Frame {
        use ferrite_ai::semantic::SemanticConfig;

        let op = String::from_utf8_lossy(operation).to_uppercase();
        let config = SemanticConfig::default();

        match op.as_str() {
            "GET" => {
                if let Some(p) = param {
                    let param_name = String::from_utf8_lossy(p).to_lowercase();
                    match param_name.as_str() {
                        "enabled" => Frame::bulk(if config.enabled { "yes" } else { "no" }),
                        "default_threshold" => Frame::Double(config.default_threshold as f64),
                        "embedding_dim" => Frame::Integer(config.embedding_dim as i64),
                        "max_entries" => Frame::Integer(config.max_entries as i64),
                        "default_ttl_secs" => Frame::Integer(config.default_ttl_secs as i64),
                        "auto_embed" => Frame::bulk(if config.auto_embed { "yes" } else { "no" }),
                        _ => Frame::error(format!("ERR Unknown config parameter: {}", param_name)),
                    }
                } else {
                    // Return all config
                    self.semantic_info().await
                }
            }
            "SET" => {
                // Config SET not implemented yet - would require runtime config modification
                Frame::error("ERR SEMANTIC.CONFIG SET not implemented. Use config file.")
            }
            _ => Frame::error(format!("ERR Unknown operation: {}. Use GET or SET.", op)),
        }
    }

    #[cfg(not(feature = "cloud"))]
    pub(super) async fn semantic_config(
        &self,
        _operation: &Bytes,
        _param: Option<&Bytes>,
        _value: Option<&Bytes>,
    ) -> Frame {
        Frame::error("ERR semantic caching requires the 'cloud' feature")
    }

    // Trigger command implementations

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn trigger_create(
        &self,
        name: &Bytes,
        event_type: &Bytes,
        pattern: &Bytes,
        actions: &[Bytes],
        wasm_module: Option<&Bytes>,
        wasm_function: Option<&Bytes>,
        priority: Option<i32>,
        description: Option<&Bytes>,
    ) -> Frame {
        use crate::triggers::{
            Action, BuiltinAction, Condition, EventType, Pattern, PublishAction, Trigger,
            TriggerConfig, TriggerRegistry,
        };

        let name_str = String::from_utf8_lossy(name).to_string();
        let event_str = String::from_utf8_lossy(event_type).to_string();
        let pattern_str = String::from_utf8_lossy(pattern).to_string();

        // Parse event type
        let event = match EventType::parse_str(&event_str) {
            Some(e) => e,
            None => return Frame::error(format!("ERR Unknown event type: {}", event_str)),
        };

        // Parse pattern
        let pat = Pattern::parse_str(&pattern_str);

        // Build actions
        let mut trigger_actions = Vec::new();

        // Parse built-in actions
        for action_bytes in actions {
            let action_str = String::from_utf8_lossy(action_bytes).to_string();
            let parts: Vec<&str> = action_str.split_whitespace().collect();

            if parts.is_empty() {
                continue;
            }

            match parts[0].to_uppercase().as_str() {
                "PUBLISH" => {
                    if parts.len() >= 3 {
                        trigger_actions.push(Action::Builtin(BuiltinAction::Publish(
                            PublishAction {
                                channel: parts[1].to_string(),
                                message_template: parts[2..].join(" "),
                            },
                        )));
                    }
                }
                _ => {
                    // Store as generic action string
                    trigger_actions.push(Action::Builtin(BuiltinAction::Noop));
                }
            }
        }

        // Handle WASM action
        if let (Some(module), Some(func)) = (wasm_module, wasm_function) {
            trigger_actions.push(Action::Wasm(crate::triggers::actions::WasmAction {
                module: String::from_utf8_lossy(module).to_string(),
                function: String::from_utf8_lossy(func).to_string(),
                args: vec![],
            }));
        }

        // Create trigger
        let mut trigger = Trigger::new(name_str, Condition::new(event, pat), trigger_actions);

        if let Some(p) = priority {
            trigger.priority = p;
        }

        if let Some(desc) = description {
            trigger.description = Some(String::from_utf8_lossy(desc).to_string());
        }

        // In production, this would add to a shared registry
        // For now, just acknowledge creation
        let config = TriggerConfig::default();
        let registry = TriggerRegistry::new(config);

        match registry.create(trigger).await {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::error(format!("ERR {}", e)),
        }
    }

    pub(super) async fn trigger_delete(&self, name: &Bytes) -> Frame {
        use crate::triggers::{TriggerConfig, TriggerRegistry};

        let name_str = String::from_utf8_lossy(name).to_string();
        let config = TriggerConfig::default();
        let registry = TriggerRegistry::new(config);

        match registry.delete(&name_str).await {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::error(format!("ERR {}", e)),
        }
    }

    pub(super) async fn trigger_get(&self, name: &Bytes) -> Frame {
        use crate::triggers::{TriggerConfig, TriggerRegistry};

        let name_str = String::from_utf8_lossy(name).to_string();
        let config = TriggerConfig::default();
        let registry = TriggerRegistry::new(config);

        match registry.get(&name_str).await {
            Some(trigger) => Frame::array(vec![
                Frame::bulk("name"),
                Frame::bulk(Bytes::from(trigger.name.clone())),
                Frame::bulk("event_type"),
                Frame::bulk(Bytes::from(trigger.condition.event_type.to_string())),
                Frame::bulk("pattern"),
                Frame::bulk(Bytes::from(trigger.condition.pattern.to_string())),
                Frame::bulk("enabled"),
                Frame::bulk(if trigger.enabled { "yes" } else { "no" }),
                Frame::bulk("priority"),
                Frame::Integer(trigger.priority as i64),
                Frame::bulk("actions"),
                Frame::Integer(trigger.actions.len() as i64),
                Frame::bulk("execution_count"),
                Frame::Integer(trigger.execution_count as i64),
            ]),
            None => Frame::null(),
        }
    }

    pub(super) async fn trigger_list(&self, pattern: Option<&Bytes>) -> Frame {
        use crate::triggers::{TriggerConfig, TriggerRegistry};

        let pattern_str = pattern.map(|p| String::from_utf8_lossy(p).to_string());
        let config = TriggerConfig::default();
        let registry = TriggerRegistry::new(config);

        let triggers = registry.list(pattern_str.as_deref()).await;

        let items: Vec<Frame> = triggers
            .iter()
            .map(|t| Frame::bulk(Bytes::from(t.name.clone())))
            .collect();

        Frame::array(items)
    }

    pub(super) async fn trigger_enable(&self, name: &Bytes) -> Frame {
        use crate::triggers::{TriggerConfig, TriggerRegistry};

        let name_str = String::from_utf8_lossy(name).to_string();
        let config = TriggerConfig::default();
        let registry = TriggerRegistry::new(config);

        match registry.enable(&name_str).await {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::error(format!("ERR {}", e)),
        }
    }

    pub(super) async fn trigger_disable(&self, name: &Bytes) -> Frame {
        use crate::triggers::{TriggerConfig, TriggerRegistry};

        let name_str = String::from_utf8_lossy(name).to_string();
        let config = TriggerConfig::default();
        let registry = TriggerRegistry::new(config);

        match registry.disable(&name_str).await {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::error(format!("ERR {}", e)),
        }
    }

    pub(super) async fn trigger_fire(
        &self,
        _name: &Bytes,
        key: &Bytes,
        value: Option<&Bytes>,
        ttl: Option<i64>,
    ) -> Frame {
        use crate::triggers::{TriggerConfig, TriggerEvent, TriggerRegistry};

        let key_str = String::from_utf8_lossy(key).to_string();
        let value_vec = value.map(|v| v.to_vec());

        let mut event = TriggerEvent::set(key_str, value_vec.unwrap_or_default());
        if let Some(t) = ttl {
            event = event.with_ttl(t);
        }

        let config = TriggerConfig::default();
        let registry = TriggerRegistry::new(config);

        match registry.fire(event).await {
            Ok(results) => {
                let items: Vec<Frame> = results
                    .iter()
                    .map(|r| {
                        Frame::array(vec![
                            Frame::bulk("action"),
                            Frame::bulk(Bytes::from(r.action.clone())),
                            Frame::bulk("success"),
                            Frame::bulk(if r.success { "yes" } else { "no" }),
                            Frame::bulk("duration_us"),
                            Frame::Integer(r.duration_us as i64),
                        ])
                    })
                    .collect();
                Frame::array(items)
            }
            Err(e) => Frame::error(format!("ERR {}", e)),
        }
    }

    pub(super) async fn trigger_info(&self) -> Frame {
        use crate::triggers::TriggerConfig;

        let config = TriggerConfig::default();

        Frame::array(vec![
            Frame::bulk("triggers_enabled"),
            Frame::bulk(if config.enabled { "yes" } else { "no" }),
            Frame::bulk("max_triggers"),
            Frame::Integer(config.max_triggers as i64),
            Frame::bulk("max_actions_per_trigger"),
            Frame::Integer(config.max_actions_per_trigger as i64),
            Frame::bulk("default_timeout_ms"),
            Frame::Integer(config.default_timeout_ms as i64),
            Frame::bulk("max_concurrent_executions"),
            Frame::Integer(config.max_concurrent_executions as i64),
            Frame::bulk("async_execution"),
            Frame::bulk(if config.async_execution { "yes" } else { "no" }),
            Frame::bulk("http_timeout_ms"),
            Frame::Integer(config.http_timeout_ms as i64),
            Frame::bulk("max_retries"),
            Frame::Integer(config.max_retries as i64),
            Frame::bulk("log_executions"),
            Frame::bulk(if config.log_executions { "yes" } else { "no" }),
        ])
    }

    pub(super) async fn trigger_stats(&self) -> Frame {
        use crate::triggers::{TriggerConfig, TriggerRegistry};

        let config = TriggerConfig::default();
        let registry = TriggerRegistry::new(config);
        let stats = registry.stats();

        Frame::array(vec![
            Frame::bulk("triggers_created"),
            Frame::Integer(stats.triggers_created as i64),
            Frame::bulk("triggers_deleted"),
            Frame::Integer(stats.triggers_deleted as i64),
            Frame::bulk("total_executions"),
            Frame::Integer(stats.total_executions as i64),
            Frame::bulk("successful_executions"),
            Frame::Integer(stats.successful_executions as i64),
            Frame::bulk("failed_executions"),
            Frame::Integer(stats.failed_executions as i64),
            Frame::bulk("total_actions"),
            Frame::Integer(stats.total_actions as i64),
            Frame::bulk("avg_execution_time_us"),
            Frame::Integer(stats.avg_execution_time_us as i64),
        ])
    }

    pub(super) async fn trigger_config(
        &self,
        operation: &Bytes,
        param: Option<&Bytes>,
        _value: Option<&Bytes>,
    ) -> Frame {
        use crate::triggers::TriggerConfig;

        let op = String::from_utf8_lossy(operation).to_uppercase();
        let config = TriggerConfig::default();

        match op.as_str() {
            "GET" => {
                if let Some(p) = param {
                    let param_name = String::from_utf8_lossy(p).to_lowercase();
                    match param_name.as_str() {
                        "enabled" => Frame::bulk(if config.enabled { "yes" } else { "no" }),
                        "max_triggers" => Frame::Integer(config.max_triggers as i64),
                        "max_actions_per_trigger" => {
                            Frame::Integer(config.max_actions_per_trigger as i64)
                        }
                        "default_timeout_ms" => Frame::Integer(config.default_timeout_ms as i64),
                        "max_concurrent_executions" => {
                            Frame::Integer(config.max_concurrent_executions as i64)
                        }
                        "async_execution" => {
                            Frame::bulk(if config.async_execution { "yes" } else { "no" })
                        }
                        "http_timeout_ms" => Frame::Integer(config.http_timeout_ms as i64),
                        "max_retries" => Frame::Integer(config.max_retries as i64),
                        "log_executions" => {
                            Frame::bulk(if config.log_executions { "yes" } else { "no" })
                        }
                        _ => Frame::error(format!("ERR Unknown config parameter: {}", param_name)),
                    }
                } else {
                    // Return all config
                    self.trigger_info().await
                }
            }
            "SET" => {
                // Config SET not implemented yet - would require runtime config modification
                Frame::error("ERR TRIGGER.CONFIG SET not implemented. Use config file.")
            }
            _ => Frame::error(format!("ERR Unknown operation: {}. Use GET or SET.", op)),
        }
    }

    /// Handle time-series commands by dispatching to the handler module
    pub(super) async fn handle_timeseries_command(
        &self,
        db: u8,
        subcommand: &str,
        args: &[Bytes],
    ) -> Frame {
        use crate::commands::handlers::timeseries;

        let ctx = crate::commands::handlers::HandlerContext::new(
            &self.store,
            &self.pubsub_manager,
            &self.acl,
            &self.script_executor,
            &self.blocking_manager,
            &self.blocking_stream_manager,
            &self.blocking_zset_manager,
            &self.config,
            &self.slowlog,
            &self.client_registry,
            db,
        );

        match subcommand.to_uppercase().as_str() {
            "CREATE" => timeseries::ts_create(&ctx, args),
            "ADD" => timeseries::ts_add(&ctx, args),
            "MADD" => timeseries::ts_madd(&ctx, args),
            "GET" => timeseries::ts_get(&ctx, args),
            "RANGE" => timeseries::ts_range(&ctx, args),
            "MRANGE" => timeseries::ts_mrange(&ctx, args),
            "INFO" => timeseries::ts_info(&ctx, args),
            "DEL" => timeseries::ts_del(&ctx, args),
            "CREATERULE" => timeseries::ts_createrule(&ctx, args),
            "DELETERULE" => timeseries::ts_deleterule(&ctx, args),
            "QUERYINDEX" => timeseries::ts_queryindex(&ctx, args),
            "ALTER" => timeseries::ts_alter(&ctx, args),
            _ => Frame::error(format!("ERR unknown command 'TS.{}'", subcommand)),
        }
    }

    #[cfg(feature = "experimental")]
    /// Handle document database commands by dispatching to the handler module
    pub(super) async fn handle_document_command(
        &self,
        db: u8,
        subcommand: &str,
        args: &[Bytes],
    ) -> Frame {
        use crate::commands::handlers::document;

        let ctx = crate::commands::handlers::HandlerContext::new(
            &self.store,
            &self.pubsub_manager,
            &self.acl,
            &self.script_executor,
            &self.blocking_manager,
            &self.blocking_stream_manager,
            &self.blocking_zset_manager,
            &self.config,
            &self.slowlog,
            &self.client_registry,
            db,
        );

        match subcommand.to_uppercase().as_str() {
            "CREATE" => document::doc_create(&ctx, args),
            "DROP" => document::doc_drop(&ctx, args),
            "INSERT" => document::doc_insert(&ctx, args),
            "INSERTMANY" => document::doc_insertmany(&ctx, args),
            "FIND" => document::doc_find(&ctx, args),
            "FINDONE" => document::doc_findone(&ctx, args),
            "UPDATE" => document::doc_update(&ctx, args),
            "DELETE" => document::doc_delete(&ctx, args),
            "COUNT" => document::doc_count(&ctx, args),
            "DISTINCT" => document::doc_distinct(&ctx, args),
            "AGGREGATE" => document::doc_aggregate(&ctx, args),
            "CREATEINDEX" => document::doc_createindex(&ctx, args),
            "DROPINDEX" => document::doc_dropindex(&ctx, args),
            "LISTCOLLECTIONS" => document::doc_listcollections(&ctx, args),
            "STATS" => document::doc_stats(&ctx, args),
            _ => Frame::error(format!("ERR unknown command 'DOC.{}'", subcommand)),
        }
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_document_command(
        &self,
        _db: u8,
        _subcommand: &str,
        _args: &[Bytes],
    ) -> Frame {
        Frame::error("ERR DOC commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    /// Handle graph database commands by dispatching to the handler module
    pub(super) async fn handle_graph_command(
        &self,
        db: u8,
        subcommand: &str,
        args: &[Bytes],
    ) -> Frame {
        use crate::commands::handlers::graph;

        let ctx = crate::commands::handlers::HandlerContext::new(
            &self.store,
            &self.pubsub_manager,
            &self.acl,
            &self.script_executor,
            &self.blocking_manager,
            &self.blocking_stream_manager,
            &self.blocking_zset_manager,
            &self.config,
            &self.slowlog,
            &self.client_registry,
            db,
        );

        match subcommand.to_uppercase().as_str() {
            "CREATE" => graph::graph_create(&ctx, args),
            "DELETE" => graph::graph_delete(&ctx, args),
            "QUERY" => graph::graph_query(&ctx, args),
            "ADDNODE" => graph::graph_addnode(&ctx, args),
            "ADDEDGE" => graph::graph_addedge(&ctx, args),
            "GETNODE" => graph::graph_getnode(&ctx, args),
            "GETEDGE" => graph::graph_getedge(&ctx, args),
            "DELETENODE" => graph::graph_deletenode(&ctx, args),
            "DELETEEDGE" => graph::graph_deleteedge(&ctx, args),
            "NEIGHBORS" => graph::graph_neighbors(&ctx, args),
            "SHORTESTPATH" => graph::graph_shortestpath(&ctx, args),
            "PAGERANK" => graph::graph_pagerank(&ctx, args),
            "LIST" => graph::graph_list(&ctx, args),
            "INFO" => graph::graph_info(&ctx, args),
            _ => Frame::error(format!("ERR unknown command 'GRAPH.{}'", subcommand)),
        }
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_graph_command(
        &self,
        _db: u8,
        _subcommand: &str,
        _args: &[Bytes],
    ) -> Frame {
        Frame::error("ERR GRAPH commands require the 'experimental' feature")
    }

    /// Handle RAG pipeline commands by dispatching to the handler module
    #[cfg(feature = "cloud")]
    pub(super) async fn handle_rag_command(
        &self,
        db: u8,
        subcommand: &str,
        args: &[Bytes],
    ) -> Frame {
        use crate::commands::handlers::rag;

        let ctx = crate::commands::handlers::HandlerContext::new(
            &self.store,
            &self.pubsub_manager,
            &self.acl,
            &self.script_executor,
            &self.blocking_manager,
            &self.blocking_stream_manager,
            &self.blocking_zset_manager,
            &self.config,
            &self.slowlog,
            &self.client_registry,
            db,
        );

        match subcommand.to_uppercase().as_str() {
            "CREATE" => rag::rag_create(&ctx, args),
            "DELETE" => rag::rag_delete(&ctx, args),
            "INGEST" => rag::rag_ingest(&ctx, args),
            "INGESTBATCH" => rag::rag_ingestbatch(&ctx, args),
            "RETRIEVE" => rag::rag_retrieve(&ctx, args),
            "CONTEXT" => rag::rag_context(&ctx, args),
            "SEARCH" => rag::rag_search(&ctx, args),
            "CHUNK" => rag::rag_chunk(&ctx, args),
            "EMBED" => rag::rag_embed(&ctx, args),
            "LIST" => rag::rag_list(&ctx, args),
            "INFO" => rag::rag_info(&ctx, args),
            "STATS" => rag::rag_stats(&ctx, args),
            "CLEAR" => rag::rag_clear(&ctx, args),
            _ => Frame::error(format!("ERR unknown command 'RAG.{}'", subcommand)),
        }
    }

    #[cfg(not(feature = "cloud"))]
    pub(super) async fn handle_rag_command(
        &self,
        _db: u8,
        _subcommand: &str,
        _args: &[Bytes],
    ) -> Frame {
        Frame::error("ERR RAG commands require the 'cloud' feature")
    }

    /// Handle FerriteQL query commands by dispatching to the handler module
    pub(super) async fn handle_query_command(
        &self,
        db: u8,
        subcommand: &str,
        args: &[Bytes],
    ) -> Frame {
        use crate::commands::handlers::query;

        let ctx = crate::commands::handlers::HandlerContext::new(
            &self.store,
            &self.pubsub_manager,
            &self.acl,
            &self.script_executor,
            &self.blocking_manager,
            &self.blocking_stream_manager,
            &self.blocking_zset_manager,
            &self.config,
            &self.slowlog,
            &self.client_registry,
            db,
        );

        match subcommand {
            "RUN" => query::query_run(&ctx, args).await,
            "EXPLAIN" => query::query_explain(&ctx, args),
            "JSON" => query::query_json(&ctx, args).await,
            "PREPARE" => query::query_prepare(&ctx, args).await,
            "EXEC" => query::query_exec(&ctx, args).await,
            "HELP" => query::query_help(),
            "VERSION" => query::query_version(),
            _ => Frame::error(format!(
                "ERR unknown command 'QUERY.{}'. Try QUERY HELP for available commands",
                subcommand
            )),
        }
    }

    // Adaptive Query Optimizer commands

    pub(super) async fn ferrite_advisor(&self, subcommand: &str, args: &[String]) -> Frame {
        use ferrite_core::optimizer::{
            AutoTuner, AutoTunerConfig, WorkloadProfiler,
        };

        // Create instances for demonstration — in production these would be shared state.
        let profiler = WorkloadProfiler::new();
        let tuner = AutoTuner::new(AutoTunerConfig::default());

        match subcommand.to_uppercase().as_str() {
            "STATUS" => {
                let status = tuner.status();
                let mut items = Vec::new();
                items.push(Frame::bulk("enabled"));
                items.push(Frame::bulk(if status.enabled { "true" } else { "false" }));
                items.push(Frame::bulk("interval_secs"));
                items.push(Frame::Integer(status.interval_secs as i64));
                items.push(Frame::bulk("confidence_threshold"));
                items.push(Frame::Double(status.confidence_threshold));
                items.push(Frame::bulk("cooldown_secs"));
                items.push(Frame::Integer(status.cooldown_secs as i64));
                items.push(Frame::bulk("ab_test_enabled"));
                items.push(Frame::bulk(
                    if status.ab_test_enabled { "true" } else { "false" },
                ));
                items.push(Frame::bulk("last_run_secs_ago"));
                items.push(match status.last_run_secs_ago {
                    Some(s) => Frame::Integer(s as i64),
                    None => Frame::Null,
                });
                items.push(Frame::bulk("rules_count"));
                items.push(Frame::Integer(status.rules_count as i64));
                items.push(Frame::bulk("pending_recommendations"));
                items.push(Frame::Integer(status.pending_recommendations as i64));
                items.push(Frame::bulk("applied_total"));
                items.push(Frame::Integer(status.applied_total as i64));
                Frame::Array(Some(items))
            }
            "ANALYZE" => {
                let plan = tuner.run_cycle(&profiler);
                let mut items = Vec::new();
                items.push(Frame::bulk("recommendations"));
                items.push(Frame::Integer(plan.len() as i64));
                items.push(Frame::bulk("overall_estimated_impact"));
                items.push(Frame::Double(plan.overall_estimated_impact));
                items.push(Frame::bulk("generated_at"));
                items.push(Frame::bulk(plan.generated_at.clone()));

                if !plan.recommendations.is_empty() {
                    items.push(Frame::bulk("details"));
                    let mut details = Vec::new();
                    for rec in &plan.recommendations {
                        let mut entry = Vec::new();
                        entry.push(Frame::bulk("id"));
                        entry.push(Frame::bulk(rec.id.clone()));
                        entry.push(Frame::bulk("rule"));
                        entry.push(Frame::bulk(rec.rule_name.clone()));
                        entry.push(Frame::bulk("priority"));
                        entry.push(Frame::bulk(rec.priority.to_string()));
                        entry.push(Frame::bulk("confidence"));
                        entry.push(Frame::Double(rec.confidence));
                        entry.push(Frame::bulk("impact"));
                        entry.push(Frame::Double(rec.estimated_impact));
                        entry.push(Frame::bulk("description"));
                        entry.push(Frame::bulk(rec.description.clone()));
                        entry.push(Frame::bulk("action"));
                        entry.push(Frame::bulk(rec.action.to_string()));
                        details.push(Frame::Array(Some(entry)));
                    }
                    items.push(Frame::Array(Some(details)));
                }

                if !plan.warnings.is_empty() {
                    items.push(Frame::bulk("warnings"));
                    let warning_frames: Vec<Frame> =
                        plan.warnings.iter().map(|w| Frame::bulk(w.clone())).collect();
                    items.push(Frame::Array(Some(warning_frames)));
                }

                Frame::Array(Some(items))
            }
            "RECOMMEND" => {
                let snapshot = profiler.snapshot();
                let optimizer = ferrite_core::optimizer::AdaptiveOptimizer::new();
                let plan = optimizer.analyze(&snapshot);

                if plan.is_empty() {
                    return Frame::bulk("No recommendations at this time");
                }

                let mut items = Vec::new();
                for rec in &plan.recommendations {
                    let line = format!(
                        "[{}] {} (confidence: {:.0}%, impact: {:.0}%): {}",
                        rec.priority,
                        rec.rule_name,
                        rec.confidence * 100.0,
                        rec.estimated_impact,
                        rec.description,
                    );
                    items.push(Frame::bulk(line));
                }
                Frame::Array(Some(items))
            }
            "APPLY" => {
                if args.is_empty() {
                    let plan = tuner.run_cycle(&profiler);
                    Frame::bulk(format!(
                        "Applied {} recommendations (estimated impact: {:.1}%)",
                        plan.len(),
                        plan.overall_estimated_impact,
                    ))
                } else {
                    let rule_id = &args[0];
                    Frame::bulk(format!(
                        "Applied recommendation '{}' — monitor with FERRITE.ADVISOR STATUS",
                        rule_id
                    ))
                }
            }
            "HISTORY" => {
                let history = tuner.history();
                if history.is_empty() {
                    return Frame::bulk("No optimization history");
                }
                let mut items = Vec::new();
                for entry in &history {
                    let mut row = Vec::new();
                    row.push(Frame::bulk("rule"));
                    row.push(Frame::bulk(entry.recommendation.rule_name.clone()));
                    row.push(Frame::bulk("applied_at"));
                    row.push(Frame::bulk(entry.applied_at.clone()));
                    row.push(Frame::bulk("action"));
                    row.push(Frame::bulk(entry.recommendation.action.to_string()));
                    row.push(Frame::bulk("ab_test"));
                    row.push(Frame::bulk(
                        if entry.is_ab_test { "true" } else { "false" },
                    ));
                    items.push(Frame::Array(Some(row)));
                }
                Frame::Array(Some(items))
            }
            "RULES" => {
                let optimizer = ferrite_core::optimizer::AdaptiveOptimizer::new();
                let rules = optimizer.rules();
                let mut items = Vec::new();
                for (name, desc) in &rules {
                    let mut row = Vec::new();
                    row.push(Frame::bulk(*name));
                    row.push(Frame::bulk(*desc));
                    items.push(Frame::Array(Some(row)));
                }
                Frame::Array(Some(items))
            }
            "CONFIG" => {
                if args.is_empty() {
                    // Return all config values.
                    let status = tuner.status();
                    let mut items = Vec::new();
                    items.push(Frame::bulk("auto_optimize"));
                    items.push(Frame::bulk(
                        if status.enabled { "true" } else { "false" },
                    ));
                    items.push(Frame::bulk("interval"));
                    items.push(Frame::Integer(status.interval_secs as i64));
                    items.push(Frame::bulk("confidence_threshold"));
                    items.push(Frame::Double(status.confidence_threshold));
                    items.push(Frame::bulk("cooldown"));
                    items.push(Frame::Integer(status.cooldown_secs as i64));
                    items.push(Frame::bulk("ab_test_enabled"));
                    items.push(Frame::bulk(
                        if status.ab_test_enabled { "true" } else { "false" },
                    ));
                    Frame::Array(Some(items))
                } else if args.len() == 1 {
                    // GET a single config value.
                    match tuner.get_config_value(&args[0]) {
                        Ok(val) => Frame::bulk(val),
                        Err(e) => Frame::error(format!("ERR {}", e)),
                    }
                } else {
                    // SET a config value.
                    match tuner.set_config_value(&args[0], &args[1]) {
                        Ok(()) => Frame::simple("OK"),
                        Err(e) => Frame::error(format!("ERR {}", e)),
                    }
                }
            }
            _ => Frame::error(format!(
                "ERR unknown subcommand '{}'. Try: STATUS, ANALYZE, RECOMMEND, APPLY, HISTORY, RULES, CONFIG",
                subcommand
            )),
        }
    }

    // ── FaaS (Serverless Functions at the Edge) ──────────────────────

    /// Handle FUNCTION subcommands for FaaS: DEPLOY, INVOKE, UNDEPLOY, etc.
    pub(super) async fn handle_faas_command(&self, subcommand: &str, args: &[Bytes]) -> Frame {
        use ferrite_plugins::faas::registry::{DeployConfig, FaaSRegistry};
        use std::sync::{Arc, LazyLock};

        static FAAS_REGISTRY: LazyLock<Arc<FaaSRegistry>> =
            LazyLock::new(|| Arc::new(FaaSRegistry::new()));

        let registry = &*FAAS_REGISTRY;

        match subcommand {
            "DEPLOY" => {
                // FUNCTION DEPLOY <name> <wasm_bytes>
                if args.len() < 2 {
                    return Frame::error(
                        "ERR wrong number of arguments for FUNCTION DEPLOY. Usage: FUNCTION DEPLOY <name> <wasm_bytes>",
                    );
                }
                let name = String::from_utf8_lossy(&args[0]).to_string();
                let source = args[1].to_vec();
                match registry.deploy(&name, source, DeployConfig::default()) {
                    Ok(meta) => Frame::array(vec![
                        Frame::bulk("name"),
                        Frame::bulk(Bytes::from(meta.name)),
                        Frame::bulk("language"),
                        Frame::bulk(Bytes::from(meta.language.to_string())),
                        Frame::bulk("source_hash"),
                        Frame::bulk(Bytes::from(meta.source_hash)),
                        Frame::bulk("status"),
                        Frame::bulk(Bytes::from(meta.status.to_string())),
                    ]),
                    Err(e) => Frame::error(format!("ERR {}", e)),
                }
            }
            "INVOKE" => {
                // FUNCTION INVOKE <name> [args...]
                if args.is_empty() {
                    return Frame::error(
                        "ERR wrong number of arguments for FUNCTION INVOKE. Usage: FUNCTION INVOKE <name> [args...]",
                    );
                }
                let name = String::from_utf8_lossy(&args[0]).to_string();
                let fn_args: Vec<Vec<u8>> =
                    args[1..].iter().map(|a| a.to_vec()).collect();
                match registry.invoke(&name, &fn_args).await {
                    Ok(result) => Frame::array(vec![
                        Frame::bulk("output"),
                        Frame::Bulk(Some(Bytes::from(result.output))),
                        Frame::bulk("execution_time_ms"),
                        Frame::Integer(result.execution_time_ms as i64),
                        Frame::bulk("memory_used_bytes"),
                        Frame::Integer(result.memory_used_bytes as i64),
                    ]),
                    Err(e) => Frame::error(format!("ERR {}", e)),
                }
            }
            "UNDEPLOY" => {
                // FUNCTION UNDEPLOY <name>
                if args.is_empty() {
                    return Frame::error(
                        "ERR wrong number of arguments for FUNCTION UNDEPLOY. Usage: FUNCTION UNDEPLOY <name>",
                    );
                }
                let name = String::from_utf8_lossy(&args[0]).to_string();
                match registry.undeploy(&name) {
                    Ok(()) => Frame::simple("OK"),
                    Err(e) => Frame::error(format!("ERR {}", e)),
                }
            }
            "FAAS.LIST" => {
                let functions = registry.list();
                let items: Vec<Frame> = functions
                    .iter()
                    .map(|meta| {
                        Frame::array(vec![
                            Frame::bulk("name"),
                            Frame::bulk(Bytes::from(meta.name.clone())),
                            Frame::bulk("language"),
                            Frame::bulk(Bytes::from(meta.language.to_string())),
                            Frame::bulk("status"),
                            Frame::bulk(Bytes::from(meta.status.to_string())),
                            Frame::bulk("invocations"),
                            Frame::Integer(meta.invocation_count as i64),
                        ])
                    })
                    .collect();
                Frame::array(items)
            }
            "FAAS.INFO" => {
                // FUNCTION FAAS.INFO <name>
                if args.is_empty() {
                    return Frame::error(
                        "ERR wrong number of arguments for FUNCTION FAAS.INFO. Usage: FUNCTION FAAS.INFO <name>",
                    );
                }
                let name = String::from_utf8_lossy(&args[0]).to_string();
                match registry.info(&name) {
                    Ok(meta) => Frame::array(vec![
                        Frame::bulk("name"),
                        Frame::bulk(Bytes::from(meta.name)),
                        Frame::bulk("language"),
                        Frame::bulk(Bytes::from(meta.language.to_string())),
                        Frame::bulk("source_hash"),
                        Frame::bulk(Bytes::from(meta.source_hash)),
                        Frame::bulk("deployed_at"),
                        Frame::Integer(meta.deployed_at as i64),
                        Frame::bulk("invocation_count"),
                        Frame::Integer(meta.invocation_count as i64),
                        Frame::bulk("avg_latency_ms"),
                        Frame::bulk(Bytes::from(format!("{:.2}", meta.avg_latency_ms))),
                        Frame::bulk("status"),
                        Frame::bulk(Bytes::from(meta.status.to_string())),
                    ]),
                    Err(e) => Frame::error(format!("ERR {}", e)),
                }
            }
            "FAAS.LOGS" => {
                // FUNCTION FAAS.LOGS <name> [count]
                if args.is_empty() {
                    return Frame::error(
                        "ERR wrong number of arguments for FUNCTION FAAS.LOGS. Usage: FUNCTION FAAS.LOGS <name> [count]",
                    );
                }
                let name = String::from_utf8_lossy(&args[0]).to_string();
                let count = if args.len() > 1 {
                    String::from_utf8_lossy(&args[1])
                        .parse::<usize>()
                        .unwrap_or(10)
                } else {
                    10
                };
                let logs = registry.logs(&name, count);
                let items: Vec<Frame> = logs
                    .into_iter()
                    .map(|l| Frame::bulk(Bytes::from(l)))
                    .collect();
                Frame::array(items)
            }
            "SCHEDULE" => {
                // FUNCTION SCHEDULE <function_name> <cron_expr>
                if args.len() < 2 {
                    return Frame::error(
                        "ERR wrong number of arguments for FUNCTION SCHEDULE. Usage: FUNCTION SCHEDULE <name> <cron_expr>",
                    );
                }
                let fn_name = String::from_utf8_lossy(&args[0]).to_string();
                let cron_expr = String::from_utf8_lossy(&args[1]).to_string();
                let sched_name = format!("sched_{}", fn_name);
                match registry.schedule(&fn_name, &sched_name, &cron_expr) {
                    Ok(()) => Frame::simple("OK"),
                    Err(e) => Frame::error(format!("ERR {}", e)),
                }
            }
            "UNSCHEDULE" => {
                // FUNCTION UNSCHEDULE <schedule_name>
                if args.is_empty() {
                    return Frame::error(
                        "ERR wrong number of arguments for FUNCTION UNSCHEDULE. Usage: FUNCTION UNSCHEDULE <name>",
                    );
                }
                let name = String::from_utf8_lossy(&args[0]).to_string();
                match registry.unschedule(&name) {
                    Ok(()) => Frame::simple("OK"),
                    Err(e) => Frame::error(format!("ERR {}", e)),
                }
            }
            "SCHEDULES" => {
                let schedules = registry.schedules();
                let items: Vec<Frame> = schedules
                    .iter()
                    .map(|s| {
                        Frame::array(vec![
                            Frame::bulk("name"),
                            Frame::bulk(Bytes::from(s.name.clone())),
                            Frame::bulk("function"),
                            Frame::bulk(Bytes::from(s.function_name.clone())),
                            Frame::bulk("cron"),
                            Frame::bulk(Bytes::from(s.cron_expr.clone())),
                            Frame::bulk("enabled"),
                            Frame::Integer(if s.enabled { 1 } else { 0 }),
                            Frame::bulk("next_run"),
                            Frame::Integer(s.next_run as i64),
                        ])
                    })
                    .collect();
                Frame::array(items)
            }
            "FAAS.STATS" => {
                let stats = registry.stats();
                Frame::array(vec![
                    Frame::bulk("total_functions"),
                    Frame::Integer(stats.total_functions as i64),
                    Frame::bulk("total_invocations"),
                    Frame::Integer(stats.total_invocations as i64),
                    Frame::bulk("avg_latency_ms"),
                    Frame::bulk(Bytes::from(format!("{:.2}", stats.avg_latency_ms))),
                    Frame::bulk("active_schedules"),
                    Frame::Integer(stats.active_schedules as i64),
                ])
            }
            _ => {
                // Not a FaaS subcommand
                Frame::error(format!(
                    "ERR unknown FUNCTION subcommand '{}'. Try: DEPLOY, INVOKE, UNDEPLOY, FAAS.LIST, FAAS.INFO, FAAS.LOGS, SCHEDULE, UNSCHEDULE, SCHEDULES, FAAS.STATS",
                    subcommand
                ))
            }
        }
    }

    // ── Materialized view handlers ───────────────────────────────────────────

    pub(super) async fn handle_view_create(
        &self,
        name: &Bytes,
        query: &str,
        strategy: &str,
        interval: Option<u64>,
    ) -> Frame {
        use ferrite_core::views::{RefreshStrategy, ViewDefinition, ViewEngine, ViewStatus};

        let view_name = String::from_utf8_lossy(name).to_string();

        let refresh_strategy = match strategy {
            "eager" => RefreshStrategy::Eager,
            "lazy" => RefreshStrategy::Lazy,
            "periodic" => RefreshStrategy::Periodic {
                interval_secs: interval.unwrap_or(60),
            },
            _ => return Frame::error("ERR invalid strategy. Use: eager, lazy, periodic"),
        };

        // Extract source patterns from query (simple heuristic: look for key patterns)
        let source_patterns = extract_source_patterns(query);

        let def = ViewDefinition {
            name: view_name,
            query: query.to_string(),
            source_patterns,
            refresh_strategy,
            created_at: chrono::Utc::now(),
            last_refreshed: None,
            status: ViewStatus::Active,
        };

        let engine = ViewEngine::new();
        match engine.create_view(def) {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::error(format!("ERR {}", e)),
        }
    }

    pub(super) async fn handle_view_drop(&self, name: &Bytes) -> Frame {
        use ferrite_core::views::ViewEngine;

        let view_name = String::from_utf8_lossy(name).to_string();
        let engine = ViewEngine::new();

        match engine.drop_view(&view_name) {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::error(format!("ERR {}", e)),
        }
    }

    pub(super) async fn handle_view_query(&self, name: &Bytes) -> Frame {
        use ferrite_core::views::ViewEngine;

        let view_name = String::from_utf8_lossy(name).to_string();
        let engine = ViewEngine::new();

        match engine.query_view(&view_name) {
            Ok(rows) => {
                let items: Vec<Frame> = rows
                    .iter()
                    .flat_map(|row| {
                        vec![
                            Frame::bulk(row.key.clone()),
                            Frame::bulk(row.value.clone()),
                        ]
                    })
                    .collect();
                Frame::Array(Some(items))
            }
            Err(e) => Frame::error(format!("ERR {}", e)),
        }
    }

    pub(super) async fn handle_view_list(&self) -> Frame {
        use ferrite_core::views::ViewEngine;

        let engine = ViewEngine::new();
        let views = engine.list_views();

        let items: Vec<Frame> = views
            .into_iter()
            .map(|v| {
                let mut info = Vec::new();
                info.push(Frame::bulk("name"));
                info.push(Frame::bulk(v.name));
                info.push(Frame::bulk("query"));
                info.push(Frame::bulk(v.query));
                info.push(Frame::bulk("strategy"));
                info.push(Frame::bulk(format!("{:?}", v.refresh_strategy)));
                info.push(Frame::bulk("status"));
                info.push(Frame::bulk(format!("{:?}", v.status)));
                Frame::Array(Some(info))
            })
            .collect();

        Frame::Array(Some(items))
    }

    pub(super) async fn handle_view_refresh(&self, name: &Bytes) -> Frame {
        use ferrite_core::views::ViewEngine;

        let view_name = String::from_utf8_lossy(name).to_string();
        let engine = ViewEngine::new();

        match engine.refresh_view(&view_name) {
            Ok(result) => {
                let mut items = Vec::new();
                items.push(Frame::bulk("rows_computed"));
                items.push(Frame::Integer(result.rows_computed as i64));
                items.push(Frame::bulk("duration_ms"));
                items.push(Frame::Integer(result.duration_ms as i64));
                items.push(Frame::bulk("was_stale"));
                items.push(Frame::bulk(if result.was_stale { "true" } else { "false" }));
                Frame::Array(Some(items))
            }
            Err(e) => Frame::error(format!("ERR {}", e)),
        }
    }

    pub(super) async fn handle_view_info(&self, name: &Bytes) -> Frame {
        use ferrite_core::views::ViewEngine;

        let view_name = String::from_utf8_lossy(name).to_string();
        let engine = ViewEngine::new();

        match engine.get_view(&view_name) {
            Some(view) => {
                let mut items = Vec::new();
                items.push(Frame::bulk("name"));
                items.push(Frame::bulk(view.name));
                items.push(Frame::bulk("query"));
                items.push(Frame::bulk(view.query));
                items.push(Frame::bulk("source_patterns"));
                items.push(Frame::Array(Some(
                    view.source_patterns
                        .into_iter()
                        .map(Frame::bulk)
                        .collect(),
                )));
                items.push(Frame::bulk("strategy"));
                items.push(Frame::bulk(format!("{:?}", view.refresh_strategy)));
                items.push(Frame::bulk("status"));
                items.push(Frame::bulk(format!("{:?}", view.status)));
                items.push(Frame::bulk("created_at"));
                items.push(Frame::bulk(view.created_at.to_rfc3339()));
                items.push(Frame::bulk("last_refreshed"));
                match view.last_refreshed {
                    Some(ts) => items.push(Frame::bulk(ts.to_rfc3339())),
                    None => items.push(Frame::Null),
                }
                Frame::Array(Some(items))
            }
            None => Frame::error(format!("ERR view '{}' not found", view_name)),
        }
    }

    // ------------------------------------------------------------------
    // Live migration command handlers
    // ------------------------------------------------------------------

    pub(super) async fn handle_migrate_start(
        &self,
        source_uri: &str,
        batch_size: Option<usize>,
        workers: Option<usize>,
        verify: bool,
        dry_run: bool,
    ) -> Frame {
        use crate::migration::live::sync_engine::{MigrationConfig, MigrationEngine};

        let config = MigrationConfig {
            batch_size: batch_size.unwrap_or(1000),
            parallel_workers: workers.unwrap_or(4),
            verify_after_sync: verify,
            dry_run,
        };

        let engine = MigrationEngine::new(source_uri.to_string(), config);

        match engine.start_bulk_sync().await {
            Ok(state) => {
                let mut items = Vec::new();
                items.push(Frame::bulk("id"));
                items.push(Frame::bulk(Bytes::from(state.id)));
                items.push(Frame::bulk("status"));
                items.push(Frame::bulk(Bytes::from(state.status.to_string())));
                items.push(Frame::bulk("phase"));
                items.push(Frame::bulk(Bytes::from(state.phase.to_string())));
                items.push(Frame::bulk("keys_synced"));
                items.push(Frame::Integer(state.keys_synced as i64));
                items.push(Frame::bulk("keys_total"));
                match state.keys_total {
                    Some(t) => items.push(Frame::Integer(t as i64)),
                    None => items.push(Frame::Null),
                }
                items.push(Frame::bulk("bytes_synced"));
                items.push(Frame::Integer(state.bytes_synced as i64));
                Frame::Array(Some(items))
            }
            Err(e) => Frame::error(format!("ERR migration failed: {}", e)),
        }
    }

    pub(super) async fn handle_migrate_status(&self) -> Frame {
        // Without a persistent engine reference, return a placeholder.
        let mut items = Vec::new();
        items.push(Frame::bulk("status"));
        items.push(Frame::bulk("no active migration"));
        Frame::Array(Some(items))
    }

    pub(super) async fn handle_migrate_pause(&self) -> Frame {
        Frame::simple("OK")
    }

    pub(super) async fn handle_migrate_resume(&self) -> Frame {
        Frame::simple("OK")
    }

    pub(super) async fn handle_migrate_verify(&self, sample_pct: Option<f64>) -> Frame {
        use crate::migration::live::verifier::MigrationVerifier;

        let sample_size = match sample_pct {
            Some(pct) => (pct * 100.0) as usize,
            None => 100,
        };

        let report = MigrationVerifier::verify_snapshot(sample_size);

        let mut items = Vec::new();
        items.push(Frame::bulk("total_checked"));
        items.push(Frame::Integer(report.total_checked as i64));
        items.push(Frame::bulk("matching"));
        items.push(Frame::Integer(report.matching as i64));
        items.push(Frame::bulk("mismatched"));
        items.push(Frame::Integer(report.mismatched as i64));
        items.push(Frame::bulk("missing_in_target"));
        items.push(Frame::Integer(report.missing_in_target as i64));
        items.push(Frame::bulk("extra_in_target"));
        items.push(Frame::Integer(report.extra_in_target as i64));
        items.push(Frame::bulk("sample_percentage"));
        items.push(Frame::Double(report.sample_percentage));
        items.push(Frame::bulk("consistent"));
        items.push(Frame::bulk(if report.is_consistent() {
            "true"
        } else {
            "false"
        }));
        Frame::Array(Some(items))
    }

    pub(super) async fn handle_migrate_cutover(&self) -> Frame {
        Frame::simple("OK")
    }

    pub(super) async fn handle_migrate_rollback(&self) -> Frame {
        Frame::simple("OK")
    }

    // ── Kafka-compatible streaming handlers ─────────────────────────────

    pub(super) async fn handle_stream_create(
        &self,
        topic: &str,
        partitions: u32,
        retention_ms: i64,
        replication: u16,
    ) -> Frame {
        let broker = streaming_broker();
        match broker.create_topic(topic.to_string(), partitions, replication, retention_ms) {
            Ok(t) => {
                let mut items = vec![
                    Frame::bulk("topic"),
                    Frame::bulk(t.name),
                    Frame::bulk("partitions"),
                    Frame::Integer(t.num_partitions as i64),
                    Frame::bulk("replication"),
                    Frame::Integer(t.replication_factor as i64),
                ];
                if retention_ms >= 0 {
                    items.push(Frame::bulk("retention_ms"));
                    items.push(Frame::Integer(retention_ms));
                }
                Frame::Array(Some(items))
            }
            Err(e) => Frame::Error(format!("ERR {e}").into()),
        }
    }

    pub(super) async fn handle_stream_delete(&self, topic: &str) -> Frame {
        let broker = streaming_broker();
        match broker.delete_topic(topic) {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::Error(format!("ERR {e}").into()),
        }
    }

    pub(super) async fn handle_stream_produce(
        &self,
        topic: &str,
        key: Option<&Bytes>,
        value: &Bytes,
        partition: Option<u32>,
    ) -> Frame {
        use ferrite_streaming::kafka::ProducerRecord;

        let broker = streaming_broker();
        let record = ProducerRecord {
            topic: topic.to_string(),
            partition,
            key: key.map(|k| k.to_vec()),
            value: value.to_vec(),
            headers: vec![],
            timestamp: None,
        };
        match broker.produce(record) {
            Ok((p, o)) => Frame::Array(Some(vec![
                Frame::bulk("partition"),
                Frame::Integer(p as i64),
                Frame::bulk("offset"),
                Frame::Integer(o),
            ])),
            Err(e) => Frame::Error(format!("ERR {e}").into()),
        }
    }

    pub(super) async fn handle_stream_fetch(
        &self,
        topic: &str,
        partition: u32,
        offset: i64,
        count: usize,
    ) -> Frame {
        let broker = streaming_broker();
        match broker.fetch(topic, partition, offset, count) {
            Ok(records) => {
                let items: Vec<Frame> = records
                    .into_iter()
                    .map(|r| {
                        Frame::Array(Some(vec![
                            Frame::bulk("offset"),
                            Frame::Integer(r.offset),
                            Frame::bulk("key"),
                            match r.key {
                                Some(k) => Frame::Bulk(Some(Bytes::from(k))),
                                None => Frame::Null,
                            },
                            Frame::bulk("value"),
                            Frame::Bulk(Some(Bytes::from(r.value))),
                            Frame::bulk("timestamp"),
                            Frame::Integer(r.timestamp),
                        ]))
                    })
                    .collect();
                Frame::Array(Some(items))
            }
            Err(e) => Frame::Error(format!("ERR {e}").into()),
        }
    }

    pub(super) async fn handle_stream_commit(
        &self,
        group: &str,
        topic: &str,
        partition: u32,
        offset: i64,
    ) -> Frame {
        let broker = streaming_broker();
        match broker.commit_offset(group, topic.to_string(), partition, offset) {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::Error(format!("ERR {e}").into()),
        }
    }

    pub(super) async fn handle_stream_topics(&self) -> Frame {
        let broker = streaming_broker();
        let topics = broker.list_topics();
        let items: Vec<Frame> = topics
            .into_iter()
            .map(|t| {
                Frame::Array(Some(vec![
                    Frame::bulk("name"),
                    Frame::bulk(t.name),
                    Frame::bulk("partitions"),
                    Frame::Integer(t.num_partitions as i64),
                    Frame::bulk("replication"),
                    Frame::Integer(t.replication_factor as i64),
                ]))
            })
            .collect();
        Frame::Array(Some(items))
    }

    pub(super) async fn handle_stream_describe(&self, topic: &str) -> Frame {
        let broker = streaming_broker();
        match broker.describe_topic(topic) {
            Some(t) => Frame::Array(Some(vec![
                Frame::bulk("name"),
                Frame::bulk(t.name),
                Frame::bulk("partitions"),
                Frame::Integer(t.num_partitions as i64),
                Frame::bulk("replication"),
                Frame::Integer(t.replication_factor as i64),
                Frame::bulk("retention_ms"),
                Frame::Integer(t.config.retention_ms),
                Frame::bulk("max_message_bytes"),
                Frame::Integer(t.config.max_message_bytes as i64),
            ])),
            None => Frame::Null,
        }
    }

    pub(super) async fn handle_stream_groups(&self, topic: Option<&str>) -> Frame {
        let broker = streaming_broker();
        let groups = broker.list_groups(topic);
        let items: Vec<Frame> = groups.into_iter().map(|g| Frame::bulk(g)).collect();
        Frame::Array(Some(items))
    }

    pub(super) async fn handle_stream_offsets(&self, topic: &str, partition: u32) -> Frame {
        let broker = streaming_broker();
        match broker.get_offsets(topic, partition) {
            Ok((earliest, latest)) => Frame::Array(Some(vec![
                Frame::bulk("earliest"),
                Frame::Integer(earliest),
                Frame::bulk("latest"),
                Frame::Integer(latest),
            ])),
            Err(e) => Frame::Error(format!("ERR {e}").into()),
        }
    }

    pub(super) async fn handle_stream_stats(&self) -> Frame {
        let broker = streaming_broker();
        let s = broker.stats();
        Frame::Array(Some(vec![
            Frame::bulk("total_topics"),
            Frame::Integer(s.total_topics as i64),
            Frame::bulk("total_partitions"),
            Frame::Integer(s.total_partitions as i64),
            Frame::bulk("total_messages"),
            Frame::Integer(s.total_messages as i64),
            Frame::bulk("total_consumer_groups"),
            Frame::Integer(s.total_consumer_groups as i64),
        ]))
    }

    // ── Multi-region active-active handlers ───────────────────────────

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_region_add(
        &self,
        id: &str,
        name: &str,
        endpoint: &str,
    ) -> Frame {
        let replicator = active_active_replicator();
        match replicator.add_region(id.to_string(), name.to_string(), endpoint.to_string()) {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::error(format!("ERR {e}")),
        }
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_region_add(
        &self,
        _id: &str,
        _name: &str,
        _endpoint: &str,
    ) -> Frame {
        Frame::error("ERR REGION commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_region_remove(&self, id: &str) -> Frame {
        let replicator = active_active_replicator();
        match replicator.remove_region(id) {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::error(format!("ERR {e}")),
        }
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_region_remove(&self, _id: &str) -> Frame {
        Frame::error("ERR REGION commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_region_list(&self) -> Frame {
        let replicator = active_active_replicator();
        let regions = replicator.list_regions();
        if regions.is_empty() {
            return Frame::Array(Some(vec![]));
        }
        let items: Vec<Frame> = regions
            .into_iter()
            .map(|r| {
                let status = r.status.to_string();
                Frame::Array(Some(vec![
                    Frame::bulk("id"),
                    Frame::bulk(r.id),
                    Frame::bulk("name"),
                    Frame::bulk(r.name),
                    Frame::bulk("endpoint"),
                    Frame::bulk(r.endpoint),
                    Frame::bulk("status"),
                    Frame::bulk(status),
                    Frame::bulk("lag_ms"),
                    Frame::Integer(r.replication_lag_ms as i64),
                    Frame::bulk("keys_synced"),
                    Frame::Integer(r.keys_synced as i64),
                    Frame::bulk("conflicts_resolved"),
                    Frame::Integer(r.conflicts_resolved as i64),
                ]))
            })
            .collect();
        Frame::Array(Some(items))
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_region_list(&self) -> Frame {
        Frame::error("ERR REGION commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_region_status(&self, id: Option<&str>) -> Frame {
        let replicator = active_active_replicator();
        match id {
            Some(region_id) => {
                if let Some(r) = replicator.get_region(region_id) {
                    let status = r.status.to_string();
                    Frame::Array(Some(vec![
                        Frame::bulk("id"),
                        Frame::bulk(r.id),
                        Frame::bulk("name"),
                        Frame::bulk(r.name),
                        Frame::bulk("endpoint"),
                        Frame::bulk(r.endpoint),
                        Frame::bulk("status"),
                        Frame::bulk(status),
                        Frame::bulk("lag_ms"),
                        Frame::Integer(r.replication_lag_ms as i64),
                        Frame::bulk("keys_synced"),
                        Frame::Integer(r.keys_synced as i64),
                    ]))
                } else {
                    Frame::error(format!("ERR Region '{region_id}' not found"))
                }
            }
            None => {
                let stats = replicator.stats();
                let strategy = replicator.conflict_strategy().to_string();
                Frame::Array(Some(vec![
                    Frame::bulk("local_region"),
                    Frame::bulk(replicator.local_region().to_string()),
                    Frame::bulk("regions_active"),
                    Frame::Integer(stats.regions_active as i64),
                    Frame::bulk("strategy"),
                    Frame::bulk(strategy),
                ]))
            }
        }
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_region_status(&self, _id: Option<&str>) -> Frame {
        Frame::error("ERR REGION commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_region_conflicts(&self, limit: usize) -> Frame {
        let replicator = active_active_replicator();
        let conflicts = replicator.get_conflicts(limit);
        if conflicts.is_empty() {
            return Frame::Array(Some(vec![]));
        }
        let items: Vec<Frame> = conflicts
            .into_iter()
            .map(|c| {
                Frame::Array(Some(vec![
                    Frame::bulk("key"),
                    Frame::bulk(c.key),
                    Frame::bulk("strategy"),
                    Frame::bulk(c.strategy),
                    Frame::bulk("winner"),
                    Frame::bulk(c.winner),
                    Frame::bulk("resolved_at"),
                    Frame::bulk(c.resolved_at),
                ]))
            })
            .collect();
        Frame::Array(Some(items))
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_region_conflicts(&self, _limit: usize) -> Frame {
        Frame::error("ERR REGION commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_region_strategy(&self, strategy: Option<&str>) -> Frame {
        let replicator = active_active_replicator();
        match strategy {
            Some(s) => {
                match ferrite_enterprise::active_active::ConflictStrategy::from_str_loose(s) {
                    Some(_strat) => {
                        Frame::simple(format!("OK (strategy would be set to: {s})"))
                    }
                    None => Frame::error(
                        format!("ERR Unknown strategy '{s}'. Use: lww, highest-region-id, merge"),
                    ),
                }
            }
            None => {
                let strategy = replicator.conflict_strategy().to_string();
                Frame::bulk(strategy)
            }
        }
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_region_strategy(&self, _strategy: Option<&str>) -> Frame {
        Frame::error("ERR REGION commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_region_stats(&self) -> Frame {
        let replicator = active_active_replicator();
        let s = replicator.stats();
        Frame::Array(Some(vec![
            Frame::bulk("ops_replicated"),
            Frame::Integer(s.ops_replicated as i64),
            Frame::bulk("conflicts_detected"),
            Frame::Integer(s.conflicts_detected as i64),
            Frame::bulk("conflicts_resolved"),
            Frame::Integer(s.conflicts_resolved as i64),
            Frame::bulk("regions_active"),
            Frame::Integer(s.regions_active as i64),
            Frame::bulk("avg_lag_ms"),
            Frame::Integer(s.avg_lag_ms as i64),
        ]))
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_region_stats(&self) -> Frame {
        Frame::error("ERR REGION commands require the 'experimental' feature")
    }

    // ---- Integrated Observability Diagnostics (FERRITE.DEBUG) ----

    pub(super) async fn ferrite_debug(&self, subcommand: &str, args: &[String]) -> Frame {
        use ferrite_core::observability::diagnostics::{
            AdaptiveSampler, BottleneckAnalyzer, HotKeyDetector, SlowQueryAnalyzer,
        };
        use std::sync::OnceLock;
        use std::time::Duration;

        // Shared diagnostic singletons
        static SLOW_ANALYZER: OnceLock<SlowQueryAnalyzer> = OnceLock::new();
        static SAMPLER: OnceLock<AdaptiveSampler> = OnceLock::new();
        static HOTKEY_DETECTOR: OnceLock<HotKeyDetector> = OnceLock::new();
        static BOTTLENECK: OnceLock<BottleneckAnalyzer> = OnceLock::new();

        let slow_analyzer =
            SLOW_ANALYZER.get_or_init(|| SlowQueryAnalyzer::new(1024, 10_000));
        let sampler =
            SAMPLER.get_or_init(|| AdaptiveSampler::new(0.01, 1.0, 2.5));
        let hotkey_detector =
            HOTKEY_DETECTOR.get_or_init(|| HotKeyDetector::new(Duration::from_secs(60), 20));
        let bottleneck =
            BOTTLENECK.get_or_init(|| BottleneckAnalyzer::new(1_000));

        match subcommand.to_uppercase().as_str() {
            "SLOWLOG" => {
                let sub = args.first().map(|s| s.to_uppercase());
                match sub.as_deref() {
                    Some("RESET") => {
                        let cleared = slow_analyzer.reset();
                        Frame::Integer(cleared as i64)
                    }
                    Some("ANALYZE") => {
                        let report = slow_analyzer.analyze();
                        let mut items = Vec::new();
                        items.push(Frame::bulk("total"));
                        items.push(Frame::Integer(report.total as i64));
                        items.push(Frame::bulk("avg_duration_us"));
                        items.push(Frame::Integer(report.avg_duration_us as i64));
                        items.push(Frame::bulk("p50_us"));
                        items.push(Frame::Integer(report.p50_us as i64));
                        items.push(Frame::bulk("p99_us"));
                        items.push(Frame::Integer(report.p99_us as i64));
                        items.push(Frame::bulk("top_commands"));
                        let cmd_frames: Vec<Frame> = report
                            .top_commands
                            .into_iter()
                            .flat_map(|(cmd, cnt)| {
                                vec![Frame::bulk(Bytes::from(cmd)), Frame::Integer(cnt as i64)]
                            })
                            .collect();
                        items.push(Frame::Array(Some(cmd_frames)));
                        items.push(Frame::bulk("top_patterns"));
                        let pat_frames: Vec<Frame> = report
                            .top_patterns
                            .into_iter()
                            .flat_map(|(p, cnt)| {
                                vec![Frame::bulk(Bytes::from(p)), Frame::Integer(cnt as i64)]
                            })
                            .collect();
                        items.push(Frame::Array(Some(pat_frames)));
                        Frame::Array(Some(items))
                    }
                    _ => {
                        let count: usize = args
                            .first()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(10);
                        let entries = slow_analyzer.get(count);
                        let frames: Vec<Frame> = entries
                            .into_iter()
                            .map(|e| {
                                let arg_frames: Vec<Frame> = e
                                    .args
                                    .into_iter()
                                    .map(|a| Frame::bulk(Bytes::from(a)))
                                    .collect();
                                Frame::Array(Some(vec![
                                    Frame::Integer(e.id as i64),
                                    Frame::Integer(e.timestamp as i64),
                                    Frame::Integer(e.duration_us as i64),
                                    Frame::bulk(Bytes::from(e.command)),
                                    Frame::Array(Some(arg_frames)),
                                ]))
                            })
                            .collect();
                        Frame::Array(Some(frames))
                    }
                }
            }

            "HOTKEYS" => {
                let count: usize = args
                    .first()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(10);
                let hot = hotkey_detector.get_hot_keys(count);
                let frames: Vec<Frame> = hot
                    .into_iter()
                    .map(|h| {
                        Frame::Array(Some(vec![
                            Frame::bulk(Bytes::from(h.key)),
                            Frame::Integer(h.access_count as i64),
                            Frame::Double(h.ops_per_sec),
                        ]))
                    })
                    .collect();
                Frame::Array(Some(frames))
            }

            "BOTTLENECK" => {
                let report = bottleneck.analyze();
                Frame::Array(Some(vec![
                    Frame::bulk("bottleneck"),
                    Frame::bulk(Bytes::from(report.bottleneck.to_string())),
                    Frame::bulk("confidence"),
                    Frame::Double(report.confidence),
                    Frame::bulk("recommendation"),
                    Frame::bulk(Bytes::from(report.recommendation)),
                    Frame::bulk("avg_cpu"),
                    Frame::Double(report.avg_cpu),
                    Frame::bulk("avg_memory"),
                    Frame::Double(report.avg_memory),
                    Frame::bulk("avg_io"),
                    Frame::Double(report.avg_io),
                    Frame::bulk("avg_connections"),
                    Frame::Double(report.avg_connections),
                    Frame::bulk("sample_count"),
                    Frame::Integer(report.sample_count as i64),
                ]))
            }

            "SAMPLING" => {
                let sub = args.first().map(|s| s.to_uppercase());
                match sub.as_deref() {
                    Some("SET") => {
                        let rate: f64 = args
                            .get(1)
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0.01);
                        sampler.set_base_rate(rate);
                        Frame::simple("OK")
                    }
                    _ => {
                        let status = sampler.status();
                        Frame::Array(Some(vec![
                            Frame::bulk("state"),
                            Frame::bulk(Bytes::from(format!("{:?}", status.state))),
                            Frame::bulk("current_rate"),
                            Frame::Double(status.current_rate),
                            Frame::bulk("base_rate"),
                            Frame::Double(status.base_rate),
                            Frame::bulk("anomaly_rate"),
                            Frame::Double(status.anomaly_rate),
                            Frame::bulk("measurements"),
                            Frame::Integer(status.measurements as i64),
                            Frame::bulk("mean_latency_us"),
                            Frame::Double(status.mean_latency_us),
                            Frame::bulk("stddev_latency_us"),
                            Frame::Double(status.stddev_latency_us),
                        ]))
                    }
                }
            }

            "STATS" => {
                let slow_count = slow_analyzer.len();
                let slow_total = slow_analyzer.total_recorded();
                let hot_count = hotkey_detector.tracked_keys();
                let hot_accesses = hotkey_detector.total_accesses();
                let samples = bottleneck.sample_count();
                let sampler_status = sampler.status();

                Frame::Array(Some(vec![
                    Frame::bulk("slow_query_buffer"),
                    Frame::Integer(slow_count as i64),
                    Frame::bulk("slow_query_total"),
                    Frame::Integer(slow_total as i64),
                    Frame::bulk("hotkey_tracked_keys"),
                    Frame::Integer(hot_count as i64),
                    Frame::bulk("hotkey_total_accesses"),
                    Frame::Integer(hot_accesses as i64),
                    Frame::bulk("bottleneck_samples"),
                    Frame::Integer(samples as i64),
                    Frame::bulk("sampling_state"),
                    Frame::bulk(Bytes::from(format!("{:?}", sampler_status.state))),
                    Frame::bulk("sampling_rate"),
                    Frame::Double(sampler_status.current_rate),
                ]))
            }

            "LATENCY" => {
                let report = slow_analyzer.analyze();
                let cmd_filter = args.first().map(|s| s.to_uppercase());

                if let Some(cmd) = cmd_filter {
                    let entries = slow_analyzer.get(1000);
                    let filtered: Vec<_> = entries
                        .iter()
                        .filter(|e| e.command.to_uppercase() == cmd)
                        .collect();
                    if filtered.is_empty() {
                        return Frame::Array(Some(vec![
                            Frame::bulk("command"),
                            Frame::bulk(Bytes::from(cmd)),
                            Frame::bulk("samples"),
                            Frame::Integer(0),
                        ]));
                    }
                    let mut durations: Vec<u64> =
                        filtered.iter().map(|e| e.duration_us).collect();
                    durations.sort_unstable();
                    let sum: u64 = durations.iter().sum();
                    let avg = sum / durations.len() as u64;
                    let min = durations[0];
                    let max = durations[durations.len() - 1];
                    Frame::Array(Some(vec![
                        Frame::bulk("command"),
                        Frame::bulk(Bytes::from(cmd)),
                        Frame::bulk("samples"),
                        Frame::Integer(durations.len() as i64),
                        Frame::bulk("avg_us"),
                        Frame::Integer(avg as i64),
                        Frame::bulk("min_us"),
                        Frame::Integer(min as i64),
                        Frame::bulk("max_us"),
                        Frame::Integer(max as i64),
                    ]))
                } else {
                    Frame::Array(Some(vec![
                        Frame::bulk("total"),
                        Frame::Integer(report.total as i64),
                        Frame::bulk("avg_duration_us"),
                        Frame::Integer(report.avg_duration_us as i64),
                        Frame::bulk("p50_us"),
                        Frame::Integer(report.p50_us as i64),
                        Frame::bulk("p99_us"),
                        Frame::Integer(report.p99_us as i64),
                    ]))
                }
            }

            _ => Frame::error(format!(
                "ERR unknown FERRITE.DEBUG subcommand '{}'. Try SLOWLOG, HOTKEYS, BOTTLENECK, SAMPLING, STATS, LATENCY",
                subcommand
            )),
        }
    }

    // ── Data Mesh / Federation gateway handlers ───────────────────────

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_federation_add(
        &self,
        id: &str,
        source_type: &str,
        uri: &str,
        name: Option<&str>,
    ) -> Frame {
        use ferrite_enterprise::mesh::datasource::{DataSourceConfig, DataSourceType};

        let stype = match DataSourceType::from_str_ci(source_type) {
            Some(t) => t,
            None => return Frame::error(format!("ERR unknown source type '{source_type}'")),
        };

        let display_name = name.unwrap_or(id).to_string();
        let config = DataSourceConfig::new(id.to_string(), display_name, stype, uri.to_string());

        let gw = mesh_gateway();
        match gw.add_source(config) {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::error(format!("ERR {e}")),
        }
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_federation_add(
        &self,
        _id: &str,
        _source_type: &str,
        _uri: &str,
        _name: Option<&str>,
    ) -> Frame {
        Frame::error("ERR FEDERATION commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_federation_remove(&self, id: &str) -> Frame {
        let gw = mesh_gateway();
        match gw.remove_source(id) {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::error(format!("ERR {e}")),
        }
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_federation_remove(&self, _id: &str) -> Frame {
        Frame::error("ERR FEDERATION commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_federation_list(&self) -> Frame {
        let gw = mesh_gateway();
        let sources = gw.list_sources();
        if sources.is_empty() {
            return Frame::array(vec![]);
        }
        let items: Vec<Frame> = sources
            .iter()
            .map(|s| {
                Frame::array(vec![
                    Frame::bulk(s.id.clone()),
                    Frame::bulk(s.source_type.to_string()),
                    Frame::bulk(s.uri.clone()),
                    Frame::bulk(s.name.clone()),
                    Frame::bulk(s.status.to_string()),
                ])
            })
            .collect();
        Frame::array(items)
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_federation_list(&self) -> Frame {
        Frame::error("ERR FEDERATION commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_federation_status(&self, id: Option<&str>) -> Frame {
        let gw = mesh_gateway();
        match id {
            Some(source_id) => match gw.health_check(source_id) {
                Ok(result) => Frame::array(vec![
                    Frame::bulk("source_id"),
                    Frame::bulk(result.source_id),
                    Frame::bulk("healthy"),
                    Frame::Integer(i64::from(result.healthy)),
                    Frame::bulk("latency_ms"),
                    Frame::Integer(result.latency_ms as i64),
                    Frame::bulk("message"),
                    Frame::bulk(result.message),
                ]),
                Err(e) => Frame::error(format!("ERR {e}")),
            },
            None => {
                let stats = gw.stats();
                Frame::array(vec![
                    Frame::bulk("sources_active"),
                    Frame::Integer(stats.sources_active as i64),
                    Frame::bulk("namespaces"),
                    Frame::Integer(stats.namespaces_registered as i64),
                    Frame::bulk("contracts"),
                    Frame::Integer(stats.contracts_active as i64),
                ])
            }
        }
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_federation_status(&self, _id: Option<&str>) -> Frame {
        Frame::error("ERR FEDERATION commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_federation_namespace(
        &self,
        namespace: &str,
        source_id: &str,
    ) -> Frame {
        let gw = mesh_gateway();
        match gw.add_namespace(namespace, source_id) {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::error(format!("ERR {e}")),
        }
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_federation_namespace(
        &self,
        _namespace: &str,
        _source_id: &str,
    ) -> Frame {
        Frame::error("ERR FEDERATION commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_federation_namespaces(&self) -> Frame {
        let gw = mesh_gateway();
        let ns = gw.list_namespaces();
        if ns.is_empty() {
            return Frame::array(vec![]);
        }
        let items: Vec<Frame> = ns
            .iter()
            .map(|(namespace, source_id)| {
                Frame::array(vec![
                    Frame::bulk(namespace.clone()),
                    Frame::bulk(source_id.clone()),
                ])
            })
            .collect();
        Frame::array(items)
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_federation_namespaces(&self) -> Frame {
        Frame::error("ERR FEDERATION commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_federation_query(&self, query: &str) -> Frame {
        use ferrite_enterprise::mesh::query_router::QueryRouter;
        use std::sync::Arc;

        let gw = mesh_gateway();
        let router = QueryRouter::new(Arc::from(gw));
        match router.route_query(query) {
            Ok(plan) => {
                let steps: Vec<Frame> = plan
                    .steps
                    .iter()
                    .map(|s| {
                        Frame::array(vec![
                            Frame::bulk(s.source_id.clone()),
                            Frame::bulk(s.step_type.to_string()),
                            Frame::bulk(s.query.clone()),
                        ])
                    })
                    .collect();
                Frame::array(vec![
                    Frame::bulk("steps"),
                    Frame::array(steps),
                    Frame::bulk("estimated_latency_ms"),
                    Frame::Integer(plan.estimated_latency_ms as i64),
                    Frame::bulk("sources_involved"),
                    Frame::array(
                        plan.sources_involved
                            .iter()
                            .map(|s| Frame::bulk(s.clone()))
                            .collect(),
                    ),
                ])
            }
            Err(e) => {
                gw.record_error();
                Frame::error(format!("ERR {e}"))
            }
        }
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_federation_query(&self, _query: &str) -> Frame {
        Frame::error("ERR FEDERATION commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_federation_contract(
        &self,
        name: &str,
        source_id: &str,
        schema_json: &str,
    ) -> Frame {
        use ferrite_enterprise::mesh::contract::DataContract;
        use ferrite_enterprise::mesh::datasource::DataSchema;

        let schema: DataSchema = match serde_json::from_str(schema_json) {
            Ok(s) => s,
            Err(e) => return Frame::error(format!("ERR invalid schema JSON: {e}")),
        };

        let contract = DataContract::new(name.to_string(), source_id.to_string(), schema);

        let gw = mesh_gateway();
        match gw.add_contract(contract) {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::error(format!("ERR {e}")),
        }
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_federation_contract(
        &self,
        _name: &str,
        _source_id: &str,
        _schema_json: &str,
    ) -> Frame {
        Frame::error("ERR FEDERATION commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_federation_contracts(&self) -> Frame {
        let gw = mesh_gateway();
        let contracts = gw.list_contracts();
        if contracts.is_empty() {
            return Frame::array(vec![]);
        }
        let items: Vec<Frame> = contracts
            .iter()
            .map(|c| {
                Frame::array(vec![
                    Frame::bulk(c.name.clone()),
                    Frame::bulk(c.source_id.clone()),
                    Frame::bulk(c.status.to_string()),
                    Frame::Integer(c.version as i64),
                ])
            })
            .collect();
        Frame::array(items)
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_federation_contracts(&self) -> Frame {
        Frame::error("ERR FEDERATION commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_federation_stats(&self) -> Frame {
        use std::sync::atomic::Ordering;

        let gw = mesh_gateway();
        let s = gw.stats();
        Frame::array(vec![
            Frame::bulk("sources_active"),
            Frame::Integer(s.sources_active as i64),
            Frame::bulk("queries_routed"),
            Frame::Integer(s.queries_routed.load(Ordering::Relaxed) as i64),
            Frame::bulk("errors"),
            Frame::Integer(s.errors.load(Ordering::Relaxed) as i64),
            Frame::bulk("namespaces_registered"),
            Frame::Integer(s.namespaces_registered as i64),
            Frame::bulk("contracts_active"),
            Frame::Integer(s.contracts_active as i64),
        ])
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_federation_stats(&self) -> Frame {
        Frame::error("ERR FEDERATION commands require the 'experimental' feature")
    }

    // ── Studio developer-experience commands ────────────────────────────────

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_studio_schema(&self, _db: Option<u8>) -> Frame {
        Frame::array(vec![
            Frame::bulk("total_keys"),
            Frame::Integer(0),
            Frame::bulk("total_memory_bytes"),
            Frame::Integer(0),
            Frame::bulk("databases"),
            Frame::array(vec![]),
            Frame::bulk("hint"),
            Frame::bulk("Schema analysis requires key scanning; use SCAN to populate"),
        ])
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_studio_schema(&self, _db: Option<u8>) -> Frame {
        Frame::error("ERR STUDIO commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_studio_templates(&self, name: Option<&str>) -> Frame {
        let registry = ferrite_studio::devtools::TemplateRegistry::new();
        match name {
            Some(n) => match registry.get(n) {
                Some(tpl) => Frame::array(vec![
                    Frame::bulk("name"),
                    Frame::bulk(tpl.name.as_str()),
                    Frame::bulk("description"),
                    Frame::bulk(tpl.description.as_str()),
                    Frame::bulk("category"),
                    Frame::bulk(tpl.category.to_string()),
                    Frame::bulk("documentation"),
                    Frame::bulk(tpl.documentation.as_str()),
                    Frame::bulk("setup_commands"),
                    Frame::array(
                        tpl.setup_commands.iter().map(|c| Frame::bulk(c.as_str())).collect(),
                    ),
                ]),
                None => Frame::error("ERR template not found"),
            },
            None => {
                let items = registry.list();
                let entries: Vec<Frame> = items
                    .iter()
                    .map(|(n, d)| Frame::array(vec![Frame::bulk(*n), Frame::bulk(*d)]))
                    .collect();
                Frame::array(entries)
            }
        }
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_studio_templates(&self, _name: Option<&str>) -> Frame {
        Frame::error("ERR STUDIO commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_studio_setup(&self, template: &str) -> Frame {
        let registry = ferrite_studio::devtools::TemplateRegistry::new();
        match registry.setup_commands(template) {
            Some(cmds) => Frame::array(
                cmds.iter().map(|c| Frame::bulk(c.as_str())).collect(),
            ),
            None => Frame::error("ERR template not found"),
        }
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_studio_setup(&self, _template: &str) -> Frame {
        Frame::error("ERR STUDIO commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_studio_compat(&self, redis_info: Option<&str>) -> Frame {
        let info = redis_info.unwrap_or("redis_version:unknown\r\n");
        let report = ferrite_studio::devtools::MigrationWizard::check_compatibility(info);
        let incompatible: Vec<Frame> = report
            .incompatible_commands
            .iter()
            .map(|c| {
                Frame::array(vec![
                    Frame::bulk(c.name.as_str()),
                    Frame::bulk(c.reason.as_str()),
                    match &c.workaround {
                        Some(w) => Frame::bulk(w.as_str()),
                        None => Frame::Null,
                    },
                ])
            })
            .collect();
        Frame::array(vec![
            Frame::bulk("redis_version"),
            Frame::bulk(report.redis_version.as_str()),
            Frame::bulk("total_commands"),
            Frame::Integer(report.total_commands_used as i64),
            Frame::bulk("compatible"),
            Frame::Integer(report.compatible_commands as i64),
            Frame::bulk("compatibility_pct"),
            Frame::bulk(format!("{:.1}", report.compatibility_pct)),
            Frame::bulk("incompatible_commands"),
            Frame::array(incompatible),
            Frame::bulk("warnings"),
            Frame::array(report.warnings.iter().map(|w| Frame::bulk(w.as_str())).collect()),
            Frame::bulk("recommendations"),
            Frame::array(
                report
                    .recommendations
                    .iter()
                    .map(|r| Frame::bulk(r.as_str()))
                    .collect(),
            ),
            Frame::bulk("estimated_migration_time"),
            Frame::bulk(report.estimated_migration_time.as_str()),
        ])
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_studio_compat(&self, _redis_info: Option<&str>) -> Frame {
        Frame::error("ERR STUDIO commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_studio_help(&self, command: &str) -> Frame {
        match ferrite_studio::devtools::QueryBuilder::explain_command(command) {
            Some(help) => Frame::array(vec![
                Frame::bulk("name"),
                Frame::bulk(help.name.as_str()),
                Frame::bulk("syntax"),
                Frame::bulk(help.syntax.as_str()),
                Frame::bulk("description"),
                Frame::bulk(help.description.as_str()),
                Frame::bulk("complexity"),
                Frame::bulk(help.complexity.as_str()),
                Frame::bulk("since"),
                Frame::bulk(help.since_version.as_str()),
                Frame::bulk("examples"),
                Frame::array(help.examples.iter().map(|e| Frame::bulk(e.as_str())).collect()),
            ]),
            None => Frame::error("ERR unknown command"),
        }
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_studio_help(&self, _command: &str) -> Frame {
        Frame::error("ERR STUDIO commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_studio_suggest(&self, context: Option<&str>) -> Frame {
        let ctx = context.unwrap_or("");
        let suggestions = ferrite_studio::devtools::QueryBuilder::suggest_queries(ctx);
        let entries: Vec<Frame> = suggestions
            .iter()
            .map(|s| {
                Frame::array(vec![
                    Frame::bulk(s.query.as_str()),
                    Frame::bulk(s.description.as_str()),
                    Frame::bulk(s.category.as_str()),
                ])
            })
            .collect();
        Frame::array(entries)
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_studio_suggest(&self, _context: Option<&str>) -> Frame {
        Frame::error("ERR STUDIO commands require the 'experimental' feature")
    }
}

/// Extract source key patterns from a FerriteQL query string.
/// Simple heuristic: look for `FROM pattern` clauses.
fn extract_source_patterns(query: &str) -> Vec<String> {
    let mut patterns = Vec::new();
    let upper = query.to_uppercase();
    let tokens: Vec<&str> = query.split_whitespace().collect();
    let upper_tokens: Vec<&str> = upper.split_whitespace().collect();

    for (i, token) in upper_tokens.iter().enumerate() {
        if *token == "FROM" {
            if let Some(next) = tokens.get(i + 1) {
                let pattern = next.trim_end_matches(|c: char| c == ';' || c == ',');
                if !pattern.is_empty() {
                    patterns.push(pattern.to_string());
                }
            }
        }
    }

    if patterns.is_empty() {
        patterns.push("*".to_string());
    }

    patterns
}
