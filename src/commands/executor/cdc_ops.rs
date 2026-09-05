//! CDC (Change Data Capture) command implementations on CommandExecutor.

use bytes::Bytes;

use crate::protocol::Frame;

use super::CommandExecutor;

impl CommandExecutor {
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
}
