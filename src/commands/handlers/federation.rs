//! FEDERATE.* command handlers
//!
//! FEDERATE SOURCE.ADD, FEDERATE SOURCE.REMOVE, FEDERATE SOURCE.LIST,
//! FEDERATE QUERY, FEDERATE CROSS, FEDERATE EXPLAIN, FEDERATE HEALTH,
//! FEDERATE STATS, FEDERATE HELP
#![allow(dead_code)]

use std::sync::OnceLock;

use bytes::Bytes;

use crate::protocol::Frame;
use ferrite_core::query::federation::{
    DataSource, FederatedQuery, FederationConfig, FederationEngine, QueryOperation, SourceType,
};

use super::err_frame;

/// Global federation engine singleton.
static FEDERATION_ENGINE: OnceLock<FederationEngine> = OnceLock::new();

fn get_engine() -> &'static FederationEngine {
    FEDERATION_ENGINE.get_or_init(|| FederationEngine::new(FederationConfig::default()))
}

/// Dispatch a `FEDERATE` subcommand.
pub fn federation_command(subcommand: &str, args: &[String]) -> Frame {
    match subcommand.to_uppercase().as_str() {
        "SOURCE.ADD" => source_add(args),
        "SOURCE.REMOVE" => source_remove(args),
        "SOURCE.LIST" => source_list(),
        "QUERY" => federate_query(args),
        "CROSS" => federate_cross(args),
        "EXPLAIN" => federate_explain(args),
        "HEALTH" => federate_health(args),
        "STATS" => federate_stats(),
        "HELP" => federate_help(),
        _ => err_frame(&format!("unknown FEDERATE subcommand '{}'", subcommand)),
    }
}

/// Parse a source type string into a SourceType.
fn parse_source_type(s: &str) -> Option<SourceType> {
    match s.to_lowercase().as_str() {
        "ferrite" => Some(SourceType::Ferrite),
        "redis" => Some(SourceType::Redis),
        "postgresql" | "postgres" | "pg" => Some(SourceType::PostgreSQL),
        "s3" => Some(SourceType::S3),
        "kafka" => Some(SourceType::Kafka),
        "http" => Some(SourceType::Http),
        other => Some(SourceType::Custom(other.to_string())),
    }
}

/// FEDERATE SOURCE.ADD name type connection_string
fn source_add(args: &[String]) -> Frame {
    if args.len() < 3 {
        return err_frame("FEDERATE SOURCE.ADD requires: name type connection_string");
    }

    let name = &args[0];
    let source_type = match parse_source_type(&args[1]) {
        Some(t) => t,
        None => return err_frame(&format!("unknown source type: {}", args[1])),
    };
    let connection_string = &args[2];

    let source = DataSource {
        name: name.clone(),
        source_type,
        connection_string: connection_string.clone(),
        schema: None,
        enabled: true,
        latency_budget_ms: 100,
    };

    match get_engine().register_source(source) {
        Ok(()) => Frame::Simple(Bytes::from("OK")),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// FEDERATE SOURCE.REMOVE name
fn source_remove(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("FEDERATE SOURCE.REMOVE requires: name");
    }

    match get_engine().remove_source(&args[0]) {
        Ok(()) => Frame::Simple(Bytes::from("OK")),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// FEDERATE SOURCE.LIST
fn source_list() -> Frame {
    let sources = get_engine().list_sources();
    let frames: Vec<Frame> = sources
        .iter()
        .map(|s| {
            let fields = vec![
                Frame::Bulk(Some(Bytes::from("name"))),
                Frame::Bulk(Some(Bytes::from(s.name.clone()))),
                Frame::Bulk(Some(Bytes::from("type"))),
                Frame::Bulk(Some(Bytes::from(s.source_type.to_string()))),
                Frame::Bulk(Some(Bytes::from("enabled"))),
                Frame::Boolean(s.enabled),
                Frame::Bulk(Some(Bytes::from("latency_ms"))),
                Frame::Integer(s.latency_ms as i64),
                Frame::Bulk(Some(Bytes::from("queries_executed"))),
                Frame::Integer(s.queries_executed as i64),
            ];
            Frame::Array(Some(fields))
        })
        .collect();
    Frame::Array(Some(frames))
}

/// FEDERATE QUERY source query_string
fn federate_query(args: &[String]) -> Frame {
    if args.len() < 2 {
        return err_frame("FEDERATE QUERY requires: source query_string");
    }

    let source = &args[0];
    let query_string = args[1..].join(" ");

    let query = FederatedQuery {
        source: source.clone(),
        operation: QueryOperation::Query { sql: query_string },
        filters: vec![],
        projections: vec![],
        limit: None,
        cache_key: None,
    };

    match get_engine().execute(query) {
        Ok(result) => {
            let mut map = std::collections::HashMap::new();
            map.insert(
                Bytes::from("source"),
                Frame::Bulk(Some(Bytes::from(result.source))),
            );
            map.insert(
                Bytes::from("total_rows"),
                Frame::Integer(result.total_rows as i64),
            );
            map.insert(Bytes::from("cached"), Frame::Boolean(result.cached));
            map.insert(
                Bytes::from("execution_ms"),
                Frame::Integer(result.execution_ms as i64),
            );
            let row_frames: Vec<Frame> = result
                .rows
                .iter()
                .map(|r| Frame::Bulk(Some(Bytes::from(r.to_string()))))
                .collect();
            map.insert(Bytes::from("rows"), Frame::Array(Some(row_frames)));
            Frame::Map(map)
        }
        Err(e) => err_frame(&e.to_string()),
    }
}

/// FEDERATE CROSS source1 query1 JOIN source2 query2 ON key
fn federate_cross(args: &[String]) -> Frame {
    // Minimal parsing: source1 query1 JOIN source2 query2 ON key
    if args.len() < 7 {
        return err_frame(
            "FEDERATE CROSS requires: source1 query1 JOIN source2 query2 ON join_key",
        );
    }

    let source1 = &args[0];
    let query1 = &args[1];

    // Find JOIN keyword
    let join_idx = args.iter().position(|a| a.to_uppercase() == "JOIN");
    let on_idx = args.iter().position(|a| a.to_uppercase() == "ON");

    let (join_idx, on_idx) = match (join_idx, on_idx) {
        (Some(j), Some(o)) => (j, o),
        _ => return err_frame("FEDERATE CROSS requires JOIN ... ON ... syntax"),
    };

    if on_idx + 1 >= args.len() || join_idx + 2 > on_idx {
        return err_frame("invalid FEDERATE CROSS syntax");
    }

    let source2 = &args[join_idx + 1];
    let query2 = args[join_idx + 2..on_idx].join(" ");
    let join_key = &args[on_idx + 1];

    let queries = vec![
        FederatedQuery {
            source: source1.clone(),
            operation: QueryOperation::Query {
                sql: query1.clone(),
            },
            filters: vec![],
            projections: vec![],
            limit: None,
            cache_key: None,
        },
        FederatedQuery {
            source: source2.clone(),
            operation: QueryOperation::Query { sql: query2 },
            filters: vec![],
            projections: vec![],
            limit: None,
            cache_key: None,
        },
    ];

    match get_engine().execute_cross_source(queries, join_key) {
        Ok(result) => {
            let mut map = std::collections::HashMap::new();
            map.insert(
                Bytes::from("source"),
                Frame::Bulk(Some(Bytes::from(result.source))),
            );
            map.insert(
                Bytes::from("total_rows"),
                Frame::Integer(result.total_rows as i64),
            );
            map.insert(
                Bytes::from("execution_ms"),
                Frame::Integer(result.execution_ms as i64),
            );
            Frame::Map(map)
        }
        Err(e) => err_frame(&e.to_string()),
    }
}

/// FEDERATE EXPLAIN query
fn federate_explain(args: &[String]) -> Frame {
    if args.len() < 2 {
        return err_frame("FEDERATE EXPLAIN requires: source query_string");
    }

    let source = &args[0];
    let query_string = args[1..].join(" ");

    let query = FederatedQuery {
        source: source.clone(),
        operation: QueryOperation::Query { sql: query_string },
        filters: vec![],
        projections: vec![],
        limit: None,
        cache_key: None,
    };

    let plan = get_engine().explain(&query);
    let steps: Vec<Frame> = plan
        .steps
        .iter()
        .map(|step| {
            let fields = vec![
                Frame::Bulk(Some(Bytes::from("operation"))),
                Frame::Bulk(Some(Bytes::from(step.operation.clone()))),
                Frame::Bulk(Some(Bytes::from("source"))),
                Frame::Bulk(Some(Bytes::from(step.source.clone()))),
                Frame::Bulk(Some(Bytes::from("estimated_rows"))),
                Frame::Integer(step.estimated_rows as i64),
                Frame::Bulk(Some(Bytes::from("cost"))),
                Frame::Double(step.cost),
            ];
            Frame::Array(Some(fields))
        })
        .collect();

    let mut map = std::collections::HashMap::new();
    map.insert(Bytes::from("steps"), Frame::Array(Some(steps)));
    map.insert(
        Bytes::from("estimated_cost"),
        Frame::Double(plan.estimated_cost),
    );
    let source_frames: Vec<Frame> = plan
        .sources_involved
        .iter()
        .map(|s| Frame::Bulk(Some(Bytes::from(s.clone()))))
        .collect();
    map.insert(
        Bytes::from("sources_involved"),
        Frame::Array(Some(source_frames)),
    );
    Frame::Map(map)
}

/// FEDERATE HEALTH [source]
fn federate_health(args: &[String]) -> Frame {
    if args.is_empty() {
        // List health for all sources
        let sources = get_engine().list_sources();
        let frames: Vec<Frame> = sources
            .iter()
            .filter_map(|s| {
                get_engine().source_health(&s.name).map(|h| {
                    let fields = vec![
                        Frame::Bulk(Some(Bytes::from("name"))),
                        Frame::Bulk(Some(Bytes::from(h.name))),
                        Frame::Bulk(Some(Bytes::from("reachable"))),
                        Frame::Boolean(h.reachable),
                        Frame::Bulk(Some(Bytes::from("latency_ms"))),
                        Frame::Integer(h.latency_ms as i64),
                    ];
                    Frame::Array(Some(fields))
                })
            })
            .collect();
        return Frame::Array(Some(frames));
    }

    match get_engine().source_health(&args[0]) {
        Some(h) => {
            let mut map = std::collections::HashMap::new();
            map.insert(Bytes::from("name"), Frame::Bulk(Some(Bytes::from(h.name))));
            map.insert(Bytes::from("reachable"), Frame::Boolean(h.reachable));
            map.insert(
                Bytes::from("latency_ms"),
                Frame::Integer(h.latency_ms as i64),
            );
            Frame::Map(map)
        }
        None => err_frame(&format!("source '{}' not found", args[0])),
    }
}

/// FEDERATE STATS
fn federate_stats() -> Frame {
    let stats = get_engine().stats();
    let mut map = std::collections::HashMap::new();
    map.insert(
        Bytes::from("total_queries"),
        Frame::Integer(stats.total_queries as i64),
    );
    map.insert(
        Bytes::from("cross_source_queries"),
        Frame::Integer(stats.cross_source_queries as i64),
    );
    map.insert(
        Bytes::from("cache_hits"),
        Frame::Integer(stats.cache_hits as i64),
    );
    map.insert(
        Bytes::from("cache_misses"),
        Frame::Integer(stats.cache_misses as i64),
    );
    map.insert(
        Bytes::from("avg_latency_ms"),
        Frame::Double(stats.avg_latency_ms),
    );
    map.insert(
        Bytes::from("sources_registered"),
        Frame::Integer(stats.sources_registered as i64),
    );
    Frame::Map(map)
}

/// FEDERATE HELP
fn federate_help() -> Frame {
    let help = vec![
        "FEDERATE SOURCE.ADD name type connection_string - Register a data source",
        "FEDERATE SOURCE.REMOVE name - Remove a data source",
        "FEDERATE SOURCE.LIST - List all registered sources",
        "FEDERATE QUERY source query_string - Query a specific source",
        "FEDERATE CROSS source1 q1 JOIN source2 q2 ON key - Cross-source query",
        "FEDERATE EXPLAIN source query_string - Show execution plan",
        "FEDERATE HEALTH [source] - Check source health",
        "FEDERATE STATS - Federation engine statistics",
        "FEDERATE HELP - This help text",
    ];

    Frame::Array(Some(
        help.into_iter()
            .map(|s| Frame::Bulk(Some(Bytes::from(s.to_string()))))
            .collect(),
    ))
}
