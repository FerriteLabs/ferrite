//! GIDX.* (Global Index) command handlers
//!
//! GIDX.CREATE, GIDX.DROP, GIDX.SEARCH, GIDX.SEARCH.VECTOR,
//! GIDX.INDEX, GIDX.DELETE, GIDX.STATUS, GIDX.LIST, GIDX.STATS, GIDX.HELP
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::OnceLock;

use crate::protocol::Frame;
use ferrite_search::global_index::{
    ClusterRef, ClusterRole, GlobalIndexConfig, GlobalIndexDefinition, GlobalIndexManager,
    GlobalIndexType, IndexField, IndexFieldType,
};

use super::{err_frame, ok_frame};

/// Global index manager singleton.
static INDEX_MANAGER: OnceLock<GlobalIndexManager> = OnceLock::new();

fn get_manager() -> &'static GlobalIndexManager {
    INDEX_MANAGER.get_or_init(|| GlobalIndexManager::new(GlobalIndexConfig::default()))
}

/// Dispatch a `GIDX` subcommand.
pub fn global_index_command(subcommand: &str, args: &[String]) -> Frame {
    match subcommand.to_uppercase().as_str() {
        "CREATE" => handle_create(args),
        "DROP" => handle_drop(args),
        "SEARCH" => handle_search(args),
        "SEARCH.VECTOR" => handle_search_vector(args),
        "INDEX" => handle_index(args),
        "DELETE" => handle_delete(args),
        "STATUS" => handle_status(args),
        "LIST" => handle_list(),
        "STATS" => handle_stats(),
        "HELP" => handle_help(),
        other => err_frame(&format!("unknown GIDX subcommand: {}", other)),
    }
}

// ---------------------------------------------------------------------------
// Subcommand handlers
// ---------------------------------------------------------------------------

fn handle_create(args: &[String]) -> Frame {
    // GIDX.CREATE <name> ON <pattern> SCHEMA <field> <type> [<field> <type> ...]
    //             [TYPE <index_type>] [CLUSTER <id> <endpoint> <role>]
    if args.len() < 5 {
        return err_frame(
            "Usage: GIDX.CREATE <name> ON <pattern> SCHEMA <field> <type> ...",
        );
    }

    let name = args[0].clone();
    let args_upper: Vec<String> = args.iter().map(|a| a.to_uppercase()).collect();

    // ON <pattern>
    let on_idx = args_upper.iter().position(|a| a == "ON");
    let source_pattern = match on_idx {
        Some(i) if i + 1 < args.len() => args[i + 1].clone(),
        _ => return err_frame("GIDX.CREATE: missing ON <pattern>"),
    };

    // SCHEMA <field> <type> ...
    let schema_idx = args_upper.iter().position(|a| a == "SCHEMA");
    let fields = match schema_idx {
        Some(si) => parse_schema_fields(&args[si + 1..], &args_upper[si + 1..]),
        None => return err_frame("GIDX.CREATE: missing SCHEMA <field> <type> ..."),
    };

    if fields.is_empty() {
        return err_frame("GIDX.CREATE: at least one field is required in SCHEMA");
    }

    // TYPE
    let index_type = {
        let type_idx = args_upper.iter().position(|a| a == "TYPE");
        match type_idx {
            Some(i) if i + 1 < args.len() => match args_upper[i + 1].as_str() {
                "FULLTEXT" => GlobalIndexType::FullText,
                "VECTOR" => GlobalIndexType::Vector,
                "SORTED" => GlobalIndexType::Sorted,
                "COMPOSITE" => GlobalIndexType::Composite,
                _ => GlobalIndexType::FullText,
            },
            _ => GlobalIndexType::FullText,
        }
    };

    // CLUSTER
    let clusters = parse_clusters(&args, &args_upper);

    let def = GlobalIndexDefinition {
        name,
        source_pattern,
        fields,
        index_type,
        clusters,
        replication_factor: 1,
        created_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
    };

    match get_manager().create_index(def) {
        Ok(()) => ok_frame(),
        Err(e) => err_frame(&e.to_string()),
    }
}

fn handle_drop(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("Usage: GIDX.DROP <name>");
    }
    match get_manager().drop_index(&args[0]) {
        Ok(()) => ok_frame(),
        Err(e) => err_frame(&e.to_string()),
    }
}

fn handle_search(args: &[String]) -> Frame {
    // GIDX.SEARCH <index> <query> [LIMIT <n>]
    if args.len() < 2 {
        return err_frame("Usage: GIDX.SEARCH <index> <query> [LIMIT <n>]");
    }
    let index = &args[0];
    let query = &args[1];
    let limit = parse_limit_arg(args, 10);

    match get_manager().search(index, query, limit) {
        Ok(result) => {
            let json =
                serde_json::to_string(&result).unwrap_or_else(|_| "{}".to_string());
            Frame::Bulk(Some(bytes::Bytes::from(json)))
        }
        Err(e) => err_frame(&e.to_string()),
    }
}

fn handle_search_vector(args: &[String]) -> Frame {
    // GIDX.SEARCH.VECTOR <index> <vector_json> [TOP <k>]
    if args.len() < 2 {
        return err_frame("Usage: GIDX.SEARCH.VECTOR <index> <vector_json> [TOP <k>]");
    }
    let index = &args[0];
    let vector: Vec<f32> = match serde_json::from_str(&args[1]) {
        Ok(v) => v,
        Err(_) => return err_frame("GIDX.SEARCH.VECTOR: invalid vector JSON"),
    };

    let top_k = {
        let args_upper: Vec<String> = args.iter().map(|a| a.to_uppercase()).collect();
        let top_idx = args_upper.iter().position(|a| a == "TOP");
        match top_idx {
            Some(i) if i + 1 < args.len() => args[i + 1].parse().unwrap_or(10),
            _ => 10,
        }
    };

    match get_manager().search_vector(index, &vector, top_k) {
        Ok(result) => {
            let json =
                serde_json::to_string(&result).unwrap_or_else(|_| "{}".to_string());
            Frame::Bulk(Some(bytes::Bytes::from(json)))
        }
        Err(e) => err_frame(&e.to_string()),
    }
}

fn handle_index(args: &[String]) -> Frame {
    // GIDX.INDEX <index> <doc_id> <field> <value> [<field> <value> ...]
    if args.len() < 4 {
        return err_frame(
            "Usage: GIDX.INDEX <index> <doc_id> <field> <value> [<field> <value> ...]",
        );
    }
    let index = &args[0];
    let doc_id = &args[1];

    let mut fields = HashMap::new();
    let mut i = 2;
    while i + 1 < args.len() {
        let key = args[i].clone();
        let val: serde_json::Value = serde_json::from_str(&args[i + 1])
            .unwrap_or_else(|_| serde_json::Value::String(args[i + 1].clone()));
        fields.insert(key, val);
        i += 2;
    }

    match get_manager().index_document(index, doc_id, fields) {
        Ok(()) => ok_frame(),
        Err(e) => err_frame(&e.to_string()),
    }
}

fn handle_delete(args: &[String]) -> Frame {
    // GIDX.DELETE <index> <doc_id>
    if args.len() < 2 {
        return err_frame("Usage: GIDX.DELETE <index> <doc_id>");
    }
    match get_manager().delete_document(&args[0], &args[1]) {
        Ok(()) => ok_frame(),
        Err(e) => err_frame(&e.to_string()),
    }
}

fn handle_status(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("Usage: GIDX.STATUS <index>");
    }
    match get_manager().replication_status(&args[0]) {
        Some(status) => {
            let json =
                serde_json::to_string(&status).unwrap_or_else(|_| "{}".to_string());
            Frame::Bulk(Some(bytes::Bytes::from(json)))
        }
        None => err_frame(&format!("index not found: {}", args[0])),
    }
}

fn handle_list() -> Frame {
    let indexes = get_manager().list_indexes();
    let json =
        serde_json::to_string(&indexes).unwrap_or_else(|_| "[]".to_string());
    Frame::Bulk(Some(bytes::Bytes::from(json)))
}

fn handle_stats() -> Frame {
    let stats = get_manager().stats();
    let json =
        serde_json::to_string(&stats).unwrap_or_else(|_| "{}".to_string());
    Frame::Bulk(Some(bytes::Bytes::from(json)))
}

fn handle_help() -> Frame {
    let help = vec![
        "GIDX.CREATE <name> ON <pattern> SCHEMA <field> <type> ... [TYPE <index_type>] [CLUSTER <id> <endpoint> <role>]",
        "GIDX.DROP <name>",
        "GIDX.SEARCH <index> <query> [LIMIT <n>]",
        "GIDX.SEARCH.VECTOR <index> <vector_json> [TOP <k>]",
        "GIDX.INDEX <index> <doc_id> <field> <value> [<field> <value> ...]",
        "GIDX.DELETE <index> <doc_id>",
        "GIDX.STATUS <index>",
        "GIDX.LIST",
        "GIDX.STATS",
        "GIDX.HELP",
    ];
    let frames: Vec<Frame> = help
        .into_iter()
        .map(|s| Frame::Bulk(Some(bytes::Bytes::from(s.to_string()))))
        .collect();
    Frame::Array(Some(frames))
}

// ---------------------------------------------------------------------------
// Parsing helpers
// ---------------------------------------------------------------------------

fn parse_schema_fields(args: &[String], args_upper: &[String]) -> Vec<IndexField> {
    let mut fields = Vec::new();
    let stop_keywords = ["TYPE", "CLUSTER", "REPLICATION"];
    let mut i = 0;
    while i + 1 < args.len() {
        if stop_keywords.contains(&args_upper[i].as_str()) {
            break;
        }
        let name = args[i].clone();
        let field_type = match args_upper[i + 1].as_str() {
            "TEXT" => IndexFieldType::Text,
            "NUMERIC" | "NUMBER" => IndexFieldType::Numeric,
            "TAG" => IndexFieldType::Tag,
            "GEO" => IndexFieldType::Geo,
            t if t.starts_with("VECTOR") => {
                // VECTOR(128) or VECTOR 128
                let dim = t
                    .trim_start_matches("VECTOR")
                    .trim_start_matches('(')
                    .trim_end_matches(')')
                    .parse()
                    .unwrap_or(128);
                IndexFieldType::Vector(dim)
            }
            _ => break,
        };
        fields.push(IndexField {
            name,
            field_type,
            weight: 1.0,
        });
        i += 2;
    }
    fields
}

fn parse_clusters(args: &[String], args_upper: &[String]) -> Vec<ClusterRef> {
    let mut clusters = Vec::new();
    let mut i = 0;
    while i < args_upper.len() {
        if args_upper[i] == "CLUSTER" && i + 3 < args.len() {
            let cluster_id = args[i + 1].clone();
            let endpoint = args[i + 2].clone();
            let role = match args_upper[i + 3].as_str() {
                "PRIMARY" => ClusterRole::Primary,
                "REPLICA" => ClusterRole::Replica,
                "READONLY" => ClusterRole::ReadOnly,
                _ => ClusterRole::Replica,
            };
            clusters.push(ClusterRef {
                cluster_id,
                endpoint,
                role,
                lag_ms: 0,
            });
            i += 4;
        } else {
            i += 1;
        }
    }
    clusters
}

fn parse_limit_arg(args: &[String], default: usize) -> usize {
    let args_upper: Vec<String> = args.iter().map(|a| a.to_uppercase()).collect();
    let lim_idx = args_upper.iter().position(|a| a == "LIMIT");
    match lim_idx {
        Some(i) if i + 1 < args.len() => args[i + 1].parse().unwrap_or(default),
        _ => default,
    }
}
