//! FEATURE.* command handlers
//!
//! FEATURE.DEFINE, FEATURE.SET, FEATURE.GET, FEATURE.GET.AT, FEATURE.GET.BATCH,
//! FEATURE.FRESHNESS, FEATURE.DRIFT, FEATURE.LIST, FEATURE.INFO,
//! FEATURE.ENTITY, FEATURE.STATS, FEATURE.HELP
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;

use bytes::Bytes;

use crate::protocol::Frame;
use ferrite_ai::agent_memory::feature_store::{
    FeatureDefinition, FeatureStore, FeatureStoreConfig, FeatureValueType, FreshnessStatus,
};

use super::err_frame;

/// Global feature store singleton.
static FEATURE_STORE: OnceLock<FeatureStore> = OnceLock::new();

fn get_store() -> &'static FeatureStore {
    FEATURE_STORE.get_or_init(|| FeatureStore::new(FeatureStoreConfig::default()))
}

/// Dispatch a `FEATURE` subcommand.
pub fn feature_command(subcommand: &str, args: &[String]) -> Frame {
    match subcommand.to_uppercase().as_str() {
        "DEFINE" => handle_define(args),
        "SET" => handle_set(args),
        "GET" => handle_get(args),
        "GET.AT" => handle_get_at(args),
        "GET.BATCH" => handle_get_batch(args),
        "FRESHNESS" => handle_freshness(args),
        "DRIFT" => handle_drift(args),
        "LIST" => handle_list(),
        "INFO" => handle_info(args),
        "ENTITY" => handle_entity(args),
        "STATS" => handle_stats(),
        "HELP" | "" => handle_help(),
        other => err_frame(&format!("unknown FEATURE subcommand '{}'", other)),
    }
}

/// FEATURE.DEFINE name entity_type value_type source [freshness_sla_secs] [description]
fn handle_define(args: &[String]) -> Frame {
    if args.len() < 4 {
        return err_frame(
            "FEATURE.DEFINE requires: name entity_type value_type source [freshness_sla_secs] [description]",
        );
    }

    let value_type = match args[2].to_uppercase().as_str() {
        "NUMERIC" => FeatureValueType::Numeric,
        "CATEGORICAL" => FeatureValueType::Categorical,
        "BOOLEAN" => FeatureValueType::Boolean,
        "TEXT" => FeatureValueType::Text,
        "TIMESTAMP" => FeatureValueType::Timestamp,
        other if other.starts_with("VECTOR(") && other.ends_with(')') => {
            let inner = &other[7..other.len() - 1];
            match inner.parse::<usize>() {
                Ok(n) => FeatureValueType::Vector(n),
                Err(_) => return err_frame("invalid vector dimension"),
            }
        }
        _ => return err_frame("invalid value_type (NUMERIC|CATEGORICAL|BOOLEAN|TEXT|TIMESTAMP|VECTOR(n))"),
    };

    let freshness_sla_secs: u64 = if args.len() > 4 {
        match args[4].parse() {
            Ok(v) => v,
            Err(_) => return err_frame("freshness_sla_secs must be a positive integer"),
        }
    } else {
        60
    };

    let description = if args.len() > 5 {
        args[5..].join(" ")
    } else {
        String::new()
    };

    let def = FeatureDefinition {
        name: args[0].clone(),
        entity_type: args[1].clone(),
        value_type,
        source: args[3].clone(),
        freshness_sla: Duration::from_secs(freshness_sla_secs),
        description,
        tags: vec![],
        created_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        version: 1,
    };

    match get_store().define_feature(def) {
        Ok(()) => Frame::Simple(Bytes::from("OK")),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// FEATURE.SET entity_id feature_name value_json
fn handle_set(args: &[String]) -> Frame {
    if args.len() < 3 {
        return err_frame("FEATURE.SET requires: entity_id feature_name value_json");
    }

    let value: serde_json::Value = match serde_json::from_str(&args[2]) {
        Ok(v) => v,
        Err(_) => {
            // Try as plain number or string.
            if let Ok(n) = args[2].parse::<f64>() {
                serde_json::json!(n)
            } else {
                serde_json::Value::String(args[2].clone())
            }
        }
    };

    match get_store().set_feature(&args[0], &args[1], value) {
        Ok(()) => Frame::Simple(Bytes::from("OK")),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// FEATURE.GET entity_id feature_name
fn handle_get(args: &[String]) -> Frame {
    if args.len() < 2 {
        return err_frame("FEATURE.GET requires: entity_id feature_name");
    }
    match get_store().get_feature(&args[0], &args[1]) {
        Ok(Some(fv)) => {
            let mut map = HashMap::new();
            map.insert(
                Bytes::from_static(b"value"),
                Frame::Bulk(Some(Bytes::from(fv.value.to_string()))),
            );
            map.insert(
                Bytes::from_static(b"timestamp"),
                Frame::Integer(fv.timestamp as i64),
            );
            map.insert(
                Bytes::from_static(b"version"),
                Frame::Integer(fv.version as i64),
            );
            map.insert(
                Bytes::from_static(b"source"),
                Frame::Bulk(Some(Bytes::from(fv.source))),
            );
            Frame::Map(map)
        }
        Ok(None) => Frame::Null,
        Err(e) => err_frame(&e.to_string()),
    }
}

/// FEATURE.GET.AT entity_id feature_name timestamp
fn handle_get_at(args: &[String]) -> Frame {
    if args.len() < 3 {
        return err_frame("FEATURE.GET.AT requires: entity_id feature_name timestamp");
    }
    let timestamp: u64 = match args[2].parse() {
        Ok(v) => v,
        Err(_) => return err_frame("timestamp must be a positive integer"),
    };
    match get_store().get_feature_at(&args[0], &args[1], timestamp) {
        Ok(Some(fv)) => {
            let mut map = HashMap::new();
            map.insert(
                Bytes::from_static(b"value"),
                Frame::Bulk(Some(Bytes::from(fv.value.to_string()))),
            );
            map.insert(
                Bytes::from_static(b"timestamp"),
                Frame::Integer(fv.timestamp as i64),
            );
            map.insert(
                Bytes::from_static(b"version"),
                Frame::Integer(fv.version as i64),
            );
            map.insert(
                Bytes::from_static(b"source"),
                Frame::Bulk(Some(Bytes::from(fv.source))),
            );
            Frame::Map(map)
        }
        Ok(None) => Frame::Null,
        Err(e) => err_frame(&e.to_string()),
    }
}

/// FEATURE.GET.BATCH entity_id feature1 [feature2 ...]
fn handle_get_batch(args: &[String]) -> Frame {
    if args.len() < 2 {
        return err_frame("FEATURE.GET.BATCH requires: entity_id feature1 [feature2 ...]");
    }
    let entity_id = &args[0];
    let feature_names: Vec<&str> = args[1..].iter().map(|s| s.as_str()).collect();
    let results = get_store().get_features(entity_id, &feature_names);

    let mut map = HashMap::new();
    for (name, maybe_val) in results {
        let frame = match maybe_val {
            Some(fv) => Frame::Bulk(Some(Bytes::from(fv.value.to_string()))),
            None => Frame::Null,
        };
        map.insert(Bytes::from(name), frame);
    }
    Frame::Map(map)
}

/// FEATURE.FRESHNESS entity_id feature_name
fn handle_freshness(args: &[String]) -> Frame {
    if args.len() < 2 {
        return err_frame("FEATURE.FRESHNESS requires: entity_id feature_name");
    }
    let status = get_store().check_freshness(&args[0], &args[1]);
    match status {
        FreshnessStatus::Fresh => {
            let mut map = HashMap::new();
            map.insert(
                Bytes::from_static(b"status"),
                Frame::Bulk(Some(Bytes::from("fresh"))),
            );
            Frame::Map(map)
        }
        FreshnessStatus::Stale(duration) => {
            let mut map = HashMap::new();
            map.insert(
                Bytes::from_static(b"status"),
                Frame::Bulk(Some(Bytes::from("stale"))),
            );
            map.insert(
                Bytes::from_static(b"age_secs"),
                Frame::Integer(duration.as_secs() as i64),
            );
            Frame::Map(map)
        }
        FreshnessStatus::Unknown => {
            let mut map = HashMap::new();
            map.insert(
                Bytes::from_static(b"status"),
                Frame::Bulk(Some(Bytes::from("unknown"))),
            );
            Frame::Map(map)
        }
    }
}

/// FEATURE.DRIFT feature_name
fn handle_drift(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("FEATURE.DRIFT requires: feature_name");
    }
    let report = get_store().detect_drift(&args[0]);
    let mut map = HashMap::new();
    map.insert(
        Bytes::from_static(b"feature"),
        Frame::Bulk(Some(Bytes::from(report.feature))),
    );
    map.insert(
        Bytes::from_static(b"drift_score"),
        Frame::Double(report.drift_score),
    );
    map.insert(
        Bytes::from_static(b"is_drifted"),
        Frame::Integer(if report.is_drifted { 1 } else { 0 }),
    );
    map.insert(
        Bytes::from_static(b"baseline_mean"),
        Frame::Double(report.baseline_mean),
    );
    map.insert(
        Bytes::from_static(b"current_mean"),
        Frame::Double(report.current_mean),
    );
    map.insert(
        Bytes::from_static(b"baseline_stddev"),
        Frame::Double(report.baseline_stddev),
    );
    map.insert(
        Bytes::from_static(b"current_stddev"),
        Frame::Double(report.current_stddev),
    );
    map.insert(
        Bytes::from_static(b"samples"),
        Frame::Integer(report.samples as i64),
    );
    Frame::Map(map)
}

/// FEATURE.LIST
fn handle_list() -> Frame {
    let features = get_store().list_features();
    let frames: Vec<Frame> = features
        .into_iter()
        .map(|def| {
            let mut map = HashMap::new();
            map.insert(
                Bytes::from_static(b"name"),
                Frame::Bulk(Some(Bytes::from(def.name))),
            );
            map.insert(
                Bytes::from_static(b"entity_type"),
                Frame::Bulk(Some(Bytes::from(def.entity_type))),
            );
            map.insert(
                Bytes::from_static(b"value_type"),
                Frame::Bulk(Some(Bytes::from(def.value_type.to_string()))),
            );
            map.insert(
                Bytes::from_static(b"source"),
                Frame::Bulk(Some(Bytes::from(def.source))),
            );
            map.insert(
                Bytes::from_static(b"version"),
                Frame::Integer(def.version as i64),
            );
            Frame::Map(map)
        })
        .collect();
    Frame::Array(Some(frames))
}

/// FEATURE.INFO feature_name
fn handle_info(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("FEATURE.INFO requires: feature_name");
    }
    match get_store().feature_info(&args[0]) {
        Some(info) => {
            let mut map = HashMap::new();
            map.insert(
                Bytes::from_static(b"name"),
                Frame::Bulk(Some(Bytes::from(info.definition.name))),
            );
            map.insert(
                Bytes::from_static(b"entity_type"),
                Frame::Bulk(Some(Bytes::from(info.definition.entity_type))),
            );
            map.insert(
                Bytes::from_static(b"value_type"),
                Frame::Bulk(Some(Bytes::from(info.definition.value_type.to_string()))),
            );
            map.insert(
                Bytes::from_static(b"entity_count"),
                Frame::Integer(info.entity_count as i64),
            );
            map.insert(
                Bytes::from_static(b"latest_update"),
                match info.latest_update {
                    Some(ts) => Frame::Integer(ts as i64),
                    None => Frame::Null,
                },
            );
            map.insert(
                Bytes::from_static(b"avg_freshness_ms"),
                Frame::Double(info.avg_freshness_ms),
            );
            Frame::Map(map)
        }
        None => Frame::Null,
    }
}

/// FEATURE.ENTITY entity_id
fn handle_entity(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("FEATURE.ENTITY requires: entity_id");
    }
    let features = get_store().entity_features(&args[0]);
    if features.is_empty() {
        return Frame::Array(Some(vec![]));
    }
    let mut map = HashMap::new();
    for (name, fv) in features {
        map.insert(
            Bytes::from(name),
            Frame::Bulk(Some(Bytes::from(fv.value.to_string()))),
        );
    }
    Frame::Map(map)
}

/// FEATURE.STATS
fn handle_stats() -> Frame {
    let stats = get_store().stats();
    let mut map = HashMap::new();
    map.insert(
        Bytes::from_static(b"features_defined"),
        Frame::Integer(stats.features_defined as i64),
    );
    map.insert(
        Bytes::from_static(b"entities"),
        Frame::Integer(stats.entities as i64),
    );
    map.insert(
        Bytes::from_static(b"total_values"),
        Frame::Integer(stats.total_values as i64),
    );
    map.insert(
        Bytes::from_static(b"stale_values"),
        Frame::Integer(stats.stale_values as i64),
    );
    map.insert(
        Bytes::from_static(b"drift_alerts"),
        Frame::Integer(stats.drift_alerts as i64),
    );
    Frame::Map(map)
}

/// FEATURE.HELP
fn handle_help() -> Frame {
    let lines = vec![
        "FEATURE.DEFINE name entity_type value_type source [sla] [desc] - Define a feature",
        "FEATURE.SET entity_id feature value                            - Set feature value",
        "FEATURE.GET entity_id feature                                  - Get latest value",
        "FEATURE.GET.AT entity_id feature timestamp                     - Point-in-time get",
        "FEATURE.GET.BATCH entity_id feature1 [feature2 ...]           - Batch get",
        "FEATURE.FRESHNESS entity_id feature                           - Check freshness",
        "FEATURE.DRIFT feature                                         - Detect drift",
        "FEATURE.LIST                                                  - List all features",
        "FEATURE.INFO feature                                          - Feature information",
        "FEATURE.ENTITY entity_id                                      - All entity features",
        "FEATURE.STATS                                                 - Store statistics",
        "FEATURE.HELP                                                  - Show this help",
    ];
    let frames: Vec<Frame> = lines
        .into_iter()
        .map(|l| Frame::Bulk(Some(Bytes::from(l))))
        .collect();
    Frame::Array(Some(frames))
}
