//! OPTIMIZER.* command handlers
//!
//! OPTIMIZER.ANALYZE, OPTIMIZER.RECOMMEND, OPTIMIZER.WARMING, OPTIMIZER.TUNE,
//! OPTIMIZER.PROFILE, OPTIMIZER.CONFIG, OPTIMIZER.STATS, OPTIMIZER.HELP
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::OnceLock;

use bytes::Bytes;

use crate::protocol::Frame;
use ferrite_core::optimizer::adaptive::{AdaptiveQueryOptimizer, OptimizerConfig};

use super::err_frame;

/// Global adaptive query optimizer singleton.
static ADAPTIVE_OPTIMIZER: OnceLock<AdaptiveQueryOptimizer> = OnceLock::new();

fn get_optimizer() -> &'static AdaptiveQueryOptimizer {
    ADAPTIVE_OPTIMIZER.get_or_init(|| AdaptiveQueryOptimizer::new(OptimizerConfig::default()))
}

/// Dispatch an `OPTIMIZER` subcommand.
pub fn optimizer_command(subcommand: &str, args: &[String]) -> Frame {
    match subcommand.to_uppercase().as_str() {
        "ANALYZE" => handle_analyze(),
        "RECOMMEND" => handle_recommend(),
        "WARMING" => handle_warming(),
        "TUNE" => handle_tune(),
        "PROFILE" => handle_profile(args),
        "CONFIG" => handle_config(args),
        "STATS" => handle_stats(),
        "HELP" | "" => handle_help(),
        other => err_frame(&format!("unknown OPTIMIZER subcommand '{}'", other)),
    }
}

/// OPTIMIZER.ANALYZE — Run workload analysis and return the profile.
fn handle_analyze() -> Frame {
    let profile = get_optimizer().analyze_workload();
    let mut map = HashMap::new();

    map.insert(
        Bytes::from_static(b"total_ops"),
        Frame::Integer(profile.total_ops as i64),
    );
    map.insert(
        Bytes::from_static(b"read_write_ratio"),
        Frame::Double(profile.read_write_ratio),
    );
    map.insert(
        Bytes::from_static(b"patterns"),
        Frame::Integer(profile.query_patterns.len() as i64),
    );

    let hot: Vec<Frame> = profile
        .hot_keys
        .iter()
        .take(10)
        .map(|(k, c)| {
            let mut entry = HashMap::new();
            entry.insert(
                Bytes::from_static(b"key"),
                Frame::Bulk(Some(Bytes::from(k.clone()))),
            );
            entry.insert(Bytes::from_static(b"count"), Frame::Integer(*c as i64));
            Frame::Map(entry)
        })
        .collect();
    map.insert(Bytes::from_static(b"hot_keys"), Frame::Array(Some(hot)));

    Frame::Map(map)
}

/// OPTIMIZER.RECOMMEND — Get index recommendations.
fn handle_recommend() -> Frame {
    let recs = get_optimizer().recommend_indexes();
    let frames: Vec<Frame> = recs
        .iter()
        .map(|r| {
            let mut entry = HashMap::new();
            entry.insert(
                Bytes::from_static(b"key_pattern"),
                Frame::Bulk(Some(Bytes::from(r.key_pattern.clone()))),
            );
            entry.insert(
                Bytes::from_static(b"index_type"),
                Frame::Bulk(Some(Bytes::from(format!("{:?}", r.index_type)))),
            );
            entry.insert(
                Bytes::from_static(b"estimated_speedup"),
                Frame::Double(r.estimated_speedup),
            );
            entry.insert(
                Bytes::from_static(b"memory_cost_bytes"),
                Frame::Integer(r.memory_cost_bytes as i64),
            );
            entry.insert(
                Bytes::from_static(b"confidence"),
                Frame::Double(r.confidence),
            );
            Frame::Map(entry)
        })
        .collect();
    Frame::Array(Some(frames))
}

/// OPTIMIZER.WARMING — Generate a cache warming plan.
fn handle_warming() -> Frame {
    let plan = get_optimizer().generate_warming_plan();
    let mut map = HashMap::new();

    let keys: Vec<Frame> = plan
        .keys
        .iter()
        .map(|k| Frame::Bulk(Some(Bytes::from(k.clone()))))
        .collect();
    map.insert(Bytes::from_static(b"keys"), Frame::Array(Some(keys)));
    map.insert(
        Bytes::from_static(b"priority"),
        Frame::Double(plan.priority),
    );
    map.insert(
        Bytes::from_static(b"predicted_access_time"),
        Frame::Integer(plan.predicted_access_time as i64),
    );
    map.insert(
        Bytes::from_static(b"source_tier"),
        Frame::Bulk(Some(Bytes::from(plan.source_tier))),
    );

    Frame::Map(map)
}

/// OPTIMIZER.TUNE — Run automatic tuning and return actions.
fn handle_tune() -> Frame {
    let actions = get_optimizer().auto_tune();
    let frames: Vec<Frame> = actions
        .iter()
        .map(|a| {
            let mut entry = HashMap::new();
            entry.insert(
                Bytes::from_static(b"description"),
                Frame::Bulk(Some(Bytes::from(a.description.clone()))),
            );
            entry.insert(
                Bytes::from_static(b"parameter"),
                Frame::Bulk(Some(Bytes::from(a.parameter.clone()))),
            );
            entry.insert(
                Bytes::from_static(b"old_value"),
                Frame::Bulk(Some(Bytes::from(a.old_value.clone()))),
            );
            entry.insert(
                Bytes::from_static(b"new_value"),
                Frame::Bulk(Some(Bytes::from(a.new_value.clone()))),
            );
            entry.insert(
                Bytes::from_static(b"impact"),
                Frame::Bulk(Some(Bytes::from(a.impact.clone()))),
            );
            Frame::Map(entry)
        })
        .collect();
    Frame::Array(Some(frames))
}

/// OPTIMIZER.PROFILE command key latency_us — Record a query observation.
fn handle_profile(args: &[String]) -> Frame {
    if args.len() < 3 {
        return err_frame("OPTIMIZER.PROFILE requires: command key latency_us");
    }
    let command = &args[0];
    let key = &args[1];
    let latency_us: u64 = match args[2].parse() {
        Ok(v) => v,
        Err(_) => return err_frame("latency_us must be a positive integer"),
    };
    get_optimizer().record_query(command, key, latency_us);
    Frame::Simple(Bytes::from("OK"))
}

/// OPTIMIZER.CONFIG [key value] — Show or update configuration.
fn handle_config(args: &[String]) -> Frame {
    if args.is_empty() {
        let mut map = HashMap::new();
        map.insert(
            Bytes::from_static(b"enabled"),
            Frame::Bulk(Some(Bytes::from("true"))),
        );
        map.insert(
            Bytes::from_static(b"max_recommendations"),
            Frame::Integer(10),
        );
        map.insert(Bytes::from_static(b"min_confidence"), Frame::Double(0.7));
        map.insert(
            Bytes::from_static(b"auto_apply"),
            Frame::Bulk(Some(Bytes::from("false"))),
        );
        return Frame::Map(map);
    }
    err_frame("OPTIMIZER.CONFIG SET is not yet supported; read-only for now")
}

/// OPTIMIZER.STATS — Return optimizer runtime statistics.
fn handle_stats() -> Frame {
    let stats = get_optimizer().stats();
    let mut map = HashMap::new();
    map.insert(
        Bytes::from_static(b"queries_analyzed"),
        Frame::Integer(stats.queries_analyzed as i64),
    );
    map.insert(
        Bytes::from_static(b"recommendations_made"),
        Frame::Integer(stats.recommendations_made as i64),
    );
    map.insert(
        Bytes::from_static(b"auto_tunes_applied"),
        Frame::Integer(stats.auto_tunes_applied as i64),
    );
    map.insert(
        Bytes::from_static(b"last_analysis"),
        Frame::Bulk(Some(Bytes::from(
            stats
                .last_analysis
                .map(|i| format!("{:.1}s ago", i.elapsed().as_secs_f64()))
                .unwrap_or_else(|| "never".to_string()),
        ))),
    );
    Frame::Map(map)
}

/// OPTIMIZER.HELP — Return usage information.
fn handle_help() -> Frame {
    let lines = vec![
        "OPTIMIZER.ANALYZE           - Analyze workload and return profile",
        "OPTIMIZER.RECOMMEND         - Get index recommendations",
        "OPTIMIZER.WARMING           - Generate cache warming plan",
        "OPTIMIZER.TUNE              - Run automatic tuning",
        "OPTIMIZER.PROFILE cmd k us  - Record a query observation",
        "OPTIMIZER.CONFIG            - Show optimizer configuration",
        "OPTIMIZER.STATS             - Show runtime statistics",
        "OPTIMIZER.HELP              - Show this help",
    ];
    let frames: Vec<Frame> = lines
        .into_iter()
        .map(|l| Frame::Bulk(Some(Bytes::from(l))))
        .collect();
    Frame::Array(Some(frames))
}
