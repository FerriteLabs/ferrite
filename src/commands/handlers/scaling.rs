//! SCALING.* command handlers
//!
//! SCALING.PREDICT, SCALING.HISTORY, SCALING.PATTERN, SCALING.SAMPLE,
//! SCALING.CONFIG, SCALING.STATS, SCALING.HELP
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;

use bytes::Bytes;

use crate::protocol::Frame;
use ferrite_cloud::managed::predictive::{
    PredictiveConfig, PredictiveScaler, TrafficPattern, TrafficSample,
};

use super::err_frame;

/// Global predictive scaler singleton.
static PREDICTIVE_SCALER: OnceLock<PredictiveScaler> = OnceLock::new();

fn get_scaler() -> &'static PredictiveScaler {
    PREDICTIVE_SCALER.get_or_init(|| PredictiveScaler::new(PredictiveConfig::default()))
}

/// Dispatch a `SCALING` subcommand.
pub fn scaling_command(subcommand: &str, args: &[String]) -> Frame {
    match subcommand.to_uppercase().as_str() {
        "PREDICT" => handle_predict(args),
        "HISTORY" => handle_history(),
        "PATTERN" => handle_pattern(),
        "SAMPLE" => handle_sample(args),
        "CONFIG" => handle_config(args),
        "STATS" => handle_stats(),
        "HELP" | "" => handle_help(),
        other => err_frame(&format!("unknown SCALING subcommand '{}'", other)),
    }
}

/// SCALING.PREDICT [horizon_secs] — Forecast traffic and recommend replica count.
fn handle_predict(args: &[String]) -> Frame {
    let horizon_secs: u64 = args
        .first()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1800);

    let result = get_scaler().predict(Duration::from_secs(horizon_secs));
    let mut map = HashMap::new();
    map.insert(
        Bytes::from_static(b"predicted_ops"),
        Frame::Double(result.predicted_ops),
    );
    map.insert(
        Bytes::from_static(b"predicted_memory_pct"),
        Frame::Double(result.predicted_memory_pct),
    );
    map.insert(
        Bytes::from_static(b"confidence"),
        Frame::Double(result.confidence),
    );
    map.insert(
        Bytes::from_static(b"recommended_replicas"),
        Frame::Integer(result.recommended_replicas as i64),
    );
    map.insert(
        Bytes::from_static(b"lead_time_secs"),
        Frame::Integer(result.lead_time_secs as i64),
    );
    map.insert(
        Bytes::from_static(b"reason"),
        Frame::Bulk(Some(Bytes::from(result.reason))),
    );
    Frame::Map(map)
}

/// SCALING.HISTORY — Return past scaling events.
fn handle_history() -> Frame {
    let events = get_scaler().scaling_history();
    let frames: Vec<Frame> = events
        .iter()
        .map(|e| {
            let mut entry = HashMap::new();
            entry.insert(
                Bytes::from_static(b"from_replicas"),
                Frame::Integer(e.from_replicas as i64),
            );
            entry.insert(
                Bytes::from_static(b"to_replicas"),
                Frame::Integer(e.to_replicas as i64),
            );
            entry.insert(
                Bytes::from_static(b"trigger"),
                Frame::Bulk(Some(Bytes::from(format!("{:?}", e.trigger)))),
            );
            entry.insert(
                Bytes::from_static(b"outcome"),
                Frame::Bulk(Some(Bytes::from(e.outcome.clone()))),
            );
            Frame::Map(entry)
        })
        .collect();
    Frame::Array(Some(frames))
}

/// SCALING.PATTERN — Detect traffic pattern from collected samples.
fn handle_pattern() -> Frame {
    match get_scaler().detect_pattern() {
        Some(TrafficPattern::DailyCycle {
            peak_hour,
            trough_hour,
        }) => {
            let mut map = HashMap::new();
            map.insert(
                Bytes::from_static(b"pattern"),
                Frame::Bulk(Some(Bytes::from("DailyCycle"))),
            );
            map.insert(
                Bytes::from_static(b"peak_hour"),
                Frame::Integer(peak_hour as i64),
            );
            map.insert(
                Bytes::from_static(b"trough_hour"),
                Frame::Integer(trough_hour as i64),
            );
            Frame::Map(map)
        }
        Some(TrafficPattern::WeeklyCycle { peak_day }) => {
            let mut map = HashMap::new();
            map.insert(
                Bytes::from_static(b"pattern"),
                Frame::Bulk(Some(Bytes::from("WeeklyCycle"))),
            );
            map.insert(
                Bytes::from_static(b"peak_day"),
                Frame::Integer(peak_day as i64),
            );
            Frame::Map(map)
        }
        Some(TrafficPattern::Steady) => {
            let mut map = HashMap::new();
            map.insert(
                Bytes::from_static(b"pattern"),
                Frame::Bulk(Some(Bytes::from("Steady"))),
            );
            Frame::Map(map)
        }
        Some(TrafficPattern::Unpredictable) | None => {
            let mut map = HashMap::new();
            map.insert(
                Bytes::from_static(b"pattern"),
                Frame::Bulk(Some(Bytes::from("Unpredictable"))),
            );
            Frame::Map(map)
        }
    }
}

/// SCALING.SAMPLE ops_per_sec memory_pct cpu_pct latency_p99_us connections [timestamp]
fn handle_sample(args: &[String]) -> Frame {
    if args.len() < 5 {
        return err_frame(
            "SCALING.SAMPLE requires: ops_per_sec memory_pct cpu_pct latency_p99_us connections [timestamp]",
        );
    }

    let ops_per_sec: f64 = match args[0].parse() {
        Ok(v) => v,
        Err(_) => return err_frame("ops_per_sec must be a number"),
    };
    let memory_pct: f64 = match args[1].parse() {
        Ok(v) => v,
        Err(_) => return err_frame("memory_pct must be a number"),
    };
    let cpu_pct: f64 = match args[2].parse() {
        Ok(v) => v,
        Err(_) => return err_frame("cpu_pct must be a number"),
    };
    let latency_p99_us: f64 = match args[3].parse() {
        Ok(v) => v,
        Err(_) => return err_frame("latency_p99_us must be a number"),
    };
    let connections: u32 = match args[4].parse() {
        Ok(v) => v,
        Err(_) => return err_frame("connections must be a positive integer"),
    };
    let timestamp: u64 = if args.len() > 5 {
        match args[5].parse() {
            Ok(v) => v,
            Err(_) => return err_frame("timestamp must be a positive integer"),
        }
    } else {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    };

    let sample = TrafficSample {
        timestamp,
        ops_per_sec,
        memory_pct,
        cpu_pct,
        latency_p99_us,
        connections,
    };
    get_scaler().record_sample(sample);
    Frame::Simple(Bytes::from("OK"))
}

/// SCALING.CONFIG — Show current predictive scaling configuration.
fn handle_config(args: &[String]) -> Frame {
    if !args.is_empty() {
        return err_frame("SCALING.CONFIG SET is not yet supported; read-only for now");
    }
    let mut map = HashMap::new();
    map.insert(
        Bytes::from_static(b"enabled"),
        Frame::Bulk(Some(Bytes::from("true"))),
    );
    map.insert(
        Bytes::from_static(b"lookback_window_secs"),
        Frame::Integer(7 * 24 * 3600),
    );
    map.insert(
        Bytes::from_static(b"prediction_horizon_secs"),
        Frame::Integer(1800),
    );
    map.insert(
        Bytes::from_static(b"confidence_threshold"),
        Frame::Double(0.8),
    );
    map.insert(
        Bytes::from_static(b"min_scale_interval_secs"),
        Frame::Integer(300),
    );
    map.insert(
        Bytes::from_static(b"max_scale_factor"),
        Frame::Double(3.0),
    );
    Frame::Map(map)
}

/// SCALING.STATS — Return predictive scaler statistics.
fn handle_stats() -> Frame {
    let stats = get_scaler().stats();
    let mut map = HashMap::new();
    map.insert(
        Bytes::from_static(b"samples_collected"),
        Frame::Integer(stats.samples_collected as i64),
    );
    map.insert(
        Bytes::from_static(b"predictions_made"),
        Frame::Integer(stats.predictions_made as i64),
    );
    map.insert(
        Bytes::from_static(b"accurate_predictions"),
        Frame::Integer(stats.accurate_predictions as i64),
    );
    map.insert(
        Bytes::from_static(b"accuracy_pct"),
        Frame::Double(stats.accuracy_pct),
    );
    map.insert(
        Bytes::from_static(b"pattern"),
        Frame::Bulk(Some(Bytes::from(
            stats
                .pattern
                .map(|p| format!("{:?}", p))
                .unwrap_or_else(|| "none".to_string()),
        ))),
    );
    Frame::Map(map)
}

/// SCALING.HELP — Return usage information.
fn handle_help() -> Frame {
    let lines = vec![
        "SCALING.PREDICT [horizon_secs]     - Forecast traffic and recommend replicas",
        "SCALING.HISTORY                     - Show past scaling events",
        "SCALING.PATTERN                     - Detect traffic pattern",
        "SCALING.SAMPLE ops mem cpu lat conn - Record a traffic sample",
        "SCALING.CONFIG                      - Show scaling configuration",
        "SCALING.STATS                       - Show scaler statistics",
        "SCALING.HELP                        - Show this help",
    ];
    let frames: Vec<Frame> = lines
        .into_iter()
        .map(|l| Frame::Bulk(Some(Bytes::from(l))))
        .collect();
    Frame::Array(Some(frames))
}
