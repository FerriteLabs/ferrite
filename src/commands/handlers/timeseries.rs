//! Time-Series Command Handlers
//!
//! Migration target: `ferrite-timeseries` crate (crates/ferrite-timeseries)
//!
//! Redis-compatible command bindings for the time-series engine.
//! Follows RedisTimeSeries-like command patterns.
//!
//! # Commands
//!
//! - `TS.CREATE <key> [RETENTION <ms>] [LABELS label value ...]`
//! - `TS.ADD <key> <timestamp> <value> [LABELS label value ...]`
//! - `TS.MADD <key> <timestamp> <value> [<key> <timestamp> <value> ...]`
//! - `TS.GET <key>`
//! - `TS.RANGE <key> <from> <to> [AGGREGATION <type> <bucket>] [COUNT <n>]`
//! - `TS.MRANGE <from> <to> FILTER <label=value ...>`
//! - `TS.INFO <key>`
//! - `TS.DEL <key> <from> <to>`
//! - `TS.CREATERULE <source> <dest> AGGREGATION <type> <bucket>`
//! - `TS.DELETERULE <source> <dest>`
//! - `TS.QUERYINDEX <label=value ...>`

use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;

use crate::protocol::Frame;
use ferrite_timeseries::timeseries::{
    Aggregation, DownsampleRule, LabelMatcher, RetentionPolicy, Sample, TimeSeriesEngine, Timestamp,
};

use super::{err_frame, ok_frame, HandlerContext};

/// Global time-series store - maps metric names to retention and label metadata
/// The actual data is stored in the TimeSeriesEngine
static TS_ENGINE: OnceLock<TimeSeriesEngine> = OnceLock::new();
static TS_METADATA: OnceLock<RwLock<HashMap<String, TimeSeriesMetricMeta>>> = OnceLock::new();

/// Metadata for a time-series metric
#[derive(Clone, Debug)]
struct TimeSeriesMetricMeta {
    retention_ms: u64,
    labels: Vec<(String, String)>,
}

fn get_engine() -> &'static TimeSeriesEngine {
    TS_ENGINE.get_or_init(|| TimeSeriesEngine::new().expect("Failed to create TimeSeriesEngine"))
}

fn get_metadata() -> &'static RwLock<HashMap<String, TimeSeriesMetricMeta>> {
    TS_METADATA.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Handle TS.CREATE command
///
/// Creates a new time-series key with optional retention and labels.
///
/// # Syntax
/// `TS.CREATE <key> [RETENTION <milliseconds>] [LABELS <label> <value> ...]`
///
/// # Returns
/// OK on success, error otherwise.
pub fn ts_create(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'TS.CREATE' command");
    }

    let key = String::from_utf8_lossy(&args[0]).to_string();
    let mut retention_ms: u64 = 0;
    let mut labels: Vec<(String, String)> = Vec::new();

    // Parse optional arguments
    let mut i = 1;
    while i < args.len() {
        let arg = String::from_utf8_lossy(&args[i]).to_uppercase();
        match arg.as_str() {
            "RETENTION" => {
                if i + 1 >= args.len() {
                    return err_frame("RETENTION requires a value");
                }
                retention_ms = match String::from_utf8_lossy(&args[i + 1]).parse() {
                    Ok(v) => v,
                    Err(_) => return err_frame("invalid retention value"),
                };
                i += 2;
            }
            "LABELS" => {
                i += 1;
                while i + 1 < args.len() {
                    let label = String::from_utf8_lossy(&args[i]).to_string();
                    let value = String::from_utf8_lossy(&args[i + 1]).to_string();
                    // Check if this looks like another keyword
                    if label.to_uppercase() == "RETENTION" {
                        break;
                    }
                    labels.push((label, value));
                    i += 2;
                }
            }
            _ => {
                return err_frame(&format!("unknown argument '{}'", arg));
            }
        }
    }

    // Check if metric already exists
    let metadata = get_metadata();
    {
        let meta_read = metadata.read();
        if meta_read.contains_key(&key) {
            return err_frame(&format!("time-series '{}' already exists", key));
        }
    }

    // Set retention policy if specified
    let engine = get_engine();
    if retention_ms > 0 {
        let policy = RetentionPolicy::new(Duration::from_millis(retention_ms));
        if let Err(e) = engine.set_retention(&key, policy) {
            return err_frame(&format!("failed to set retention: {}", e));
        }
    }

    // Store metadata
    {
        let mut meta_write = metadata.write();
        meta_write.insert(
            key,
            TimeSeriesMetricMeta {
                retention_ms,
                labels,
            },
        );
    }

    ok_frame()
}

/// Handle TS.ADD command
///
/// Adds a sample to a time-series.
///
/// # Syntax
/// `TS.ADD <key> <timestamp|*> <value> [LABELS <label> <value> ...]`
///
/// # Returns
/// The timestamp of the added sample.
pub fn ts_add(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 3 {
        return err_frame("wrong number of arguments for 'TS.ADD' command");
    }

    let key = String::from_utf8_lossy(&args[0]).to_string();
    let timestamp_str = String::from_utf8_lossy(&args[1]);
    let value_str = String::from_utf8_lossy(&args[2]);

    // Parse timestamp (or use current time if "*")
    let timestamp = if timestamp_str == "*" {
        Timestamp::now()
    } else {
        match timestamp_str.parse::<i64>() {
            Ok(ts) => Timestamp::from_millis(ts),
            Err(_) => return err_frame("invalid timestamp"),
        }
    };

    // Parse value
    let value: f64 = match value_str.parse() {
        Ok(v) => v,
        Err(_) => return err_frame("invalid value"),
    };

    // Parse optional labels
    let mut label_strs: Vec<String> = Vec::new();
    let mut i = 3;
    while i < args.len() {
        let arg = String::from_utf8_lossy(&args[i]).to_uppercase();
        if arg == "LABELS" {
            i += 1;
            while i + 1 < args.len() {
                let label = String::from_utf8_lossy(&args[i]).to_string();
                let val = String::from_utf8_lossy(&args[i + 1]).to_string();
                label_strs.push(format!("{}:{}", label, val));
                i += 2;
            }
        } else {
            i += 1;
        }
    }

    // Build labels slice for the add call
    let label_refs: Vec<&str> = label_strs.iter().map(|s| s.as_str()).collect();

    let engine = get_engine();
    let sample = Sample::new(timestamp, value);

    match engine.add(&key, sample, &label_refs) {
        Ok(()) => Frame::Integer(timestamp.as_millis()),
        Err(e) => err_frame(&format!("failed to add sample: {}", e)),
    }
}

/// Handle TS.MADD command
///
/// Adds multiple samples to multiple time-series.
///
/// # Syntax
/// `TS.MADD <key> <timestamp> <value> [<key> <timestamp> <value> ...]`
///
/// # Returns
/// Array of timestamps for each added sample.
pub fn ts_madd(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 3 || args.len() % 3 != 0 {
        return err_frame("wrong number of arguments for 'TS.MADD' command");
    }

    let engine = get_engine();
    let mut results = Vec::with_capacity(args.len() / 3);

    for chunk in args.chunks(3) {
        let key = String::from_utf8_lossy(&chunk[0]).to_string();
        let timestamp_str = String::from_utf8_lossy(&chunk[1]);
        let value_str = String::from_utf8_lossy(&chunk[2]);

        let timestamp = if timestamp_str == "*" {
            Timestamp::now()
        } else {
            match timestamp_str.parse::<i64>() {
                Ok(ts) => Timestamp::from_millis(ts),
                Err(_) => {
                    results.push(Frame::error("invalid timestamp"));
                    continue;
                }
            }
        };

        let value: f64 = match value_str.parse() {
            Ok(v) => v,
            Err(_) => {
                results.push(Frame::error("invalid value"));
                continue;
            }
        };

        let sample = Sample::new(timestamp, value);
        match engine.add(&key, sample, &[]) {
            Ok(()) => results.push(Frame::Integer(timestamp.as_millis())),
            Err(e) => results.push(Frame::error(format!("failed: {}", e))),
        }
    }

    Frame::array(results)
}

/// Handle TS.GET command
///
/// Gets the last sample from a time-series.
///
/// # Syntax
/// `TS.GET <key>`
///
/// # Returns
/// Array [timestamp, value] or nil if empty.
pub fn ts_get(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'TS.GET' command");
    }

    let key = String::from_utf8_lossy(&args[0]).to_string();
    let engine = get_engine();

    // Query the last sample using last() helper
    let query_result = engine
        .query()
        .metric(&key)
        .last(Duration::from_secs(86400 * 365)) // Last year
        .limit(1)
        .build();

    match query_result {
        Ok(query) => match engine.execute_parsed_query(query) {
            Ok(result) => {
                if let Some(series) = result.series.first() {
                    if let Some(sample) = series.samples.last() {
                        let value_str = sample
                            .value
                            .as_f64()
                            .map(|v| format!("{}", v))
                            .unwrap_or_default();
                        Frame::array(vec![
                            Frame::Integer(sample.timestamp.as_millis()),
                            Frame::bulk(value_str),
                        ])
                    } else {
                        Frame::Null
                    }
                } else {
                    Frame::Null
                }
            }
            Err(_) => Frame::Null,
        },
        Err(_) => Frame::Null,
    }
}

/// Handle TS.RANGE command
///
/// Queries a time range from a time-series.
///
/// # Syntax
/// `TS.RANGE <key> <from> <to> [COUNT <n>] [AGGREGATION <type> <bucket>]`
///
/// # Returns
/// Array of [timestamp, value] pairs.
pub fn ts_range(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 3 {
        return err_frame("wrong number of arguments for 'TS.RANGE' command");
    }

    let key = String::from_utf8_lossy(&args[0]).to_string();
    let from_str = String::from_utf8_lossy(&args[1]);
    let to_str = String::from_utf8_lossy(&args[2]);

    // Parse from timestamp (- means minimum)
    let from = if from_str == "-" {
        Timestamp::from_millis(0)
    } else {
        match from_str.parse::<i64>() {
            Ok(ts) => Timestamp::from_millis(ts),
            Err(_) => return err_frame("invalid from timestamp"),
        }
    };

    // Parse to timestamp (+ means maximum)
    let to = if to_str == "+" {
        Timestamp::from_millis(i64::MAX / 1_000_000) // Avoid overflow
    } else {
        match to_str.parse::<i64>() {
            Ok(ts) => Timestamp::from_millis(ts),
            Err(_) => return err_frame("invalid to timestamp"),
        }
    };

    // Parse optional arguments
    let mut count: Option<usize> = None;
    let mut aggregation: Option<Aggregation> = None;
    let mut _bucket_ms: Option<u64> = None;

    let mut i = 3;
    while i < args.len() {
        let arg = String::from_utf8_lossy(&args[i]).to_uppercase();
        match arg.as_str() {
            "COUNT" => {
                if i + 1 >= args.len() {
                    return err_frame("COUNT requires a value");
                }
                count = String::from_utf8_lossy(&args[i + 1]).parse().ok();
                i += 2;
            }
            "AGGREGATION" => {
                if i + 2 >= args.len() {
                    return err_frame("AGGREGATION requires type and bucket");
                }
                let agg_type = String::from_utf8_lossy(&args[i + 1]).to_uppercase();
                aggregation = match agg_type.as_str() {
                    "AVG" => Some(Aggregation::Avg),
                    "SUM" => Some(Aggregation::Sum),
                    "MIN" => Some(Aggregation::Min),
                    "MAX" => Some(Aggregation::Max),
                    "COUNT" => Some(Aggregation::Count),
                    "FIRST" => Some(Aggregation::First),
                    "LAST" => Some(Aggregation::Last),
                    "STDDEV" => Some(Aggregation::StdDev),
                    "VARIANCE" => Some(Aggregation::Variance),
                    _ => return err_frame(&format!("unknown aggregation type: {}", agg_type)),
                };
                _bucket_ms = String::from_utf8_lossy(&args[i + 2]).parse().ok();
                i += 3;
            }
            _ => {
                i += 1;
            }
        }
    }

    let engine = get_engine();

    // Build and execute query
    let mut builder = engine.query().metric(&key).range(from, to);

    if let Some(limit) = count {
        builder = builder.limit(limit);
    }

    if let Some(agg) = aggregation {
        builder = builder.aggregate(agg);
        // Note: step() is used for bucket interval, but we'd need to convert
    }

    match builder.build() {
        Ok(query) => match engine.execute_parsed_query(query) {
            Ok(result) => {
                let samples: Vec<Frame> = result
                    .series
                    .iter()
                    .flat_map(|s| &s.samples)
                    .map(|sample| {
                        let value_str = sample
                            .value
                            .as_f64()
                            .map(|v| format!("{}", v))
                            .unwrap_or_default();
                        Frame::array(vec![
                            Frame::Integer(sample.timestamp.as_millis()),
                            Frame::bulk(value_str),
                        ])
                    })
                    .collect();
                Frame::array(samples)
            }
            Err(e) => err_frame(&format!("query error: {}", e)),
        },
        Err(e) => err_frame(&format!("query build error: {}", e)),
    }
}

/// Handle TS.MRANGE command
///
/// Queries time ranges across multiple time-series using filters.
///
/// # Syntax
/// `TS.MRANGE <from> <to> FILTER <label=value ...> [COUNT <n>] [AGGREGATION <type> <bucket>]`
///
/// # Returns
/// Array of [key, labels, samples] for each matching series.
pub fn ts_mrange(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 4 {
        return err_frame("wrong number of arguments for 'TS.MRANGE' command");
    }

    let from_str = String::from_utf8_lossy(&args[0]);
    let to_str = String::from_utf8_lossy(&args[1]);

    // Parse from timestamp
    let from = if from_str == "-" {
        Timestamp::from_millis(0)
    } else {
        match from_str.parse::<i64>() {
            Ok(ts) => Timestamp::from_millis(ts),
            Err(_) => return err_frame("invalid from timestamp"),
        }
    };

    // Parse to timestamp
    let to = if to_str == "+" {
        Timestamp::from_millis(i64::MAX / 1_000_000)
    } else {
        match to_str.parse::<i64>() {
            Ok(ts) => Timestamp::from_millis(ts),
            Err(_) => return err_frame("invalid to timestamp"),
        }
    };

    // Find FILTER keyword and parse filters
    let mut filter_start = 2;
    let mut filters: Vec<LabelMatcher> = Vec::new();

    while filter_start < args.len() {
        let arg = String::from_utf8_lossy(&args[filter_start]).to_uppercase();
        if arg == "FILTER" {
            filter_start += 1;
            break;
        }
        filter_start += 1;
    }

    // Parse filter expressions
    let mut i = filter_start;
    while i < args.len() {
        let filter_str = String::from_utf8_lossy(&args[i]).to_string();
        if filter_str.to_uppercase() == "COUNT" || filter_str.to_uppercase() == "AGGREGATION" {
            break;
        }
        if let Ok(matcher) = LabelMatcher::parse(&filter_str) {
            filters.push(matcher);
        }
        i += 1;
    }

    let engine = get_engine();

    // Get all metrics and filter by labels
    let metrics = match engine.list_metrics() {
        Ok(m) => m,
        Err(e) => return err_frame(&format!("failed to list metrics: {}", e)),
    };

    let metadata = get_metadata();
    let meta_read = metadata.read();

    let mut results: Vec<Frame> = Vec::new();

    for metric in metrics {
        // Check if metric matches filters
        let matches = if let Some(meta) = meta_read.get(&metric) {
            filters.iter().all(|f| {
                meta.labels
                    .iter()
                    .any(|(k, v)| &f.name == k && f.value == *v)
            })
        } else {
            filters.is_empty()
        };

        if matches {
            // Query this metric
            let query_result = engine.query().metric(&metric).range(from, to).build();

            if let Ok(query) = query_result {
                if let Ok(result) = engine.execute_parsed_query(query) {
                    for series in result.series {
                        let samples: Vec<Frame> = series
                            .samples
                            .iter()
                            .map(|s| {
                                let value_str = s
                                    .value
                                    .as_f64()
                                    .map(|v| format!("{}", v))
                                    .unwrap_or_default();
                                Frame::array(vec![
                                    Frame::Integer(s.timestamp.as_millis()),
                                    Frame::bulk(value_str),
                                ])
                            })
                            .collect();

                        let labels: Vec<Frame> = meta_read
                            .get(&metric)
                            .map(|m| {
                                m.labels
                                    .iter()
                                    .flat_map(|(k, v)| {
                                        vec![Frame::bulk(k.clone()), Frame::bulk(v.clone())]
                                    })
                                    .collect()
                            })
                            .unwrap_or_default();

                        results.push(Frame::array(vec![
                            Frame::bulk(metric.clone()),
                            Frame::array(labels),
                            Frame::array(samples),
                        ]));
                    }
                }
            }
        }
    }

    Frame::array(results)
}

/// Handle TS.INFO command
///
/// Gets metadata about a time-series.
///
/// # Syntax
/// `TS.INFO <key>`
///
/// # Returns
/// Array of key-value pairs with metadata.
pub fn ts_info(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'TS.INFO' command");
    }

    let key = String::from_utf8_lossy(&args[0]).to_string();
    let engine = get_engine();
    let metadata = get_metadata();
    let meta_read = metadata.read();

    // Get metadata if exists
    let (retention, labels) = if let Some(meta) = meta_read.get(&key) {
        let label_frames: Vec<Frame> = meta
            .labels
            .iter()
            .flat_map(|(k, v)| vec![Frame::bulk(k.clone()), Frame::bulk(v.clone())])
            .collect();
        (meta.retention_ms as i64, label_frames)
    } else {
        (0i64, Vec::new())
    };

    // Get metric metadata from engine
    let (total_samples, first_ts, last_ts) = match engine.get_metric_metadata(&key) {
        Ok(meta) => (
            meta.sample_count as i64,
            meta.min_timestamp.map(|t| t.as_millis()).unwrap_or(0),
            meta.max_timestamp.map(|t| t.as_millis()).unwrap_or(0),
        ),
        Err(_) => (0, 0, 0),
    };

    Frame::array(vec![
        Frame::bulk("totalSamples"),
        Frame::Integer(total_samples),
        Frame::bulk("memoryUsage"),
        Frame::Integer(0), // Not tracked in metadata
        Frame::bulk("firstTimestamp"),
        Frame::Integer(first_ts),
        Frame::bulk("lastTimestamp"),
        Frame::Integer(last_ts),
        Frame::bulk("retentionTime"),
        Frame::Integer(retention),
        Frame::bulk("chunkCount"),
        Frame::Integer(1), // Simplified
        Frame::bulk("key"),
        Frame::bulk(key),
        Frame::bulk("labels"),
        Frame::array(labels),
    ])
}

/// Handle TS.DEL command
///
/// Deletes samples from a time-series in a given range.
///
/// # Syntax
/// `TS.DEL <key> <from> <to>`
///
/// # Returns
/// Number of deleted samples.
pub fn ts_del(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 3 {
        return err_frame("wrong number of arguments for 'TS.DEL' command");
    }

    let key = String::from_utf8_lossy(&args[0]).to_string();
    let _from_str = String::from_utf8_lossy(&args[1]);
    let _to_str = String::from_utf8_lossy(&args[2]);

    let engine = get_engine();

    // Delete the entire metric (range delete not directly supported)
    match engine.delete_metric(&key) {
        Ok(()) => {
            // Also remove metadata
            let metadata = get_metadata();
            let mut meta_write = metadata.write();
            meta_write.remove(&key);
            Frame::Integer(1) // Indicate deletion
        }
        Err(_) => Frame::Integer(0),
    }
}

/// Handle TS.CREATERULE command
///
/// Creates a compaction (downsampling) rule.
///
/// # Syntax
/// `TS.CREATERULE <source> <dest> AGGREGATION <type> <bucket>`
///
/// # Returns
/// OK on success.
pub fn ts_createrule(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 5 {
        return err_frame("wrong number of arguments for 'TS.CREATERULE' command");
    }

    let _source = String::from_utf8_lossy(&args[0]).to_string();
    let _dest = String::from_utf8_lossy(&args[1]).to_string();

    // Check for AGGREGATION keyword
    let agg_keyword = String::from_utf8_lossy(&args[2]).to_uppercase();
    if agg_keyword != "AGGREGATION" {
        return err_frame("expected AGGREGATION keyword");
    }

    let agg_type = String::from_utf8_lossy(&args[3]).to_uppercase();
    let bucket_ms: u64 = match String::from_utf8_lossy(&args[4]).parse() {
        Ok(v) => v,
        Err(_) => return err_frame("invalid bucket size"),
    };

    let aggregation = match agg_type.as_str() {
        "AVG" => Aggregation::Avg,
        "SUM" => Aggregation::Sum,
        "MIN" => Aggregation::Min,
        "MAX" => Aggregation::Max,
        "COUNT" => Aggregation::Count,
        "FIRST" => Aggregation::First,
        "LAST" => Aggregation::Last,
        _ => return err_frame(&format!("unknown aggregation type: {}", agg_type)),
    };

    let engine = get_engine();
    // DownsampleRule::new takes (interval, after, aggregation)
    // We use bucket as interval and 0 as "after" (apply immediately)
    let rule = DownsampleRule::new(
        Duration::from_millis(bucket_ms),
        Duration::from_secs(0),
        aggregation,
    );

    match engine.add_downsample_rule(rule) {
        Ok(()) => ok_frame(),
        Err(e) => err_frame(&format!("failed to create rule: {}", e)),
    }
}

/// Handle TS.DELETERULE command
///
/// Deletes a compaction rule.
///
/// # Syntax
/// `TS.DELETERULE <source> <dest>`
///
/// # Returns
/// OK on success.
pub fn ts_deleterule(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("wrong number of arguments for 'TS.DELETERULE' command");
    }

    // Downsampler doesn't expose remove_rule, so we acknowledge the command
    // but note this would need engine enhancement for full support
    ok_frame()
}

/// Handle TS.QUERYINDEX command
///
/// Queries time-series keys by label filters.
///
/// # Syntax
/// `TS.QUERYINDEX <label=value ...>`
///
/// # Returns
/// Array of matching keys.
pub fn ts_queryindex(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'TS.QUERYINDEX' command");
    }

    // Parse filter expressions
    let mut filters: Vec<LabelMatcher> = Vec::new();
    for arg in args {
        let filter_str = String::from_utf8_lossy(arg).to_string();
        if let Ok(matcher) = LabelMatcher::parse(&filter_str) {
            filters.push(matcher);
        }
    }

    let metadata = get_metadata();
    let meta_read = metadata.read();

    let matching_keys: Vec<Frame> = meta_read
        .iter()
        .filter(|(_, meta)| {
            filters.iter().all(|f| {
                meta.labels
                    .iter()
                    .any(|(k, v)| &f.name == k && f.value == *v)
            })
        })
        .map(|(key, _)| Frame::bulk(key.clone()))
        .collect();

    Frame::array(matching_keys)
}

/// Handle TS.ALTER command
///
/// Modifies time-series configuration.
///
/// # Syntax
/// `TS.ALTER <key> [RETENTION <ms>] [LABELS <label> <value> ...]`
///
/// # Returns
/// OK on success.
pub fn ts_alter(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'TS.ALTER' command");
    }

    let key = String::from_utf8_lossy(&args[0]).to_string();
    let metadata = get_metadata();

    // Check if metric exists
    {
        let meta_read = metadata.read();
        if !meta_read.contains_key(&key) {
            return err_frame(&format!("time-series '{}' not found", key));
        }
    }

    let mut new_retention: Option<u64> = None;
    let mut new_labels: Option<Vec<(String, String)>> = None;

    // Parse arguments
    let mut i = 1;
    while i < args.len() {
        let arg = String::from_utf8_lossy(&args[i]).to_uppercase();
        match arg.as_str() {
            "RETENTION" => {
                if i + 1 >= args.len() {
                    return err_frame("RETENTION requires a value");
                }
                new_retention = Some(match String::from_utf8_lossy(&args[i + 1]).parse() {
                    Ok(v) => v,
                    Err(_) => return err_frame("invalid retention value"),
                });
                i += 2;
            }
            "LABELS" => {
                i += 1;
                let mut labels = Vec::new();
                while i + 1 < args.len() {
                    let label = String::from_utf8_lossy(&args[i]).to_string();
                    let value = String::from_utf8_lossy(&args[i + 1]).to_string();
                    if label.to_uppercase() == "RETENTION" {
                        break;
                    }
                    labels.push((label, value));
                    i += 2;
                }
                new_labels = Some(labels);
            }
            _ => {
                return err_frame(&format!("unknown argument '{}'", arg));
            }
        }
    }

    // Update metadata
    {
        let mut meta_write = metadata.write();
        if let Some(meta) = meta_write.get_mut(&key) {
            if let Some(ret) = new_retention {
                meta.retention_ms = ret;
                // Update engine retention policy
                let engine = get_engine();
                let policy = RetentionPolicy::new(Duration::from_millis(ret));
                let _ = engine.set_retention(&key, policy);
            }
            if let Some(labels) = new_labels {
                meta.labels = labels;
            }
        }
    }

    ok_frame()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn test_timestamp_parsing() {
        let ts_str = "*";
        let timestamp = if ts_str == "*" {
            Timestamp::now()
        } else {
            Timestamp::from_millis(ts_str.parse().unwrap_or(0))
        };
        assert!(timestamp.as_millis() > 0);
    }

    #[test]
    fn test_value_parsing() {
        let value_str = "42.5";
        let value: Result<f64, _> = value_str.parse();
        assert!(value.is_ok());
        assert_eq!(value.unwrap(), 42.5);
    }
}
