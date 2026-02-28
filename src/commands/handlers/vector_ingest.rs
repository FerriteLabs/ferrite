//! VECTOR.INGEST command handlers
//!
//! Wire streaming vector ingestion pipeline commands into the server.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::OnceLock;

use bytes::Bytes;
use parking_lot::RwLock;

use crate::protocol::Frame;
use ferrite_streaming::pipeline::vector_ingest::{
    EmbeddingProvider, ErrorStrategy, IngestSource, PipelineState, PipelineStats,
    VectorIngestConfig, VectorIngestPipeline,
};

use super::err_frame;

/// Tracked pipeline record
struct PipelineRecord {
    id: String,
    index_name: String,
    source: IngestSource,
    state: PipelineState,
    stats: PipelineStats,
    pipeline: VectorIngestPipeline,
}

/// Global pipeline registry
static PIPELINES: OnceLock<RwLock<HashMap<String, PipelineRecord>>> = OnceLock::new();

fn get_pipelines() -> &'static RwLock<HashMap<String, PipelineRecord>> {
    PIPELINES.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Handle VECTOR.INGEST subcommands
pub fn vector_ingest(subcommand: &str, args: &[String]) -> Frame {
    match subcommand.to_uppercase().as_str() {
        "START" => ingest_start(args),
        "STOP" => ingest_stop(args),
        "PAUSE" => ingest_pause(args),
        "RESUME" => ingest_resume(args),
        "STATUS" => ingest_status(args),
        "LIST" => ingest_list(),
        "HELP" => ingest_help(),
        other => err_frame(&format!("unknown VECTOR.INGEST subcommand '{}'", other)),
    }
}

/// VECTOR.INGEST START index_name SOURCE kafka BROKERS host:port TOPIC topic GROUP group_id
fn ingest_start(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'VECTOR.INGEST START'");
    }

    let index_name = args[0].clone();
    let mut brokers = Vec::new();
    let mut topic = String::new();
    let mut group_id = String::new();

    let mut i = 1;
    while i < args.len() {
        match args[i].to_uppercase().as_str() {
            "SOURCE" => {
                // skip "kafka" token
                i += 1;
            }
            "BROKERS" => {
                i += 1;
                if i < args.len() {
                    brokers = args[i].split(',').map(|s| s.to_string()).collect();
                }
            }
            "TOPIC" => {
                i += 1;
                if i < args.len() {
                    topic = args[i].clone();
                }
            }
            "GROUP" => {
                i += 1;
                if i < args.len() {
                    group_id = args[i].clone();
                }
            }
            _ => {}
        }
        i += 1;
    }

    if brokers.is_empty() {
        brokers.push("localhost:9092".to_string());
    }
    if topic.is_empty() {
        return err_frame("TOPIC is required for VECTOR.INGEST START");
    }
    if group_id.is_empty() {
        group_id = format!("ferrite-ingest-{}", index_name);
    }

    let source = IngestSource::Kafka {
        brokers: brokers.clone(),
        topic: topic.clone(),
        group_id: group_id.clone(),
    };

    let config = VectorIngestConfig {
        source: source.clone(),
        embedding_provider: EmbeddingProvider::Mock,
        target_index: index_name.clone(),
        batch_size: 100,
        max_concurrent_embeddings: 4,
        backpressure_threshold: 10_000,
        error_handling: ErrorStrategy::default(),
    };

    let pipeline = VectorIngestPipeline::new(config);
    let handle = match pipeline.start() {
        Ok(h) => h,
        Err(e) => return err_frame(&format!("failed to start pipeline: {}", e)),
    };

    let pipeline_id = handle.pipeline_id.clone();
    let stats = pipeline.stats();

    let record = PipelineRecord {
        id: pipeline_id.clone(),
        index_name: index_name.clone(),
        source,
        state: PipelineState::Running,
        stats,
        pipeline,
    };

    get_pipelines().write().insert(pipeline_id.clone(), record);

    Frame::array(vec![
        Frame::bulk("pipeline_id"),
        Frame::bulk(Bytes::from(pipeline_id)),
        Frame::bulk("index"),
        Frame::bulk(Bytes::from(index_name)),
        Frame::bulk("state"),
        Frame::bulk("Running"),
    ])
}

/// VECTOR.INGEST STOP pipeline_id
fn ingest_stop(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'VECTOR.INGEST STOP'");
    }
    let pipeline_id = &args[0];
    let mut pipelines = get_pipelines().write();
    match pipelines.get_mut(pipeline_id.as_str()) {
        Some(record) => {
            if let Err(e) = record.pipeline.stop() {
                return err_frame(&format!("failed to stop pipeline: {}", e));
            }
            record.state = PipelineState::Stopped;
            record.stats = record.pipeline.stats();
            Frame::Simple(Bytes::from("OK"))
        }
        None => err_frame(&format!("pipeline '{}' not found", pipeline_id)),
    }
}

/// VECTOR.INGEST PAUSE pipeline_id
fn ingest_pause(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'VECTOR.INGEST PAUSE'");
    }
    let pipeline_id = &args[0];
    let mut pipelines = get_pipelines().write();
    match pipelines.get_mut(pipeline_id.as_str()) {
        Some(record) => {
            if let Err(e) = record.pipeline.pause() {
                return err_frame(&format!("failed to pause pipeline: {}", e));
            }
            record.state = PipelineState::Paused;
            record.stats = record.pipeline.stats();
            Frame::Simple(Bytes::from("OK"))
        }
        None => err_frame(&format!("pipeline '{}' not found", pipeline_id)),
    }
}

/// VECTOR.INGEST RESUME pipeline_id
fn ingest_resume(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'VECTOR.INGEST RESUME'");
    }
    let pipeline_id = &args[0];
    let mut pipelines = get_pipelines().write();
    match pipelines.get_mut(pipeline_id.as_str()) {
        Some(record) => {
            if let Err(e) = record.pipeline.resume() {
                return err_frame(&format!("failed to resume pipeline: {}", e));
            }
            record.state = PipelineState::Running;
            record.stats = record.pipeline.stats();
            Frame::Simple(Bytes::from("OK"))
        }
        None => err_frame(&format!("pipeline '{}' not found", pipeline_id)),
    }
}

/// VECTOR.INGEST STATUS [pipeline_id]
fn ingest_status(args: &[String]) -> Frame {
    let pipelines = get_pipelines().read();

    if let Some(pipeline_id) = args.first() {
        match pipelines.get(pipeline_id.as_str()) {
            Some(record) => {
                let stats = record.pipeline.stats();
                Frame::array(vec![
                    Frame::bulk("pipeline_id"),
                    Frame::bulk(Bytes::from(record.id.clone())),
                    Frame::bulk("index"),
                    Frame::bulk(Bytes::from(record.index_name.clone())),
                    Frame::bulk("state"),
                    Frame::bulk(Bytes::from(format!("{:?}", record.state))),
                    Frame::bulk("docs_processed"),
                    Frame::integer(stats.docs_processed as i64),
                    Frame::bulk("docs_failed"),
                    Frame::integer(stats.docs_failed as i64),
                    Frame::bulk("throughput_per_sec"),
                    Frame::bulk(Bytes::from(format!("{:.2}", stats.throughput_per_sec))),
                    Frame::bulk("avg_latency_ms"),
                    Frame::bulk(Bytes::from(format!("{:.2}", stats.avg_latency_ms))),
                    Frame::bulk("last_error"),
                    Frame::bulk(Bytes::from(
                        stats.last_error.unwrap_or_else(|| "none".to_string()),
                    )),
                ])
            }
            None => err_frame(&format!("pipeline '{}' not found", pipeline_id)),
        }
    } else {
        // Summary of all pipelines
        let count = pipelines.len();
        let running = pipelines
            .values()
            .filter(|r| r.state == PipelineState::Running)
            .count();
        Frame::array(vec![
            Frame::bulk("total_pipelines"),
            Frame::integer(count as i64),
            Frame::bulk("running"),
            Frame::integer(running as i64),
        ])
    }
}

/// VECTOR.INGEST LIST
fn ingest_list() -> Frame {
    let pipelines = get_pipelines().read();
    if pipelines.is_empty() {
        return Frame::array(vec![]);
    }

    let mut items = Vec::new();
    for record in pipelines.values() {
        items.push(Frame::array(vec![
            Frame::bulk(Bytes::from(record.id.clone())),
            Frame::bulk(Bytes::from(record.index_name.clone())),
            Frame::bulk(Bytes::from(format!("{:?}", record.state))),
        ]));
    }
    Frame::array(items)
}

/// VECTOR.INGEST HELP
fn ingest_help() -> Frame {
    let help = vec![
        Frame::bulk("VECTOR.INGEST START <index> SOURCE kafka BROKERS <host:port> TOPIC <topic> GROUP <group_id> - Start ingestion pipeline"),
        Frame::bulk("VECTOR.INGEST STOP <pipeline_id> - Stop pipeline"),
        Frame::bulk("VECTOR.INGEST PAUSE <pipeline_id> - Pause pipeline"),
        Frame::bulk("VECTOR.INGEST RESUME <pipeline_id> - Resume pipeline"),
        Frame::bulk("VECTOR.INGEST STATUS [pipeline_id] - Get pipeline status/stats"),
        Frame::bulk("VECTOR.INGEST LIST - List all pipelines"),
        Frame::bulk("VECTOR.INGEST HELP - Show this help"),
    ];
    Frame::array(help)
}
