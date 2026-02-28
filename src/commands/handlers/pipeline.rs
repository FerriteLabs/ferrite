//! PIPELINE.* command handlers
//!
//! PIPELINE.CREATE, PIPELINE.ADD.SOURCE, PIPELINE.ADD.TRANSFORM, PIPELINE.ADD.SINK,
//! PIPELINE.LINK, PIPELINE.START, PIPELINE.STOP, PIPELINE.PAUSE, PIPELINE.RESUME,
//! PIPELINE.DELETE, PIPELINE.LIST, PIPELINE.INFO, PIPELINE.TOPOLOGY,
//! PIPELINE.METRICS, PIPELINE.VALIDATE, PIPELINE.STATS, PIPELINE.HELP
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::OnceLock;

use crate::protocol::Frame;
use ferrite_streaming::pipeline::composable::{
    ComposablePipelineDefinition, ComposablePipelineManager, ErrorStrategy, PipelineStage,
    SinkConfig, SinkKind, SourceConfig, SourceKind, StageType, TransformConfig, TransformKind,
};

use super::{err_frame, ok_frame};

/// Global pipeline manager singleton.
static PIPELINE_MANAGER: OnceLock<ComposablePipelineManager> = OnceLock::new();

/// In-progress pipeline definitions being built step-by-step.
static PIPELINE_BUILDERS: OnceLock<parking_lot::RwLock<HashMap<String, ComposablePipelineDefinition>>> =
    OnceLock::new();

fn get_manager() -> &'static ComposablePipelineManager {
    PIPELINE_MANAGER.get_or_init(ComposablePipelineManager::new)
}

fn get_builders(
) -> &'static parking_lot::RwLock<HashMap<String, ComposablePipelineDefinition>> {
    PIPELINE_BUILDERS.get_or_init(|| parking_lot::RwLock::new(HashMap::new()))
}

/// Dispatch a `PIPELINE` subcommand.
pub fn pipeline_command(subcommand: &str, args: &[String]) -> Frame {
    match subcommand.to_uppercase().as_str() {
        "CREATE" => handle_create(args),
        "ADD.SOURCE" => handle_add_source(args),
        "ADD.TRANSFORM" => handle_add_transform(args),
        "ADD.SINK" => handle_add_sink(args),
        "LINK" => handle_link(args),
        "START" => handle_start(args),
        "STOP" => handle_stop(args),
        "PAUSE" => handle_pause(args),
        "RESUME" => handle_resume(args),
        "DELETE" => handle_delete(args),
        "LIST" => handle_list(),
        "INFO" => handle_info(args),
        "TOPOLOGY" => handle_topology(args),
        "METRICS" => handle_metrics(args),
        "VALIDATE" => handle_validate(args),
        "STATS" => handle_stats(),
        "HELP" | "" => handle_help(),
        other => err_frame(&format!("unknown PIPELINE subcommand '{}'", other)),
    }
}

/// PIPELINE.CREATE name — Create an empty pipeline definition for step-by-step building.
fn handle_create(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("PIPELINE.CREATE requires: name");
    }

    let name = &args[0];
    let mut builders = get_builders().write();
    if builders.contains_key(name.as_str()) || get_manager().get_pipeline(name).is_some() {
        return err_frame(&format!("pipeline '{}' already exists", name));
    }

    let mut def = ComposablePipelineDefinition::default();
    def.name = name.clone();

    // Parse optional args
    let mut i = 1;
    while i < args.len() {
        match args[i].to_uppercase().as_str() {
            "BATCH" => {
                if i + 1 < args.len() {
                    def.batch_size = args[i + 1].parse().unwrap_or(100);
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "PARALLEL" => {
                if i + 1 < args.len() {
                    def.parallelism = args[i + 1].parse().unwrap_or(1);
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "ON_ERROR" => {
                if i + 1 < args.len() {
                    def.error_handling = parse_error_strategy(&args[i + 1]);
                    i += 2;
                } else {
                    i += 1;
                }
            }
            _ => i += 1,
        }
    }

    builders.insert(name.clone(), def);
    ok_frame()
}

/// PIPELINE.ADD.SOURCE name stage source_type connection [TOPIC topic]
fn handle_add_source(args: &[String]) -> Frame {
    if args.len() < 4 {
        return err_frame(
            "PIPELINE.ADD.SOURCE requires: pipeline_name stage_name source_type connection [TOPIC topic]",
        );
    }

    let pipeline_name = &args[0];
    let stage_name = &args[1];
    let source_type = match SourceKind::from_str_loose(&args[2]) {
        Some(t) => t,
        None => return err_frame(&format!("unknown source type '{}'", args[2])),
    };
    let connection = &args[3];

    let mut topic = String::new();
    let mut i = 4;
    while i < args.len() {
        match args[i].to_uppercase().as_str() {
            "TOPIC" => {
                if i + 1 < args.len() {
                    topic = args[i + 1].clone();
                    i += 2;
                } else {
                    i += 1;
                }
            }
            _ => i += 1,
        }
    }

    let stage = PipelineStage {
        name: stage_name.clone(),
        stage_type: StageType::Source(SourceConfig {
            source_type,
            connection: connection.clone(),
            topic_or_pattern: topic,
        }),
        config: HashMap::new(),
        next: Vec::new(),
    };

    add_stage_to_builder(pipeline_name, stage)
}

/// PIPELINE.ADD.TRANSFORM name stage transform_type [params...]
fn handle_add_transform(args: &[String]) -> Frame {
    if args.len() < 3 {
        return err_frame(
            "PIPELINE.ADD.TRANSFORM requires: pipeline_name stage_name transform_type [params...]",
        );
    }

    let pipeline_name = &args[0];
    let stage_name = &args[1];
    let transform_kind = match TransformKind::from_str_loose(&args[2]) {
        Some(t) => t,
        None => return err_frame(&format!("unknown transform type '{}'", args[2])),
    };

    // Parse additional params into a map
    let mut params = HashMap::new();
    let mut i = 3;
    while i + 1 < args.len() {
        params.insert(args[i].clone(), args[i + 1].clone());
        i += 2;
    }

    let stage = PipelineStage {
        name: stage_name.clone(),
        stage_type: StageType::Transform(TransformConfig {
            transform_type: transform_kind,
            params,
        }),
        config: HashMap::new(),
        next: Vec::new(),
    };

    add_stage_to_builder(pipeline_name, stage)
}

/// PIPELINE.ADD.SINK name stage sink_type connection [TARGET target]
fn handle_add_sink(args: &[String]) -> Frame {
    if args.len() < 4 {
        return err_frame(
            "PIPELINE.ADD.SINK requires: pipeline_name stage_name sink_type connection [TARGET target]",
        );
    }

    let pipeline_name = &args[0];
    let stage_name = &args[1];
    let sink_type = match SinkKind::from_str_loose(&args[2]) {
        Some(t) => t,
        None => return err_frame(&format!("unknown sink type '{}'", args[2])),
    };
    let connection = &args[3];

    let mut target = String::new();
    let mut i = 4;
    while i < args.len() {
        match args[i].to_uppercase().as_str() {
            "TARGET" => {
                if i + 1 < args.len() {
                    target = args[i + 1].clone();
                    i += 2;
                } else {
                    i += 1;
                }
            }
            _ => i += 1,
        }
    }

    let stage = PipelineStage {
        name: stage_name.clone(),
        stage_type: StageType::Sink(SinkConfig {
            sink_type,
            connection: connection.clone(),
            target,
        }),
        config: HashMap::new(),
        next: Vec::new(),
    };

    add_stage_to_builder(pipeline_name, stage)
}

/// PIPELINE.LINK name from_stage to_stage
fn handle_link(args: &[String]) -> Frame {
    if args.len() < 3 {
        return err_frame("PIPELINE.LINK requires: pipeline_name from_stage to_stage");
    }

    let pipeline_name = &args[0];
    let from_stage = &args[1];
    let to_stage = &args[2];

    let mut builders = get_builders().write();
    let def = match builders.get_mut(pipeline_name.as_str()) {
        Some(d) => d,
        None => return err_frame(&format!("pipeline '{}' not found in builder", pipeline_name)),
    };

    let stage = match def.stages.iter_mut().find(|s| s.name == *from_stage) {
        Some(s) => s,
        None => {
            return err_frame(&format!(
                "stage '{}' not found in pipeline '{}'",
                from_stage, pipeline_name
            ))
        }
    };

    if !stage.next.contains(to_stage) {
        stage.next.push(to_stage.clone());
    }

    ok_frame()
}

/// PIPELINE.START name — Finalize builder (if exists) and start pipeline.
fn handle_start(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("PIPELINE.START requires: name");
    }
    let name = &args[0];

    // If pipeline is still in builder, finalize it first
    {
        let mut builders = get_builders().write();
        if let Some(def) = builders.remove(name.as_str()) {
            if let Err(e) = get_manager().create_pipeline(def) {
                return err_frame(&format!("failed to create pipeline: {}", e));
            }
        }
    }

    match get_manager().start_pipeline(name) {
        Ok(()) => ok_frame(),
        Err(e) => err_frame(&format!("{}", e)),
    }
}

/// PIPELINE.STOP name
fn handle_stop(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("PIPELINE.STOP requires: name");
    }
    match get_manager().stop_pipeline(&args[0]) {
        Ok(()) => ok_frame(),
        Err(e) => err_frame(&format!("{}", e)),
    }
}

/// PIPELINE.PAUSE name
fn handle_pause(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("PIPELINE.PAUSE requires: name");
    }
    match get_manager().pause_pipeline(&args[0]) {
        Ok(()) => ok_frame(),
        Err(e) => err_frame(&format!("{}", e)),
    }
}

/// PIPELINE.RESUME name
fn handle_resume(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("PIPELINE.RESUME requires: name");
    }
    match get_manager().resume_pipeline(&args[0]) {
        Ok(()) => ok_frame(),
        Err(e) => err_frame(&format!("{}", e)),
    }
}

/// PIPELINE.DELETE name
fn handle_delete(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("PIPELINE.DELETE requires: name");
    }
    let name = &args[0];

    // Remove from builder if exists
    get_builders().write().remove(name.as_str());

    // Remove from manager if exists
    match get_manager().delete_pipeline(name) {
        Ok(()) => ok_frame(),
        Err(_) => ok_frame(), // idempotent delete
    }
}

/// PIPELINE.LIST
fn handle_list() -> Frame {
    let list = get_manager().list_pipelines();
    let items: Vec<Frame> = list
        .into_iter()
        .map(|info| {
            Frame::array(vec![
                Frame::bulk("name"),
                Frame::bulk(info.name),
                Frame::bulk("state"),
                Frame::bulk(info.state.to_string()),
                Frame::bulk("stages"),
                Frame::Integer(info.stages as i64),
                Frame::bulk("records_processed"),
                Frame::Integer(info.records_processed as i64),
                Frame::bulk("errors"),
                Frame::Integer(info.errors as i64),
            ])
        })
        .collect();
    Frame::array(items)
}

/// PIPELINE.INFO name
fn handle_info(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("PIPELINE.INFO requires: name");
    }

    match get_manager().get_pipeline(&args[0]) {
        Some(info) => Frame::array(vec![
            Frame::bulk("name"),
            Frame::bulk(info.name),
            Frame::bulk("state"),
            Frame::bulk(info.state.to_string()),
            Frame::bulk("stages"),
            Frame::Integer(info.stages as i64),
            Frame::bulk("created_at"),
            Frame::Integer(info.created_at as i64),
            Frame::bulk("records_processed"),
            Frame::Integer(info.records_processed as i64),
            Frame::bulk("errors"),
            Frame::Integer(info.errors as i64),
        ]),
        None => Frame::Null,
    }
}

/// PIPELINE.TOPOLOGY name
fn handle_topology(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("PIPELINE.TOPOLOGY requires: name");
    }

    match get_manager().pipeline_topology(&args[0]) {
        Some(topo) => {
            let mut items = Vec::new();

            items.push(Frame::bulk("stages"));
            let stage_frames: Vec<Frame> = topo
                .stages
                .into_iter()
                .map(|s| {
                    Frame::array(vec![
                        Frame::bulk(s.name),
                        Frame::bulk(s.stage_type),
                        Frame::bulk(s.state),
                    ])
                })
                .collect();
            items.push(Frame::array(stage_frames));

            items.push(Frame::bulk("edges"));
            let edge_frames: Vec<Frame> = topo
                .edges
                .into_iter()
                .map(|e| {
                    Frame::array(vec![
                        Frame::bulk(e.from),
                        Frame::bulk(e.to),
                        Frame::Integer(e.records_passed as i64),
                    ])
                })
                .collect();
            items.push(Frame::array(edge_frames));

            Frame::array(items)
        }
        None => Frame::Null,
    }
}

/// PIPELINE.METRICS name
fn handle_metrics(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("PIPELINE.METRICS requires: name");
    }

    match get_manager().pipeline_metrics(&args[0]) {
        Some(m) => Frame::array(vec![
            Frame::bulk("name"),
            Frame::bulk(m.name),
            Frame::bulk("records_in"),
            Frame::Integer(m.records_in as i64),
            Frame::bulk("records_out"),
            Frame::Integer(m.records_out as i64),
            Frame::bulk("records_error"),
            Frame::Integer(m.records_error as i64),
            Frame::bulk("throughput_per_sec"),
            Frame::bulk(format!("{:.2}", m.throughput_per_sec)),
            Frame::bulk("avg_latency_ms"),
            Frame::bulk(format!("{:.2}", m.avg_latency_ms)),
            Frame::bulk("backpressure_pct"),
            Frame::bulk(format!("{:.2}", m.backpressure_pct)),
        ]),
        None => Frame::Null,
    }
}

/// PIPELINE.VALIDATE name
fn handle_validate(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("PIPELINE.VALIDATE requires: name");
    }

    let name = &args[0];

    // Check builder first
    let builders = get_builders().read();
    let def = match builders.get(name.as_str()) {
        Some(d) => d.clone(),
        None => {
            // Not in builder — if it's already registered, it was valid
            if get_manager().get_pipeline(name).is_some() {
                return Frame::array(vec![]); // empty = valid
            }
            return err_frame(&format!("pipeline '{}' not found", name));
        }
    };
    drop(builders);

    let errors = get_manager().validate_pipeline(&def);
    if errors.is_empty() {
        return Frame::array(vec![]);
    }

    let items: Vec<Frame> = errors
        .into_iter()
        .map(|e| {
            Frame::array(vec![
                Frame::bulk("stage"),
                Frame::bulk(e.stage),
                Frame::bulk("message"),
                Frame::bulk(e.message),
                Frame::bulk("severity"),
                Frame::bulk(e.severity),
            ])
        })
        .collect();
    Frame::array(items)
}

/// PIPELINE.STATS
fn handle_stats() -> Frame {
    let stats = get_manager().stats();
    Frame::array(vec![
        Frame::bulk("total_pipelines"),
        Frame::Integer(stats.total_pipelines as i64),
        Frame::bulk("running"),
        Frame::Integer(stats.running as i64),
        Frame::bulk("total_records"),
        Frame::Integer(stats.total_records as i64),
        Frame::bulk("total_errors"),
        Frame::Integer(stats.total_errors as i64),
    ])
}

/// PIPELINE.HELP
fn handle_help() -> Frame {
    let help = vec![
        "PIPELINE.CREATE name [BATCH n] [PARALLEL n] [ON_ERROR skip|retry|deadletter|fail] — Create pipeline",
        "PIPELINE.ADD.SOURCE pipeline stage source_type connection [TOPIC topic] — Add source stage",
        "PIPELINE.ADD.TRANSFORM pipeline stage transform_type [key value ...] — Add transform stage",
        "PIPELINE.ADD.SINK pipeline stage sink_type connection [TARGET target] — Add sink stage",
        "PIPELINE.LINK pipeline from_stage to_stage — Link stages in DAG",
        "PIPELINE.START name — Finalize and start pipeline",
        "PIPELINE.STOP name — Stop pipeline",
        "PIPELINE.PAUSE name — Pause running pipeline",
        "PIPELINE.RESUME name — Resume paused pipeline",
        "PIPELINE.DELETE name — Delete pipeline",
        "PIPELINE.LIST — List all pipelines",
        "PIPELINE.INFO name — Pipeline details",
        "PIPELINE.TOPOLOGY name — DAG topology (stages + edges)",
        "PIPELINE.METRICS name — Pipeline metrics",
        "PIPELINE.VALIDATE name — Validate pipeline (empty = valid)",
        "PIPELINE.STATS — Manager statistics",
        "PIPELINE.HELP — Show this help",
        "",
        "Source types: KAFKA, FERRITE, HTTP, S3, TIMER",
        "Transform types: FILTER, MAP, ENRICH, AGGREGATE, DEDUPLICATE, FLATMAP, RENAME, DEFAULT, SAMPLE, RATELIMIT",
        "Sink types: FERRITE, KAFKA, HTTP, S3, LOG, NULL",
        "Error strategies: SKIP, RETRY, DEADLETTER, FAIL",
    ];

    Frame::array(help.into_iter().map(Frame::bulk).collect())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn add_stage_to_builder(pipeline_name: &str, stage: PipelineStage) -> Frame {
    let mut builders = get_builders().write();
    let def = match builders.get_mut(pipeline_name) {
        Some(d) => d,
        None => {
            return err_frame(&format!(
                "pipeline '{}' not found in builder (use PIPELINE.CREATE first)",
                pipeline_name
            ))
        }
    };

    // Check duplicate stage name
    if def.stages.iter().any(|s| s.name == stage.name) {
        return err_frame(&format!(
            "stage '{}' already exists in pipeline '{}'",
            stage.name, pipeline_name
        ));
    }

    def.stages.push(stage);
    ok_frame()
}

fn parse_error_strategy(s: &str) -> ErrorStrategy {
    match s.to_uppercase().as_str() {
        "SKIP" => ErrorStrategy::Skip,
        "RETRY" => ErrorStrategy::Retry {
            max_retries: 3,
            backoff_ms: 1000,
        },
        "DEADLETTER" => ErrorStrategy::DeadLetter {
            target: "dead_letter".to_string(),
        },
        "FAIL" => ErrorStrategy::Fail,
        _ => ErrorStrategy::Skip,
    }
}
