//! Composable Data Pipelines
//!
//! DAG-based ETL/ELT pipelines with source, transform, and sink stages.
//! Execute entirely inside Ferrite with backpressure control.
#![allow(dead_code)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Pipeline definition
// ---------------------------------------------------------------------------

/// A complete composable pipeline definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComposablePipelineDefinition {
    /// Pipeline name (unique identifier).
    pub name: String,
    /// Ordered list of stages in the pipeline.
    pub stages: Vec<PipelineStage>,
    /// Error handling strategy.
    pub error_handling: ErrorStrategy,
    /// Parallelism level.
    pub parallelism: usize,
    /// Batch size for processing.
    pub batch_size: usize,
    /// Creation timestamp (epoch seconds).
    pub created_at: u64,
    /// Whether the pipeline is enabled.
    pub enabled: bool,
}

impl Default for ComposablePipelineDefinition {
    fn default() -> Self {
        Self {
            name: String::new(),
            stages: Vec::new(),
            error_handling: ErrorStrategy::Skip,
            parallelism: 1,
            batch_size: 100,
            created_at: 0,
            enabled: true,
        }
    }
}

/// A single stage in the pipeline DAG.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineStage {
    /// Stage name (unique within pipeline).
    pub name: String,
    /// What this stage does.
    pub stage_type: StageType,
    /// Configuration parameters.
    pub config: HashMap<String, String>,
    /// Names of downstream stages this feeds into.
    pub next: Vec<String>,
}

/// Type of a pipeline stage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StageType {
    Source(SourceConfig),
    Transform(TransformConfig),
    Sink(SinkConfig),
}

// ---------------------------------------------------------------------------
// Source configuration
// ---------------------------------------------------------------------------

/// Configuration for a source stage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    pub source_type: SourceKind,
    pub connection: String,
    pub topic_or_pattern: String,
}

/// Kind of data source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SourceKind {
    /// Apache Kafka consumer.
    Kafka,
    /// Ferrite key pattern (CDC).
    Ferrite,
    /// HTTP webhook receiver.
    Http,
    /// S3 bucket/prefix.
    S3,
    /// Timer/cron-based source.
    Timer,
}

impl SourceKind {
    pub fn from_str_loose(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "KAFKA" => Some(Self::Kafka),
            "FERRITE" => Some(Self::Ferrite),
            "HTTP" => Some(Self::Http),
            "S3" => Some(Self::S3),
            "TIMER" => Some(Self::Timer),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Transform configuration
// ---------------------------------------------------------------------------

/// Configuration for a transform stage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformConfig {
    pub transform_type: TransformKind,
    pub params: HashMap<String, String>,
}

/// Kind of data transformation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransformKind {
    Filter { condition: String },
    Map { expression: String },
    Enrich { lookup_key: String },
    Aggregate { window_secs: u64, function: String },
    Deduplicate { key_field: String, window_secs: u64 },
    FlatMap { field: String },
    Rename { from: String, to: String },
    Default { field: String, value: String },
    Sample { rate: f64 },
    RateLimit { max_per_sec: u64 },
}

impl TransformKind {
    pub fn from_str_loose(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "FILTER" => Some(Self::Filter {
                condition: String::new(),
            }),
            "MAP" => Some(Self::Map {
                expression: String::new(),
            }),
            "ENRICH" => Some(Self::Enrich {
                lookup_key: String::new(),
            }),
            "AGGREGATE" => Some(Self::Aggregate {
                window_secs: 60,
                function: "count".to_string(),
            }),
            "DEDUPLICATE" => Some(Self::Deduplicate {
                key_field: String::new(),
                window_secs: 60,
            }),
            "FLATMAP" => Some(Self::FlatMap {
                field: String::new(),
            }),
            "RENAME" => Some(Self::Rename {
                from: String::new(),
                to: String::new(),
            }),
            "DEFAULT" => Some(Self::Default {
                field: String::new(),
                value: String::new(),
            }),
            "SAMPLE" => Some(Self::Sample { rate: 1.0 }),
            "RATELIMIT" | "RATE_LIMIT" => Some(Self::RateLimit { max_per_sec: 100 }),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Sink configuration
// ---------------------------------------------------------------------------

/// Configuration for a sink stage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkConfig {
    pub sink_type: SinkKind,
    pub connection: String,
    pub target: String,
}

/// Kind of data sink.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SinkKind {
    /// Write to Ferrite keys.
    Ferrite,
    /// Produce to Kafka topic.
    Kafka,
    /// POST to HTTP webhook URL.
    Http,
    /// Write to S3 bucket/prefix.
    S3,
    /// Log to stdout.
    Log,
    /// Discard (for testing).
    Null,
}

impl SinkKind {
    pub fn from_str_loose(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "FERRITE" => Some(Self::Ferrite),
            "KAFKA" => Some(Self::Kafka),
            "HTTP" => Some(Self::Http),
            "S3" => Some(Self::S3),
            "LOG" => Some(Self::Log),
            "NULL" => Some(Self::Null),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Error strategy
// ---------------------------------------------------------------------------

/// How to handle errors during pipeline execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ErrorStrategy {
    /// Skip failed records.
    Skip,
    /// Retry with exponential backoff.
    Retry { max_retries: u32, backoff_ms: u64 },
    /// Send failed records to a dead-letter target.
    DeadLetter { target: String },
    /// Fail the pipeline immediately.
    Fail,
}

impl Default for ErrorStrategy {
    fn default() -> Self {
        Self::Skip
    }
}

// ---------------------------------------------------------------------------
// Pipeline state & info types
// ---------------------------------------------------------------------------

/// Runtime state of a composable pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ComposablePipelineState {
    Created,
    Running,
    Paused,
    Stopped,
    Failed,
}

impl std::fmt::Display for ComposablePipelineState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Created => write!(f, "CREATED"),
            Self::Running => write!(f, "RUNNING"),
            Self::Paused => write!(f, "PAUSED"),
            Self::Stopped => write!(f, "STOPPED"),
            Self::Failed => write!(f, "FAILED"),
        }
    }
}

/// Summary information about a pipeline.
#[derive(Debug, Clone, Serialize)]
pub struct ComposablePipelineInfo {
    pub name: String,
    pub state: ComposablePipelineState,
    pub stages: usize,
    pub created_at: u64,
    pub started_at: Option<u64>,
    pub records_processed: u64,
    pub errors: u64,
}

/// DAG topology representation.
#[derive(Debug, Clone, Serialize)]
pub struct PipelineTopology {
    pub stages: Vec<TopologyNode>,
    pub edges: Vec<TopologyEdge>,
}

/// A node in the topology.
#[derive(Debug, Clone, Serialize)]
pub struct TopologyNode {
    pub name: String,
    pub stage_type: String,
    pub state: String,
}

/// An edge in the topology.
#[derive(Debug, Clone, Serialize)]
pub struct TopologyEdge {
    pub from: String,
    pub to: String,
    pub records_passed: u64,
}

/// Metrics for a running pipeline.
#[derive(Debug, Clone, Serialize)]
pub struct PipelineMetrics {
    pub name: String,
    pub records_in: u64,
    pub records_out: u64,
    pub records_error: u64,
    pub throughput_per_sec: f64,
    pub avg_latency_ms: f64,
    pub backpressure_pct: f64,
    pub per_stage: HashMap<String, StageMetrics>,
}

/// Per-stage metrics.
#[derive(Debug, Clone, Serialize)]
pub struct StageMetrics {
    pub records_in: u64,
    pub records_out: u64,
    pub errors: u64,
    pub avg_latency_ms: f64,
}

/// A validation error found during pipeline validation.
#[derive(Debug, Clone, Serialize)]
pub struct ValidationError {
    pub stage: String,
    pub message: String,
    pub severity: String,
}

/// Aggregate stats across all pipelines.
#[derive(Debug, Clone, Serialize)]
pub struct PipelineManagerStats {
    pub total_pipelines: usize,
    pub running: usize,
    pub total_records: u64,
    pub total_errors: u64,
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors from composable pipeline operations.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ComposablePipelineError {
    #[error("pipeline not found: {0}")]
    NotFound(String),

    #[error("pipeline already exists: {0}")]
    AlreadyExists(String),

    #[error("invalid pipeline definition: {0:?}")]
    InvalidDefinition(Vec<ValidationError>),

    #[error("pipeline is already running: {0}")]
    AlreadyRunning(String),

    #[error("pipeline is not running: {0}")]
    NotRunning(String),

    #[error("stage failed: {0}")]
    StageFailed(String),
}

// ---------------------------------------------------------------------------
// Pipeline runtime state
// ---------------------------------------------------------------------------

struct PipelineRuntime {
    definition: ComposablePipelineDefinition,
    state: ComposablePipelineState,
    started_at: Option<Instant>,
    records_processed: u64,
    errors: u64,
    created_instant: Instant,
}

// ---------------------------------------------------------------------------
// PipelineManager
// ---------------------------------------------------------------------------

/// Manages composable DAG-based data pipelines.
pub struct ComposablePipelineManager {
    pipelines: RwLock<HashMap<String, PipelineRuntime>>,
}

impl ComposablePipelineManager {
    /// Create a new pipeline manager.
    pub fn new() -> Self {
        Self {
            pipelines: RwLock::new(HashMap::new()),
        }
    }

    /// Create and register a new pipeline. Returns the pipeline name.
    pub fn create_pipeline(
        &self,
        def: ComposablePipelineDefinition,
    ) -> Result<String, ComposablePipelineError> {
        let errors = self.validate_pipeline(&def);
        if !errors.is_empty() {
            return Err(ComposablePipelineError::InvalidDefinition(errors));
        }

        let name = def.name.clone();
        let mut pipelines = self.pipelines.write();
        if pipelines.contains_key(&name) {
            return Err(ComposablePipelineError::AlreadyExists(name));
        }

        pipelines.insert(
            name.clone(),
            PipelineRuntime {
                definition: def,
                state: ComposablePipelineState::Created,
                started_at: None,
                records_processed: 0,
                errors: 0,
                created_instant: Instant::now(),
            },
        );

        Ok(name)
    }

    /// Start a pipeline.
    pub fn start_pipeline(&self, name: &str) -> Result<(), ComposablePipelineError> {
        let mut pipelines = self.pipelines.write();
        let rt = pipelines
            .get_mut(name)
            .ok_or_else(|| ComposablePipelineError::NotFound(name.to_string()))?;

        if rt.state == ComposablePipelineState::Running {
            return Err(ComposablePipelineError::AlreadyRunning(name.to_string()));
        }

        rt.state = ComposablePipelineState::Running;
        rt.started_at = Some(Instant::now());
        Ok(())
    }

    /// Stop a pipeline.
    pub fn stop_pipeline(&self, name: &str) -> Result<(), ComposablePipelineError> {
        let mut pipelines = self.pipelines.write();
        let rt = pipelines
            .get_mut(name)
            .ok_or_else(|| ComposablePipelineError::NotFound(name.to_string()))?;

        rt.state = ComposablePipelineState::Stopped;
        rt.started_at = None;
        Ok(())
    }

    /// Pause a running pipeline.
    pub fn pause_pipeline(&self, name: &str) -> Result<(), ComposablePipelineError> {
        let mut pipelines = self.pipelines.write();
        let rt = pipelines
            .get_mut(name)
            .ok_or_else(|| ComposablePipelineError::NotFound(name.to_string()))?;

        if rt.state != ComposablePipelineState::Running {
            return Err(ComposablePipelineError::NotRunning(name.to_string()));
        }

        rt.state = ComposablePipelineState::Paused;
        Ok(())
    }

    /// Resume a paused pipeline.
    pub fn resume_pipeline(&self, name: &str) -> Result<(), ComposablePipelineError> {
        let mut pipelines = self.pipelines.write();
        let rt = pipelines
            .get_mut(name)
            .ok_or_else(|| ComposablePipelineError::NotFound(name.to_string()))?;

        if rt.state != ComposablePipelineState::Paused {
            return Err(ComposablePipelineError::NotRunning(name.to_string()));
        }

        rt.state = ComposablePipelineState::Running;
        Ok(())
    }

    /// Delete a pipeline.
    pub fn delete_pipeline(&self, name: &str) -> Result<(), ComposablePipelineError> {
        let mut pipelines = self.pipelines.write();
        if pipelines.remove(name).is_none() {
            return Err(ComposablePipelineError::NotFound(name.to_string()));
        }
        Ok(())
    }

    /// Get pipeline info.
    pub fn get_pipeline(&self, name: &str) -> Option<ComposablePipelineInfo> {
        let pipelines = self.pipelines.read();
        pipelines.get(name).map(|rt| ComposablePipelineInfo {
            name: rt.definition.name.clone(),
            state: rt.state,
            stages: rt.definition.stages.len(),
            created_at: rt.definition.created_at,
            started_at: rt.started_at.map(|s| s.elapsed().as_secs()),
            records_processed: rt.records_processed,
            errors: rt.errors,
        })
    }

    /// List all pipelines.
    pub fn list_pipelines(&self) -> Vec<ComposablePipelineInfo> {
        let pipelines = self.pipelines.read();
        pipelines
            .values()
            .map(|rt| ComposablePipelineInfo {
                name: rt.definition.name.clone(),
                state: rt.state,
                stages: rt.definition.stages.len(),
                created_at: rt.definition.created_at,
                started_at: rt.started_at.map(|s| s.elapsed().as_secs()),
                records_processed: rt.records_processed,
                errors: rt.errors,
            })
            .collect()
    }

    /// Get the DAG topology of a pipeline.
    pub fn pipeline_topology(&self, name: &str) -> Option<PipelineTopology> {
        let pipelines = self.pipelines.read();
        let rt = pipelines.get(name)?;

        let stages: Vec<TopologyNode> = rt
            .definition
            .stages
            .iter()
            .map(|s| TopologyNode {
                name: s.name.clone(),
                stage_type: match &s.stage_type {
                    StageType::Source(_) => "SOURCE".to_string(),
                    StageType::Transform(_) => "TRANSFORM".to_string(),
                    StageType::Sink(_) => "SINK".to_string(),
                },
                state: rt.state.to_string(),
            })
            .collect();

        let mut edges = Vec::new();
        for stage in &rt.definition.stages {
            for next in &stage.next {
                edges.push(TopologyEdge {
                    from: stage.name.clone(),
                    to: next.clone(),
                    records_passed: 0,
                });
            }
        }

        Some(PipelineTopology { stages, edges })
    }

    /// Get metrics for a pipeline.
    pub fn pipeline_metrics(&self, name: &str) -> Option<PipelineMetrics> {
        let pipelines = self.pipelines.read();
        let rt = pipelines.get(name)?;

        let elapsed = rt
            .started_at
            .map(|s| s.elapsed().as_secs_f64())
            .unwrap_or(1.0);

        let throughput = if elapsed > 0.0 {
            rt.records_processed as f64 / elapsed
        } else {
            0.0
        };

        let per_stage: HashMap<String, StageMetrics> = rt
            .definition
            .stages
            .iter()
            .map(|s| {
                (
                    s.name.clone(),
                    StageMetrics {
                        records_in: rt.records_processed / rt.definition.stages.len().max(1) as u64,
                        records_out: rt.records_processed
                            / rt.definition.stages.len().max(1) as u64,
                        errors: 0,
                        avg_latency_ms: 0.0,
                    },
                )
            })
            .collect();

        Some(PipelineMetrics {
            name: name.to_string(),
            records_in: rt.records_processed,
            records_out: rt.records_processed.saturating_sub(rt.errors),
            records_error: rt.errors,
            throughput_per_sec: throughput,
            avg_latency_ms: 0.0,
            backpressure_pct: 0.0,
            per_stage,
        })
    }

    /// Validate a pipeline definition. Returns a list of errors (empty = valid).
    pub fn validate_pipeline(&self, def: &ComposablePipelineDefinition) -> Vec<ValidationError> {
        let mut errors = Vec::new();

        if def.name.is_empty() {
            errors.push(ValidationError {
                stage: "(pipeline)".to_string(),
                message: "pipeline name is required".to_string(),
                severity: "error".to_string(),
            });
        }

        // Check for at least one source and one sink
        let has_source = def
            .stages
            .iter()
            .any(|s| matches!(s.stage_type, StageType::Source(_)));
        let has_sink = def
            .stages
            .iter()
            .any(|s| matches!(s.stage_type, StageType::Sink(_)));

        if !has_source {
            errors.push(ValidationError {
                stage: "(pipeline)".to_string(),
                message: "pipeline must have at least one source stage".to_string(),
                severity: "error".to_string(),
            });
        }
        if !has_sink {
            errors.push(ValidationError {
                stage: "(pipeline)".to_string(),
                message: "pipeline must have at least one sink stage".to_string(),
                severity: "error".to_string(),
            });
        }

        // Check all stage references in next are valid
        let stage_names: HashSet<&str> = def.stages.iter().map(|s| s.name.as_str()).collect();
        for stage in &def.stages {
            for next in &stage.next {
                if !stage_names.contains(next.as_str()) {
                    errors.push(ValidationError {
                        stage: stage.name.clone(),
                        message: format!("references unknown stage '{}'", next),
                        severity: "error".to_string(),
                    });
                }
            }
        }

        // Check unique stage names
        let mut seen: HashSet<&str> = HashSet::new();
        for stage in &def.stages {
            if !seen.insert(stage.name.as_str()) {
                errors.push(ValidationError {
                    stage: stage.name.clone(),
                    message: "duplicate stage name".to_string(),
                    severity: "error".to_string(),
                });
            }
        }

        // Check for cycles via topological sort
        if self.has_cycle(def) {
            errors.push(ValidationError {
                stage: "(pipeline)".to_string(),
                message: "pipeline DAG contains a cycle".to_string(),
                severity: "error".to_string(),
            });
        }

        errors
    }

    /// Get aggregate stats across all managed pipelines.
    pub fn stats(&self) -> PipelineManagerStats {
        let pipelines = self.pipelines.read();
        let mut total_records = 0u64;
        let mut total_errors = 0u64;
        let mut running = 0usize;

        for rt in pipelines.values() {
            total_records += rt.records_processed;
            total_errors += rt.errors;
            if rt.state == ComposablePipelineState::Running {
                running += 1;
            }
        }

        PipelineManagerStats {
            total_pipelines: pipelines.len(),
            running,
            total_records,
            total_errors,
        }
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Check for cycles using Kahn's algorithm (topological sort).
    fn has_cycle(&self, def: &ComposablePipelineDefinition) -> bool {
        let mut in_degree: HashMap<&str, usize> = HashMap::new();
        let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();

        for stage in &def.stages {
            in_degree.entry(stage.name.as_str()).or_insert(0);
            adj.entry(stage.name.as_str()).or_default();
            for next in &stage.next {
                *in_degree.entry(next.as_str()).or_insert(0) += 1;
                adj.entry(stage.name.as_str())
                    .or_default()
                    .push(next.as_str());
            }
        }

        let mut queue: VecDeque<&str> = in_degree
            .iter()
            .filter(|(_, &deg)| deg == 0)
            .map(|(&name, _)| name)
            .collect();

        let mut visited_count = 0usize;

        while let Some(node) = queue.pop_front() {
            visited_count += 1;
            if let Some(neighbors) = adj.get(node) {
                for &next in neighbors {
                    if let Some(deg) = in_degree.get_mut(next) {
                        *deg -= 1;
                        if *deg == 0 {
                            queue.push_back(next);
                        }
                    }
                }
            }
        }

        visited_count != in_degree.len()
    }
}

impl Default for ComposablePipelineManager {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_source_stage(name: &str) -> PipelineStage {
        PipelineStage {
            name: name.to_string(),
            stage_type: StageType::Source(SourceConfig {
                source_type: SourceKind::Ferrite,
                connection: "localhost".to_string(),
                topic_or_pattern: "events:*".to_string(),
            }),
            config: HashMap::new(),
            next: Vec::new(),
        }
    }

    fn make_transform_stage(name: &str) -> PipelineStage {
        PipelineStage {
            name: name.to_string(),
            stage_type: StageType::Transform(TransformConfig {
                transform_type: TransformKind::Filter {
                    condition: "type == click".to_string(),
                },
                params: HashMap::new(),
            }),
            config: HashMap::new(),
            next: Vec::new(),
        }
    }

    fn make_sink_stage(name: &str) -> PipelineStage {
        PipelineStage {
            name: name.to_string(),
            stage_type: StageType::Sink(SinkConfig {
                sink_type: SinkKind::Ferrite,
                connection: "localhost".to_string(),
                target: "output:*".to_string(),
            }),
            config: HashMap::new(),
            next: Vec::new(),
        }
    }

    fn make_valid_pipeline(name: &str) -> ComposablePipelineDefinition {
        let mut source = make_source_stage("src");
        source.next = vec!["xform".to_string()];
        let mut xform = make_transform_stage("xform");
        xform.next = vec!["sink".to_string()];
        let sink = make_sink_stage("sink");

        ComposablePipelineDefinition {
            name: name.to_string(),
            stages: vec![source, xform, sink],
            ..Default::default()
        }
    }

    #[test]
    fn test_create_pipeline() {
        let mgr = ComposablePipelineManager::new();
        let def = make_valid_pipeline("test-pipe");
        let result = mgr.create_pipeline(def);
        assert!(result.is_ok());
        assert_eq!(result.as_ref().ok(), Some(&"test-pipe".to_string()));
    }

    #[test]
    fn test_duplicate_pipeline() {
        let mgr = ComposablePipelineManager::new();
        let def1 = make_valid_pipeline("dup");
        let def2 = make_valid_pipeline("dup");
        assert!(mgr.create_pipeline(def1).is_ok());
        assert!(matches!(
            mgr.create_pipeline(def2),
            Err(ComposablePipelineError::AlreadyExists(_))
        ));
    }

    #[test]
    fn test_start_stop_lifecycle() {
        let mgr = ComposablePipelineManager::new();
        mgr.create_pipeline(make_valid_pipeline("lc")).ok();

        assert!(mgr.start_pipeline("lc").is_ok());
        let info = mgr.get_pipeline("lc");
        assert!(info.is_some());
        assert_eq!(
            info.as_ref().map(|i| i.state),
            Some(ComposablePipelineState::Running)
        );

        assert!(mgr.stop_pipeline("lc").is_ok());
        let info = mgr.get_pipeline("lc");
        assert_eq!(
            info.as_ref().map(|i| i.state),
            Some(ComposablePipelineState::Stopped)
        );
    }

    #[test]
    fn test_pause_resume() {
        let mgr = ComposablePipelineManager::new();
        mgr.create_pipeline(make_valid_pipeline("pr")).ok();
        mgr.start_pipeline("pr").ok();
        assert!(mgr.pause_pipeline("pr").is_ok());
        assert_eq!(
            mgr.get_pipeline("pr").map(|i| i.state),
            Some(ComposablePipelineState::Paused)
        );
        assert!(mgr.resume_pipeline("pr").is_ok());
        assert_eq!(
            mgr.get_pipeline("pr").map(|i| i.state),
            Some(ComposablePipelineState::Running)
        );
    }

    #[test]
    fn test_delete_pipeline() {
        let mgr = ComposablePipelineManager::new();
        mgr.create_pipeline(make_valid_pipeline("del")).ok();
        assert!(mgr.delete_pipeline("del").is_ok());
        assert!(mgr.get_pipeline("del").is_none());
        assert!(matches!(
            mgr.delete_pipeline("del"),
            Err(ComposablePipelineError::NotFound(_))
        ));
    }

    #[test]
    fn test_validation_no_source() {
        let mgr = ComposablePipelineManager::new();
        let def = ComposablePipelineDefinition {
            name: "bad".to_string(),
            stages: vec![make_sink_stage("sink")],
            ..Default::default()
        };
        let errors = mgr.validate_pipeline(&def);
        assert!(errors.iter().any(|e| e.message.contains("source")));
    }

    #[test]
    fn test_validation_no_sink() {
        let mgr = ComposablePipelineManager::new();
        let def = ComposablePipelineDefinition {
            name: "bad".to_string(),
            stages: vec![make_source_stage("src")],
            ..Default::default()
        };
        let errors = mgr.validate_pipeline(&def);
        assert!(errors.iter().any(|e| e.message.contains("sink")));
    }

    #[test]
    fn test_cycle_detection() {
        let mgr = ComposablePipelineManager::new();
        let mut s1 = make_source_stage("s1");
        s1.next = vec!["s2".to_string()];
        let mut s2 = make_transform_stage("s2");
        s2.next = vec!["s3".to_string()];
        let mut s3 = make_sink_stage("s3");
        s3.next = vec!["s1".to_string()]; // cycle!

        let def = ComposablePipelineDefinition {
            name: "cyclic".to_string(),
            stages: vec![s1, s2, s3],
            ..Default::default()
        };
        let errors = mgr.validate_pipeline(&def);
        assert!(errors.iter().any(|e| e.message.contains("cycle")));
    }

    #[test]
    fn test_invalid_stage_reference() {
        let mgr = ComposablePipelineManager::new();
        let mut src = make_source_stage("src");
        src.next = vec!["nonexistent".to_string()];
        let sink = make_sink_stage("sink");

        let def = ComposablePipelineDefinition {
            name: "bad-ref".to_string(),
            stages: vec![src, sink],
            ..Default::default()
        };
        let errors = mgr.validate_pipeline(&def);
        assert!(errors.iter().any(|e| e.message.contains("unknown stage")));
    }

    #[test]
    fn test_topology() {
        let mgr = ComposablePipelineManager::new();
        mgr.create_pipeline(make_valid_pipeline("topo")).ok();
        let topo = mgr.pipeline_topology("topo");
        assert!(topo.is_some());
        let topo = topo.as_ref().expect("topology");
        assert_eq!(topo.stages.len(), 3);
        assert_eq!(topo.edges.len(), 2);
    }

    #[test]
    fn test_metrics() {
        let mgr = ComposablePipelineManager::new();
        mgr.create_pipeline(make_valid_pipeline("met")).ok();
        mgr.start_pipeline("met").ok();
        let metrics = mgr.pipeline_metrics("met");
        assert!(metrics.is_some());
        assert_eq!(metrics.as_ref().map(|m| m.name.as_str()), Some("met"));
    }

    #[test]
    fn test_list_and_stats() {
        let mgr = ComposablePipelineManager::new();
        mgr.create_pipeline(make_valid_pipeline("p1")).ok();
        mgr.create_pipeline(make_valid_pipeline("p2")).ok();
        mgr.start_pipeline("p1").ok();

        let list = mgr.list_pipelines();
        assert_eq!(list.len(), 2);

        let stats = mgr.stats();
        assert_eq!(stats.total_pipelines, 2);
        assert_eq!(stats.running, 1);
    }

    #[test]
    fn test_already_running_error() {
        let mgr = ComposablePipelineManager::new();
        mgr.create_pipeline(make_valid_pipeline("ar")).ok();
        mgr.start_pipeline("ar").ok();
        assert!(matches!(
            mgr.start_pipeline("ar"),
            Err(ComposablePipelineError::AlreadyRunning(_))
        ));
    }
}
