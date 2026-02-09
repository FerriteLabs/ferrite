//! Observability Correlation Engine
//!
//! Correlates signals across the observability stack — slow queries, anomaly
//! alerts, distributed traces, and system metrics — to produce unified
//! incident insights and auto-generated runbooks.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐
//! │  Slow    │ │ Anomaly  │ │  Trace   │ │  Metric  │
//! │ Queries  │ │ Alerts   │ │  Spans   │ │  Samples │
//! └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘
//!      │            │            │             │
//!      └────────────┴────────────┴─────────────┘
//!                         │
//!                ┌────────▼────────┐
//!                │  Correlation    │
//!                │    Engine       │
//!                └────────┬────────┘
//!                         │
//!             ┌───────────┼───────────┐
//!             ▼           ▼           ▼
//!      ┌──────────┐ ┌──────────┐ ┌──────────┐
//!      │ Incident │ │ Runbook  │ │ Root     │
//!      │ Timeline │ │ Generator│ │ Cause    │
//!      └──────────┘ └──────────┘ └──────────┘
//! ```
//!
//! # Features
//!
//! - **Time-windowed correlation**: Groups related signals within configurable windows
//! - **Incident detection**: Identifies cascading failures across components
//! - **Root cause analysis**: Ranks probable causes by temporal and causal ordering
//! - **Auto-runbook generation**: Produces step-by-step remediation for known patterns

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Signal Types
// ---------------------------------------------------------------------------

/// A signal from any observability source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signal {
    /// Unique signal identifier
    pub id: String,
    /// When the signal was emitted (epoch microseconds)
    pub timestamp_us: u64,
    /// Which component/node generated this signal
    pub source: String,
    /// The kind of signal
    pub kind: SignalKind,
    /// Severity level
    pub severity: Severity,
    /// Arbitrary metadata
    pub metadata: HashMap<String, String>,
}

/// The kind of observability signal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SignalKind {
    /// A query exceeded the slow query threshold
    SlowQuery {
        /// The command that was slow.
        command: String,
        /// Observed latency in microseconds.
        latency_us: u64,
    },
    /// Anomaly detector fired an alert
    AnomalyAlert {
        /// Name of the metric that triggered the alert.
        metric: String,
        /// Z-score that exceeded the threshold.
        z_score: i64,
    },
    /// Distributed trace span
    TraceSpan {
        /// Trace identifier.
        trace_id: String,
        /// Span identifier.
        span_id: String,
        /// Operation name.
        operation: String,
        /// Span duration in microseconds.
        duration_us: u64,
    },
    /// System metric sample
    MetricSample {
        /// Metric name.
        name: String,
        /// Observed value.
        value: f64,
        /// Unit of measurement.
        unit: String,
    },
    /// Error event
    Error {
        /// Type/class of the error.
        error_type: String,
        /// Error message.
        message: String,
    },
    /// Resource exhaustion signal
    ResourceExhaustion {
        /// Name of the exhausted resource.
        resource: String,
        /// Current usage as a percentage.
        usage_percent: f64,
    },
}

impl fmt::Display for SignalKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SignalKind::SlowQuery {
                command,
                latency_us,
            } => {
                write!(f, "SlowQuery({}, {}us)", command, latency_us)
            }
            SignalKind::AnomalyAlert { metric, z_score } => {
                write!(f, "Anomaly({}, z={})", metric, z_score)
            }
            SignalKind::TraceSpan {
                operation,
                duration_us,
                ..
            } => {
                write!(f, "Trace({}, {}us)", operation, duration_us)
            }
            SignalKind::MetricSample { name, value, .. } => {
                write!(f, "Metric({}, {:.2})", name, value)
            }
            SignalKind::Error { error_type, .. } => write!(f, "Error({})", error_type),
            SignalKind::ResourceExhaustion {
                resource,
                usage_percent,
            } => {
                write!(f, "Resource({}, {:.1}%)", resource, usage_percent)
            }
        }
    }
}

/// Severity level for observability signals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Severity {
    /// Informational event.
    Info,
    /// Warning that may require attention.
    Warning,
    /// Error requiring investigation.
    Error,
    /// Critical issue requiring immediate action.
    Critical,
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Severity::Info => write!(f, "INFO"),
            Severity::Warning => write!(f, "WARN"),
            Severity::Error => write!(f, "ERROR"),
            Severity::Critical => write!(f, "CRITICAL"),
        }
    }
}

// ---------------------------------------------------------------------------
// Correlation
// ---------------------------------------------------------------------------

/// Configuration for the correlation engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorrelationConfig {
    /// Time window for grouping related signals (microseconds)
    pub correlation_window_us: u64,
    /// Minimum signals to form an incident
    pub min_signals_for_incident: usize,
    /// Maximum signals retained in the buffer
    pub max_buffer_size: usize,
    /// Minimum severity to trigger incident creation
    pub incident_threshold: Severity,
    /// Maximum number of incidents to retain
    pub max_incidents: usize,
}

impl Default for CorrelationConfig {
    fn default() -> Self {
        Self {
            correlation_window_us: 5_000_000, // 5 seconds
            min_signals_for_incident: 3,
            max_buffer_size: 10_000,
            incident_threshold: Severity::Warning,
            max_incidents: 100,
        }
    }
}

/// A correlated group of signals forming an incident.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Incident {
    /// Unique incident identifier.
    pub id: String,
    /// When the incident started (earliest signal)
    pub start_us: u64,
    /// When the incident ended (latest signal, or ongoing)
    pub end_us: Option<u64>,
    /// Overall severity (max of constituent signals)
    pub severity: Severity,
    /// Signals grouped into this incident
    pub signals: Vec<Signal>,
    /// Detected root cause, if any
    pub root_cause: Option<RootCause>,
    /// Generated runbook steps
    pub runbook: Vec<RunbookStep>,
    /// Current incident status.
    pub status: IncidentStatus,
}

impl Incident {
    fn new(id: String, first_signal: Signal) -> Self {
        let start = first_signal.timestamp_us;
        let severity = first_signal.severity;
        Self {
            id,
            start_us: start,
            end_us: None,
            severity,
            signals: vec![first_signal],
            root_cause: None,
            runbook: Vec::new(),
            status: IncidentStatus::Active,
        }
    }

    fn add_signal(&mut self, signal: Signal) {
        if signal.severity > self.severity {
            self.severity = signal.severity;
        }
        self.end_us = Some(signal.timestamp_us);
        self.signals.push(signal);
    }

    fn duration_us(&self) -> u64 {
        self.end_us.unwrap_or(self.start_us) - self.start_us
    }

    /// Affected components (unique sources)
    pub fn affected_components(&self) -> Vec<String> {
        let mut sources: Vec<String> = self.signals.iter().map(|s| s.source.clone()).collect();
        sources.sort();
        sources.dedup();
        sources
    }
}

impl fmt::Display for Incident {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Incident[{}] {} severity={} signals={} duration={}us",
            self.id,
            self.status,
            self.severity,
            self.signals.len(),
            self.duration_us(),
        )
    }
}

/// Status of an incident in its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IncidentStatus {
    /// Incident is ongoing and unresolved.
    Active,
    /// Incident has been resolved.
    Resolved,
    /// Incident has been acknowledged by an operator.
    Acknowledged,
}

impl fmt::Display for IncidentStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IncidentStatus::Active => write!(f, "ACTIVE"),
            IncidentStatus::Resolved => write!(f, "RESOLVED"),
            IncidentStatus::Acknowledged => write!(f, "ACK"),
        }
    }
}

// ---------------------------------------------------------------------------
// Root Cause Analysis
// ---------------------------------------------------------------------------

/// Identified root cause of an incident.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RootCause {
    /// Category of the root cause.
    pub category: RootCauseCategory,
    /// Human-readable description of the root cause.
    pub description: String,
    /// Confidence score (0.0–1.0).
    pub confidence: f64,
    /// IDs of signals that contributed to this diagnosis.
    pub contributing_signals: Vec<String>,
}

/// Category of an identified root cause.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RootCauseCategory {
    /// High memory usage leading to evictions / OOM
    MemoryPressure,
    /// CPU saturation causing latency spikes
    CpuSaturation,
    /// Disk I/O bottleneck
    DiskBottleneck,
    /// Network issues (partition, high latency)
    NetworkIssue,
    /// Hot key causing contention
    HotKey,
    /// Connection pool exhaustion
    ConnectionExhaustion,
    /// Replication lag
    ReplicationLag,
    /// Unknown
    Unknown,
}

impl fmt::Display for RootCauseCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RootCauseCategory::MemoryPressure => write!(f, "Memory Pressure"),
            RootCauseCategory::CpuSaturation => write!(f, "CPU Saturation"),
            RootCauseCategory::DiskBottleneck => write!(f, "Disk Bottleneck"),
            RootCauseCategory::NetworkIssue => write!(f, "Network Issue"),
            RootCauseCategory::HotKey => write!(f, "Hot Key Contention"),
            RootCauseCategory::ConnectionExhaustion => write!(f, "Connection Exhaustion"),
            RootCauseCategory::ReplicationLag => write!(f, "Replication Lag"),
            RootCauseCategory::Unknown => write!(f, "Unknown"),
        }
    }
}

// ---------------------------------------------------------------------------
// Runbook Generation
// ---------------------------------------------------------------------------

/// A step in an auto-generated runbook.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunbookStep {
    /// Step execution order (1-based).
    pub order: u32,
    /// Short title for the step.
    pub title: String,
    /// Detailed instructions for the step.
    pub description: String,
    /// Type of action this step represents.
    pub action_type: ActionType,
}

/// Type of action in a runbook step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActionType {
    /// Gather diagnostic information.
    Diagnostic,
    /// Apply a fix or remediation.
    Remediation,
    /// Escalate to another team or person.
    Escalation,
    /// Verify that the remediation was effective.
    Verification,
}

impl fmt::Display for ActionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ActionType::Diagnostic => write!(f, "DIAGNOSTIC"),
            ActionType::Remediation => write!(f, "REMEDIATION"),
            ActionType::Escalation => write!(f, "ESCALATION"),
            ActionType::Verification => write!(f, "VERIFICATION"),
        }
    }
}

/// Generate runbook steps based on the root cause category.
fn generate_runbook(category: &RootCauseCategory) -> Vec<RunbookStep> {
    match category {
        RootCauseCategory::MemoryPressure => vec![
            RunbookStep {
                order: 1,
                title: "Check memory usage".to_string(),
                description: "Run INFO MEMORY and check used_memory_rss vs maxmemory".to_string(),
                action_type: ActionType::Diagnostic,
            },
            RunbookStep {
                order: 2,
                title: "Identify large keys".to_string(),
                description: "Use MEMORY USAGE on suspected large keys or run memory analysis"
                    .to_string(),
                action_type: ActionType::Diagnostic,
            },
            RunbookStep {
                order: 3,
                title: "Set eviction policy".to_string(),
                description: "Configure maxmemory-policy to allkeys-lru or volatile-lru"
                    .to_string(),
                action_type: ActionType::Remediation,
            },
            RunbookStep {
                order: 4,
                title: "Verify memory usage decreased".to_string(),
                description: "Re-check INFO MEMORY after remediation".to_string(),
                action_type: ActionType::Verification,
            },
        ],
        RootCauseCategory::CpuSaturation => vec![
            RunbookStep {
                order: 1,
                title: "Check CPU-heavy commands".to_string(),
                description: "Review SLOWLOG and identify O(N) commands like KEYS, SMEMBERS"
                    .to_string(),
                action_type: ActionType::Diagnostic,
            },
            RunbookStep {
                order: 2,
                title: "Optimize hot commands".to_string(),
                description: "Replace KEYS with SCAN, add indexes, or cache results".to_string(),
                action_type: ActionType::Remediation,
            },
        ],
        RootCauseCategory::DiskBottleneck => vec![
            RunbookStep {
                order: 1,
                title: "Check I/O wait".to_string(),
                description: "Monitor disk I/O with iostat, check for high await times".to_string(),
                action_type: ActionType::Diagnostic,
            },
            RunbookStep {
                order: 2,
                title: "Adjust AOF fsync policy".to_string(),
                description: "Consider setting appendfsync to everysec instead of always"
                    .to_string(),
                action_type: ActionType::Remediation,
            },
        ],
        RootCauseCategory::NetworkIssue => vec![
            RunbookStep {
                order: 1,
                title: "Check network connectivity".to_string(),
                description: "Verify reachability between nodes, check for packet loss".to_string(),
                action_type: ActionType::Diagnostic,
            },
            RunbookStep {
                order: 2,
                title: "Escalate to network team".to_string(),
                description: "If packet loss detected, escalate to infrastructure team".to_string(),
                action_type: ActionType::Escalation,
            },
        ],
        RootCauseCategory::HotKey => vec![
            RunbookStep {
                order: 1,
                title: "Identify hot keys".to_string(),
                description: "Use MONITOR or client-side tracking to find hot keys".to_string(),
                action_type: ActionType::Diagnostic,
            },
            RunbookStep {
                order: 2,
                title: "Distribute hot key access".to_string(),
                description: "Use read replicas, client-side caching, or key sharding".to_string(),
                action_type: ActionType::Remediation,
            },
        ],
        RootCauseCategory::ConnectionExhaustion => vec![
            RunbookStep {
                order: 1,
                title: "Check connection count".to_string(),
                description: "Run CLIENT LIST, check against maxclients setting".to_string(),
                action_type: ActionType::Diagnostic,
            },
            RunbookStep {
                order: 2,
                title: "Enable connection pooling".to_string(),
                description: "Ensure clients use connection pools with appropriate limits"
                    .to_string(),
                action_type: ActionType::Remediation,
            },
        ],
        RootCauseCategory::ReplicationLag => vec![
            RunbookStep {
                order: 1,
                title: "Check replication status".to_string(),
                description: "Run INFO REPLICATION, check master_link_status and lag".to_string(),
                action_type: ActionType::Diagnostic,
            },
            RunbookStep {
                order: 2,
                title: "Tune replication buffer".to_string(),
                description: "Increase repl-backlog-size if lag is due to buffer overflow"
                    .to_string(),
                action_type: ActionType::Remediation,
            },
        ],
        RootCauseCategory::Unknown => vec![RunbookStep {
            order: 1,
            title: "Collect diagnostic data".to_string(),
            description: "Gather INFO ALL, SLOWLOG, and system metrics for analysis".to_string(),
            action_type: ActionType::Diagnostic,
        }],
    }
}

// ---------------------------------------------------------------------------
// Correlation Engine
// ---------------------------------------------------------------------------

/// Metrics for the correlation engine.
#[derive(Debug, Default)]
pub struct CorrelationMetrics {
    /// Total signals ingested.
    pub signals_ingested: AtomicU64,
    /// Total incidents created.
    pub incidents_created: AtomicU64,
    /// Total incidents resolved.
    pub incidents_resolved: AtomicU64,
    /// Total root causes successfully identified.
    pub root_causes_identified: AtomicU64,
}

/// The main correlation engine that groups signals into incidents.
pub struct CorrelationEngine {
    config: CorrelationConfig,
    /// Buffered signals awaiting correlation
    signal_buffer: RwLock<VecDeque<Signal>>,
    /// Active and recent incidents
    incidents: RwLock<Vec<Incident>>,
    /// Metrics
    metrics: Arc<CorrelationMetrics>,
    /// Next incident ID counter
    next_incident_id: AtomicU64,
}

impl CorrelationEngine {
    /// Create a new correlation engine with the given configuration.
    pub fn new(config: CorrelationConfig) -> Self {
        Self {
            config,
            signal_buffer: RwLock::new(VecDeque::new()),
            incidents: RwLock::new(Vec::new()),
            metrics: Arc::new(CorrelationMetrics::default()),
            next_incident_id: AtomicU64::new(1),
        }
    }

    /// Ingest a new signal into the correlation engine.
    pub fn ingest(&self, signal: Signal) {
        self.metrics
            .signals_ingested
            .fetch_add(1, Ordering::Relaxed);

        let mut buffer = self.signal_buffer.write();

        // Evict old signals if buffer is full
        while buffer.len() >= self.config.max_buffer_size {
            buffer.pop_front();
        }

        buffer.push_back(signal);
    }

    /// Run correlation on buffered signals, producing/updating incidents.
    pub fn correlate(&self) -> Vec<Incident> {
        let mut buffer = self.signal_buffer.write();
        let mut incidents = self.incidents.write();

        let signals: Vec<Signal> = buffer.drain(..).collect();
        let mut new_incidents = Vec::new();

        for signal in signals {
            if signal.severity < self.config.incident_threshold {
                continue;
            }

            // Try to correlate with an existing active incident
            let mut matched = false;
            for incident in incidents.iter_mut() {
                if incident.status != IncidentStatus::Active {
                    continue;
                }
                if self.signals_correlate(&signal, incident) {
                    incident.add_signal(signal.clone());
                    matched = true;
                    break;
                }
            }

            if !matched {
                // Start a new incident
                let id = format!(
                    "INC-{}",
                    self.next_incident_id.fetch_add(1, Ordering::Relaxed)
                );
                let incident = Incident::new(id, signal);
                self.metrics
                    .incidents_created
                    .fetch_add(1, Ordering::Relaxed);
                incidents.push(incident);
            }
        }

        // Analyze incidents with enough signals
        for incident in incidents.iter_mut() {
            if incident.status == IncidentStatus::Active
                && incident.signals.len() >= self.config.min_signals_for_incident
                && incident.root_cause.is_none()
            {
                if let Some(root_cause) = self.analyze_root_cause(incident) {
                    let runbook = generate_runbook(&root_cause.category);
                    incident.root_cause = Some(root_cause);
                    incident.runbook = runbook;
                    self.metrics
                        .root_causes_identified
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        // Cap incidents
        while incidents.len() > self.config.max_incidents {
            incidents.remove(0);
        }

        // Return newly created/updated active incidents
        for inc in incidents.iter() {
            if inc.status == IncidentStatus::Active {
                new_incidents.push(inc.clone());
            }
        }

        new_incidents
    }

    /// Check if a signal correlates with an existing incident.
    fn signals_correlate(&self, signal: &Signal, incident: &Incident) -> bool {
        // Time proximity check
        let last_signal_time = incident
            .signals
            .last()
            .map(|s| s.timestamp_us)
            .unwrap_or(incident.start_us);

        let time_diff = signal.timestamp_us.abs_diff(last_signal_time);

        if time_diff > self.config.correlation_window_us {
            return false;
        }

        // Source proximity: same source is always correlated
        if incident.signals.iter().any(|s| s.source == signal.source) {
            return true;
        }

        // Signal kind proximity: same type of signal correlates
        let sig_tag = signal_kind_tag(&signal.kind);
        if incident
            .signals
            .iter()
            .any(|s| signal_kind_tag(&s.kind) == sig_tag)
        {
            return true;
        }

        false
    }

    /// Analyze signals in an incident to determine probable root cause.
    fn analyze_root_cause(&self, incident: &Incident) -> Option<RootCause> {
        let mut category_scores: HashMap<RootCauseCategory, (f64, Vec<String>)> = HashMap::new();

        for signal in &incident.signals {
            let (cat, score) = match &signal.kind {
                SignalKind::ResourceExhaustion {
                    resource,
                    usage_percent,
                } => {
                    let cat = if resource.contains("memory") || resource.contains("mem") {
                        RootCauseCategory::MemoryPressure
                    } else if resource.contains("cpu") {
                        RootCauseCategory::CpuSaturation
                    } else if resource.contains("disk") || resource.contains("io") {
                        RootCauseCategory::DiskBottleneck
                    } else if resource.contains("connection") || resource.contains("conn") {
                        RootCauseCategory::ConnectionExhaustion
                    } else {
                        RootCauseCategory::Unknown
                    };
                    (cat, *usage_percent / 100.0)
                }
                SignalKind::SlowQuery { latency_us, .. } => {
                    let score = (*latency_us as f64 / 1_000_000.0).min(1.0);
                    (RootCauseCategory::CpuSaturation, score)
                }
                SignalKind::AnomalyAlert { metric, .. } => {
                    let cat = if metric.contains("memory") {
                        RootCauseCategory::MemoryPressure
                    } else if metric.contains("latency") || metric.contains("cpu") {
                        RootCauseCategory::CpuSaturation
                    } else if metric.contains("disk") {
                        RootCauseCategory::DiskBottleneck
                    } else if metric.contains("replication") || metric.contains("lag") {
                        RootCauseCategory::ReplicationLag
                    } else {
                        RootCauseCategory::Unknown
                    };
                    (cat, 0.8)
                }
                SignalKind::Error { error_type, .. } => {
                    let cat = if error_type.contains("OOM") || error_type.contains("memory") {
                        RootCauseCategory::MemoryPressure
                    } else if error_type.contains("network") || error_type.contains("connection") {
                        RootCauseCategory::NetworkIssue
                    } else if error_type.contains("disk") || error_type.contains("io") {
                        RootCauseCategory::DiskBottleneck
                    } else {
                        RootCauseCategory::Unknown
                    };
                    (cat, 0.9)
                }
                SignalKind::TraceSpan { duration_us, .. } => {
                    let score = (*duration_us as f64 / 1_000_000.0).min(1.0);
                    (RootCauseCategory::CpuSaturation, score * 0.5)
                }
                SignalKind::MetricSample { name, value, .. } => {
                    let cat = if name.contains("memory") && *value > 80.0 {
                        RootCauseCategory::MemoryPressure
                    } else if name.contains("cpu") && *value > 80.0 {
                        RootCauseCategory::CpuSaturation
                    } else {
                        continue;
                    };
                    (cat, *value / 100.0)
                }
            };

            let entry = category_scores
                .entry(cat)
                .or_insert_with(|| (0.0, Vec::new()));
            entry.0 += score;
            entry.1.push(signal.id.clone());
        }

        // Select highest-scoring category
        category_scores
            .into_iter()
            .max_by(|a, b| {
                a.1 .0
                    .partial_cmp(&b.1 .0)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(cat, (score, signal_ids))| {
                let max_possible = incident.signals.len() as f64;
                RootCause {
                    category: cat.clone(),
                    description: format!(
                        "Probable {} based on {} correlated signals",
                        cat,
                        signal_ids.len()
                    ),
                    confidence: (score / max_possible).min(1.0),
                    contributing_signals: signal_ids,
                }
            })
    }

    /// Resolve an incident by ID.
    pub fn resolve(&self, incident_id: &str) -> bool {
        let mut incidents = self.incidents.write();
        if let Some(inc) = incidents.iter_mut().find(|i| i.id == incident_id) {
            inc.status = IncidentStatus::Resolved;
            self.metrics
                .incidents_resolved
                .fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Acknowledge an incident by ID.
    pub fn acknowledge(&self, incident_id: &str) -> bool {
        let mut incidents = self.incidents.write();
        if let Some(inc) = incidents.iter_mut().find(|i| i.id == incident_id) {
            inc.status = IncidentStatus::Acknowledged;
            true
        } else {
            false
        }
    }

    /// Get all active incidents.
    pub fn active_incidents(&self) -> Vec<Incident> {
        self.incidents
            .read()
            .iter()
            .filter(|i| i.status == IncidentStatus::Active)
            .cloned()
            .collect()
    }

    /// Get a specific incident by ID.
    pub fn get_incident(&self, id: &str) -> Option<Incident> {
        self.incidents.read().iter().find(|i| i.id == id).cloned()
    }

    /// Get all incidents regardless of status.
    pub fn all_incidents(&self) -> Vec<Incident> {
        self.incidents.read().clone()
    }

    /// Get the correlation engine metrics.
    pub fn metrics(&self) -> &CorrelationMetrics {
        &self.metrics
    }

    /// Get a summary of the engine state.
    pub fn summary(&self) -> EngineSummary {
        let incidents = self.incidents.read();
        let active = incidents
            .iter()
            .filter(|i| i.status == IncidentStatus::Active)
            .count();
        let resolved = incidents
            .iter()
            .filter(|i| i.status == IncidentStatus::Resolved)
            .count();
        let with_root_cause = incidents.iter().filter(|i| i.root_cause.is_some()).count();

        EngineSummary {
            total_signals: self.metrics.signals_ingested.load(Ordering::Relaxed),
            total_incidents: incidents.len() as u64,
            active_incidents: active as u64,
            resolved_incidents: resolved as u64,
            root_causes_identified: with_root_cause as u64,
        }
    }
}

impl fmt::Debug for CorrelationEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CorrelationEngine")
            .field("config", &self.config)
            .field("buffer_size", &self.signal_buffer.read().len())
            .field("incidents", &self.incidents.read().len())
            .finish()
    }
}

fn signal_kind_tag(kind: &SignalKind) -> &'static str {
    match kind {
        SignalKind::SlowQuery { .. } => "slow_query",
        SignalKind::AnomalyAlert { .. } => "anomaly",
        SignalKind::TraceSpan { .. } => "trace",
        SignalKind::MetricSample { .. } => "metric",
        SignalKind::Error { .. } => "error",
        SignalKind::ResourceExhaustion { .. } => "resource",
    }
}

/// Summary of the correlation engine state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineSummary {
    /// Total signals ingested since engine creation.
    pub total_signals: u64,
    /// Total incidents (all statuses).
    pub total_incidents: u64,
    /// Currently active incidents.
    pub active_incidents: u64,
    /// Resolved incidents.
    pub resolved_incidents: u64,
    /// Incidents with an identified root cause.
    pub root_causes_identified: u64,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors from the correlation engine.
#[derive(Debug, thiserror::Error)]
pub enum CorrelationError {
    /// The requested incident was not found.
    #[error("incident not found: {id}")]
    IncidentNotFound {
        /// The incident ID that was not found.
        id: String,
    },

    /// The signal buffer is full.
    #[error("signal buffer overflow")]
    BufferOverflow,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_signal(
        id: &str,
        source: &str,
        kind: SignalKind,
        severity: Severity,
        ts: u64,
    ) -> Signal {
        Signal {
            id: id.to_string(),
            timestamp_us: ts,
            source: source.to_string(),
            kind,
            severity,
            metadata: HashMap::new(),
        }
    }

    #[test]
    fn test_signal_kind_display() {
        let kind = SignalKind::SlowQuery {
            command: "GET".to_string(),
            latency_us: 5000,
        };
        assert!(format!("{}", kind).contains("GET"));
        assert!(format!("{}", kind).contains("5000"));
    }

    #[test]
    fn test_severity_ordering() {
        assert!(Severity::Info < Severity::Warning);
        assert!(Severity::Warning < Severity::Error);
        assert!(Severity::Error < Severity::Critical);
    }

    #[test]
    fn test_engine_ingest() {
        let engine = CorrelationEngine::new(CorrelationConfig::default());
        let signal = make_signal(
            "s1",
            "node-0",
            SignalKind::SlowQuery {
                command: "GET".to_string(),
                latency_us: 5000,
            },
            Severity::Warning,
            1000,
        );
        engine.ingest(signal);
        assert_eq!(engine.metrics().signals_ingested.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_engine_correlate_creates_incident() {
        let config = CorrelationConfig {
            min_signals_for_incident: 2,
            correlation_window_us: 10_000,
            incident_threshold: Severity::Warning,
            ..Default::default()
        };
        let engine = CorrelationEngine::new(config);

        // Ingest related signals within the correlation window
        engine.ingest(make_signal(
            "s1",
            "node-0",
            SignalKind::SlowQuery {
                command: "GET".to_string(),
                latency_us: 5000,
            },
            Severity::Warning,
            1000,
        ));
        engine.ingest(make_signal(
            "s2",
            "node-0",
            SignalKind::SlowQuery {
                command: "SET".to_string(),
                latency_us: 8000,
            },
            Severity::Warning,
            2000,
        ));

        let incidents = engine.correlate();
        // At least one incident should exist
        assert!(!incidents.is_empty());
    }

    #[test]
    fn test_engine_root_cause_memory() {
        let config = CorrelationConfig {
            min_signals_for_incident: 2,
            correlation_window_us: 100_000,
            incident_threshold: Severity::Warning,
            ..Default::default()
        };
        let engine = CorrelationEngine::new(config);

        engine.ingest(make_signal(
            "s1",
            "node-0",
            SignalKind::ResourceExhaustion {
                resource: "memory".to_string(),
                usage_percent: 95.0,
            },
            Severity::Critical,
            1000,
        ));
        engine.ingest(make_signal(
            "s2",
            "node-0",
            SignalKind::Error {
                error_type: "OOM".to_string(),
                message: "out of memory".to_string(),
            },
            Severity::Critical,
            2000,
        ));
        engine.ingest(make_signal(
            "s3",
            "node-0",
            SignalKind::AnomalyAlert {
                metric: "memory_usage".to_string(),
                z_score: 5,
            },
            Severity::Error,
            3000,
        ));

        let incidents = engine.correlate();
        assert!(!incidents.is_empty());

        let inc = &incidents[0];
        assert!(inc.root_cause.is_some());
        let rc = inc.root_cause.as_ref().unwrap();
        assert_eq!(rc.category, RootCauseCategory::MemoryPressure);
        assert!(!inc.runbook.is_empty());
    }

    #[test]
    fn test_engine_resolve_incident() {
        let config = CorrelationConfig {
            min_signals_for_incident: 1,
            incident_threshold: Severity::Warning,
            ..Default::default()
        };
        let engine = CorrelationEngine::new(config);

        engine.ingest(make_signal(
            "s1",
            "node-0",
            SignalKind::Error {
                error_type: "test".to_string(),
                message: "test error".to_string(),
            },
            Severity::Error,
            1000,
        ));

        let incidents = engine.correlate();
        assert!(!incidents.is_empty());
        let inc_id = incidents[0].id.clone();

        assert!(engine.resolve(&inc_id));
        assert!(engine.active_incidents().is_empty());

        let resolved = engine.get_incident(&inc_id).unwrap();
        assert_eq!(resolved.status, IncidentStatus::Resolved);
    }

    #[test]
    fn test_engine_acknowledge_incident() {
        let config = CorrelationConfig {
            min_signals_for_incident: 1,
            incident_threshold: Severity::Warning,
            ..Default::default()
        };
        let engine = CorrelationEngine::new(config);

        engine.ingest(make_signal(
            "s1",
            "node-0",
            SignalKind::SlowQuery {
                command: "KEYS".to_string(),
                latency_us: 100_000,
            },
            Severity::Warning,
            1000,
        ));

        let incidents = engine.correlate();
        let inc_id = incidents[0].id.clone();

        assert!(engine.acknowledge(&inc_id));
        let acked = engine.get_incident(&inc_id).unwrap();
        assert_eq!(acked.status, IncidentStatus::Acknowledged);
    }

    #[test]
    fn test_runbook_generation() {
        let runbook = generate_runbook(&RootCauseCategory::MemoryPressure);
        assert!(runbook.len() >= 2);
        assert_eq!(runbook[0].order, 1);
        assert_eq!(runbook[0].action_type, ActionType::Diagnostic);

        let runbook = generate_runbook(&RootCauseCategory::NetworkIssue);
        assert!(runbook
            .iter()
            .any(|s| s.action_type == ActionType::Escalation));
    }

    #[test]
    fn test_incident_display() {
        let signal = make_signal(
            "s1",
            "node-0",
            SignalKind::Error {
                error_type: "test".to_string(),
                message: "err".to_string(),
            },
            Severity::Error,
            1000,
        );
        let incident = Incident::new("INC-1".to_string(), signal);
        let display = format!("{}", incident);
        assert!(display.contains("INC-1"));
        assert!(display.contains("ERROR"));
    }

    #[test]
    fn test_incident_affected_components() {
        let mut incident = Incident::new(
            "INC-1".to_string(),
            make_signal(
                "s1",
                "node-0",
                SignalKind::Error {
                    error_type: "test".to_string(),
                    message: "err".to_string(),
                },
                Severity::Error,
                1000,
            ),
        );
        incident.add_signal(make_signal(
            "s2",
            "node-1",
            SignalKind::Error {
                error_type: "test".to_string(),
                message: "err".to_string(),
            },
            Severity::Error,
            2000,
        ));

        let components = incident.affected_components();
        assert_eq!(components.len(), 2);
        assert!(components.contains(&"node-0".to_string()));
        assert!(components.contains(&"node-1".to_string()));
    }

    #[test]
    fn test_engine_filters_low_severity() {
        let config = CorrelationConfig {
            incident_threshold: Severity::Error,
            ..Default::default()
        };
        let engine = CorrelationEngine::new(config);

        engine.ingest(make_signal(
            "s1",
            "node-0",
            SignalKind::MetricSample {
                name: "cpu".to_string(),
                value: 30.0,
                unit: "%".to_string(),
            },
            Severity::Info,
            1000,
        ));

        let incidents = engine.correlate();
        assert!(incidents.is_empty());
    }

    #[test]
    fn test_engine_summary() {
        let engine = CorrelationEngine::new(CorrelationConfig {
            min_signals_for_incident: 1,
            incident_threshold: Severity::Warning,
            ..Default::default()
        });

        engine.ingest(make_signal(
            "s1",
            "node-0",
            SignalKind::Error {
                error_type: "test".to_string(),
                message: "err".to_string(),
            },
            Severity::Error,
            1000,
        ));
        engine.correlate();

        let summary = engine.summary();
        assert_eq!(summary.total_signals, 1);
        assert_eq!(summary.active_incidents, 1);
    }

    #[test]
    fn test_root_cause_category_display() {
        assert_eq!(
            RootCauseCategory::MemoryPressure.to_string(),
            "Memory Pressure"
        );
        assert_eq!(
            RootCauseCategory::CpuSaturation.to_string(),
            "CPU Saturation"
        );
        assert_eq!(
            RootCauseCategory::DiskBottleneck.to_string(),
            "Disk Bottleneck"
        );
    }

    #[test]
    fn test_correlation_error_display() {
        let err = CorrelationError::IncidentNotFound {
            id: "INC-99".to_string(),
        };
        assert!(err.to_string().contains("INC-99"));
    }

    #[test]
    fn test_engine_buffer_overflow_protection() {
        let config = CorrelationConfig {
            max_buffer_size: 5,
            incident_threshold: Severity::Warning,
            ..Default::default()
        };
        let engine = CorrelationEngine::new(config);

        for i in 0..10 {
            engine.ingest(make_signal(
                &format!("s{}", i),
                "node-0",
                SignalKind::SlowQuery {
                    command: "GET".to_string(),
                    latency_us: 1000,
                },
                Severity::Warning,
                i * 100,
            ));
        }

        // Buffer should be capped
        assert!(engine.signal_buffer.read().len() <= 5);
    }

    #[test]
    fn test_action_type_display() {
        assert_eq!(ActionType::Diagnostic.to_string(), "DIAGNOSTIC");
        assert_eq!(ActionType::Remediation.to_string(), "REMEDIATION");
        assert_eq!(ActionType::Escalation.to_string(), "ESCALATION");
        assert_eq!(ActionType::Verification.to_string(), "VERIFICATION");
    }

    #[test]
    fn test_incident_status_display() {
        assert_eq!(IncidentStatus::Active.to_string(), "ACTIVE");
        assert_eq!(IncidentStatus::Resolved.to_string(), "RESOLVED");
        assert_eq!(IncidentStatus::Acknowledged.to_string(), "ACK");
    }

    #[test]
    fn test_resolve_nonexistent_incident() {
        let engine = CorrelationEngine::new(CorrelationConfig::default());
        assert!(!engine.resolve("INC-999"));
        assert!(!engine.acknowledge("INC-999"));
    }
}
