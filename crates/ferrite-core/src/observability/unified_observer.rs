//! Unified Observability Orchestrator
//!
//! Merges query profiling, slow query logging, anomaly detection, distributed
//! tracing, profiling, and eBPF probe management behind a single interface.
//!
//! # Example
//!
//! ```ignore
//! let observer = UnifiedObserver::new(UnifiedObserverConfig::default());
//! let handle = observer.start_session(ObserveOptions::default()).unwrap();
//! observer.record_command(Some(&handle.id), "GET", 120, true);
//! let report = observer.stop_session(&handle.id).unwrap();
//! ```

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn now_epoch_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Errors returned by the unified observer.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ObserveError {
    /// Observer is disabled.
    #[error("observer is disabled")]
    Disabled,

    /// Session not found.
    #[error("session not found: {0}")]
    SessionNotFound(String),

    /// Maximum session limit reached.
    #[error("session limit reached (max {0})")]
    SessionLimitReached(usize),

    /// Export failed.
    #[error("export error: {0}")]
    ExportError(String),

    /// Probe registration failed.
    #[error("probe error: {0}")]
    ProbeError(String),
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the unified observer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UnifiedObserverConfig {
    /// Master enable switch.
    pub enabled: bool,
    /// Maximum concurrent observe sessions.
    pub max_sessions: usize,
    /// Default session time-to-live in seconds.
    pub default_session_ttl_secs: u64,
    /// Latency threshold (µs) above which a command is considered slow.
    pub slow_query_threshold_us: u64,
    /// Enable statistical anomaly detection.
    pub anomaly_detection_enabled: bool,
    /// Z-score threshold for anomaly flagging.
    pub anomaly_z_score_threshold: f64,
    /// Sampling rate (0.0–1.0, where 1.0 = 100%).
    pub sampling_rate: f64,
    /// Maximum recorded commands per session.
    pub max_command_history_per_session: usize,
    /// Enable eBPF probes (Linux + root only).
    pub ebpf_enabled: bool,
    /// Enable report export.
    pub export_enabled: bool,
}

impl Default for UnifiedObserverConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_sessions: 100,
            default_session_ttl_secs: 300,
            slow_query_threshold_us: 10_000,
            anomaly_detection_enabled: true,
            anomaly_z_score_threshold: 3.0,
            sampling_rate: 1.0,
            max_command_history_per_session: 10_000,
            ebpf_enabled: false,
            export_enabled: true,
        }
    }
}

// ---------------------------------------------------------------------------
// Latency buckets
// ---------------------------------------------------------------------------

/// Fixed-width latency histogram with 8 buckets.
///
/// Bucket boundaries: <100µs, <500µs, <1ms, <5ms, <10ms, <50ms, <100ms, ≥100ms
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LatencyBuckets {
    /// Counts per bucket.
    pub buckets: [u64; 8],
}

/// Upper bounds (µs) for buckets 0..7. The last bucket has no upper bound.
const BUCKET_UPPER: [u64; 7] = [100, 500, 1_000, 5_000, 10_000, 50_000, 100_000];

/// Human-readable labels for each bucket.
const BUCKET_LABELS: [&str; 8] = [
    "<100us", "<500us", "<1ms", "<5ms", "<10ms", "<50ms", "<100ms", ">=100ms",
];

impl LatencyBuckets {
    /// Record a single latency value.
    pub fn record(&mut self, latency_us: u64) {
        let idx = BUCKET_UPPER
            .iter()
            .position(|&upper| latency_us < upper)
            .unwrap_or(7);
        self.buckets[idx] += 1;
    }

    /// Estimate the latency at a given percentile (0.0–1.0).
    pub fn percentile(&self, p: f64) -> u64 {
        let total: u64 = self.buckets.iter().sum();
        if total == 0 {
            return 0;
        }
        let target = ((total as f64) * p).ceil() as u64;
        let mut cumulative: u64 = 0;
        for (i, &count) in self.buckets.iter().enumerate() {
            cumulative += count;
            if cumulative >= target {
                return if i < BUCKET_UPPER.len() {
                    BUCKET_UPPER[i]
                } else {
                    100_000
                };
            }
        }
        100_000
    }
}

// ---------------------------------------------------------------------------
// Command metrics
// ---------------------------------------------------------------------------

/// Aggregated metrics for a single command type.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommandMetrics {
    /// Command name (e.g. `GET`).
    pub command: String,
    /// Total invocation count.
    pub call_count: u64,
    /// Average latency in µs.
    pub avg_latency_us: f64,
    /// 50th percentile latency.
    pub p50_us: u64,
    /// 95th percentile latency.
    pub p95_us: u64,
    /// 99th percentile latency.
    pub p99_us: u64,
    /// Number of failed invocations.
    pub error_count: u64,
    /// Sum of all latency values in µs.
    pub total_latency_us: u64,
}

/// Per-command accumulator used internally.
#[derive(Clone, Debug, Default)]
struct CommandAccumulator {
    call_count: u64,
    error_count: u64,
    total_latency_us: u64,
    latency_buckets: LatencyBuckets,
}

impl CommandAccumulator {
    fn to_metrics(&self, command: &str) -> CommandMetrics {
        let avg = if self.call_count > 0 {
            self.total_latency_us as f64 / self.call_count as f64
        } else {
            0.0
        };
        CommandMetrics {
            command: command.to_string(),
            call_count: self.call_count,
            avg_latency_us: avg,
            p50_us: self.latency_buckets.percentile(0.50),
            p95_us: self.latency_buckets.percentile(0.95),
            p99_us: self.latency_buckets.percentile(0.99),
            error_count: self.error_count,
            total_latency_us: self.total_latency_us,
        }
    }
}

// ---------------------------------------------------------------------------
// Slow query / anomaly records
// ---------------------------------------------------------------------------

/// Record of a single slow command.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SlowQueryRecord {
    /// Command string.
    pub command: String,
    /// Observed latency in µs.
    pub latency_us: u64,
    /// Unix timestamp (seconds) when the command was recorded.
    pub timestamp: u64,
}

/// Record of a detected anomaly.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AnomalyRecord {
    /// Metric name that triggered the anomaly.
    pub metric: String,
    /// Expected (baseline) value.
    pub expected: f64,
    /// Observed value.
    pub actual: f64,
    /// Computed z-score.
    pub z_score: f64,
    /// Unix timestamp (seconds).
    pub timestamp: u64,
}

// ---------------------------------------------------------------------------
// Observe session
// ---------------------------------------------------------------------------

/// Options controlling what a session tracks.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObserveOptions {
    /// Track commands that exceed the slow-query threshold.
    pub track_slow_queries: bool,
    /// Track statistical anomalies.
    pub track_anomalies: bool,
    /// Maintain per-command breakdown.
    pub track_per_command: bool,
    /// Capture full command arguments (privacy-sensitive).
    pub capture_arguments: bool,
    /// Only track commands in this list (if set).
    pub filter_commands: Option<Vec<String>>,
    /// Human-readable label for the session.
    pub label: Option<String>,
}

impl Default for ObserveOptions {
    fn default() -> Self {
        Self {
            track_slow_queries: true,
            track_anomalies: true,
            track_per_command: true,
            capture_arguments: false,
            filter_commands: None,
            label: None,
        }
    }
}

/// Internal session state.
#[derive(Clone, Debug)]
pub struct ObserveSession {
    /// Unique session id.
    pub id: String,
    /// Unix timestamp (seconds) when the session was created.
    pub started_at: u64,
    /// Options that govern this session.
    pub options: ObserveOptions,
    /// Total commands recorded.
    pub command_count: u64,
    /// Recorded slow queries.
    pub slow_queries: Vec<SlowQueryRecord>,
    /// Detected anomalies.
    pub anomalies: Vec<AnomalyRecord>,
    /// Latency histogram.
    pub latency_histogram: LatencyBuckets,
    /// Per-command accumulator.
    pub command_breakdown: HashMap<String, CommandAccumulator>,
    /// Whether the session is still accepting data.
    pub active: bool,
}

/// Lightweight handle returned when a session starts.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObserveSessionHandle {
    /// Session id.
    pub id: String,
    /// Unix timestamp (seconds).
    pub started_at: u64,
}

/// Read-only snapshot of an active session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionSnapshot {
    /// Session id.
    pub id: String,
    /// Unix timestamp (seconds).
    pub started_at: u64,
    /// Total commands recorded so far.
    pub command_count: u64,
    /// Number of slow queries captured.
    pub slow_query_count: u64,
    /// Number of anomalies detected.
    pub anomaly_count: u64,
    /// Whether the session is still active.
    pub active: bool,
    /// Optional label.
    pub label: Option<String>,
}

// ---------------------------------------------------------------------------
// Reports & summaries
// ---------------------------------------------------------------------------

/// Latency summary included in reports.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LatencySummary {
    /// 50th percentile latency (µs).
    pub p50_us: u64,
    /// 95th percentile latency (µs).
    pub p95_us: u64,
    /// 99th percentile latency (µs).
    pub p99_us: u64,
    /// 99.9th percentile latency (µs).
    pub p999_us: u64,
    /// Average latency (µs).
    pub avg_us: f64,
    /// Minimum latency (µs).
    pub min_us: u64,
    /// Maximum latency (µs).
    pub max_us: u64,
    /// Bucket labels and their counts.
    pub distribution: Vec<(String, u64)>,
}

/// Report produced when a session is stopped.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObserveReport {
    /// Session id.
    pub session_id: String,
    /// Elapsed wall-clock time in seconds.
    pub duration_secs: f64,
    /// Total commands processed.
    pub total_commands: u64,
    /// Number of slow queries recorded.
    pub slow_query_count: u64,
    /// Number of anomalies detected.
    pub anomaly_count: u64,
    /// Top 10 commands by call count.
    pub top_commands: Vec<CommandMetrics>,
    /// Top 10 commands by p99 latency.
    pub slowest_commands: Vec<CommandMetrics>,
    /// Overall latency summary.
    pub latency_summary: LatencySummary,
    /// Auto-generated recommendations.
    pub recommendations: Vec<String>,
}

// ---------------------------------------------------------------------------
// Alerts
// ---------------------------------------------------------------------------

/// Severity of an alert.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlertSeverity {
    /// Informational.
    Info,
    /// Warning — may need attention.
    Warning,
    /// Critical — requires immediate action.
    Critical,
}

/// Condition that triggers an alert.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AlertCondition {
    /// Slow queries exceeding a rate.
    SlowQueryRate {
        /// Maximum slow queries per minute.
        threshold_per_min: f64,
    },
    /// P99 latency above a threshold.
    LatencyP99Above {
        /// Threshold in µs.
        threshold_us: u64,
    },
    /// Error rate above a percentage.
    ErrorRateAbove {
        /// Threshold percentage (0–100).
        threshold_pct: f64,
    },
    /// Throughput below a floor.
    ThroughputBelow {
        /// Minimum ops/sec.
        threshold_ops_sec: f64,
    },
    /// Z-score anomaly detected.
    AnomalyDetected {
        /// Z-score threshold.
        z_score_threshold: f64,
    },
}

/// A user-defined alerting rule.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AlertRule {
    /// Unique rule id.
    pub id: String,
    /// Human-readable rule name.
    pub name: String,
    /// Condition that triggers the alert.
    pub condition: AlertCondition,
    /// Severity when fired.
    pub severity: AlertSeverity,
    /// Minimum seconds between successive firings.
    pub cooldown_secs: u64,
    /// Timestamp of last firing (if any).
    pub last_fired: Option<u64>,
}

/// An alert that has fired.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FiredAlert {
    /// Rule id that fired.
    pub rule_id: String,
    /// Rule name.
    pub rule_name: String,
    /// Severity.
    pub severity: AlertSeverity,
    /// Human-readable description.
    pub message: String,
    /// Unix timestamp (seconds).
    pub timestamp: u64,
    /// Observed metric value that triggered the alert.
    pub metric_value: f64,
}

// ---------------------------------------------------------------------------
// Probes
// ---------------------------------------------------------------------------

/// Type of eBPF/observability probe.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ProbeType {
    /// Track per-command latency.
    CommandLatency,
    /// Track storage I/O operations.
    StorageIO,
    /// Track network I/O operations.
    NetworkIO,
    /// Track memory allocation.
    MemoryAllocation,
    /// User-defined probe.
    Custom(String),
}

/// Specification for a new probe.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProbeSpec {
    /// Kind of probe.
    pub probe_type: ProbeType,
    /// Target (e.g. function name, subsystem).
    pub target: String,
    /// Sampling rate (0.0–1.0).
    pub sampling_rate: f64,
}

/// A registered probe.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProbeRegistration {
    /// Unique id.
    pub id: String,
    /// Original specification.
    pub spec: ProbeSpec,
    /// Unix timestamp (seconds) when registered.
    pub registered_at: u64,
    /// Whether the probe is active.
    pub active: bool,
}

// ---------------------------------------------------------------------------
// Export
// ---------------------------------------------------------------------------

/// Supported export formats.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExportFormat {
    /// JSON (serde_json serialization of [`ObserveReport`]).
    Json,
    /// CSV with header row.
    Csv,
    /// Prometheus exposition format.
    Prometheus,
    /// OTLP-style JSON.
    OpenTelemetry,
}

// ---------------------------------------------------------------------------
// Live metrics
// ---------------------------------------------------------------------------

/// Real-time metrics snapshot.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LiveMetrics {
    /// Operations per second.
    pub ops_per_sec: f64,
    /// Average latency (µs).
    pub avg_latency_us: f64,
    /// Slow queries per minute.
    pub slow_queries_per_min: f64,
    /// Error rate (0–100%).
    pub error_rate_pct: f64,
    /// Number of active observe sessions.
    pub active_sessions: usize,
    /// Number of active probes.
    pub active_probes: usize,
    /// Estimated memory used (bytes).
    pub memory_used_bytes: u64,
    /// Active connections (placeholder).
    pub connections: u64,
}

// ---------------------------------------------------------------------------
// Global stats
// ---------------------------------------------------------------------------

/// Atomic counters for global statistics.
pub struct GlobalObsStats {
    total_commands: AtomicU64,
    total_errors: AtomicU64,
    total_slow_queries: AtomicU64,
    total_latency_us: AtomicU64,
    first_command_at: AtomicU64,
    last_command_at: AtomicU64,
}

impl Default for GlobalObsStats {
    fn default() -> Self {
        Self {
            total_commands: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            total_slow_queries: AtomicU64::new(0),
            total_latency_us: AtomicU64::new(0),
            first_command_at: AtomicU64::new(0),
            last_command_at: AtomicU64::new(0),
        }
    }
}

/// Serializable snapshot of global stats.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GlobalObsStatsSnapshot {
    /// Total commands observed.
    pub total_commands: u64,
    /// Total errors observed.
    pub total_errors: u64,
    /// Total slow queries observed.
    pub total_slow_queries: u64,
    /// Total latency accumulated (µs).
    pub total_latency_us: u64,
    /// Timestamp of the first command.
    pub first_command_at: u64,
    /// Timestamp of the most recent command.
    pub last_command_at: u64,
}

// ---------------------------------------------------------------------------
// UnifiedObserver
// ---------------------------------------------------------------------------

/// Unified observability orchestrator.
///
/// Coordinates session-based tracing, slow query detection, anomaly detection,
/// alert evaluation, probe management, and metric export through a single API.
pub struct UnifiedObserver {
    /// Observer configuration.
    config: UnifiedObserverConfig,
    /// Active and completed observe sessions.
    sessions: RwLock<HashMap<String, ObserveSession>>,
    /// Global counters shared across all sessions.
    global_stats: GlobalObsStats,
    /// User-defined alert rules.
    alert_rules: RwLock<Vec<AlertRule>>,
    /// Registered probes.
    active_probes: RwLock<Vec<ProbeRegistration>>,
}

impl UnifiedObserver {
    /// Create a new unified observer with the given configuration.
    pub fn new(config: UnifiedObserverConfig) -> Self {
        Self {
            config,
            sessions: RwLock::new(HashMap::new()),
            global_stats: GlobalObsStats::default(),
            alert_rules: RwLock::new(Vec::new()),
            active_probes: RwLock::new(Vec::new()),
        }
    }

    /// Start a new observe session, returning a handle.
    pub fn start_session(
        &self,
        options: ObserveOptions,
    ) -> Result<ObserveSessionHandle, ObserveError> {
        if !self.config.enabled {
            return Err(ObserveError::Disabled);
        }

        let mut sessions = self.sessions.write();
        let active_count = sessions.values().filter(|s| s.active).count();
        if active_count >= self.config.max_sessions {
            return Err(ObserveError::SessionLimitReached(self.config.max_sessions));
        }

        let id = Uuid::new_v4().to_string();
        let started_at = now_epoch_secs();
        let session = ObserveSession {
            id: id.clone(),
            started_at,
            options,
            command_count: 0,
            slow_queries: Vec::new(),
            anomalies: Vec::new(),
            latency_histogram: LatencyBuckets::default(),
            command_breakdown: HashMap::new(),
            active: true,
        };
        sessions.insert(id.clone(), session);

        Ok(ObserveSessionHandle { id, started_at })
    }

    /// Stop a session and produce an [`ObserveReport`].
    pub fn stop_session(&self, id: &str) -> Result<ObserveReport, ObserveError> {
        let mut sessions = self.sessions.write();
        let session = sessions
            .get_mut(id)
            .ok_or_else(|| ObserveError::SessionNotFound(id.to_string()))?;

        session.active = false;
        let report = build_report(session);
        Ok(report)
    }

    /// Get a read-only snapshot of a session.
    pub fn get_session(&self, id: &str) -> Option<SessionSnapshot> {
        let sessions = self.sessions.read();
        sessions.get(id).map(|s| SessionSnapshot {
            id: s.id.clone(),
            started_at: s.started_at,
            command_count: s.command_count,
            slow_query_count: s.slow_queries.len() as u64,
            anomaly_count: s.anomalies.len() as u64,
            active: s.active,
            label: s.options.label.clone(),
        })
    }

    /// Record a command execution across all subsystems.
    ///
    /// Always updates global stats. If `session_id` is provided and the
    /// session is active, updates session-specific data as well.
    pub fn record_command(
        &self,
        session_id: Option<&str>,
        cmd: &str,
        latency_us: u64,
        success: bool,
    ) {
        if !self.config.enabled {
            return;
        }

        // --- Global stats ---
        let now = now_epoch_secs();
        self.global_stats
            .total_commands
            .fetch_add(1, Ordering::Relaxed);
        self.global_stats
            .total_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);
        if !success {
            self.global_stats
                .total_errors
                .fetch_add(1, Ordering::Relaxed);
        }
        let is_slow = latency_us > self.config.slow_query_threshold_us;
        if is_slow {
            self.global_stats
                .total_slow_queries
                .fetch_add(1, Ordering::Relaxed);
        }
        self.global_stats
            .first_command_at
            .compare_exchange(0, now, Ordering::Relaxed, Ordering::Relaxed)
            .ok();
        self.global_stats
            .last_command_at
            .store(now, Ordering::Relaxed);

        // --- Session stats ---
        if let Some(sid) = session_id {
            let mut sessions = self.sessions.write();
            if let Some(session) = sessions.get_mut(sid) {
                if !session.active {
                    return;
                }

                // Command filter
                if let Some(ref filter) = session.options.filter_commands {
                    let upper = cmd.to_uppercase();
                    if !filter.iter().any(|f| f.eq_ignore_ascii_case(&upper) || f.eq_ignore_ascii_case(cmd)) {
                        return;
                    }
                }

                session.command_count += 1;

                // Latency histogram
                session.latency_histogram.record(latency_us);

                // Per-command breakdown
                if session.options.track_per_command {
                    let acc = session
                        .command_breakdown
                        .entry(cmd.to_uppercase())
                        .or_default();
                    acc.call_count += 1;
                    acc.total_latency_us += latency_us;
                    acc.latency_buckets.record(latency_us);
                    if !success {
                        acc.error_count += 1;
                    }
                }

                // Slow query tracking
                if session.options.track_slow_queries
                    && is_slow
                    && session.slow_queries.len() < self.config.max_command_history_per_session
                {
                    session.slow_queries.push(SlowQueryRecord {
                        command: cmd.to_string(),
                        latency_us,
                        timestamp: now,
                    });
                }

                // Simple anomaly detection: running z-score on latency
                if session.options.track_anomalies
                    && self.config.anomaly_detection_enabled
                    && session.command_count > 10
                {
                    let total_latency: u64 = session.latency_histogram.buckets.iter().sum();
                    if total_latency > 0 {
                        let mean = self.global_stats.total_latency_us.load(Ordering::Relaxed)
                            as f64
                            / self
                                .global_stats
                                .total_commands
                                .load(Ordering::Relaxed)
                                .max(1) as f64;
                        if mean > 0.0 {
                            let stddev = mean * 0.5; // simplified estimate
                            let z = (latency_us as f64 - mean) / stddev;
                            if z.abs() > self.config.anomaly_z_score_threshold {
                                session.anomalies.push(AnomalyRecord {
                                    metric: "latency_us".to_string(),
                                    expected: mean,
                                    actual: latency_us as f64,
                                    z_score: z,
                                    timestamp: now,
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    /// Return a snapshot of live metrics.
    pub fn get_live_metrics(&self) -> LiveMetrics {
        let total_cmds = self.global_stats.total_commands.load(Ordering::Relaxed);
        let total_lat = self.global_stats.total_latency_us.load(Ordering::Relaxed);
        let total_errs = self.global_stats.total_errors.load(Ordering::Relaxed);
        let total_slow = self.global_stats.total_slow_queries.load(Ordering::Relaxed);

        let first = self.global_stats.first_command_at.load(Ordering::Relaxed);
        let last = self.global_stats.last_command_at.load(Ordering::Relaxed);
        let elapsed_secs = if last > first { last - first } else { 1 };

        let ops_per_sec = total_cmds as f64 / elapsed_secs.max(1) as f64;
        let avg_latency = if total_cmds > 0 {
            total_lat as f64 / total_cmds as f64
        } else {
            0.0
        };
        let error_rate = if total_cmds > 0 {
            total_errs as f64 / total_cmds as f64 * 100.0
        } else {
            0.0
        };
        let slow_per_min = total_slow as f64 / (elapsed_secs.max(1) as f64 / 60.0);

        let sessions = self.sessions.read();
        let active_sessions = sessions.values().filter(|s| s.active).count();
        let active_probes = self.active_probes.read().len();

        LiveMetrics {
            ops_per_sec,
            avg_latency_us: avg_latency,
            slow_queries_per_min: slow_per_min,
            error_rate_pct: error_rate,
            active_sessions,
            active_probes,
            memory_used_bytes: 0,
            connections: 0,
        }
    }

    /// Add an alert rule. Returns the rule id.
    pub fn add_alert_rule(&self, mut rule: AlertRule) -> String {
        if rule.id.is_empty() {
            rule.id = Uuid::new_v4().to_string();
        }
        let id = rule.id.clone();
        self.alert_rules.write().push(rule);
        id
    }

    /// Remove an alert rule by id. Returns `true` if found.
    pub fn remove_alert_rule(&self, id: &str) -> bool {
        let mut rules = self.alert_rules.write();
        let before = rules.len();
        rules.retain(|r| r.id != id);
        rules.len() < before
    }

    /// Evaluate all alert rules against current metrics and return any that fire.
    pub fn check_alerts(&self) -> Vec<FiredAlert> {
        let now = now_epoch_secs();
        let total_cmds = self.global_stats.total_commands.load(Ordering::Relaxed);
        let total_errs = self.global_stats.total_errors.load(Ordering::Relaxed);
        let total_slow = self.global_stats.total_slow_queries.load(Ordering::Relaxed);
        let total_lat = self.global_stats.total_latency_us.load(Ordering::Relaxed);

        let first = self.global_stats.first_command_at.load(Ordering::Relaxed);
        let last = self.global_stats.last_command_at.load(Ordering::Relaxed);
        let elapsed_secs = if last > first { last - first } else { 1 };
        let elapsed_min = elapsed_secs as f64 / 60.0;

        let mut fired = Vec::new();
        let mut rules = self.alert_rules.write();

        for rule in rules.iter_mut() {
            // Cooldown check
            if let Some(last_fired) = rule.last_fired {
                if now.saturating_sub(last_fired) < rule.cooldown_secs {
                    continue;
                }
            }

            let (should_fire, metric_value, message) = match &rule.condition {
                AlertCondition::SlowQueryRate { threshold_per_min } => {
                    let rate = total_slow as f64 / elapsed_min.max(0.01);
                    (
                        rate > *threshold_per_min,
                        rate,
                        format!("Slow query rate {:.1}/min exceeds {:.1}/min", rate, threshold_per_min),
                    )
                }
                AlertCondition::LatencyP99Above { threshold_us } => {
                    // Approximate from global average (sessions have real histograms)
                    let avg = if total_cmds > 0 {
                        total_lat / total_cmds
                    } else {
                        0
                    };
                    (
                        avg > *threshold_us,
                        avg as f64,
                        format!("Average latency {}µs exceeds {}µs threshold", avg, threshold_us),
                    )
                }
                AlertCondition::ErrorRateAbove { threshold_pct } => {
                    let rate = if total_cmds > 0 {
                        total_errs as f64 / total_cmds as f64 * 100.0
                    } else {
                        0.0
                    };
                    (
                        rate > *threshold_pct,
                        rate,
                        format!("Error rate {:.2}% exceeds {:.2}%", rate, threshold_pct),
                    )
                }
                AlertCondition::ThroughputBelow { threshold_ops_sec } => {
                    let ops = total_cmds as f64 / elapsed_secs.max(1) as f64;
                    (
                        ops < *threshold_ops_sec && total_cmds > 0,
                        ops,
                        format!("Throughput {:.1} ops/s below {:.1} ops/s", ops, threshold_ops_sec),
                    )
                }
                AlertCondition::AnomalyDetected { z_score_threshold } => {
                    let sessions = self.sessions.read();
                    let max_z = sessions
                        .values()
                        .flat_map(|s| s.anomalies.iter())
                        .map(|a| a.z_score)
                        .fold(0.0_f64, f64::max);
                    (
                        max_z > *z_score_threshold,
                        max_z,
                        format!("Anomaly z-score {:.2} exceeds {:.2}", max_z, z_score_threshold),
                    )
                }
            };

            if should_fire {
                rule.last_fired = Some(now);
                fired.push(FiredAlert {
                    rule_id: rule.id.clone(),
                    rule_name: rule.name.clone(),
                    severity: rule.severity.clone(),
                    message,
                    timestamp: now,
                    metric_value,
                });
            }
        }

        fired
    }

    /// Register a new probe. Returns the probe id.
    pub fn register_probe(&self, spec: ProbeSpec) -> Result<String, ObserveError> {
        if !self.config.enabled {
            return Err(ObserveError::Disabled);
        }
        if !self.config.ebpf_enabled {
            if matches!(spec.probe_type, ProbeType::StorageIO | ProbeType::NetworkIO | ProbeType::MemoryAllocation) {
                return Err(ObserveError::ProbeError(
                    "eBPF probes require ebpf_enabled=true (Linux + root)".to_string(),
                ));
            }
        }

        let id = Uuid::new_v4().to_string();
        let reg = ProbeRegistration {
            id: id.clone(),
            spec,
            registered_at: now_epoch_secs(),
            active: true,
        };
        self.active_probes.write().push(reg);
        Ok(id)
    }

    /// Unregister a probe by id. Returns `true` if found.
    pub fn unregister_probe(&self, id: &str) -> bool {
        let mut probes = self.active_probes.write();
        let before = probes.len();
        probes.retain(|p| p.id != id);
        probes.len() < before
    }

    /// Number of currently registered probes.
    pub fn active_probe_count(&self) -> usize {
        self.active_probes.read().len()
    }

    /// Export a session report in the given format.
    pub fn export_report(
        &self,
        session_id: &str,
        format: ExportFormat,
    ) -> Result<String, ObserveError> {
        if !self.config.export_enabled {
            return Err(ObserveError::ExportError("export is disabled".to_string()));
        }

        let sessions = self.sessions.read();
        let session = sessions
            .get(session_id)
            .ok_or_else(|| ObserveError::SessionNotFound(session_id.to_string()))?;

        let report = build_report(session);

        match format {
            ExportFormat::Json => serde_json::to_string_pretty(&report)
                .map_err(|e| ObserveError::ExportError(e.to_string())),

            ExportFormat::Csv => {
                let mut csv = String::from("command,call_count,avg_latency_us,p50_us,p95_us,p99_us,error_count,total_latency_us\n");
                for m in report
                    .top_commands
                    .iter()
                    .chain(report.slowest_commands.iter())
                {
                    csv.push_str(&format!(
                        "{},{},{:.2},{},{},{},{},{}\n",
                        m.command,
                        m.call_count,
                        m.avg_latency_us,
                        m.p50_us,
                        m.p95_us,
                        m.p99_us,
                        m.error_count,
                        m.total_latency_us,
                    ));
                }
                Ok(csv)
            }

            ExportFormat::Prometheus => {
                let mut out = String::new();
                out.push_str(&format!(
                    "# HELP ferrite_observe_total_commands Total commands in session\n\
                     # TYPE ferrite_observe_total_commands counter\n\
                     ferrite_observe_total_commands{{session=\"{}\"}} {}\n",
                    report.session_id, report.total_commands,
                ));
                out.push_str(&format!(
                    "# HELP ferrite_observe_slow_queries Slow query count\n\
                     # TYPE ferrite_observe_slow_queries counter\n\
                     ferrite_observe_slow_queries{{session=\"{}\"}} {}\n",
                    report.session_id, report.slow_query_count,
                ));
                out.push_str(&format!(
                    "# HELP ferrite_observe_latency_p99_us P99 latency in microseconds\n\
                     # TYPE ferrite_observe_latency_p99_us gauge\n\
                     ferrite_observe_latency_p99_us{{session=\"{}\"}} {}\n",
                    report.session_id, report.latency_summary.p99_us,
                ));
                for m in &report.top_commands {
                    out.push_str(&format!(
                        "ferrite_observe_command_calls{{session=\"{}\",command=\"{}\"}} {}\n",
                        report.session_id, m.command, m.call_count,
                    ));
                }
                Ok(out)
            }

            ExportFormat::OpenTelemetry => {
                // OTLP-style JSON envelope
                let otlp = serde_json::json!({
                    "resourceMetrics": [{
                        "resource": {
                            "attributes": [
                                {"key": "service.name", "value": {"stringValue": "ferrite"}},
                                {"key": "session.id", "value": {"stringValue": report.session_id}},
                            ]
                        },
                        "scopeMetrics": [{
                            "scope": {"name": "ferrite.observe"},
                            "metrics": [
                                {
                                    "name": "ferrite.observe.total_commands",
                                    "sum": {"dataPoints": [{"asInt": report.total_commands.to_string()}]}
                                },
                                {
                                    "name": "ferrite.observe.slow_queries",
                                    "sum": {"dataPoints": [{"asInt": report.slow_query_count.to_string()}]}
                                },
                                {
                                    "name": "ferrite.observe.latency_p99_us",
                                    "gauge": {"dataPoints": [{"asInt": report.latency_summary.p99_us.to_string()}]}
                                },
                            ]
                        }]
                    }]
                });
                serde_json::to_string_pretty(&otlp)
                    .map_err(|e| ObserveError::ExportError(e.to_string()))
            }
        }
    }

    /// Return a snapshot of global stats.
    pub fn stats(&self) -> GlobalObsStatsSnapshot {
        GlobalObsStatsSnapshot {
            total_commands: self.global_stats.total_commands.load(Ordering::Relaxed),
            total_errors: self.global_stats.total_errors.load(Ordering::Relaxed),
            total_slow_queries: self.global_stats.total_slow_queries.load(Ordering::Relaxed),
            total_latency_us: self.global_stats.total_latency_us.load(Ordering::Relaxed),
            first_command_at: self.global_stats.first_command_at.load(Ordering::Relaxed),
            last_command_at: self.global_stats.last_command_at.load(Ordering::Relaxed),
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build an [`ObserveReport`] from a session snapshot.
fn build_report(session: &ObserveSession) -> ObserveReport {
    let now = now_epoch_secs();
    let duration_secs = now.saturating_sub(session.started_at) as f64;

    // Collect command metrics
    let mut all_metrics: Vec<CommandMetrics> = session
        .command_breakdown
        .iter()
        .map(|(name, acc)| acc.to_metrics(name))
        .collect();

    // Top 10 by call count
    let mut top_by_count = all_metrics.clone();
    top_by_count.sort_by(|a, b| b.call_count.cmp(&a.call_count));
    top_by_count.truncate(10);

    // Top 10 by p99
    all_metrics.sort_by(|a, b| b.p99_us.cmp(&a.p99_us));
    all_metrics.truncate(10);

    // Overall latency summary
    let latency_summary = LatencySummary {
        p50_us: session.latency_histogram.percentile(0.50),
        p95_us: session.latency_histogram.percentile(0.95),
        p99_us: session.latency_histogram.percentile(0.99),
        p999_us: session.latency_histogram.percentile(0.999),
        avg_us: if session.command_count > 0 {
            let total: u64 = session
                .command_breakdown
                .values()
                .map(|a| a.total_latency_us)
                .sum();
            total as f64 / session.command_count as f64
        } else {
            0.0
        },
        min_us: session
            .latency_histogram
            .buckets
            .iter()
            .enumerate()
            .find(|(_, &c)| c > 0)
            .map(|(i, _)| if i == 0 { 0 } else { BUCKET_UPPER[i - 1] })
            .unwrap_or(0),
        max_us: session
            .latency_histogram
            .buckets
            .iter()
            .enumerate()
            .rev()
            .find(|(_, &c)| c > 0)
            .map(|(i, _)| {
                if i < BUCKET_UPPER.len() {
                    BUCKET_UPPER[i]
                } else {
                    100_000
                }
            })
            .unwrap_or(0),
        distribution: BUCKET_LABELS
            .iter()
            .zip(session.latency_histogram.buckets.iter())
            .map(|(label, &count)| (label.to_string(), count))
            .collect(),
    };

    // Recommendations
    let mut recommendations = Vec::new();
    if session.slow_queries.len() > 10 {
        recommendations.push(format!(
            "High slow-query count ({}). Consider indexing hot keys or increasing timeout.",
            session.slow_queries.len()
        ));
    }
    if !session.anomalies.is_empty() {
        recommendations.push(format!(
            "{} latency anomalies detected. Investigate spikes for resource contention.",
            session.anomalies.len()
        ));
    }
    if latency_summary.p99_us >= 50_000 {
        recommendations.push(
            "P99 latency ≥50ms — check disk I/O and network round-trips.".to_string(),
        );
    }
    let total_errors: u64 = session
        .command_breakdown
        .values()
        .map(|a| a.error_count)
        .sum();
    if session.command_count > 0 && total_errors as f64 / session.command_count as f64 > 0.05 {
        recommendations.push("Error rate >5%. Review error logs for recurring failures.".to_string());
    }

    ObserveReport {
        session_id: session.id.clone(),
        duration_secs,
        total_commands: session.command_count,
        slow_query_count: session.slow_queries.len() as u64,
        anomaly_count: session.anomalies.len() as u64,
        top_commands: top_by_count,
        slowest_commands: all_metrics,
        latency_summary,
        recommendations,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let cfg = UnifiedObserverConfig::default();
        assert!(cfg.enabled);
        assert_eq!(cfg.max_sessions, 100);
        assert_eq!(cfg.slow_query_threshold_us, 10_000);
        assert!((cfg.sampling_rate - 1.0).abs() < f64::EPSILON);
        assert!(!cfg.ebpf_enabled);
    }

    #[test]
    fn test_start_and_stop_session() {
        let obs = UnifiedObserver::new(UnifiedObserverConfig::default());
        let handle = obs.start_session(ObserveOptions::default()).unwrap();
        assert!(!handle.id.is_empty());

        let snap = obs.get_session(&handle.id).unwrap();
        assert!(snap.active);
        assert_eq!(snap.command_count, 0);

        let report = obs.stop_session(&handle.id).unwrap();
        assert_eq!(report.total_commands, 0);

        let snap = obs.get_session(&handle.id).unwrap();
        assert!(!snap.active);
    }

    #[test]
    fn test_session_limit() {
        let mut cfg = UnifiedObserverConfig::default();
        cfg.max_sessions = 2;
        let obs = UnifiedObserver::new(cfg);

        obs.start_session(ObserveOptions::default()).unwrap();
        obs.start_session(ObserveOptions::default()).unwrap();
        let err = obs.start_session(ObserveOptions::default()).unwrap_err();
        assert!(matches!(err, ObserveError::SessionLimitReached(2)));
    }

    #[test]
    fn test_record_command_updates_global_stats() {
        let obs = UnifiedObserver::new(UnifiedObserverConfig::default());
        obs.record_command(None, "GET", 500, true);
        obs.record_command(None, "SET", 200, false);

        let stats = obs.stats();
        assert_eq!(stats.total_commands, 2);
        assert_eq!(stats.total_errors, 1);
        assert_eq!(stats.total_latency_us, 700);
    }

    #[test]
    fn test_record_command_updates_session() {
        let obs = UnifiedObserver::new(UnifiedObserverConfig::default());
        let handle = obs.start_session(ObserveOptions::default()).unwrap();

        obs.record_command(Some(&handle.id), "GET", 500, true);
        obs.record_command(Some(&handle.id), "GET", 300, true);
        obs.record_command(Some(&handle.id), "SET", 200, true);

        let snap = obs.get_session(&handle.id).unwrap();
        assert_eq!(snap.command_count, 3);
    }

    #[test]
    fn test_slow_query_detection() {
        let obs = UnifiedObserver::new(UnifiedObserverConfig::default());
        let handle = obs.start_session(ObserveOptions::default()).unwrap();

        // Below threshold
        obs.record_command(Some(&handle.id), "GET", 5_000, true);
        // Above threshold (10_000)
        obs.record_command(Some(&handle.id), "SCAN", 15_000, true);
        obs.record_command(Some(&handle.id), "KEYS", 50_000, true);

        let report = obs.stop_session(&handle.id).unwrap();
        assert_eq!(report.slow_query_count, 2);
        assert_eq!(report.total_commands, 3);
    }

    #[test]
    fn test_latency_buckets() {
        let mut buckets = LatencyBuckets::default();
        buckets.record(50); // <100us
        buckets.record(200); // <500us
        buckets.record(800); // <1ms
        buckets.record(60_000); // <100ms
        buckets.record(200_000); // >=100ms

        assert_eq!(buckets.buckets[0], 1);
        assert_eq!(buckets.buckets[1], 1);
        assert_eq!(buckets.buckets[2], 1);
        assert_eq!(buckets.buckets[6], 1);
        assert_eq!(buckets.buckets[7], 1);

        assert!(buckets.percentile(0.5) > 0);
    }

    #[test]
    fn test_alert_rules() {
        let obs = UnifiedObserver::new(UnifiedObserverConfig::default());

        let rule = AlertRule {
            id: String::new(),
            name: "High error rate".to_string(),
            condition: AlertCondition::ErrorRateAbove { threshold_pct: 1.0 },
            severity: AlertSeverity::Critical,
            cooldown_secs: 60,
            last_fired: None,
        };
        let rule_id = obs.add_alert_rule(rule);
        assert!(!rule_id.is_empty());

        // Generate errors
        for _ in 0..5 {
            obs.record_command(None, "BAD", 100, false);
        }

        let alerts = obs.check_alerts();
        assert!(!alerts.is_empty());
        assert_eq!(alerts[0].severity, AlertSeverity::Critical);

        assert!(obs.remove_alert_rule(&rule_id));
        assert!(!obs.remove_alert_rule("nonexistent"));
    }

    #[test]
    fn test_probe_registration() {
        let mut cfg = UnifiedObserverConfig::default();
        cfg.ebpf_enabled = true;
        let obs = UnifiedObserver::new(cfg);

        let spec = ProbeSpec {
            probe_type: ProbeType::CommandLatency,
            target: "GET".to_string(),
            sampling_rate: 0.5,
        };
        let id = obs.register_probe(spec).unwrap();
        assert_eq!(obs.active_probe_count(), 1);

        assert!(obs.unregister_probe(&id));
        assert_eq!(obs.active_probe_count(), 0);
        assert!(!obs.unregister_probe("nope"));
    }

    #[test]
    fn test_export_json() {
        let obs = UnifiedObserver::new(UnifiedObserverConfig::default());
        let handle = obs.start_session(ObserveOptions::default()).unwrap();
        obs.record_command(Some(&handle.id), "GET", 500, true);

        let json = obs.export_report(&handle.id, ExportFormat::Json).unwrap();
        assert!(json.contains("session_id"));
        assert!(json.contains("total_commands"));
    }

    #[test]
    fn test_export_csv() {
        let obs = UnifiedObserver::new(UnifiedObserverConfig::default());
        let handle = obs.start_session(ObserveOptions::default()).unwrap();
        obs.record_command(Some(&handle.id), "SET", 300, true);

        let csv = obs.export_report(&handle.id, ExportFormat::Csv).unwrap();
        assert!(csv.starts_with("command,call_count"));
        assert!(csv.contains("SET"));
    }

    #[test]
    fn test_export_prometheus() {
        let obs = UnifiedObserver::new(UnifiedObserverConfig::default());
        let handle = obs.start_session(ObserveOptions::default()).unwrap();
        obs.record_command(Some(&handle.id), "GET", 1_000, true);

        let prom = obs
            .export_report(&handle.id, ExportFormat::Prometheus)
            .unwrap();
        assert!(prom.contains("ferrite_observe_total_commands"));
    }

    #[test]
    fn test_export_opentelemetry() {
        let obs = UnifiedObserver::new(UnifiedObserverConfig::default());
        let handle = obs.start_session(ObserveOptions::default()).unwrap();
        obs.record_command(Some(&handle.id), "GET", 1_000, true);

        let otlp = obs
            .export_report(&handle.id, ExportFormat::OpenTelemetry)
            .unwrap();
        assert!(otlp.contains("resourceMetrics"));
        assert!(otlp.contains("ferrite.observe.total_commands"));
    }

    #[test]
    fn test_disabled_observer() {
        let mut cfg = UnifiedObserverConfig::default();
        cfg.enabled = false;
        let obs = UnifiedObserver::new(cfg);

        let err = obs.start_session(ObserveOptions::default()).unwrap_err();
        assert!(matches!(err, ObserveError::Disabled));
    }

    #[test]
    fn test_live_metrics() {
        let obs = UnifiedObserver::new(UnifiedObserverConfig::default());
        let handle = obs.start_session(ObserveOptions::default()).unwrap();
        obs.record_command(Some(&handle.id), "GET", 500, true);

        let metrics = obs.get_live_metrics();
        assert_eq!(metrics.active_sessions, 1);
        assert!(metrics.ops_per_sec >= 0.0);
    }

    #[test]
    fn test_session_not_found() {
        let obs = UnifiedObserver::new(UnifiedObserverConfig::default());
        assert!(obs.get_session("nope").is_none());
        let err = obs.stop_session("nope").unwrap_err();
        assert!(matches!(err, ObserveError::SessionNotFound(_)));
    }
}
