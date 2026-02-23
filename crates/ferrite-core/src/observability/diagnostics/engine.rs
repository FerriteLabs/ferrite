//! Structured Logging & Diagnostics Engine
//!
//! Provides a comprehensive diagnostics subsystem for latency tracking,
//! slow log management, crash reporting, and structured logging. Designed
//! for Redis-compatible `LATENCY`, `SLOWLOG`, and `DEBUG` commands.
//!
//! # Example
//!
//! ```ignore
//! use ferrite_core::observability::diagnostics::{DiagnosticsEngine, DiagnosticsConfig, LatencyEvent};
//! use chrono::Utc;
//!
//! let engine = DiagnosticsEngine::new(DiagnosticsConfig::default());
//! engine.record_latency_event(LatencyEvent {
//!     event_type: "command".into(),
//!     latency_us: 15_000,
//!     timestamp: Utc::now(),
//! });
//! let history = engine.get_latency_history("command");
//! assert_eq!(history.len(), 1);
//! ```

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

// ---------------------------------------------------------------------------
// Pre-defined latency event types
// ---------------------------------------------------------------------------

/// Pre-defined latency event type constants matching Redis-style diagnostics.
pub mod latency_event_types {
    /// Overall command execution.
    pub const COMMAND: &str = "command";
    /// AOF persistence write.
    pub const AOF_WRITE: &str = "aof-write";
    /// RDB snapshot.
    pub const RDB_SAVE: &str = "rdb-save";
    /// io_uring submission.
    pub const IO_URING_SUBMIT: &str = "io-uring-submit";
    /// io_uring completion.
    pub const IO_URING_COMPLETE: &str = "io-uring-complete";
    /// Process fork for background save.
    pub const FORK: &str = "fork";
    /// TTL expiration scan.
    pub const EXPIRE_CYCLE: &str = "expire-cycle";
    /// Memory eviction.
    pub const EVICTION_CYCLE: &str = "eviction-cycle";
    /// Data tier migration.
    pub const TIERING_MIGRATION: &str = "tiering-migration";
    /// Replication data sync.
    pub const REPLICATION_SYNC: &str = "replication-sync";
    /// Cluster bus message processing.
    pub const CLUSTER_MESSAGE: &str = "cluster-message";
    /// Active defragmentation.
    pub const DEFRAG_CYCLE: &str = "defrag-cycle";
    /// Client pause/unpause.
    pub const CLIENT_PAUSE: &str = "client-pause";

    /// All pre-defined event types.
    pub const ALL: &[&str] = &[
        COMMAND,
        AOF_WRITE,
        RDB_SAVE,
        IO_URING_SUBMIT,
        IO_URING_COMPLETE,
        FORK,
        EXPIRE_CYCLE,
        EVICTION_CYCLE,
        TIERING_MIGRATION,
        REPLICATION_SYNC,
        CLUSTER_MESSAGE,
        DEFRAG_CYCLE,
        CLIENT_PAUSE,
    ];
}

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

/// Errors produced by the diagnostics engine.
#[derive(Debug, Clone, thiserror::Error)]
pub enum DiagnosticsError {
    /// Requested event type has not been recorded.
    #[error("event type not found: {0}")]
    EventTypeNotFound(String),

    /// Slow log has reached capacity.
    #[error("slow log is full")]
    SlowLogFull,

    /// Crash dump could not be written.
    #[error("crash dump failed: {0}")]
    CrashDumpFailed(String),

    /// Catch-all internal error.
    #[error("internal error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

/// Output format for structured log entries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogFormat {
    /// Pretty-printed JSON.
    Json,
    /// Human-readable key=value text.
    Text,
    /// Minimal single-line output.
    Compact,
}

impl Default for LogFormat {
    fn default() -> Self {
        Self::Text
    }
}

/// Severity level for log entries and per-module filtering.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum LogLevel {
    /// Finest-grained informational events.
    Trace,
    /// Detailed debug-level information.
    Debug,
    /// General informational messages.
    Info,
    /// Potentially harmful situations.
    Warn,
    /// Error events that might still allow continued operation.
    Error,
}

impl Default for LogLevel {
    fn default() -> Self {
        Self::Info
    }
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Trace => write!(f, "TRACE"),
            Self::Debug => write!(f, "DEBUG"),
            Self::Info => write!(f, "INFO"),
            Self::Warn => write!(f, "WARN"),
            Self::Error => write!(f, "ERROR"),
        }
    }
}

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

/// A single latency measurement submitted to the engine.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LatencyEvent {
    /// Category of the event (e.g. `"command"`, `"aof-write"`).
    pub event_type: String,
    /// Measured latency in microseconds.
    pub latency_us: u64,
    /// When the event occurred.
    pub timestamp: DateTime<Utc>,
}

/// Aggregated latency sample for a given event type.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LatencySample {
    /// When this sample was recorded.
    pub timestamp: DateTime<Utc>,
    /// Latency of the individual event in microseconds.
    pub latency_us: u64,
    /// Minimum latency observed so far for this event type.
    pub min_us: u64,
    /// Maximum latency observed so far for this event type.
    pub max_us: u64,
    /// Running average latency in microseconds.
    pub avg_us: u64,
    /// Total number of samples recorded for this event type.
    pub samples: u64,
}

/// An entry in the slow log, recording a command that exceeded the threshold.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SlowLogEntry {
    /// Monotonically increasing entry identifier.
    pub id: u64,
    /// When the command was executed.
    pub timestamp: DateTime<Utc>,
    /// Execution duration in microseconds.
    pub duration_us: u64,
    /// The command name (e.g. `"SET"`, `"HGETALL"`).
    pub command: String,
    /// Command arguments.
    pub args: Vec<String>,
    /// Client network address.
    pub client_addr: Option<String>,
    /// Client name set via `CLIENT SETNAME`.
    pub client_name: Option<String>,
    /// Logical database index.
    pub database: u8,
}

/// Snapshot of the server state generated on panic or fatal error.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CrashReport {
    /// When the report was generated.
    pub timestamp: DateTime<Utc>,
    /// Ferrite server version.
    pub version: String,
    /// Operating system description.
    pub os: String,
    /// Rust compiler version.
    pub rust_version: String,
    /// Seconds since the engine was created.
    pub uptime_secs: u64,
    /// Most recent commands processed before the crash.
    pub last_commands: Vec<String>,
    /// Human-readable memory usage string.
    pub memory_usage: String,
    /// Key/value summary of active configuration.
    pub config_summary: HashMap<String, String>,
    /// Stack trace captured at the time of the crash.
    pub stack_trace: Option<String>,
    /// Number of open client connections.
    pub open_connections: u64,
    /// Descriptive error message.
    pub error_message: String,
}

/// Runtime debug information exposed via the `DEBUG` command family.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DebugInfo {
    /// Ferrite server version string.
    pub server_version: String,
    /// Rust compiler version used to build the server.
    pub rust_version: String,
    /// Operating system name.
    pub os: String,
    /// CPU architecture (e.g., x86_64, aarch64).
    pub arch: String,
    /// Process identifier.
    pub pid: u32,
    /// Seconds since the engine was started.
    pub uptime_secs: u64,
    /// Number of currently connected clients.
    pub connected_clients: u64,
    /// Current memory usage in bytes.
    pub used_memory_bytes: u64,
    /// Peak memory usage in bytes since startup.
    pub peak_memory_bytes: u64,
    /// Total number of commands processed since startup.
    pub total_commands_processed: u64,
    /// Current operations per second.
    pub instantaneous_ops_per_sec: f64,
    /// Duration of the most recent fork in microseconds.
    pub latest_fork_usec: u64,
    /// Number of active I/O threads.
    pub io_threads_active: u64,
    /// Whether append-only file persistence is enabled.
    pub aof_enabled: bool,
    /// Whether cluster mode is enabled.
    pub cluster_enabled: bool,
    /// Total event loop iterations since startup.
    pub event_loop_iterations: u64,
}

/// A structured log entry that can be rendered in multiple formats.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogEntry {
    /// When the entry was emitted.
    pub timestamp: DateTime<Utc>,
    /// Severity level.
    pub level: LogLevel,
    /// Originating module path.
    pub module: String,
    /// Human-readable message.
    pub message: String,
    /// Arbitrary structured fields.
    pub fields: HashMap<String, String>,
}

impl LogEntry {
    /// Render the entry as a JSON string.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| String::from("{}"))
    }

    /// Render the entry as human-readable text.
    pub fn to_text(&self) -> String {
        let fields_str = if self.fields.is_empty() {
            String::new()
        } else {
            let pairs: Vec<String> = self
                .fields
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect();
            format!(" {}", pairs.join(" "))
        };
        format!(
            "{} [{}] {}: {}{}",
            self.timestamp.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
            self.level,
            self.module,
            self.message,
            fields_str,
        )
    }

    /// Render the entry in compact single-line format.
    pub fn to_compact(&self) -> String {
        format!(
            "{} {} {} {}",
            self.timestamp.timestamp(),
            self.level,
            self.module,
            self.message,
        )
    }
}

/// High-level statistics about the diagnostics engine.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiagnosticsStats {
    /// Total latency events recorded.
    pub latency_events_recorded: u64,
    /// Current slow log entry count.
    pub slow_log_entries: u64,
    /// Total crash reports generated.
    pub crash_reports_generated: u64,
    /// Number of distinct event types currently being tracked.
    pub event_types_tracked: usize,
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the diagnostics engine.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiagnosticsConfig {
    /// Commands slower than this value (in microseconds) are added to the
    /// slow log.
    pub slow_log_threshold_us: u64,
    /// Maximum number of entries retained in the slow log ring buffer.
    pub slow_log_max_len: usize,
    /// Number of latency samples retained per event type.
    pub latency_history_size: usize,
    /// Whether latency tracking is active.
    pub enable_latency_tracking: bool,
    /// Default output format for log entries.
    pub log_format: LogFormat,
    /// File path for crash dumps. `None` disables file-based dumps.
    pub crash_dump_path: Option<String>,
    /// Per-module minimum log levels.
    pub per_module_levels: HashMap<String, LogLevel>,
}

impl Default for DiagnosticsConfig {
    fn default() -> Self {
        Self {
            slow_log_threshold_us: 10_000,
            slow_log_max_len: 128,
            latency_history_size: 160,
            enable_latency_tracking: true,
            log_format: LogFormat::default(),
            crash_dump_path: None,
            per_module_levels: HashMap::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Running aggregates for a single event type.
#[derive(Debug)]
struct LatencyTracker {
    samples: VecDeque<LatencySample>,
    max_size: usize,
    total_latency: u64,
    count: u64,
    min_us: u64,
    max_us: u64,
}

impl LatencyTracker {
    fn new(max_size: usize) -> Self {
        Self {
            samples: VecDeque::with_capacity(max_size),
            max_size,
            total_latency: 0,
            count: 0,
            min_us: u64::MAX,
            max_us: 0,
        }
    }

    fn record(&mut self, latency_us: u64, timestamp: DateTime<Utc>) {
        self.total_latency += latency_us;
        self.count += 1;
        if latency_us < self.min_us {
            self.min_us = latency_us;
        }
        if latency_us > self.max_us {
            self.max_us = latency_us;
        }

        let sample = LatencySample {
            timestamp,
            latency_us,
            min_us: self.min_us,
            max_us: self.max_us,
            avg_us: self.total_latency / self.count,
            samples: self.count,
        };

        if self.samples.len() >= self.max_size {
            self.samples.pop_front();
        }
        self.samples.push_back(sample);
    }

    fn history(&self) -> Vec<LatencySample> {
        self.samples.iter().cloned().collect()
    }

    fn latest(&self) -> Option<LatencySample> {
        self.samples.back().cloned()
    }
}

// ---------------------------------------------------------------------------
// DiagnosticsEngine
// ---------------------------------------------------------------------------

/// Core diagnostics engine providing latency tracking, slow log management,
/// crash reporting, and runtime debug information.
pub struct DiagnosticsEngine {
    config: DiagnosticsConfig,
    /// Per-event-type latency trackers.
    latency_trackers: DashMap<String, LatencyTracker>,
    /// FIFO slow log ring buffer.
    slow_log: RwLock<VecDeque<SlowLogEntry>>,
    /// Monotonically increasing slow log entry id.
    slow_log_next_id: AtomicU64,
    /// Total latency events recorded (all event types).
    total_latency_events: AtomicU64,
    /// Total crash reports generated.
    crash_reports_generated: AtomicU64,
    /// Engine creation time – used for uptime calculation.
    created_at: Instant,
}

impl DiagnosticsEngine {
    /// Create a new diagnostics engine with the given configuration.
    pub fn new(config: DiagnosticsConfig) -> Self {
        Self {
            latency_trackers: DashMap::new(),
            slow_log: RwLock::new(VecDeque::with_capacity(config.slow_log_max_len)),
            slow_log_next_id: AtomicU64::new(0),
            total_latency_events: AtomicU64::new(0),
            crash_reports_generated: AtomicU64::new(0),
            created_at: Instant::now(),
            config,
        }
    }

    /// Record a latency event. If latency tracking is disabled the event is
    /// silently dropped.
    pub fn record_latency_event(&self, event: LatencyEvent) {
        if !self.config.enable_latency_tracking {
            return;
        }

        self.total_latency_events.fetch_add(1, Ordering::Relaxed);

        let history_size = self.config.latency_history_size;
        self.latency_trackers
            .entry(event.event_type.clone())
            .or_insert_with(|| LatencyTracker::new(history_size))
            .record(event.latency_us, event.timestamp);
    }

    /// Return the latency history for `event_type`. Returns an empty `Vec` if
    /// the event type has never been recorded.
    pub fn get_latency_history(&self, event_type: &str) -> Vec<LatencySample> {
        self.latency_trackers
            .get(event_type)
            .map(|t| t.history())
            .unwrap_or_default()
    }

    /// Return the most recent latency sample for `event_type`.
    pub fn get_latency_latest(&self, event_type: &str) -> Option<LatencySample> {
        self.latency_trackers
            .get(event_type)
            .and_then(|t| t.latest())
    }

    /// Return the most recent `count` slow log entries (newest first).
    pub fn get_slow_log(&self, count: usize) -> Vec<SlowLogEntry> {
        let log = self.slow_log.read();
        log.iter().rev().take(count).cloned().collect()
    }

    /// Clear all slow log entries.
    pub fn reset_slow_log(&self) {
        let mut log = self.slow_log.write();
        log.clear();
    }

    /// Add a slow log entry. If the log has reached its maximum length the
    /// oldest entry is evicted.
    pub fn add_slow_entry(&self, mut entry: SlowLogEntry) {
        entry.id = self.slow_log_next_id.fetch_add(1, Ordering::Relaxed);

        let mut log = self.slow_log.write();
        if log.len() >= self.config.slow_log_max_len {
            log.pop_front();
        }
        log.push_back(entry);
    }

    /// Generate a crash report snapshot capturing current engine state.
    pub fn generate_crash_report(&self) -> CrashReport {
        self.crash_reports_generated.fetch_add(1, Ordering::Relaxed);

        let slow_log = self.slow_log.read();
        let last_commands: Vec<String> = slow_log
            .iter()
            .rev()
            .take(10)
            .map(|e| e.command.clone())
            .collect();

        let mut config_summary = HashMap::new();
        config_summary.insert(
            "slow_log_threshold_us".into(),
            self.config.slow_log_threshold_us.to_string(),
        );
        config_summary.insert(
            "slow_log_max_len".into(),
            self.config.slow_log_max_len.to_string(),
        );
        config_summary.insert(
            "latency_history_size".into(),
            self.config.latency_history_size.to_string(),
        );
        config_summary.insert(
            "enable_latency_tracking".into(),
            self.config.enable_latency_tracking.to_string(),
        );

        CrashReport {
            timestamp: Utc::now(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            os: std::env::consts::OS.to_string(),
            rust_version: rustc_version_string(),
            uptime_secs: self.created_at.elapsed().as_secs(),
            last_commands,
            memory_usage: format_memory_usage(),
            config_summary,
            stack_trace: None,
            open_connections: 0,
            error_message: String::new(),
        }
    }

    /// Return runtime debug information.
    pub fn get_debug_info(&self) -> DebugInfo {
        DebugInfo {
            server_version: env!("CARGO_PKG_VERSION").to_string(),
            rust_version: rustc_version_string(),
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            pid: std::process::id(),
            uptime_secs: self.created_at.elapsed().as_secs(),
            connected_clients: 0,
            used_memory_bytes: 0,
            peak_memory_bytes: 0,
            total_commands_processed: 0,
            instantaneous_ops_per_sec: 0.0,
            latest_fork_usec: 0,
            io_threads_active: 0,
            aof_enabled: false,
            cluster_enabled: false,
            event_loop_iterations: 0,
        }
    }

    /// Return aggregate statistics about the diagnostics engine.
    pub fn get_stats(&self) -> DiagnosticsStats {
        let slow_log = self.slow_log.read();
        DiagnosticsStats {
            latency_events_recorded: self.total_latency_events.load(Ordering::Relaxed),
            slow_log_entries: slow_log.len() as u64,
            crash_reports_generated: self.crash_reports_generated.load(Ordering::Relaxed),
            event_types_tracked: self.latency_trackers.len(),
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Best-effort rustc version string.
fn rustc_version_string() -> String {
    option_env!("RUSTC_VERSION")
        .map(String::from)
        .unwrap_or_else(|| "unknown".to_string())
}

/// Best-effort human-readable memory usage.
fn format_memory_usage() -> String {
    // On Linux we could parse /proc/self/status; keep it simple here.
    "unavailable".to_string()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    // -- helpers -----------------------------------------------------------

    fn default_engine() -> DiagnosticsEngine {
        DiagnosticsEngine::new(DiagnosticsConfig::default())
    }

    fn make_event(event_type: &str, latency_us: u64) -> LatencyEvent {
        LatencyEvent {
            event_type: event_type.to_string(),
            latency_us,
            timestamp: Utc::now(),
        }
    }

    fn make_slow_entry(command: &str, duration_us: u64) -> SlowLogEntry {
        SlowLogEntry {
            id: 0,
            timestamp: Utc::now(),
            duration_us,
            command: command.to_string(),
            args: vec![],
            client_addr: None,
            client_name: None,
            database: 0,
        }
    }

    // -- latency event recording & history ---------------------------------

    #[test]
    fn test_record_and_retrieve_latency() {
        let engine = default_engine();
        engine.record_latency_event(make_event("command", 500));
        engine.record_latency_event(make_event("command", 1_500));

        let history = engine.get_latency_history("command");
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].latency_us, 500);
        assert_eq!(history[1].latency_us, 1_500);
    }

    #[test]
    fn test_latency_aggregates() {
        let engine = default_engine();
        engine.record_latency_event(make_event("command", 100));
        engine.record_latency_event(make_event("command", 300));

        let latest = engine.get_latency_latest("command").unwrap();
        assert_eq!(latest.min_us, 100);
        assert_eq!(latest.max_us, 300);
        assert_eq!(latest.avg_us, 200);
        assert_eq!(latest.samples, 2);
    }

    #[test]
    fn test_latency_history_ring_buffer() {
        let config = DiagnosticsConfig {
            latency_history_size: 3,
            ..Default::default()
        };
        let engine = DiagnosticsEngine::new(config);

        for i in 1..=5 {
            engine.record_latency_event(make_event("command", i * 100));
        }

        let history = engine.get_latency_history("command");
        assert_eq!(history.len(), 3);
        // oldest two (100, 200) evicted
        assert_eq!(history[0].latency_us, 300);
        assert_eq!(history[1].latency_us, 400);
        assert_eq!(history[2].latency_us, 500);
    }

    #[test]
    fn test_latency_unknown_event_type() {
        let engine = default_engine();
        let history = engine.get_latency_history("nonexistent");
        assert!(history.is_empty());
        assert!(engine.get_latency_latest("nonexistent").is_none());
    }

    #[test]
    fn test_latency_tracking_disabled() {
        let config = DiagnosticsConfig {
            enable_latency_tracking: false,
            ..Default::default()
        };
        let engine = DiagnosticsEngine::new(config);
        engine.record_latency_event(make_event("command", 500));

        assert!(engine.get_latency_history("command").is_empty());
        assert_eq!(engine.get_stats().latency_events_recorded, 0);
    }

    #[test]
    fn test_multiple_event_types() {
        let engine = default_engine();
        engine.record_latency_event(make_event("command", 100));
        engine.record_latency_event(make_event("aof-write", 200));
        engine.record_latency_event(make_event("rdb-save", 300));

        assert_eq!(engine.get_latency_history("command").len(), 1);
        assert_eq!(engine.get_latency_history("aof-write").len(), 1);
        assert_eq!(engine.get_latency_history("rdb-save").len(), 1);
        assert_eq!(engine.get_stats().event_types_tracked, 3);
    }

    // -- slow log ----------------------------------------------------------

    #[test]
    fn test_slow_log_add_and_retrieve() {
        let engine = default_engine();
        engine.add_slow_entry(make_slow_entry("SET", 15_000));
        engine.add_slow_entry(make_slow_entry("GET", 20_000));

        let entries = engine.get_slow_log(10);
        assert_eq!(entries.len(), 2);
        // newest first
        assert_eq!(entries[0].command, "GET");
        assert_eq!(entries[1].command, "SET");
    }

    #[test]
    fn test_slow_log_fifo_eviction() {
        let config = DiagnosticsConfig {
            slow_log_max_len: 3,
            ..Default::default()
        };
        let engine = DiagnosticsEngine::new(config);

        engine.add_slow_entry(make_slow_entry("CMD1", 100));
        engine.add_slow_entry(make_slow_entry("CMD2", 200));
        engine.add_slow_entry(make_slow_entry("CMD3", 300));
        engine.add_slow_entry(make_slow_entry("CMD4", 400));

        let entries = engine.get_slow_log(10);
        assert_eq!(entries.len(), 3);
        // newest first; CMD1 was evicted
        assert_eq!(entries[0].command, "CMD4");
        assert_eq!(entries[1].command, "CMD3");
        assert_eq!(entries[2].command, "CMD2");
    }

    #[test]
    fn test_slow_log_count_limit() {
        let engine = default_engine();
        for i in 0..10 {
            engine.add_slow_entry(make_slow_entry(&format!("CMD{i}"), 1000));
        }

        let entries = engine.get_slow_log(3);
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn test_slow_log_monotonic_ids() {
        let engine = default_engine();
        engine.add_slow_entry(make_slow_entry("A", 100));
        engine.add_slow_entry(make_slow_entry("B", 200));

        let entries = engine.get_slow_log(10);
        assert!(entries[0].id > entries[1].id);
    }

    #[test]
    fn test_reset_slow_log() {
        let engine = default_engine();
        engine.add_slow_entry(make_slow_entry("SET", 15_000));
        assert_eq!(engine.get_slow_log(10).len(), 1);

        engine.reset_slow_log();
        assert!(engine.get_slow_log(10).is_empty());
    }

    // -- crash report ------------------------------------------------------

    #[test]
    fn test_generate_crash_report() {
        let engine = default_engine();
        engine.add_slow_entry(make_slow_entry("SET", 15_000));

        let report = engine.generate_crash_report();
        assert!(!report.version.is_empty());
        assert!(!report.os.is_empty());
        assert_eq!(report.last_commands.len(), 1);
        assert_eq!(report.last_commands[0], "SET");
        assert!(report.config_summary.contains_key("slow_log_threshold_us"));
    }

    #[test]
    fn test_crash_report_increments_stats() {
        let engine = default_engine();
        assert_eq!(engine.get_stats().crash_reports_generated, 0);

        let _ = engine.generate_crash_report();
        assert_eq!(engine.get_stats().crash_reports_generated, 1);

        let _ = engine.generate_crash_report();
        assert_eq!(engine.get_stats().crash_reports_generated, 2);
    }

    // -- debug info --------------------------------------------------------

    #[test]
    fn test_get_debug_info() {
        let engine = default_engine();
        let info = engine.get_debug_info();

        assert!(!info.server_version.is_empty());
        assert!(!info.os.is_empty());
        assert!(!info.arch.is_empty());
        assert!(info.pid > 0);
    }

    // -- log entry formatting ----------------------------------------------

    #[test]
    fn test_log_entry_to_json() {
        let entry = LogEntry {
            timestamp: Utc::now(),
            level: LogLevel::Info,
            module: "ferrite::server".into(),
            message: "listening".into(),
            fields: HashMap::from([("port".into(), "6379".into())]),
        };

        let json = entry.to_json();
        assert!(json.contains("\"level\":\"Info\""));
        assert!(json.contains("\"message\":\"listening\""));
        assert!(json.contains("\"port\":\"6379\""));
    }

    #[test]
    fn test_log_entry_to_text() {
        let entry = LogEntry {
            timestamp: Utc::now(),
            level: LogLevel::Warn,
            module: "ferrite::auth".into(),
            message: "bad password".into(),
            fields: HashMap::new(),
        };

        let text = entry.to_text();
        assert!(text.contains("[WARN]"));
        assert!(text.contains("ferrite::auth"));
        assert!(text.contains("bad password"));
    }

    #[test]
    fn test_log_entry_to_text_with_fields() {
        let entry = LogEntry {
            timestamp: Utc::now(),
            level: LogLevel::Info,
            module: "ferrite::server".into(),
            message: "connected".into(),
            fields: HashMap::from([("client".into(), "127.0.0.1".into())]),
        };

        let text = entry.to_text();
        assert!(text.contains("client=127.0.0.1"));
    }

    #[test]
    fn test_log_entry_to_compact() {
        let entry = LogEntry {
            timestamp: Utc::now(),
            level: LogLevel::Error,
            module: "ferrite::storage".into(),
            message: "disk full".into(),
            fields: HashMap::new(),
        };

        let compact = entry.to_compact();
        assert!(compact.contains("ERROR"));
        assert!(compact.contains("ferrite::storage"));
        assert!(compact.contains("disk full"));
        // compact uses unix timestamp
        assert!(compact.starts_with(char::is_numeric));
    }

    // -- per-module log levels ---------------------------------------------

    #[test]
    fn test_per_module_levels_config() {
        let mut levels = HashMap::new();
        levels.insert("ferrite::storage".into(), LogLevel::Debug);
        levels.insert("ferrite::network".into(), LogLevel::Warn);

        let config = DiagnosticsConfig {
            per_module_levels: levels,
            ..Default::default()
        };

        assert_eq!(
            config.per_module_levels.get("ferrite::storage"),
            Some(&LogLevel::Debug)
        );
        assert_eq!(
            config.per_module_levels.get("ferrite::network"),
            Some(&LogLevel::Warn)
        );
        assert!(config.per_module_levels.get("ferrite::auth").is_none());
    }

    #[test]
    fn test_log_level_ordering() {
        assert!(LogLevel::Trace < LogLevel::Debug);
        assert!(LogLevel::Debug < LogLevel::Info);
        assert!(LogLevel::Info < LogLevel::Warn);
        assert!(LogLevel::Warn < LogLevel::Error);
    }

    #[test]
    fn test_log_level_display() {
        assert_eq!(LogLevel::Trace.to_string(), "TRACE");
        assert_eq!(LogLevel::Debug.to_string(), "DEBUG");
        assert_eq!(LogLevel::Info.to_string(), "INFO");
        assert_eq!(LogLevel::Warn.to_string(), "WARN");
        assert_eq!(LogLevel::Error.to_string(), "ERROR");
    }

    // -- event type constants ----------------------------------------------

    #[test]
    fn test_latency_event_types_all() {
        assert_eq!(latency_event_types::ALL.len(), 13);
        assert!(latency_event_types::ALL.contains(&"command"));
        assert!(latency_event_types::ALL.contains(&"aof-write"));
        assert!(latency_event_types::ALL.contains(&"rdb-save"));
        assert!(latency_event_types::ALL.contains(&"io-uring-submit"));
        assert!(latency_event_types::ALL.contains(&"io-uring-complete"));
        assert!(latency_event_types::ALL.contains(&"fork"));
        assert!(latency_event_types::ALL.contains(&"expire-cycle"));
        assert!(latency_event_types::ALL.contains(&"eviction-cycle"));
        assert!(latency_event_types::ALL.contains(&"tiering-migration"));
        assert!(latency_event_types::ALL.contains(&"replication-sync"));
        assert!(latency_event_types::ALL.contains(&"cluster-message"));
        assert!(latency_event_types::ALL.contains(&"defrag-cycle"));
        assert!(latency_event_types::ALL.contains(&"client-pause"));
    }

    #[test]
    fn test_latency_event_type_constants_match() {
        assert_eq!(latency_event_types::COMMAND, "command");
        assert_eq!(latency_event_types::AOF_WRITE, "aof-write");
        assert_eq!(latency_event_types::IO_URING_SUBMIT, "io-uring-submit");
        assert_eq!(latency_event_types::CLIENT_PAUSE, "client-pause");
    }

    // -- stats -------------------------------------------------------------

    #[test]
    fn test_stats_initial() {
        let engine = default_engine();
        let stats = engine.get_stats();
        assert_eq!(stats.latency_events_recorded, 0);
        assert_eq!(stats.slow_log_entries, 0);
        assert_eq!(stats.crash_reports_generated, 0);
        assert_eq!(stats.event_types_tracked, 0);
    }

    #[test]
    fn test_stats_after_activity() {
        let engine = default_engine();
        engine.record_latency_event(make_event("command", 100));
        engine.record_latency_event(make_event("aof-write", 200));
        engine.add_slow_entry(make_slow_entry("SET", 15_000));
        let _ = engine.generate_crash_report();

        let stats = engine.get_stats();
        assert_eq!(stats.latency_events_recorded, 2);
        assert_eq!(stats.slow_log_entries, 1);
        assert_eq!(stats.crash_reports_generated, 1);
        assert_eq!(stats.event_types_tracked, 2);
    }

    // -- config defaults ---------------------------------------------------

    #[test]
    fn test_config_defaults() {
        let config = DiagnosticsConfig::default();
        assert_eq!(config.slow_log_threshold_us, 10_000);
        assert_eq!(config.slow_log_max_len, 128);
        assert_eq!(config.latency_history_size, 160);
        assert!(config.enable_latency_tracking);
        assert_eq!(config.log_format, LogFormat::Text);
        assert!(config.crash_dump_path.is_none());
        assert!(config.per_module_levels.is_empty());
    }

    // -- error display -----------------------------------------------------

    #[test]
    fn test_error_display() {
        let err = DiagnosticsError::EventTypeNotFound("foo".into());
        assert_eq!(err.to_string(), "event type not found: foo");

        let err = DiagnosticsError::SlowLogFull;
        assert_eq!(err.to_string(), "slow log is full");

        let err = DiagnosticsError::CrashDumpFailed("disk full".into());
        assert_eq!(err.to_string(), "crash dump failed: disk full");

        let err = DiagnosticsError::Internal("oops".into());
        assert_eq!(err.to_string(), "internal error: oops");
    }

    // -- thread safety -----------------------------------------------------

    #[test]
    fn test_concurrent_latency_recording() {
        use std::sync::Arc;
        use std::thread;

        let engine = Arc::new(default_engine());
        let mut handles = vec![];

        for i in 0..8 {
            let engine = Arc::clone(&engine);
            handles.push(thread::spawn(move || {
                for j in 0..100 {
                    engine.record_latency_event(make_event(
                        latency_event_types::COMMAND,
                        (i * 100 + j) as u64,
                    ));
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(engine.get_stats().latency_events_recorded, 800);
        let history = engine.get_latency_history(latency_event_types::COMMAND);
        // capped at latency_history_size (160)
        assert!(history.len() <= 160);
    }

    #[test]
    fn test_concurrent_slow_log() {
        use std::sync::Arc;
        use std::thread;

        let config = DiagnosticsConfig {
            slow_log_max_len: 50,
            ..Default::default()
        };
        let engine = Arc::new(DiagnosticsEngine::new(config));
        let mut handles = vec![];

        for i in 0..8 {
            let engine = Arc::clone(&engine);
            handles.push(thread::spawn(move || {
                for j in 0..50 {
                    engine.add_slow_entry(make_slow_entry(&format!("CMD-{i}-{j}"), 1000));
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // never exceeds max_len
        assert!(engine.get_slow_log(100).len() <= 50);
    }
}
