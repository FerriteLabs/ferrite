//! eBPF Zero-Overhead Observability for Ferrite
//!
//! Provides kernel-level tracing via eBPF probes with zero application overhead
//! on Linux, and graceful degradation on other platforms.
//!
//! # Platform Support
//!
//! - **Linux (kernel ≥ 5.15):** Full eBPF probe management with kprobes, uprobes,
//!   tracepoints, USDT, and perf events.
//! - **Other platforms:** `is_available()` returns `false` and probe attachment
//!   returns [`EbpfError::Unsupported`]. Data structures (histograms, stats) remain
//!   fully functional.
//!
//! # Architecture
//!
//! Actual eBPF bytecode loading requires `libbpf` or the `aya` crate. This module
//! provides the management layer and data structures; real BPF program attachment
//! is stubbed with descriptive log messages until a BPF runtime dependency is added.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Built-in Ferrite probe definitions
// ---------------------------------------------------------------------------

/// USDT probe at the command dispatch entry point.
pub const PROBE_COMMAND_DISPATCH: &str = "ferrite:command_dispatch";

/// USDT probe at the storage read path.
pub const PROBE_STORAGE_READ: &str = "ferrite:storage_read";

/// USDT probe at the storage write path.
pub const PROBE_STORAGE_WRITE: &str = "ferrite:storage_write";

/// Kprobe / tracepoint at TCP receive.
pub const PROBE_NETWORK_RECV: &str = "ferrite:network_recv";

/// Kprobe / tracepoint at TCP send.
pub const PROBE_NETWORK_SEND: &str = "ferrite:network_send";

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can occur during eBPF probe management.
#[derive(Debug, thiserror::Error)]
pub enum EbpfError {
    /// eBPF is not supported on this platform or kernel.
    #[error("eBPF unsupported: {0}")]
    Unsupported(String),

    /// Insufficient permissions (CAP_BPF / CAP_SYS_ADMIN required).
    #[error("permission denied: CAP_BPF or CAP_SYS_ADMIN required")]
    PermissionDenied,

    /// The requested probe was not found.
    #[error("probe not found")]
    ProbeNotFound,

    /// Failed to attach a probe to the target.
    #[error("attach failed: {0}")]
    AttachFailed(String),

    /// Perf ring-buffer overflow — events were dropped.
    #[error("perf buffer overflow: events were dropped")]
    BufferOverflow,

    /// Kernel version is too old to support required eBPF features.
    #[error("kernel version too old: {0}")]
    KernelVersionTooOld(String),

    /// Maximum number of concurrent probes exceeded.
    #[error("max probes exceeded")]
    MaxProbesExceeded,

    /// Catch-all for unexpected internal errors.
    #[error("internal error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the eBPF subsystem.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EbpfConfig {
    /// Master switch — when `false` the manager is inert.
    pub enabled: bool,
    /// Number of pages per perf ring-buffer (must be a power of two).
    pub probe_buffer_pages: u32,
    /// Sampling frequency in Hz for perf-event based probes.
    pub sample_rate_hz: u32,
    /// Hard limit on concurrently attached probes.
    pub max_probes: usize,
    /// Latency histogram bucket boundaries in nanoseconds.
    pub histogram_buckets: Vec<u64>,
    /// Track system call counts and latency.
    pub enable_syscall_tracking: bool,
    /// Track network flow metadata.
    pub enable_network_tracking: bool,
    /// Track memory allocation patterns.
    pub enable_memory_tracking: bool,
}

impl Default for EbpfConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            probe_buffer_pages: 64,
            sample_rate_hz: 99,
            max_probes: 128,
            histogram_buckets: vec![
                1_000,      // 1 µs
                10_000,     // 10 µs
                50_000,     // 50 µs
                100_000,    // 100 µs
                500_000,    // 500 µs
                1_000_000,  // 1 ms
                5_000_000,  // 5 ms
                10_000_000, // 10 ms
                50_000_000, // 50 ms
            ],
            enable_syscall_tracking: true,
            enable_network_tracking: true,
            enable_memory_tracking: false,
        }
    }
}

impl EbpfConfig {
    /// Validate the configuration, returning an error message if invalid.
    pub fn validate(&self) -> Result<(), String> {
        if self.probe_buffer_pages == 0 || !self.probe_buffer_pages.is_power_of_two() {
            return Err(format!(
                "probe_buffer_pages must be a positive power of two, got {}",
                self.probe_buffer_pages
            ));
        }
        if self.sample_rate_hz == 0 {
            return Err("sample_rate_hz must be > 0".into());
        }
        if self.max_probes == 0 {
            return Err("max_probes must be > 0".into());
        }
        if self.histogram_buckets.is_empty() {
            return Err("histogram_buckets must not be empty".into());
        }
        for w in self.histogram_buckets.windows(2) {
            if w[0] >= w[1] {
                return Err(format!(
                    "histogram_buckets must be strictly ascending, found {} >= {}",
                    w[0], w[1]
                ));
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Probe specification & identification
// ---------------------------------------------------------------------------

/// Unique identifier for an attached probe.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProbeId(pub String);

impl ProbeId {
    /// Generate a new random probe identifier.
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

impl Default for ProbeId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ProbeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The kind of eBPF probe.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProbeType {
    /// Kernel function probe.
    Kprobe,
    /// User-space function probe.
    Uprobe,
    /// Kernel tracepoint.
    Tracepoint,
    /// User Statically Defined Tracing probe.
    Usdt,
    /// Hardware / software perf event.
    PerfEvent,
    /// Raw tracepoint (no stable ABI).
    RawTracepoint,
}

impl std::fmt::Display for ProbeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Kprobe => "kprobe",
            Self::Uprobe => "uprobe",
            Self::Tracepoint => "tracepoint",
            Self::Usdt => "usdt",
            Self::PerfEvent => "perf_event",
            Self::RawTracepoint => "raw_tracepoint",
        };
        f.write_str(s)
    }
}

/// Where to attach the probe.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProbeTarget {
    /// A kernel function (e.g. `tcp_sendmsg`).
    Kernel {
        /// Fully qualified kernel symbol name.
        function: String,
    },
    /// A user-space function in a specific binary.
    Userspace {
        /// Path to the ELF binary.
        binary: String,
        /// Symbol name within the binary.
        function: String,
    },
    /// A kernel tracepoint such as `syscalls:sys_enter_read`.
    Tracepoint {
        /// Tracepoint category (e.g. `syscalls`).
        category: String,
        /// Tracepoint name (e.g. `sys_enter_read`).
        name: String,
    },
}

impl std::fmt::Display for ProbeTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Kernel { function } => write!(f, "kernel:{function}"),
            Self::Userspace { binary, function } => write!(f, "uprobe:{binary}:{function}"),
            Self::Tracepoint { category, name } => write!(f, "tracepoint:{category}:{name}"),
        }
    }
}

/// Full specification for a probe to be attached.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProbeSpec {
    /// Human-readable name for this probe.
    pub name: String,
    /// The kind of probe.
    pub probe_type: ProbeType,
    /// Where to attach.
    pub target: ProbeTarget,
    /// Optional BPF filter expression.
    pub filter: Option<String>,
}

// ---------------------------------------------------------------------------
// Probe runtime state
// ---------------------------------------------------------------------------

/// Runtime status of an attached probe.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProbeStatus {
    /// Probe is actively collecting events.
    Active,
    /// Probe has been detached.
    Detached,
    /// Probe encountered an error.
    Error(String),
    /// Probe type is not supported on this platform.
    Unsupported,
}

impl std::fmt::Display for ProbeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => f.write_str("active"),
            Self::Detached => f.write_str("detached"),
            Self::Error(e) => write!(f, "error: {e}"),
            Self::Unsupported => f.write_str("unsupported"),
        }
    }
}

/// Metadata about an attached probe.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProbeInfo {
    /// Unique probe identifier.
    pub id: ProbeId,
    /// Human-readable name.
    pub name: String,
    /// Kind of probe.
    pub probe_type: ProbeType,
    /// Attachment target.
    pub target: ProbeTarget,
    /// Current status.
    pub status: ProbeStatus,
    /// Number of events captured since attachment.
    pub events_captured: u64,
    /// Timestamp when the probe was attached.
    pub attached_at: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Latency histogram
// ---------------------------------------------------------------------------

/// A single histogram bucket.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistogramBucket {
    /// Upper bound of this bucket in nanoseconds (inclusive).
    pub le_ns: u64,
    /// Number of observations that fell into this bucket.
    pub count: u64,
}

/// Pre-computed latency percentiles.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Percentiles {
    /// 50th percentile (median) in nanoseconds.
    pub p50_ns: u64,
    /// 90th percentile in nanoseconds.
    pub p90_ns: u64,
    /// 95th percentile in nanoseconds.
    pub p95_ns: u64,
    /// 99th percentile in nanoseconds.
    pub p99_ns: u64,
    /// 99.9th percentile in nanoseconds.
    pub p999_ns: u64,
}

/// Latency distribution collected from a single probe.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LatencyHistogram {
    /// Probe that produced these observations.
    pub probe_id: ProbeId,
    /// Ordered histogram buckets.
    pub buckets: Vec<HistogramBucket>,
    /// Total number of observations.
    pub total_count: u64,
    /// Sum of all observed latencies in nanoseconds.
    pub sum_ns: u64,
    /// Minimum observed latency in nanoseconds.
    pub min_ns: u64,
    /// Maximum observed latency in nanoseconds.
    pub max_ns: u64,
}

impl LatencyHistogram {
    /// Compute standard latency percentiles from the histogram buckets.
    ///
    /// Uses linear interpolation within buckets to estimate percentile values.
    pub fn percentiles(&self) -> Percentiles {
        let pcts = [0.50, 0.90, 0.95, 0.99, 0.999];
        let mut results = [0u64; 5];

        for (i, &pct) in pcts.iter().enumerate() {
            results[i] = self.percentile_value(pct);
        }

        Percentiles {
            p50_ns: results[0],
            p90_ns: results[1],
            p95_ns: results[2],
            p99_ns: results[3],
            p999_ns: results[4],
        }
    }

    /// Estimate the value at a given percentile (0.0–1.0).
    fn percentile_value(&self, pct: f64) -> u64 {
        if self.total_count == 0 || self.buckets.is_empty() {
            return 0;
        }

        let target = (pct * self.total_count as f64).ceil() as u64;
        let mut cumulative: u64 = 0;
        let mut prev_bound: u64 = 0;

        for bucket in &self.buckets {
            cumulative += bucket.count;
            if cumulative >= target {
                // Linear interpolation within this bucket.
                let bucket_start = prev_bound;
                let bucket_end = bucket.le_ns;
                if bucket.count == 0 {
                    return bucket_end;
                }
                let rank_in_bucket = target.saturating_sub(cumulative - bucket.count);
                let fraction = rank_in_bucket as f64 / bucket.count as f64;
                return bucket_start + ((bucket_end - bucket_start) as f64 * fraction) as u64;
            }
            prev_bound = bucket.le_ns;
        }

        // Beyond last bucket — return max.
        self.max_ns
    }
}

// ---------------------------------------------------------------------------
// Syscall stats
// ---------------------------------------------------------------------------

/// Profiling data for a single system call.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyscallEntry {
    /// System call name (e.g. `read`, `write`, `epoll_wait`).
    pub name: String,
    /// Number of invocations.
    pub count: u64,
    /// Cumulative wall-clock time in nanoseconds.
    pub total_time_ns: u64,
    /// Average latency in nanoseconds.
    pub avg_time_ns: u64,
    /// Maximum latency observed in nanoseconds.
    pub max_time_ns: u64,
    /// Number of invocations that returned an error.
    pub errors: u64,
}

/// Aggregated system call statistics.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyscallStats {
    /// Syscalls ordered by total time (descending).
    pub top_syscalls: Vec<SyscallEntry>,
    /// Total number of system calls observed.
    pub total_syscalls: u64,
    /// Duration of the collection window in seconds.
    pub collection_period_secs: u64,
}

// ---------------------------------------------------------------------------
// Network flow tracking
// ---------------------------------------------------------------------------

/// A single observed network flow.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkFlow {
    /// Source address (IP:port).
    pub source_addr: String,
    /// Destination address (IP:port).
    pub dest_addr: String,
    /// Bytes transmitted.
    pub bytes_sent: u64,
    /// Bytes received.
    pub bytes_recv: u64,
    /// Packet count.
    pub packets: u64,
    /// L4 protocol (e.g. `tcp`, `udp`).
    pub protocol: String,
    /// Observed round-trip latency in nanoseconds.
    pub latency_ns: u64,
}

// ---------------------------------------------------------------------------
// Flamegraph
// ---------------------------------------------------------------------------

/// A single function in the sampled profile.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FunctionSample {
    /// Demangled function name.
    pub function_name: String,
    /// Binary / shared-object that contains the function.
    pub module: String,
    /// Number of times this function appeared on-CPU.
    pub samples: u64,
    /// Percentage of total samples.
    pub percentage: f64,
}

/// Flame graph data in folded-stack format.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FlamegraphData {
    /// Folded stacks suitable for `flamegraph.pl` / inferno.
    pub folded_stacks: String,
    /// Total number of stack samples collected.
    pub total_samples: u64,
    /// Wall-clock duration of the sampling window in seconds.
    pub duration_secs: u64,
    /// Top functions by sample count.
    pub top_functions: Vec<FunctionSample>,
}

// ---------------------------------------------------------------------------
// Manager-level stats
// ---------------------------------------------------------------------------

/// Summary statistics for the eBPF subsystem.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EbpfStats {
    /// Number of probes currently attached.
    pub probes_attached: u64,
    /// Total events captured across all probes.
    pub events_captured: u64,
    /// Total bytes processed from perf ring-buffers.
    pub bytes_processed: u64,
    /// Number of errors encountered.
    pub errors: u64,
    /// Seconds since the manager was created.
    pub uptime_secs: u64,
}

// ---------------------------------------------------------------------------
// Internal probe state
// ---------------------------------------------------------------------------

/// Per-probe runtime bookkeeping.
struct ProbeState {
    info: ProbeInfo,
    events: AtomicU64,
    histogram: parking_lot::RwLock<Option<LatencyHistogram>>,
}

// ---------------------------------------------------------------------------
// EbpfManager
// ---------------------------------------------------------------------------

/// Manages eBPF probes and aggregates collected telemetry.
///
/// On Linux with a sufficiently recent kernel the manager can attach kprobes,
/// uprobes, tracepoints, USDT probes, and perf events. On other platforms all
/// attachment calls return [`EbpfError::Unsupported`] while the data-only
/// structures remain usable.
pub struct EbpfManager {
    config: EbpfConfig,
    probes: DashMap<ProbeId, Arc<ProbeState>>,
    total_events: AtomicU64,
    total_bytes: AtomicU64,
    total_errors: AtomicU64,
    started_at: Instant,
}

impl EbpfManager {
    /// Create a new eBPF manager with the given configuration.
    pub fn new(config: EbpfConfig) -> Self {
        Self {
            config,
            probes: DashMap::new(),
            total_events: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            started_at: Instant::now(),
        }
    }

    /// Returns `true` when eBPF is available on this platform.
    ///
    /// On Linux this checks for kernel ≥ 5.15. On all other operating systems
    /// this unconditionally returns `false`.
    pub fn is_available(&self) -> bool {
        Self::platform_available()
    }

    /// Attach a probe described by `spec`.
    ///
    /// Returns the [`ProbeId`] that can be used to query data or detach later.
    pub fn attach_probe(&self, spec: ProbeSpec) -> Result<ProbeId, EbpfError> {
        if !self.config.enabled {
            return Err(EbpfError::Unsupported(
                "eBPF subsystem is disabled by configuration".into(),
            ));
        }

        if !self.is_available() {
            return Err(EbpfError::Unsupported(
                "eBPF is not available on this platform".into(),
            ));
        }

        if self.probes.len() >= self.config.max_probes {
            return Err(EbpfError::MaxProbesExceeded);
        }

        let id = ProbeId::new();
        let histogram = self.create_empty_histogram(&id);

        let state = Arc::new(ProbeState {
            info: ProbeInfo {
                id: id.clone(),
                name: spec.name.clone(),
                probe_type: spec.probe_type.clone(),
                target: spec.target.clone(),
                status: ProbeStatus::Active,
                events_captured: 0,
                attached_at: Utc::now(),
            },
            events: AtomicU64::new(0),
            histogram: parking_lot::RwLock::new(Some(histogram)),
        });

        // NOTE: Real eBPF bytecode loading would happen here via libbpf / aya.
        // For now we log what *would* be attached.
        #[cfg(target_os = "linux")]
        {
            tracing::info!(
                probe_id = %id,
                name = %spec.name,
                probe_type = %spec.probe_type,
                target = %spec.target,
                filter = ?spec.filter,
                "eBPF probe attached (stub — no BPF bytecode loaded)"
            );
        }

        self.probes.insert(id.clone(), state);
        Ok(id)
    }

    /// Detach a previously attached probe.
    pub fn detach_probe(&self, id: &ProbeId) -> Result<(), EbpfError> {
        let mut entry = self.probes.get_mut(id).ok_or(EbpfError::ProbeNotFound)?;

        // Build updated info with Detached status.
        let state = entry.value_mut();
        let updated_info = ProbeInfo {
            status: ProbeStatus::Detached,
            events_captured: state.events.load(Ordering::Relaxed),
            ..state.info.clone()
        };

        // Replace the state with an updated snapshot. In a real implementation
        // this would also tear down the perf ring-buffer fd.
        let new_state = Arc::new(ProbeState {
            info: updated_info,
            events: AtomicU64::new(state.events.load(Ordering::Relaxed)),
            histogram: parking_lot::RwLock::new(state.histogram.read().clone()),
        });
        *state = new_state;

        drop(entry);

        #[cfg(target_os = "linux")]
        tracing::info!(probe_id = %id, "eBPF probe detached");

        Ok(())
    }

    /// List all probes (active and detached).
    pub fn list_probes(&self) -> Vec<ProbeInfo> {
        self.probes
            .iter()
            .map(|entry| {
                let state = entry.value();
                ProbeInfo {
                    events_captured: state.events.load(Ordering::Relaxed),
                    ..state.info.clone()
                }
            })
            .collect()
    }

    /// Retrieve the latency histogram for a specific probe.
    pub fn get_histogram(&self, probe_id: &ProbeId) -> Option<LatencyHistogram> {
        self.probes
            .get(probe_id)
            .and_then(|entry| entry.value().histogram.read().clone())
    }

    /// Return aggregated system call statistics.
    ///
    /// In a real deployment this would read from an eBPF map keyed by syscall
    /// number. Here we return a representative empty structure.
    pub fn get_syscall_stats(&self) -> SyscallStats {
        SyscallStats {
            top_syscalls: Vec::new(),
            total_syscalls: 0,
            collection_period_secs: self.started_at.elapsed().as_secs(),
        }
    }

    /// Return tracked network flows.
    ///
    /// In a real deployment this would drain an eBPF ring-buffer of flow
    /// events. Here we return an empty list.
    pub fn get_network_flows(&self) -> Vec<NetworkFlow> {
        Vec::new()
    }

    /// Generate a flame graph from collected stack samples.
    ///
    /// In a real deployment this would read perf-event stack traces from an
    /// eBPF map and fold them. Here we return a stub structure.
    pub fn generate_flamegraph(&self) -> Result<FlamegraphData, EbpfError> {
        if !self.is_available() {
            return Err(EbpfError::Unsupported(
                "flamegraph generation requires eBPF support".into(),
            ));
        }
        Ok(FlamegraphData {
            folded_stacks: String::new(),
            total_samples: 0,
            duration_secs: self.started_at.elapsed().as_secs(),
            top_functions: Vec::new(),
        })
    }

    /// Snapshot of manager-level statistics.
    pub fn get_stats(&self) -> EbpfStats {
        let probes_attached = self
            .probes
            .iter()
            .filter(|e| e.value().info.status == ProbeStatus::Active)
            .count() as u64;

        EbpfStats {
            probes_attached,
            events_captured: self.total_events.load(Ordering::Relaxed),
            bytes_processed: self.total_bytes.load(Ordering::Relaxed),
            errors: self.total_errors.load(Ordering::Relaxed),
            uptime_secs: self.started_at.elapsed().as_secs(),
        }
    }

    // -- internal helpers ---------------------------------------------------

    /// Create an empty histogram pre-populated with the configured buckets.
    fn create_empty_histogram(&self, probe_id: &ProbeId) -> LatencyHistogram {
        let buckets = self
            .config
            .histogram_buckets
            .iter()
            .map(|&le_ns| HistogramBucket { le_ns, count: 0 })
            .collect();

        LatencyHistogram {
            probe_id: probe_id.clone(),
            buckets,
            total_count: 0,
            sum_ns: 0,
            min_ns: u64::MAX,
            max_ns: 0,
        }
    }

    /// Platform availability check (compile-time + runtime).
    #[cfg(target_os = "linux")]
    fn platform_available() -> bool {
        Self::check_linux_kernel_version()
    }

    #[cfg(not(target_os = "linux"))]
    fn platform_available() -> bool {
        false
    }

    /// On Linux, verify the running kernel is ≥ 5.15.
    #[cfg(target_os = "linux")]
    fn check_linux_kernel_version() -> bool {
        let Ok(version_str) = std::fs::read_to_string("/proc/version") else {
            return false;
        };

        // /proc/version looks like: "Linux version 5.15.0-generic ..."
        let Some(ver) = version_str.split_whitespace().nth(2) else {
            return false;
        };

        let parts: Vec<u32> = ver
            .split(|c: char| !c.is_ascii_digit())
            .filter(|s| !s.is_empty())
            .take(2)
            .filter_map(|s| s.parse().ok())
            .collect();

        match parts.as_slice() {
            [major, minor, ..] => (*major, *minor) >= (5, 15),
            [major] => *major > 5,
            _ => false,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- helpers ------------------------------------------------------------

    fn default_manager() -> EbpfManager {
        EbpfManager::new(EbpfConfig::default())
    }

    fn sample_kprobe_spec() -> ProbeSpec {
        ProbeSpec {
            name: "test_kprobe".into(),
            probe_type: ProbeType::Kprobe,
            target: ProbeTarget::Kernel {
                function: "tcp_sendmsg".into(),
            },
            filter: None,
        }
    }

    fn sample_usdt_spec(name: &str) -> ProbeSpec {
        ProbeSpec {
            name: name.into(),
            probe_type: ProbeType::Usdt,
            target: ProbeTarget::Userspace {
                binary: "/usr/bin/ferrite".into(),
                function: name.into(),
            },
            filter: None,
        }
    }

    fn sample_tracepoint_spec() -> ProbeSpec {
        ProbeSpec {
            name: "sys_enter_read".into(),
            probe_type: ProbeType::Tracepoint,
            target: ProbeTarget::Tracepoint {
                category: "syscalls".into(),
                name: "sys_enter_read".into(),
            },
            filter: Some("pid == 1234".into()),
        }
    }

    // -- platform availability ----------------------------------------------

    #[test]
    fn test_is_available_matches_platform() {
        let mgr = default_manager();
        if cfg!(target_os = "linux") {
            // On Linux CI the result depends on kernel version; just ensure
            // it doesn't panic.
            let _ = mgr.is_available();
        } else {
            assert!(!mgr.is_available());
        }
    }

    // -- probe attach / detach / list ---------------------------------------

    #[test]
    fn test_attach_probe_unsupported_platform() {
        if cfg!(target_os = "linux") {
            return; // skip — attach may succeed on Linux
        }
        let mgr = default_manager();
        let result = mgr.attach_probe(sample_kprobe_spec());
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), EbpfError::Unsupported(_)));
    }

    #[test]
    fn test_attach_disabled_config() {
        let mut cfg = EbpfConfig::default();
        cfg.enabled = false;
        let mgr = EbpfManager::new(cfg);
        let result = mgr.attach_probe(sample_kprobe_spec());
        assert!(matches!(result.unwrap_err(), EbpfError::Unsupported(_)));
    }

    #[test]
    fn test_max_probes_exceeded() {
        // Only testable when attach succeeds (Linux).
        if !cfg!(target_os = "linux") {
            return;
        }
        let mut cfg = EbpfConfig::default();
        cfg.max_probes = 2;
        let mgr = EbpfManager::new(cfg);

        let _ = mgr.attach_probe(sample_usdt_spec("p1"));
        let _ = mgr.attach_probe(sample_usdt_spec("p2"));
        let result = mgr.attach_probe(sample_usdt_spec("p3"));

        // If attachment succeeded for the first two, the third should fail.
        if mgr.probes.len() >= 2 {
            assert!(matches!(result.unwrap_err(), EbpfError::MaxProbesExceeded));
        }
    }

    #[test]
    fn test_detach_nonexistent_probe() {
        let mgr = default_manager();
        let result = mgr.detach_probe(&ProbeId::new());
        assert!(matches!(result.unwrap_err(), EbpfError::ProbeNotFound));
    }

    #[test]
    fn test_list_probes_empty() {
        let mgr = default_manager();
        assert!(mgr.list_probes().is_empty());
    }

    #[test]
    fn test_attach_detach_list_on_linux() {
        if !cfg!(target_os = "linux") {
            return;
        }
        let mgr = default_manager();
        let id = match mgr.attach_probe(sample_kprobe_spec()) {
            Ok(id) => id,
            Err(_) => return, // kernel may not support eBPF
        };

        let probes = mgr.list_probes();
        assert_eq!(probes.len(), 1);
        assert_eq!(probes[0].status, ProbeStatus::Active);

        mgr.detach_probe(&id).unwrap();
        let probes = mgr.list_probes();
        assert_eq!(probes[0].status, ProbeStatus::Detached);
    }

    // -- histogram and percentiles ------------------------------------------

    #[test]
    fn test_empty_histogram_percentiles() {
        let hist = LatencyHistogram {
            probe_id: ProbeId::new(),
            buckets: vec![
                HistogramBucket {
                    le_ns: 1_000,
                    count: 0,
                },
                HistogramBucket {
                    le_ns: 10_000,
                    count: 0,
                },
            ],
            total_count: 0,
            sum_ns: 0,
            min_ns: u64::MAX,
            max_ns: 0,
        };
        let p = hist.percentiles();
        assert_eq!(p.p50_ns, 0);
        assert_eq!(p.p99_ns, 0);
    }

    #[test]
    fn test_histogram_percentiles_single_bucket() {
        let hist = LatencyHistogram {
            probe_id: ProbeId::new(),
            buckets: vec![
                HistogramBucket {
                    le_ns: 1_000,
                    count: 100,
                },
                HistogramBucket {
                    le_ns: 10_000,
                    count: 0,
                },
            ],
            total_count: 100,
            sum_ns: 50_000,
            min_ns: 200,
            max_ns: 980,
        };
        let p = hist.percentiles();
        // All observations are in the first bucket [0, 1000].
        assert!(p.p50_ns <= 1_000);
        assert!(p.p99_ns <= 1_000);
    }

    #[test]
    fn test_histogram_percentiles_spread() {
        let hist = LatencyHistogram {
            probe_id: ProbeId::new(),
            buckets: vec![
                HistogramBucket {
                    le_ns: 1_000,
                    count: 50,
                },
                HistogramBucket {
                    le_ns: 5_000,
                    count: 30,
                },
                HistogramBucket {
                    le_ns: 10_000,
                    count: 15,
                },
                HistogramBucket {
                    le_ns: 50_000,
                    count: 5,
                },
            ],
            total_count: 100,
            sum_ns: 500_000,
            min_ns: 100,
            max_ns: 48_000,
        };
        let p = hist.percentiles();
        assert!(p.p50_ns <= 1_000, "p50 should fall in first bucket");
        assert!(p.p90_ns <= 10_000, "p90 should fall within third bucket");
        assert!(p.p99_ns <= 50_000, "p99 should fall within last bucket");
        assert!(p.p50_ns <= p.p90_ns);
        assert!(p.p90_ns <= p.p99_ns);
        assert!(p.p99_ns <= p.p999_ns);
    }

    #[test]
    fn test_histogram_beyond_last_bucket() {
        let hist = LatencyHistogram {
            probe_id: ProbeId::new(),
            buckets: vec![HistogramBucket {
                le_ns: 1_000,
                count: 0,
            }],
            total_count: 1,
            sum_ns: 5_000,
            min_ns: 5_000,
            max_ns: 5_000,
        };
        // Observation is beyond the only bucket — percentile should return max.
        let p = hist.percentiles();
        assert_eq!(p.p50_ns, 5_000);
    }

    // -- syscall stats aggregation ------------------------------------------

    #[test]
    fn test_syscall_stats_default() {
        let mgr = default_manager();
        let stats = mgr.get_syscall_stats();
        assert!(stats.top_syscalls.is_empty());
        assert_eq!(stats.total_syscalls, 0);
    }

    #[test]
    fn test_syscall_entry_fields() {
        let entry = SyscallEntry {
            name: "read".into(),
            count: 1000,
            total_time_ns: 5_000_000,
            avg_time_ns: 5_000,
            max_time_ns: 50_000,
            errors: 3,
        };
        assert_eq!(entry.avg_time_ns, entry.total_time_ns / entry.count);
    }

    // -- network flows ------------------------------------------------------

    #[test]
    fn test_network_flows_empty() {
        let mgr = default_manager();
        assert!(mgr.get_network_flows().is_empty());
    }

    #[test]
    fn test_network_flow_construction() {
        let flow = NetworkFlow {
            source_addr: "127.0.0.1:6379".into(),
            dest_addr: "10.0.0.5:48210".into(),
            bytes_sent: 4096,
            bytes_recv: 1024,
            packets: 12,
            protocol: "tcp".into(),
            latency_ns: 150_000,
        };
        assert_eq!(flow.protocol, "tcp");
        assert!(flow.bytes_sent > flow.bytes_recv);
    }

    // -- flamegraph ---------------------------------------------------------

    #[test]
    fn test_generate_flamegraph_unsupported() {
        if cfg!(target_os = "linux") {
            return;
        }
        let mgr = default_manager();
        let result = mgr.generate_flamegraph();
        assert!(result.is_err());
    }

    #[test]
    fn test_flamegraph_data_construction() {
        let data = FlamegraphData {
            folded_stacks: "main;handle_request;storage_get 42\n".into(),
            total_samples: 42,
            duration_secs: 10,
            top_functions: vec![FunctionSample {
                function_name: "storage_get".into(),
                module: "ferrite-core".into(),
                samples: 42,
                percentage: 100.0,
            }],
        };
        assert_eq!(data.total_samples, 42);
        assert_eq!(data.top_functions.len(), 1);
        assert!((data.top_functions[0].percentage - 100.0).abs() < f64::EPSILON);
    }

    // -- manager stats ------------------------------------------------------

    #[test]
    fn test_stats_initial() {
        let mgr = default_manager();
        let stats = mgr.get_stats();
        assert_eq!(stats.probes_attached, 0);
        assert_eq!(stats.events_captured, 0);
        assert_eq!(stats.bytes_processed, 0);
        assert_eq!(stats.errors, 0);
    }

    // -- config validation --------------------------------------------------

    #[test]
    fn test_config_default_is_valid() {
        assert!(EbpfConfig::default().validate().is_ok());
    }

    #[test]
    fn test_config_invalid_buffer_pages_zero() {
        let mut cfg = EbpfConfig::default();
        cfg.probe_buffer_pages = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_invalid_buffer_pages_not_power_of_two() {
        let mut cfg = EbpfConfig::default();
        cfg.probe_buffer_pages = 3;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_invalid_sample_rate() {
        let mut cfg = EbpfConfig::default();
        cfg.sample_rate_hz = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_invalid_max_probes() {
        let mut cfg = EbpfConfig::default();
        cfg.max_probes = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_invalid_empty_buckets() {
        let mut cfg = EbpfConfig::default();
        cfg.histogram_buckets = vec![];
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_invalid_non_ascending_buckets() {
        let mut cfg = EbpfConfig::default();
        cfg.histogram_buckets = vec![10_000, 5_000, 50_000];
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_valid_single_bucket() {
        let mut cfg = EbpfConfig::default();
        cfg.histogram_buckets = vec![1_000];
        assert!(cfg.validate().is_ok());
    }

    // -- error display ------------------------------------------------------

    #[test]
    fn test_error_display() {
        let err = EbpfError::Unsupported("not linux".into());
        assert!(err.to_string().contains("not linux"));

        let err = EbpfError::PermissionDenied;
        assert!(err.to_string().contains("CAP_BPF"));

        let err = EbpfError::KernelVersionTooOld("need 5.15+".into());
        assert!(err.to_string().contains("5.15"));

        let err = EbpfError::MaxProbesExceeded;
        assert!(err.to_string().contains("max probes"));
    }

    // -- probe types display ------------------------------------------------

    #[test]
    fn test_probe_type_display() {
        assert_eq!(ProbeType::Kprobe.to_string(), "kprobe");
        assert_eq!(ProbeType::Usdt.to_string(), "usdt");
        assert_eq!(ProbeType::RawTracepoint.to_string(), "raw_tracepoint");
    }

    #[test]
    fn test_probe_target_display() {
        let t = ProbeTarget::Kernel {
            function: "tcp_sendmsg".into(),
        };
        assert_eq!(t.to_string(), "kernel:tcp_sendmsg");

        let t = ProbeTarget::Tracepoint {
            category: "syscalls".into(),
            name: "sys_enter_read".into(),
        };
        assert_eq!(t.to_string(), "tracepoint:syscalls:sys_enter_read");
    }

    #[test]
    fn test_probe_status_display() {
        assert_eq!(ProbeStatus::Active.to_string(), "active");
        assert_eq!(ProbeStatus::Error("oops".into()).to_string(), "error: oops");
    }

    // -- probe id -----------------------------------------------------------

    #[test]
    fn test_probe_id_uniqueness() {
        let a = ProbeId::new();
        let b = ProbeId::new();
        assert_ne!(a, b);
    }

    #[test]
    fn test_probe_id_display() {
        let id = ProbeId::new();
        assert!(!id.to_string().is_empty());
        assert_eq!(id.to_string(), id.0);
    }

    // -- built-in probe constants -------------------------------------------

    #[test]
    fn test_builtin_probe_constants() {
        assert!(PROBE_COMMAND_DISPATCH.starts_with("ferrite:"));
        assert!(PROBE_STORAGE_READ.starts_with("ferrite:"));
        assert!(PROBE_STORAGE_WRITE.starts_with("ferrite:"));
        assert!(PROBE_NETWORK_RECV.starts_with("ferrite:"));
        assert!(PROBE_NETWORK_SEND.starts_with("ferrite:"));
    }

    // -- serde round-trip ---------------------------------------------------

    #[test]
    fn test_config_serde_roundtrip() {
        let cfg = EbpfConfig::default();
        let json = serde_json::to_string(&cfg).unwrap();
        let decoded: EbpfConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.probe_buffer_pages, cfg.probe_buffer_pages);
        assert_eq!(decoded.sample_rate_hz, cfg.sample_rate_hz);
    }

    #[test]
    fn test_probe_spec_serde_roundtrip() {
        let spec = sample_tracepoint_spec();
        let json = serde_json::to_string(&spec).unwrap();
        let decoded: ProbeSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.name, spec.name);
        assert_eq!(decoded.probe_type, spec.probe_type);
    }

    // -- get_histogram for missing probe ------------------------------------

    #[test]
    fn test_get_histogram_missing_probe() {
        let mgr = default_manager();
        assert!(mgr.get_histogram(&ProbeId::new()).is_none());
    }
}
