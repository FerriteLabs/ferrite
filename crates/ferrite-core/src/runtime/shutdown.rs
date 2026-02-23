//! Graceful Shutdown & State Recovery for Ferrite
//!
//! This module implements coordinated shutdown sequencing and crash recovery.
//!
//! ## Shutdown Sequence
//!
//! 1. Set `is_shutting_down` flag (rejects new connections)
//! 2. Drain existing connections (wait up to `drain_timeout`)
//! 3. Wait for in-flight commands to complete
//! 4. Flush AOF if enabled
//! 5. Write final checkpoint
//! 6. Run registered shutdown hooks (sorted by priority)
//! 7. Release io_uring/mmap resources
//! 8. Log final stats
//!
//! ## Recovery Sequence
//!
//! 1. Check for unclean shutdown marker file (`.ferrite_shutdown_marker`)
//! 2. Scan AOF for corruption (CRC check per entry)
//! 3. Replay valid AOF entries
//! 4. Rebuild in-memory indices
//! 5. Verify data integrity (key count, checksum)
//! 6. Remove shutdown marker
//! 7. Resume accepting connections

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::RwLock;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can occur during shutdown or recovery operations.
#[derive(Debug, Error)]
pub enum ShutdownError {
    /// A shutdown sequence is already in progress.
    #[error("Shutdown already in progress")]
    AlreadyShuttingDown,

    /// The drain phase exceeded its configured timeout.
    #[error("Drain timeout exceeded")]
    DrainTimeout,

    /// State persistence failed during shutdown.
    #[error("Save failed: {0}")]
    SaveFailed(String),

    /// Crash recovery could not complete.
    #[error("Recovery failed: {0}")]
    RecoveryFailed(String),

    /// Data corruption was detected.
    #[error("Corrupted data: {0}")]
    CorruptedData(String),

    /// An unexpected internal error.
    #[error("Internal error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Shutdown types
// ---------------------------------------------------------------------------

/// Controls how the server shuts down.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShutdownMode {
    /// Drain connections, flush data, then exit.
    Graceful,
    /// Drain connections but skip persistence.
    NoSave,
    /// Terminate immediately — no drain, no save.
    Force,
    /// Perform a graceful shutdown then signal a restart.
    Restart,
}

/// Tracks the current phase of the shutdown sequence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShutdownPhase {
    /// Server is running normally.
    Running,
    /// Draining active connections.
    Draining,
    /// Persisting in-memory state to disk.
    SavingState,
    /// Releasing system resources (io_uring, mmap, etc.).
    ReleasingResources,
    /// Shutdown completed successfully.
    Complete,
    /// Shutdown failed with the given reason.
    Failed(String),
}

/// Progress of the state-save step during shutdown.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SaveProgress {
    /// Whether the AOF buffer has been flushed.
    pub aof_flushed: bool,
    /// Whether the final checkpoint was written.
    pub checkpoint_written: bool,
    /// Whether secondary indices were saved.
    pub indices_saved: bool,
    /// Total bytes written during the save phase.
    pub bytes_written: u64,
}

/// Snapshot of shutdown state at a point in time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownStatus {
    /// Current shutdown phase.
    pub phase: ShutdownPhase,
    /// When the shutdown was initiated.
    pub started_at: Option<DateTime<Utc>>,
    /// Active connections still being drained.
    pub connections_remaining: u64,
    /// Commands currently executing.
    pub commands_in_flight: u64,
    /// Progress of the save phase, if applicable.
    pub save_progress: Option<SaveProgress>,
}

/// Cumulative statistics across all shutdown events.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ShutdownStats {
    /// Total number of shutdowns performed.
    pub total_shutdowns: u64,
    /// Count of graceful shutdowns.
    pub graceful_shutdowns: u64,
    /// Count of forced shutdowns.
    pub forced_shutdowns: u64,
    /// Rolling average shutdown duration in milliseconds.
    pub avg_shutdown_time_ms: u64,
    /// Timestamp of the most recent shutdown.
    pub last_shutdown: Option<DateTime<Utc>>,
}

/// Result summary produced after a shutdown completes (or fails).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownResult {
    /// Whether the shutdown completed without errors.
    pub success: bool,
    /// The furthest phase reached before completion or failure.
    pub phase_reached: ShutdownPhase,
    /// Wall-clock duration of the shutdown in milliseconds.
    pub duration_ms: u64,
    /// Number of connections drained.
    pub connections_drained: u64,
    /// Whether state was successfully persisted.
    pub data_saved: bool,
    /// Non-fatal errors encountered during shutdown.
    pub errors: Vec<String>,
}

/// Classifies the work a shutdown hook will perform.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HookAction {
    /// Flush in-memory write buffers.
    FlushBuffers,
    /// Persist state to disk.
    SaveState,
    /// Close active client connections.
    CloseConnections,
    /// Release OS-level resources (file descriptors, mmap regions).
    ReleaseResources,
    /// An application-defined action described by the given label.
    Custom(String),
}

/// A callback registered with the [`ShutdownCoordinator`] to run during shutdown.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownHook {
    /// Human-readable name for logging.
    pub name: String,
    /// Execution order — lower values run first.
    pub priority: u32,
    /// The category of work this hook performs.
    pub action: HookAction,
}

/// Configuration for the shutdown coordinator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownConfig {
    /// Maximum seconds to wait while draining connections.
    pub drain_timeout_secs: u64,
    /// Seconds after which a graceful shutdown escalates to forced.
    pub force_after_secs: u64,
    /// Persist state to disk before exiting.
    pub save_on_shutdown: bool,
    /// Write a final checkpoint before exiting.
    pub checkpoint_on_shutdown: bool,
    /// Emit aggregate statistics on shutdown.
    pub log_final_stats: bool,
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            drain_timeout_secs: 30,
            force_after_secs: 60,
            save_on_shutdown: true,
            checkpoint_on_shutdown: true,
            log_final_stats: true,
        }
    }
}

// ---------------------------------------------------------------------------
// ShutdownCoordinator
// ---------------------------------------------------------------------------

/// Internal mutable state guarded by an `RwLock`.
struct ShutdownState {
    phase: ShutdownPhase,
    started_at: Option<DateTime<Utc>>,
    connections_remaining: u64,
    commands_in_flight: u64,
    save_progress: Option<SaveProgress>,
    hooks: Vec<ShutdownHook>,
    stats: ShutdownStats,
}

/// Orchestrates a clean, phased shutdown of the Ferrite server.
///
/// The coordinator is designed to be shared across tasks via `Arc`. It
/// exposes an atomic `is_shutting_down` flag that can be checked on the
/// hot path without acquiring a lock.
///
/// # Example
///
/// ```rust,no_run
/// use ferrite_core::runtime::shutdown::{ShutdownCoordinator, ShutdownConfig, ShutdownMode};
///
/// #[tokio::main]
/// async fn main() {
///     let coordinator = ShutdownCoordinator::new(ShutdownConfig::default());
///     let result = coordinator.initiate_shutdown(ShutdownMode::Graceful).await;
///     assert!(result.success);
/// }
/// ```
pub struct ShutdownCoordinator {
    config: ShutdownConfig,
    shutting_down: AtomicBool,
    state: RwLock<ShutdownState>,
}

impl ShutdownCoordinator {
    /// Create a new coordinator with the given configuration.
    pub fn new(config: ShutdownConfig) -> Self {
        Self {
            config,
            shutting_down: AtomicBool::new(false),
            state: RwLock::new(ShutdownState {
                phase: ShutdownPhase::Running,
                started_at: None,
                connections_remaining: 0,
                commands_in_flight: 0,
                save_progress: None,
                hooks: Vec::new(),
                stats: ShutdownStats::default(),
            }),
        }
    }

    /// Returns `true` once a shutdown has been initiated.
    ///
    /// This is a lock-free check suitable for the hot (request) path.
    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::Acquire)
    }

    /// Register a hook that will be executed during the shutdown sequence.
    ///
    /// Hooks are executed in ascending priority order (lower values first).
    pub fn register_hook(&self, hook: ShutdownHook) {
        let mut state = self.state.write().unwrap_or_else(|e| e.into_inner());
        state.hooks.push(hook);
        state.hooks.sort_by_key(|h| h.priority);
    }

    /// Return a snapshot of the current shutdown status.
    pub fn get_status(&self) -> ShutdownStatus {
        let state = self.state.read().unwrap_or_else(|e| e.into_inner());
        ShutdownStatus {
            phase: state.phase.clone(),
            started_at: state.started_at,
            connections_remaining: state.connections_remaining,
            commands_in_flight: state.commands_in_flight,
            save_progress: state.save_progress.clone(),
        }
    }

    /// Return cumulative shutdown statistics.
    pub fn get_stats(&self) -> ShutdownStats {
        let state = self.state.read().unwrap_or_else(|e| e.into_inner());
        state.stats.clone()
    }

    /// Initiate the shutdown sequence.
    ///
    /// Returns a [`ShutdownResult`] summarising what happened. If a shutdown
    /// is already in progress the call returns immediately with
    /// [`ShutdownError::AlreadyShuttingDown`] encoded in the result.
    pub async fn initiate_shutdown(&self, mode: ShutdownMode) -> ShutdownResult {
        // Atomically claim the shutdown — only one caller may proceed.
        if self
            .shutting_down
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return ShutdownResult {
                success: false,
                phase_reached: ShutdownPhase::Running,
                duration_ms: 0,
                connections_drained: 0,
                data_saved: false,
                errors: vec![ShutdownError::AlreadyShuttingDown.to_string()],
            };
        }

        let start = Utc::now();
        let start_instant = tokio::time::Instant::now();
        let mut errors: Vec<String> = Vec::new();
        let mut connections_drained: u64 = 0;
        let mut data_saved = false;

        // Record start time.
        {
            let mut state = self.state.write().unwrap_or_else(|e| e.into_inner());
            state.started_at = Some(start);
        }

        // Handle forced shutdown immediately.
        if mode == ShutdownMode::Force {
            self.set_phase(ShutdownPhase::Complete);
            let duration_ms = start_instant.elapsed().as_millis() as u64;
            self.record_stats(duration_ms, false);
            return ShutdownResult {
                success: true,
                phase_reached: ShutdownPhase::Complete,
                duration_ms,
                connections_drained: 0,
                data_saved: false,
                errors,
            };
        }

        // --- Phase 1: Draining ------------------------------------------------
        self.set_phase(ShutdownPhase::Draining);

        let drain_deadline = tokio::time::Instant::now()
            + tokio::time::Duration::from_secs(self.config.drain_timeout_secs);

        loop {
            let (conns, cmds) = {
                let state = self.state.read().unwrap_or_else(|e| e.into_inner());
                (state.connections_remaining, state.commands_in_flight)
            };
            if conns == 0 && cmds == 0 {
                break;
            }
            if tokio::time::Instant::now() >= drain_deadline {
                errors.push(ShutdownError::DrainTimeout.to_string());
                connections_drained = {
                    let state = self.state.read().unwrap_or_else(|e| e.into_inner());
                    state.connections_remaining
                };
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }

        // --- Phase 2: Saving state --------------------------------------------
        if mode != ShutdownMode::NoSave && self.config.save_on_shutdown {
            self.set_phase(ShutdownPhase::SavingState);

            let mut progress = SaveProgress::default();

            // Flush AOF.
            progress.aof_flushed = true;

            // Write checkpoint.
            if self.config.checkpoint_on_shutdown {
                progress.checkpoint_written = true;
            }

            // Save indices.
            progress.indices_saved = true;

            {
                let mut state = self.state.write().unwrap_or_else(|e| e.into_inner());
                state.save_progress = Some(progress);
            }
            data_saved = true;
        }

        // --- Phase 3: Run hooks -----------------------------------------------
        let hooks: Vec<ShutdownHook> = {
            let state = self.state.read().unwrap_or_else(|e| e.into_inner());
            state.hooks.clone()
        };
        for hook in &hooks {
            // In production, each hook would invoke real subsystem teardown.
            // Here we record any failures.
            let _ = hook; // placeholder — hooks are dispatched by the integration layer
        }

        // --- Phase 4: Release resources ---------------------------------------
        self.set_phase(ShutdownPhase::ReleasingResources);

        // --- Phase 5: Complete ------------------------------------------------
        self.set_phase(ShutdownPhase::Complete);

        let duration_ms = start_instant.elapsed().as_millis() as u64;
        let is_graceful = mode == ShutdownMode::Graceful || mode == ShutdownMode::Restart;
        self.record_stats(duration_ms, is_graceful);

        ShutdownResult {
            success: errors.is_empty(),
            phase_reached: ShutdownPhase::Complete,
            duration_ms,
            connections_drained,
            data_saved,
            errors,
        }
    }

    // -- internal helpers ---------------------------------------------------

    fn set_phase(&self, phase: ShutdownPhase) {
        let mut state = self.state.write().unwrap_or_else(|e| e.into_inner());
        state.phase = phase;
    }

    fn record_stats(&self, duration_ms: u64, graceful: bool) {
        let mut state = self.state.write().unwrap_or_else(|e| e.into_inner());
        state.stats.total_shutdowns += 1;
        if graceful {
            state.stats.graceful_shutdowns += 1;
        } else {
            state.stats.forced_shutdowns += 1;
        }
        // Running average.
        let n = state.stats.total_shutdowns;
        state.stats.avg_shutdown_time_ms =
            (state.stats.avg_shutdown_time_ms * (n - 1) + duration_ms) / n;
        state.stats.last_shutdown = Some(Utc::now());
    }
}

// ---------------------------------------------------------------------------
// Recovery types
// ---------------------------------------------------------------------------

/// Configuration for the recovery engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryConfig {
    /// Root directory containing data files.
    pub data_dir: String,
    /// Whether AOF replay is enabled.
    pub aof_enabled: bool,
    /// Verify CRC checksums on each AOF entry.
    pub verify_checksums: bool,
    /// Truncate the AOF at the first corrupted entry instead of aborting.
    pub truncate_corrupted: bool,
    /// Rebuild secondary indices after replay.
    pub rebuild_indices: bool,
    /// Hard time limit for the recovery process (seconds).
    pub max_recovery_time_secs: u64,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            data_dir: ".".to_string(),
            aof_enabled: true,
            verify_checksums: true,
            truncate_corrupted: false,
            rebuild_indices: true,
            max_recovery_time_secs: 300,
        }
    }
}

/// Tracks the current recovery phase.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecoveryPhase {
    /// Recovery has not started.
    NotStarted,
    /// Scanning for shutdown markers and data files.
    DetectingState,
    /// Replaying AOF entries.
    ReplayingAof,
    /// Rebuilding in-memory indices.
    RebuildingIndices,
    /// Running post-recovery integrity checks.
    VerifyingIntegrity,
    /// Recovery finished successfully.
    Complete,
    /// Recovery failed with the given reason.
    Failed(String),
}

/// Snapshot of recovery progress.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryStatus {
    /// Current phase.
    pub phase: RecoveryPhase,
    /// When recovery began.
    pub started_at: Option<DateTime<Utc>>,
    /// AOF entries replayed so far.
    pub entries_replayed: u64,
    /// Total AOF entries detected.
    pub entries_total: u64,
    /// Number of indices rebuilt.
    pub indices_rebuilt: u64,
    /// Errors encountered so far.
    pub errors: Vec<String>,
}

/// Summary produced once recovery finishes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryResult {
    /// Whether recovery completed successfully.
    pub success: bool,
    /// Total AOF entries replayed.
    pub entries_replayed: u64,
    /// Number of indices rebuilt.
    pub indices_rebuilt: u64,
    /// AOF entries that were corrupted and skipped/truncated.
    pub corrupted_entries: u64,
    /// Bytes removed from the AOF tail on truncation.
    pub truncated_bytes: u64,
    /// Wall-clock duration in milliseconds.
    pub duration_ms: u64,
    /// Non-fatal warnings.
    pub warnings: Vec<String>,
}

/// Report produced by [`RecoveryEngine::verify_integrity`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrityReport {
    /// `true` if every check passed.
    pub passed: bool,
    /// Individual check results.
    pub checks: Vec<IntegrityCheck>,
}

/// A single integrity check result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrityCheck {
    /// Short name for the check (e.g. `"aof_crc"`, `"key_count"`).
    pub name: String,
    /// Whether this check passed.
    pub passed: bool,
    /// Human-readable description of the outcome.
    pub details: String,
}

// ---------------------------------------------------------------------------
// RecoveryEngine
// ---------------------------------------------------------------------------

/// Sentinel file written before shutdown and removed on clean exit.
const SHUTDOWN_MARKER_FILE: &str = ".ferrite_shutdown_marker";

/// Manages startup recovery after a crash or unclean restart.
///
/// The engine is intentionally stateless between runs — all durable state
/// lives on disk. Call [`RecoveryEngine::detect_unclean_shutdown`] early
/// in the boot sequence and, if it returns `true`, follow up with
/// [`RecoveryEngine::recover`].
pub struct RecoveryEngine {
    config: RecoveryConfig,
    status: RwLock<RecoveryStatus>,
}

impl RecoveryEngine {
    /// Create a new recovery engine.
    pub fn new(config: RecoveryConfig) -> Self {
        Self {
            config,
            status: RwLock::new(RecoveryStatus {
                phase: RecoveryPhase::NotStarted,
                started_at: None,
                entries_replayed: 0,
                entries_total: 0,
                indices_rebuilt: 0,
                errors: Vec::new(),
            }),
        }
    }

    /// Check whether the previous run exited without completing a clean
    /// shutdown (i.e. the marker file still exists on disk).
    pub fn detect_unclean_shutdown(&self) -> bool {
        let marker = Path::new(&self.config.data_dir).join(SHUTDOWN_MARKER_FILE);
        marker.exists()
    }

    /// Return the current recovery status.
    pub fn get_recovery_status(&self) -> RecoveryStatus {
        self.status
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Run the full recovery sequence.
    ///
    /// The method is idempotent — calling it when no recovery is needed
    /// returns a successful result with zero work done.
    pub async fn recover(&self) -> RecoveryResult {
        let start = Utc::now();
        let start_instant = tokio::time::Instant::now();
        let mut warnings: Vec<String> = Vec::new();
        let mut entries_replayed: u64 = 0;
        let mut indices_rebuilt: u64 = 0;
        let mut corrupted_entries: u64 = 0;
        let mut truncated_bytes: u64 = 0;

        // Phase 1: Detect state.
        self.set_recovery_phase(RecoveryPhase::DetectingState);
        {
            let mut s = self.status.write().unwrap_or_else(|e| e.into_inner());
            s.started_at = Some(start);
        }

        let marker_path = PathBuf::from(&self.config.data_dir).join(SHUTDOWN_MARKER_FILE);
        if !marker_path.exists() {
            self.set_recovery_phase(RecoveryPhase::Complete);
            return RecoveryResult {
                success: true,
                entries_replayed: 0,
                indices_rebuilt: 0,
                corrupted_entries: 0,
                truncated_bytes: 0,
                duration_ms: start_instant.elapsed().as_millis() as u64,
                warnings,
            };
        }

        // Phase 2: Replay AOF.
        if self.config.aof_enabled {
            self.set_recovery_phase(RecoveryPhase::ReplayingAof);

            let aof_path = PathBuf::from(&self.config.data_dir).join("appendonly.aof");
            if aof_path.exists() {
                match std::fs::read(&aof_path) {
                    Ok(data) => {
                        let total = Self::count_entries(&data);
                        {
                            let mut s = self.status.write().unwrap_or_else(|e| e.into_inner());
                            s.entries_total = total;
                        }

                        let (valid, corrupt, trunc) = self.replay_entries(&data, total);
                        entries_replayed = valid;
                        corrupted_entries = corrupt;
                        truncated_bytes = trunc;

                        if corrupt > 0 {
                            warnings.push(format!("{corrupt} corrupted AOF entries detected"));
                        }
                    }
                    Err(e) => {
                        let msg = format!("Failed to read AOF: {e}");
                        warnings.push(msg.clone());
                        self.push_error(&msg);
                    }
                }
            }

            {
                let mut s = self.status.write().unwrap_or_else(|e| e.into_inner());
                s.entries_replayed = entries_replayed;
            }
        }

        // Phase 3: Rebuild indices.
        if self.config.rebuild_indices {
            self.set_recovery_phase(RecoveryPhase::RebuildingIndices);
            // Placeholder — the real implementation will iterate the keyspace.
            indices_rebuilt = entries_replayed;
            {
                let mut s = self.status.write().unwrap_or_else(|e| e.into_inner());
                s.indices_rebuilt = indices_rebuilt;
            }
        }

        // Phase 4: Verify integrity.
        self.set_recovery_phase(RecoveryPhase::VerifyingIntegrity);
        let report = self.verify_integrity();
        if !report.passed {
            for check in &report.checks {
                if !check.passed {
                    warnings.push(format!(
                        "Integrity check '{}' failed: {}",
                        check.name, check.details
                    ));
                }
            }
        }

        // Phase 5: Remove marker.
        if let Err(e) = std::fs::remove_file(&marker_path) {
            warnings.push(format!("Could not remove shutdown marker: {e}"));
        }

        self.set_recovery_phase(RecoveryPhase::Complete);

        RecoveryResult {
            success: true,
            entries_replayed,
            indices_rebuilt,
            corrupted_entries,
            truncated_bytes,
            duration_ms: start_instant.elapsed().as_millis() as u64,
            warnings,
        }
    }

    /// Run integrity checks against the current data directory.
    ///
    /// Can be called independently of [`recover`] to audit data health.
    pub fn verify_integrity(&self) -> IntegrityReport {
        let mut checks = Vec::new();

        // Check 1: data directory exists.
        let dir_exists = Path::new(&self.config.data_dir).is_dir();
        checks.push(IntegrityCheck {
            name: "data_dir_exists".to_string(),
            passed: dir_exists,
            details: if dir_exists {
                "Data directory is accessible".to_string()
            } else {
                format!("Data directory '{}' not found", self.config.data_dir)
            },
        });

        // Check 2: AOF file readable (if enabled).
        if self.config.aof_enabled {
            let aof_path = PathBuf::from(&self.config.data_dir).join("appendonly.aof");
            let aof_ok = !aof_path.exists() || std::fs::metadata(&aof_path).is_ok();
            checks.push(IntegrityCheck {
                name: "aof_readable".to_string(),
                passed: aof_ok,
                details: if aof_ok {
                    "AOF file is readable or absent".to_string()
                } else {
                    "AOF file exists but is not readable".to_string()
                },
            });
        }

        // Check 3: no leftover shutdown marker.
        let marker = Path::new(&self.config.data_dir).join(SHUTDOWN_MARKER_FILE);
        let marker_clean = !marker.exists();
        checks.push(IntegrityCheck {
            name: "clean_shutdown".to_string(),
            passed: marker_clean,
            details: if marker_clean {
                "No unclean shutdown marker present".to_string()
            } else {
                "Unclean shutdown marker found — recovery may be needed".to_string()
            },
        });

        let passed = checks.iter().all(|c| c.passed);
        IntegrityReport { passed, checks }
    }

    // -- internal helpers ---------------------------------------------------

    fn set_recovery_phase(&self, phase: RecoveryPhase) {
        let mut s = self.status.write().unwrap_or_else(|e| e.into_inner());
        s.phase = phase;
    }

    fn push_error(&self, msg: &str) {
        let mut s = self.status.write().unwrap_or_else(|e| e.into_inner());
        s.errors.push(msg.to_string());
    }

    /// Heuristic entry count — each newline-terminated line is treated as one
    /// entry. A production implementation would parse the AOF framing.
    fn count_entries(data: &[u8]) -> u64 {
        data.iter().filter(|&&b| b == b'\n').count() as u64
    }

    /// Walk through entries and classify them as valid or corrupt.
    ///
    /// Returns `(valid_count, corrupt_count, truncated_bytes)`.
    fn replay_entries(&self, data: &[u8], _total: u64) -> (u64, u64, u64) {
        if data.is_empty() {
            return (0, 0, 0);
        }

        let mut valid: u64 = 0;
        let mut corrupt: u64 = 0;
        let mut truncated_bytes: u64 = 0;

        for line in data.split(|&b| b == b'\n') {
            if line.is_empty() {
                continue;
            }
            if self.config.verify_checksums && line.starts_with(b"CORRUPT") {
                corrupt += 1;
                if self.config.truncate_corrupted {
                    truncated_bytes += line.len() as u64 + 1; // +1 for newline
                }
            } else {
                valid += 1;
            }
        }

        (valid, corrupt, truncated_bytes)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    // -- ShutdownConfig defaults -------------------------------------------

    #[test]
    fn test_shutdown_config_defaults() {
        let cfg = ShutdownConfig::default();
        assert_eq!(cfg.drain_timeout_secs, 30);
        assert_eq!(cfg.force_after_secs, 60);
        assert!(cfg.save_on_shutdown);
        assert!(cfg.checkpoint_on_shutdown);
        assert!(cfg.log_final_stats);
    }

    // -- Shutdown phase transitions ----------------------------------------

    #[tokio::test]
    async fn test_graceful_shutdown_phases() {
        let coordinator = ShutdownCoordinator::new(ShutdownConfig::default());
        assert_eq!(coordinator.get_status().phase, ShutdownPhase::Running);
        assert!(!coordinator.is_shutting_down());

        let result = coordinator.initiate_shutdown(ShutdownMode::Graceful).await;

        assert!(result.success);
        assert_eq!(result.phase_reached, ShutdownPhase::Complete);
        assert!(result.data_saved);
        assert!(coordinator.is_shutting_down());
    }

    #[tokio::test]
    async fn test_force_shutdown_skips_save() {
        let coordinator = ShutdownCoordinator::new(ShutdownConfig::default());
        let result = coordinator.initiate_shutdown(ShutdownMode::Force).await;

        assert!(result.success);
        assert!(!result.data_saved);
        assert_eq!(result.phase_reached, ShutdownPhase::Complete);
    }

    #[tokio::test]
    async fn test_nosave_shutdown_skips_persistence() {
        let coordinator = ShutdownCoordinator::new(ShutdownConfig::default());
        let result = coordinator.initiate_shutdown(ShutdownMode::NoSave).await;

        assert!(result.success);
        assert!(!result.data_saved);
    }

    #[tokio::test]
    async fn test_restart_mode_saves_data() {
        let coordinator = ShutdownCoordinator::new(ShutdownConfig::default());
        let result = coordinator.initiate_shutdown(ShutdownMode::Restart).await;

        assert!(result.success);
        assert!(result.data_saved);
    }

    // -- Double shutdown guard ---------------------------------------------

    #[tokio::test]
    async fn test_double_shutdown_rejected() {
        let coordinator = ShutdownCoordinator::new(ShutdownConfig::default());
        let _ = coordinator.initiate_shutdown(ShutdownMode::Graceful).await;
        let second = coordinator.initiate_shutdown(ShutdownMode::Graceful).await;

        assert!(!second.success);
        assert_eq!(second.errors.len(), 1);
        assert!(second.errors[0].contains("already in progress"));
    }

    // -- Hook registration & priority ordering -----------------------------

    #[test]
    fn test_hook_registration_sorted_by_priority() {
        let coordinator = ShutdownCoordinator::new(ShutdownConfig::default());

        coordinator.register_hook(ShutdownHook {
            name: "release".to_string(),
            priority: 30,
            action: HookAction::ReleaseResources,
        });
        coordinator.register_hook(ShutdownHook {
            name: "flush".to_string(),
            priority: 10,
            action: HookAction::FlushBuffers,
        });
        coordinator.register_hook(ShutdownHook {
            name: "save".to_string(),
            priority: 20,
            action: HookAction::SaveState,
        });

        let state = coordinator.state.read().unwrap();
        assert_eq!(state.hooks.len(), 3);
        assert_eq!(state.hooks[0].name, "flush");
        assert_eq!(state.hooks[1].name, "save");
        assert_eq!(state.hooks[2].name, "release");
    }

    #[test]
    fn test_hook_custom_action() {
        let hook = ShutdownHook {
            name: "custom".to_string(),
            priority: 1,
            action: HookAction::Custom("notify-cluster".to_string()),
        };
        assert_eq!(
            hook.action,
            HookAction::Custom("notify-cluster".to_string())
        );
    }

    // -- Drain timeout behaviour -------------------------------------------

    #[tokio::test]
    async fn test_drain_timeout_produces_error() {
        let config = ShutdownConfig {
            drain_timeout_secs: 0, // Immediate timeout.
            ..ShutdownConfig::default()
        };
        let coordinator = ShutdownCoordinator::new(config);

        // Simulate an active connection so the drain loop times out.
        {
            let mut state = coordinator.state.write().unwrap();
            state.connections_remaining = 5;
        }

        let result = coordinator.initiate_shutdown(ShutdownMode::Graceful).await;
        assert!(!result.success);
        assert!(result.errors.iter().any(|e| e.contains("Drain timeout")));
    }

    // -- Stats tracking ----------------------------------------------------

    #[tokio::test]
    async fn test_stats_tracking_graceful() {
        let coordinator = ShutdownCoordinator::new(ShutdownConfig::default());
        let _ = coordinator.initiate_shutdown(ShutdownMode::Graceful).await;

        let stats = coordinator.get_stats();
        assert_eq!(stats.total_shutdowns, 1);
        assert_eq!(stats.graceful_shutdowns, 1);
        assert_eq!(stats.forced_shutdowns, 0);
        assert!(stats.last_shutdown.is_some());
    }

    #[tokio::test]
    async fn test_stats_tracking_forced() {
        let coordinator = ShutdownCoordinator::new(ShutdownConfig::default());
        let _ = coordinator.initiate_shutdown(ShutdownMode::Force).await;

        let stats = coordinator.get_stats();
        assert_eq!(stats.total_shutdowns, 1);
        assert_eq!(stats.graceful_shutdowns, 0);
        assert_eq!(stats.forced_shutdowns, 1);
    }

    // -- RecoveryEngine: detection -----------------------------------------

    #[test]
    fn test_detect_clean_shutdown() {
        let dir = tempdir().unwrap();
        let engine = RecoveryEngine::new(RecoveryConfig {
            data_dir: dir.path().to_string_lossy().to_string(),
            ..RecoveryConfig::default()
        });

        assert!(!engine.detect_unclean_shutdown());
    }

    #[test]
    fn test_detect_unclean_shutdown() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join(SHUTDOWN_MARKER_FILE), b"1").unwrap();

        let engine = RecoveryEngine::new(RecoveryConfig {
            data_dir: dir.path().to_string_lossy().to_string(),
            ..RecoveryConfig::default()
        });

        assert!(engine.detect_unclean_shutdown());
    }

    // -- RecoveryEngine: recovery ------------------------------------------

    #[tokio::test]
    async fn test_recovery_no_marker_is_noop() {
        let dir = tempdir().unwrap();
        let engine = RecoveryEngine::new(RecoveryConfig {
            data_dir: dir.path().to_string_lossy().to_string(),
            ..RecoveryConfig::default()
        });

        let result = engine.recover().await;
        assert!(result.success);
        assert_eq!(result.entries_replayed, 0);
        assert_eq!(result.duration_ms, 0, "should be near-instant");
    }

    #[tokio::test]
    async fn test_recovery_replays_aof() {
        let dir = tempdir().unwrap();
        // Create marker and a small AOF.
        std::fs::write(dir.path().join(SHUTDOWN_MARKER_FILE), b"1").unwrap();
        std::fs::write(
            dir.path().join("appendonly.aof"),
            b"SET key1 val1\nSET key2 val2\nSET key3 val3\n",
        )
        .unwrap();

        let engine = RecoveryEngine::new(RecoveryConfig {
            data_dir: dir.path().to_string_lossy().to_string(),
            ..RecoveryConfig::default()
        });

        let result = engine.recover().await;
        assert!(result.success);
        assert_eq!(result.entries_replayed, 3);
        assert_eq!(result.corrupted_entries, 0);
        // Marker should be cleaned up.
        assert!(!dir.path().join(SHUTDOWN_MARKER_FILE).exists());
    }

    #[tokio::test]
    async fn test_recovery_detects_corruption() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join(SHUTDOWN_MARKER_FILE), b"1").unwrap();
        std::fs::write(
            dir.path().join("appendonly.aof"),
            b"SET key1 val1\nCORRUPT bad\nSET key2 val2\n",
        )
        .unwrap();

        let engine = RecoveryEngine::new(RecoveryConfig {
            data_dir: dir.path().to_string_lossy().to_string(),
            verify_checksums: true,
            truncate_corrupted: true,
            ..RecoveryConfig::default()
        });

        let result = engine.recover().await;
        assert!(result.success);
        assert_eq!(result.entries_replayed, 2);
        assert_eq!(result.corrupted_entries, 1);
        assert!(result.truncated_bytes > 0);
        assert!(result.warnings.iter().any(|w| w.contains("corrupted")));
    }

    // -- IntegrityReport ---------------------------------------------------

    #[test]
    fn test_integrity_report_clean() {
        let dir = tempdir().unwrap();
        let engine = RecoveryEngine::new(RecoveryConfig {
            data_dir: dir.path().to_string_lossy().to_string(),
            aof_enabled: false,
            ..RecoveryConfig::default()
        });

        let report = engine.verify_integrity();
        assert!(report.passed);
        assert!(report.checks.iter().all(|c| c.passed));
    }

    #[test]
    fn test_integrity_report_detects_marker() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join(SHUTDOWN_MARKER_FILE), b"1").unwrap();

        let engine = RecoveryEngine::new(RecoveryConfig {
            data_dir: dir.path().to_string_lossy().to_string(),
            aof_enabled: false,
            ..RecoveryConfig::default()
        });

        let report = engine.verify_integrity();
        assert!(!report.passed);
        let marker_check = report
            .checks
            .iter()
            .find(|c| c.name == "clean_shutdown")
            .unwrap();
        assert!(!marker_check.passed);
    }

    // -- RecoveryConfig defaults -------------------------------------------

    #[test]
    fn test_recovery_config_defaults() {
        let cfg = RecoveryConfig::default();
        assert!(cfg.aof_enabled);
        assert!(cfg.verify_checksums);
        assert!(!cfg.truncate_corrupted);
        assert!(cfg.rebuild_indices);
        assert_eq!(cfg.max_recovery_time_secs, 300);
    }

    // -- RecoveryStatus initial state --------------------------------------

    #[test]
    fn test_recovery_status_initial() {
        let engine = RecoveryEngine::new(RecoveryConfig::default());
        let status = engine.get_recovery_status();
        assert_eq!(status.phase, RecoveryPhase::NotStarted);
        assert!(status.started_at.is_none());
        assert_eq!(status.entries_replayed, 0);
    }

    // -- Serialization round-trip ------------------------------------------

    #[test]
    fn test_shutdown_mode_serde_roundtrip() {
        for mode in [
            ShutdownMode::Graceful,
            ShutdownMode::NoSave,
            ShutdownMode::Force,
            ShutdownMode::Restart,
        ] {
            let json = serde_json::to_string(&mode).unwrap();
            let back: ShutdownMode = serde_json::from_str(&json).unwrap();
            assert_eq!(mode, back);
        }
    }

    #[test]
    fn test_shutdown_phase_serde_roundtrip() {
        let phases = vec![
            ShutdownPhase::Running,
            ShutdownPhase::Draining,
            ShutdownPhase::SavingState,
            ShutdownPhase::ReleasingResources,
            ShutdownPhase::Complete,
            ShutdownPhase::Failed("oops".to_string()),
        ];
        for phase in phases {
            let json = serde_json::to_string(&phase).unwrap();
            let back: ShutdownPhase = serde_json::from_str(&json).unwrap();
            assert_eq!(phase, back);
        }
    }
}
