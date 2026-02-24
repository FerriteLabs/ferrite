//! WASM-specific hot reload with graceful transition and rollback
//!
//! Watches a WASM module directory for changes and reloads functions
//! without dropping in-flight calls. On load failure the previous
//! version is kept (rollback).

use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tracing::{debug, error, info, warn};

use super::types::WasmFunction;
use super::WasmError;

/// State of a hot-reload operation
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReloadState {
    /// Idle, no reload in progress
    Idle,
    /// Loading new module
    Loading,
    /// Draining in-flight calls before swap
    Draining,
    /// Swap complete
    Complete,
    /// Reload failed, rolled back
    RolledBack,
}

/// A versioned snapshot of a WASM function for rollback
#[derive(Clone)]
pub struct FunctionSnapshot {
    /// The function data
    pub function: WasmFunction,
    /// When this snapshot was taken
    pub timestamp: SystemTime,
    /// Source hash for identification
    pub source_hash: String,
}

/// Tracks in-flight calls for graceful draining
pub struct InflightTracker {
    /// Number of currently executing calls per function
    counts: RwLock<HashMap<String, Arc<AtomicU64>>>,
}

impl InflightTracker {
    /// Create a new tracker
    pub fn new() -> Self {
        Self {
            counts: RwLock::new(HashMap::new()),
        }
    }

    /// Increment in-flight count before a call
    pub fn begin_call(&self, name: &str) -> CallGuard {
        let counter = {
            let counts = self.counts.read();
            if let Some(c) = counts.get(name) {
                Arc::clone(c)
            } else {
                drop(counts);
                let mut counts = self.counts.write();
                counts
                    .entry(name.to_string())
                    .or_insert_with(|| Arc::new(AtomicU64::new(0)))
                    .clone()
            }
        };
        counter.fetch_add(1, Ordering::Relaxed);
        CallGuard { counter }
    }

    /// Get current in-flight count for a function
    pub fn inflight_count(&self, name: &str) -> u64 {
        self.counts
            .read()
            .get(name)
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Wait until in-flight count drops to zero, with timeout
    pub fn wait_drain(&self, name: &str, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        loop {
            if self.inflight_count(name) == 0 {
                return true;
            }
            if start.elapsed() >= timeout {
                return false;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Default for InflightTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII guard that decrements the in-flight counter on drop
pub struct CallGuard {
    counter: Arc<AtomicU64>,
}

impl Drop for CallGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Manages hot-reload state and rollback history for WASM functions
pub struct WasmHotReloader {
    /// Watch directory for WASM files
    watch_dir: PathBuf,
    /// Maximum number of rollback versions to keep per function
    max_rollback_versions: usize,
    /// Drain timeout for in-flight calls
    drain_timeout: Duration,
    /// In-flight call tracker
    inflight: Arc<InflightTracker>,
    /// Rollback history per function
    rollback_history: RwLock<HashMap<String, VecDeque<FunctionSnapshot>>>,
    /// Current reload state per function
    reload_states: RwLock<HashMap<String, ReloadState>>,
    /// File modification times for change detection
    file_states: RwLock<HashMap<String, FileState>>,
}

/// Tracked file state
#[derive(Clone, Debug)]
struct FileState {
    modified: SystemTime,
    size: u64,
}

/// Result of a reload attempt
#[derive(Clone, Debug)]
pub struct ReloadResult {
    /// Function name
    pub function_name: String,
    /// Whether the reload succeeded
    pub success: bool,
    /// New source hash if successful
    pub new_hash: Option<String>,
    /// Error message if failed
    pub error: Option<String>,
    /// Whether in-flight calls were drained
    pub drained: bool,
}

impl WasmHotReloader {
    /// Create a new hot reloader
    pub fn new(
        watch_dir: PathBuf,
        max_rollback_versions: usize,
        drain_timeout: Duration,
        inflight: Arc<InflightTracker>,
    ) -> Self {
        Self {
            watch_dir,
            max_rollback_versions,
            drain_timeout,
            inflight,
            rollback_history: RwLock::new(HashMap::new()),
            reload_states: RwLock::new(HashMap::new()),
            file_states: RwLock::new(HashMap::new()),
        }
    }

    /// Get the in-flight tracker (shared with the registry)
    pub fn inflight_tracker(&self) -> &Arc<InflightTracker> {
        &self.inflight
    }

    /// Save a snapshot for rollback before replacing a function
    pub fn save_snapshot(&self, name: &str, function: &WasmFunction) {
        let snapshot = FunctionSnapshot {
            function: function.clone(),
            timestamp: SystemTime::now(),
            source_hash: function.metadata.source_hash.clone(),
        };

        let mut history = self.rollback_history.write();
        let versions = history.entry(name.to_string()).or_default();

        versions.push_back(snapshot);

        // Trim old versions
        while versions.len() > self.max_rollback_versions {
            versions.pop_front();
        }

        debug!(
            function = name,
            versions = versions.len(),
            "saved rollback snapshot"
        );
    }

    /// Get the latest rollback snapshot for a function
    pub fn get_rollback(&self, name: &str) -> Option<FunctionSnapshot> {
        let history = self.rollback_history.read();
        history
            .get(name)
            .and_then(|versions| versions.back().cloned())
    }

    /// Perform a graceful reload: drain in-flight, swap, or rollback on failure
    ///
    /// `validate_fn` should attempt to load/validate the new WASM bytes and
    /// return the new `WasmFunction` or an error.
    /// `swap_fn` performs the actual replacement in the registry.
    pub fn reload_gracefully<V, S>(
        &self,
        name: &str,
        current: Option<&WasmFunction>,
        new_bytes: Vec<u8>,
        validate_fn: V,
        swap_fn: S,
    ) -> ReloadResult
    where
        V: FnOnce(&[u8]) -> Result<WasmFunction, WasmError>,
        S: FnOnce(&str, WasmFunction) -> Result<(), WasmError>,
    {
        self.set_state(name, ReloadState::Loading);

        // Save current version for rollback
        if let Some(current) = current {
            self.save_snapshot(name, current);
        }

        // Validate and compile the new module
        let new_function = match validate_fn(&new_bytes) {
            Ok(f) => f,
            Err(e) => {
                error!(function = name, error = %e, "hot reload validation failed");
                self.set_state(name, ReloadState::RolledBack);
                return ReloadResult {
                    function_name: name.to_string(),
                    success: false,
                    new_hash: None,
                    error: Some(format!("validation failed: {}", e)),
                    drained: false,
                };
            }
        };

        let new_hash = new_function.metadata.source_hash.clone();

        // Drain in-flight calls
        self.set_state(name, ReloadState::Draining);
        let drained = self.inflight.wait_drain(name, self.drain_timeout);

        if !drained {
            warn!(
                function = name,
                inflight = self.inflight.inflight_count(name),
                "drain timeout, proceeding with swap anyway"
            );
        }

        // Perform the swap
        match swap_fn(name, new_function) {
            Ok(()) => {
                self.set_state(name, ReloadState::Complete);
                info!(function = name, hash = %new_hash, "hot reload complete");
                ReloadResult {
                    function_name: name.to_string(),
                    success: true,
                    new_hash: Some(new_hash),
                    error: None,
                    drained,
                }
            }
            Err(e) => {
                error!(function = name, error = %e, "hot reload swap failed, rolling back");
                self.set_state(name, ReloadState::RolledBack);
                ReloadResult {
                    function_name: name.to_string(),
                    success: false,
                    new_hash: None,
                    error: Some(format!("swap failed: {}", e)),
                    drained,
                }
            }
        }
    }

    /// Check the watch directory for changed files
    pub fn check_for_changes(&self) -> Vec<String> {
        let mut changed = Vec::new();

        let dir = match std::fs::read_dir(&self.watch_dir) {
            Ok(d) => d,
            Err(_) => return changed,
        };

        let mut new_states = HashMap::new();

        for entry in dir.flatten() {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) != Some("wasm") {
                continue;
            }

            let name = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or_default()
                .to_string();

            if let Ok(meta) = entry.metadata() {
                let modified = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                let size = meta.len();

                let file_states = self.file_states.read();
                if let Some(old) = file_states.get(&name) {
                    if old.modified != modified || old.size != size {
                        changed.push(name.clone());
                    }
                } else {
                    changed.push(name.clone());
                }

                new_states.insert(name, FileState { modified, size });
            }
        }

        *self.file_states.write() = new_states;
        changed
    }

    /// Get the current reload state for a function
    pub fn state(&self, name: &str) -> ReloadState {
        self.reload_states
            .read()
            .get(name)
            .cloned()
            .unwrap_or(ReloadState::Idle)
    }

    /// Get rollback history count for a function
    pub fn rollback_count(&self, name: &str) -> usize {
        self.rollback_history
            .read()
            .get(name)
            .map(|v| v.len())
            .unwrap_or(0)
    }

    fn set_state(&self, name: &str, state: ReloadState) {
        self.reload_states.write().insert(name.to_string(), state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wasm::types::FunctionMetadata;

    fn make_test_function(name: &str, hash: &str) -> WasmFunction {
        let metadata = FunctionMetadata::new(name.to_string(), hash.to_string());
        let wasm_bytes = vec![0x00, 0x61, 0x73, 0x6D, 0x01, 0x00, 0x00, 0x00];
        WasmFunction::new(metadata, wasm_bytes)
    }

    #[test]
    fn test_inflight_tracker() {
        let tracker = InflightTracker::new();

        assert_eq!(tracker.inflight_count("func_a"), 0);

        let _guard1 = tracker.begin_call("func_a");
        assert_eq!(tracker.inflight_count("func_a"), 1);

        let _guard2 = tracker.begin_call("func_a");
        assert_eq!(tracker.inflight_count("func_a"), 2);

        drop(_guard1);
        assert_eq!(tracker.inflight_count("func_a"), 1);

        drop(_guard2);
        assert_eq!(tracker.inflight_count("func_a"), 0);
    }

    #[test]
    fn test_inflight_wait_drain_immediate() {
        let tracker = InflightTracker::new();
        assert!(tracker.wait_drain("func_a", Duration::from_millis(100)));
    }

    #[test]
    fn test_inflight_wait_drain_timeout() {
        let tracker = Arc::new(InflightTracker::new());
        let _guard = tracker.begin_call("func_a");

        // Should timeout since guard keeps count > 0
        assert!(!tracker.wait_drain("func_a", Duration::from_millis(50)));
    }

    #[test]
    fn test_save_and_get_rollback() {
        let inflight = Arc::new(InflightTracker::new());
        let reloader = WasmHotReloader::new(
            PathBuf::from("/tmp/wasm_test"),
            3,
            Duration::from_secs(5),
            inflight,
        );

        let func = make_test_function("test_fn", "hash_v1");
        reloader.save_snapshot("test_fn", &func);

        let snapshot = reloader.get_rollback("test_fn").unwrap();
        assert_eq!(snapshot.source_hash, "hash_v1");
        assert_eq!(reloader.rollback_count("test_fn"), 1);
    }

    #[test]
    fn test_rollback_version_limit() {
        let inflight = Arc::new(InflightTracker::new());
        let reloader = WasmHotReloader::new(
            PathBuf::from("/tmp/wasm_test"),
            2, // Keep only 2 versions
            Duration::from_secs(5),
            inflight,
        );

        for i in 0..5 {
            let func = make_test_function("fn", &format!("hash_v{}", i));
            reloader.save_snapshot("fn", &func);
        }

        assert_eq!(reloader.rollback_count("fn"), 2);
        let snapshot = reloader.get_rollback("fn").unwrap();
        assert_eq!(snapshot.source_hash, "hash_v4");
    }

    #[test]
    fn test_reload_gracefully_success() {
        let inflight = Arc::new(InflightTracker::new());
        let reloader = WasmHotReloader::new(
            PathBuf::from("/tmp/wasm_test"),
            3,
            Duration::from_secs(5),
            inflight,
        );

        let current = make_test_function("fn", "old_hash");
        let new_bytes = vec![0x00, 0x61, 0x73, 0x6D, 0x01, 0x00, 0x00, 0x00];

        let result = reloader.reload_gracefully(
            "fn",
            Some(&current),
            new_bytes,
            |_bytes| Ok(make_test_function("fn", "new_hash")),
            |_name, _func| Ok(()),
        );

        assert!(result.success);
        assert_eq!(result.new_hash, Some("new_hash".to_string()));
        assert!(result.drained);
        assert_eq!(reloader.state("fn"), ReloadState::Complete);
        assert_eq!(reloader.rollback_count("fn"), 1);
    }

    #[test]
    fn test_reload_gracefully_validation_failure() {
        let inflight = Arc::new(InflightTracker::new());
        let reloader = WasmHotReloader::new(
            PathBuf::from("/tmp/wasm_test"),
            3,
            Duration::from_secs(5),
            inflight,
        );

        let result = reloader.reload_gracefully(
            "fn",
            None,
            vec![],
            |_bytes| Err(WasmError::InvalidModule("bad module".to_string())),
            |_name, _func| Ok(()),
        );

        assert!(!result.success);
        assert!(result.error.unwrap().contains("validation failed"));
        assert_eq!(reloader.state("fn"), ReloadState::RolledBack);
    }

    #[test]
    fn test_reload_gracefully_swap_failure() {
        let inflight = Arc::new(InflightTracker::new());
        let reloader = WasmHotReloader::new(
            PathBuf::from("/tmp/wasm_test"),
            3,
            Duration::from_secs(5),
            inflight,
        );

        let result = reloader.reload_gracefully(
            "fn",
            None,
            vec![],
            |_bytes| Ok(make_test_function("fn", "new_hash")),
            |_name, _func| Err(WasmError::Internal("swap failed".to_string())),
        );

        assert!(!result.success);
        assert!(result.error.unwrap().contains("swap failed"));
        assert_eq!(reloader.state("fn"), ReloadState::RolledBack);
    }

    #[test]
    fn test_check_for_changes() {
        let tmp = tempfile::TempDir::new().unwrap();
        let inflight = Arc::new(InflightTracker::new());
        let reloader = WasmHotReloader::new(
            tmp.path().to_path_buf(),
            3,
            Duration::from_secs(5),
            inflight,
        );

        // First check: empty
        let changed = reloader.check_for_changes();
        assert!(changed.is_empty());

        // Add a WASM file
        std::fs::write(tmp.path().join("myfunc.wasm"), b"fake wasm data").unwrap();

        // Second check: should detect new file
        let changed = reloader.check_for_changes();
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0], "myfunc");

        // Third check without changes: should be empty
        let changed = reloader.check_for_changes();
        assert!(changed.is_empty());
    }

    #[test]
    fn test_reload_state_tracking() {
        let inflight = Arc::new(InflightTracker::new());
        let reloader =
            WasmHotReloader::new(PathBuf::from("/tmp"), 3, Duration::from_secs(5), inflight);

        assert_eq!(reloader.state("fn"), ReloadState::Idle);
    }
}
