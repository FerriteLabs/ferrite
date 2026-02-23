//! Log Compaction for HybridLog
//!
//! Implements background log compaction that removes stale/overwritten entries
//! from persistent storage regions. Compaction runs periodically without
//! blocking reads and tracks progress metrics.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use tracing::{debug, info};

/// Configuration for compaction behavior
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Trigger compaction when dead space exceeds this fraction (0.0–1.0)
    pub dead_space_threshold: f64,
    /// Minimum bytes of dead space before compaction is worthwhile
    pub min_dead_bytes: u64,
    /// Maximum number of entries to process per compaction batch
    pub batch_size: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            dead_space_threshold: 0.3,
            min_dead_bytes: 1024 * 1024, // 1 MB
            batch_size: 10_000,
        }
    }
}

/// Tracks compaction progress and metrics
#[derive(Debug)]
pub struct CompactionMetrics {
    /// Total bytes compacted (removed) since start
    pub bytes_compacted: AtomicU64,
    /// Total entries removed since start
    pub entries_removed: AtomicU64,
    /// Number of compaction runs completed
    pub runs_completed: AtomicU64,
    /// Whether a compaction is currently in progress
    pub in_progress: AtomicBool,
    /// Total live bytes after last compaction
    pub live_bytes_after: AtomicU64,
    /// Total dead bytes before last compaction
    pub dead_bytes_before: AtomicU64,
}

impl Default for CompactionMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl CompactionMetrics {
    /// Create new zeroed metrics
    pub fn new() -> Self {
        Self {
            bytes_compacted: AtomicU64::new(0),
            entries_removed: AtomicU64::new(0),
            runs_completed: AtomicU64::new(0),
            in_progress: AtomicBool::new(false),
            live_bytes_after: AtomicU64::new(0),
            dead_bytes_before: AtomicU64::new(0),
        }
    }

    /// Record a completed compaction run
    pub fn record_run(&self, bytes_removed: u64, entries_removed: u64, live_bytes: u64) {
        self.bytes_compacted
            .fetch_add(bytes_removed, Ordering::Relaxed);
        self.entries_removed
            .fetch_add(entries_removed, Ordering::Relaxed);
        self.runs_completed.fetch_add(1, Ordering::Relaxed);
        self.live_bytes_after.store(live_bytes, Ordering::Relaxed);
    }

    /// Get a snapshot of the current metrics
    pub fn snapshot(&self) -> CompactionSnapshot {
        CompactionSnapshot {
            bytes_compacted: self.bytes_compacted.load(Ordering::Relaxed),
            entries_removed: self.entries_removed.load(Ordering::Relaxed),
            runs_completed: self.runs_completed.load(Ordering::Relaxed),
            in_progress: self.in_progress.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time snapshot of compaction metrics
#[derive(Debug, Clone, Default)]
pub struct CompactionSnapshot {
    /// Total bytes compacted
    pub bytes_compacted: u64,
    /// Total entries removed
    pub entries_removed: u64,
    /// Number of runs completed
    pub runs_completed: u64,
    /// Whether compaction is currently in progress
    pub in_progress: bool,
}

/// Determine whether compaction should be triggered
///
/// Returns `true` when the estimated dead space ratio exceeds the threshold
/// and the absolute dead bytes exceed the minimum.
pub fn should_compact(total_bytes: u64, live_bytes: u64, config: &CompactionConfig) -> bool {
    if total_bytes == 0 {
        return false;
    }
    let dead_bytes = total_bytes.saturating_sub(live_bytes);
    let dead_ratio = dead_bytes as f64 / total_bytes as f64;

    dead_ratio >= config.dead_space_threshold && dead_bytes >= config.min_dead_bytes
}

/// Shared compaction state for background task coordination
#[derive(Debug, Clone)]
pub struct CompactionState {
    /// Compaction metrics
    pub metrics: Arc<CompactionMetrics>,
    /// Configuration
    pub config: CompactionConfig,
}

impl CompactionState {
    /// Create a new compaction state with the given config
    pub fn new(config: CompactionConfig) -> Self {
        Self {
            metrics: Arc::new(CompactionMetrics::new()),
            config,
        }
    }

    /// Mark compaction as in-progress; returns false if already running
    pub fn try_start(&self) -> bool {
        self.metrics
            .in_progress
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Mark compaction as finished
    pub fn finish(&self, bytes_removed: u64, entries_removed: u64, live_bytes: u64) {
        self.metrics
            .record_run(bytes_removed, entries_removed, live_bytes);
        self.metrics.in_progress.store(false, Ordering::Release);

        info!(
            bytes_removed = bytes_removed,
            entries_removed = entries_removed,
            "Compaction run completed"
        );
    }

    /// Check if compaction should trigger based on current stats
    pub fn should_trigger(&self, total_bytes: u64, live_bytes: u64) -> bool {
        if self.metrics.in_progress.load(Ordering::Relaxed) {
            debug!("Compaction already in progress, skipping");
            return false;
        }
        let result = should_compact(total_bytes, live_bytes, &self.config);
        if result {
            let dead = total_bytes.saturating_sub(live_bytes);
            debug!(
                total_bytes = total_bytes,
                live_bytes = live_bytes,
                dead_bytes = dead,
                "Compaction threshold exceeded"
            );
        }
        result
    }
}

impl Default for CompactionState {
    fn default() -> Self {
        Self::new(CompactionConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_compact_empty() {
        let config = CompactionConfig::default();
        assert!(!should_compact(0, 0, &config));
    }

    #[test]
    fn test_should_compact_below_threshold() {
        let config = CompactionConfig {
            dead_space_threshold: 0.3,
            min_dead_bytes: 100,
            batch_size: 1000,
        };
        // 10% dead space — below 30% threshold
        assert!(!should_compact(1000, 900, &config));
    }

    #[test]
    fn test_should_compact_above_threshold() {
        let config = CompactionConfig {
            dead_space_threshold: 0.3,
            min_dead_bytes: 100,
            batch_size: 1000,
        };
        // 50% dead space — above threshold
        assert!(should_compact(1000, 500, &config));
    }

    #[test]
    fn test_should_compact_below_min_bytes() {
        let config = CompactionConfig {
            dead_space_threshold: 0.3,
            min_dead_bytes: 1_000_000,
            batch_size: 1000,
        };
        // 50% dead but only 500 dead bytes — below min_dead_bytes
        assert!(!should_compact(1000, 500, &config));
    }

    #[test]
    fn test_compaction_metrics_record() {
        let metrics = CompactionMetrics::new();
        metrics.record_run(1000, 50, 5000);
        metrics.record_run(2000, 100, 4000);

        let snap = metrics.snapshot();
        assert_eq!(snap.bytes_compacted, 3000);
        assert_eq!(snap.entries_removed, 150);
        assert_eq!(snap.runs_completed, 2);
    }

    #[test]
    fn test_compaction_state_try_start() {
        let state = CompactionState::default();
        assert!(state.try_start());
        // Second attempt should fail
        assert!(!state.try_start());
        // After finish, should succeed again
        state.finish(0, 0, 0);
        assert!(state.try_start());
    }

    #[test]
    fn test_compaction_state_should_trigger_while_running() {
        let state = CompactionState::new(CompactionConfig {
            dead_space_threshold: 0.1,
            min_dead_bytes: 0,
            batch_size: 1000,
        });
        assert!(state.should_trigger(1000, 100));
        state.try_start();
        // Should not trigger while in progress
        assert!(!state.should_trigger(1000, 100));
    }

    #[test]
    fn test_compaction_snapshot_default() {
        let snap = CompactionSnapshot::default();
        assert_eq!(snap.bytes_compacted, 0);
        assert_eq!(snap.entries_removed, 0);
        assert_eq!(snap.runs_completed, 0);
        assert!(!snap.in_progress);
    }
}
