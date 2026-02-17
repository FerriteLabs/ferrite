//! Migration Progress Dashboard and Verification
//!
//! Real-time progress tracking with metrics, comprehensive data verification,
//! and migration report generation for the migration subsystem.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Migration phase
// ---------------------------------------------------------------------------

/// Phase of the migration lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationPhase {
    /// Analyzing source for compatibility
    Analyzing,
    /// Bulk copying keys from source to target
    InitialSync,
    /// Forwarding writes to both source and target
    DualWrite,
    /// Verifying data integrity
    Verification,
    /// Switching traffic to target
    Cutover,
    /// Rolling back to source
    Rollback,
    /// Migration completed successfully
    Complete,
    /// Migration failed
    Failed,
}

impl std::fmt::Display for MigrationPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Analyzing => write!(f, "analyzing"),
            Self::InitialSync => write!(f, "initial_sync"),
            Self::DualWrite => write!(f, "dual_write"),
            Self::Verification => write!(f, "verification"),
            Self::Cutover => write!(f, "cutover"),
            Self::Rollback => write!(f, "rollback"),
            Self::Complete => write!(f, "complete"),
            Self::Failed => write!(f, "failed"),
        }
    }
}

// ---------------------------------------------------------------------------
// Progress error
// ---------------------------------------------------------------------------

/// An error recorded during migration progress.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationProgressError {
    /// When the error occurred
    pub timestamp: SystemTime,
    /// Which phase the error occurred in
    pub phase: MigrationPhase,
    /// The key involved, if any
    pub key: Option<String>,
    /// Human-readable description
    pub message: String,
    /// Whether the migration can continue despite this error
    pub recoverable: bool,
}

// ---------------------------------------------------------------------------
// Progress snapshot
// ---------------------------------------------------------------------------

/// A point-in-time snapshot of migration progress.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressSnapshot {
    /// Current phase
    pub phase: MigrationPhase,
    /// Total keys to migrate
    pub total_keys: u64,
    /// Keys synced so far
    pub synced_keys: u64,
    /// Keys verified so far
    pub verified_keys: u64,
    /// Keys that failed
    pub failed_keys: u64,
    /// Bytes transferred so far
    pub bytes_transferred: u64,
    /// Time elapsed since migration start
    pub elapsed: Duration,
    /// Current throughput in keys per second
    pub keys_per_sec: f64,
    /// Estimated time remaining
    pub eta: Option<Duration>,
    /// Overall completion percentage
    pub percentage: f64,
    /// Number of errors recorded
    pub error_count: usize,
}

// ---------------------------------------------------------------------------
// Progress tracker
// ---------------------------------------------------------------------------

/// Window size for throughput sampling.
const THROUGHPUT_WINDOW: usize = 60;

/// Real-time migration progress tracker.
///
/// All counter fields use atomics so that concurrent tasks can update
/// progress without locking. Collections (errors, throughput samples)
/// use `parking_lot::RwLock` for efficient concurrent access.
pub struct MigrationProgressTracker {
    /// Current migration phase
    phase: RwLock<MigrationPhase>,
    /// Total number of keys to migrate
    total_keys: AtomicU64,
    /// Keys synced so far
    synced_keys: AtomicU64,
    /// Keys verified so far
    verified_keys: AtomicU64,
    /// Keys that failed to migrate
    failed_keys: AtomicU64,
    /// Total bytes transferred
    bytes_transferred: AtomicU64,
    /// When the migration started
    start_time: Instant,
    /// When the current phase started
    phase_start_time: RwLock<Instant>,
    /// Recorded errors
    errors: RwLock<Vec<MigrationProgressError>>,
    /// Sliding window of (timestamp, cumulative_synced) for throughput
    throughput_samples: RwLock<VecDeque<(Instant, u64)>>,
}

impl MigrationProgressTracker {
    /// Create a new tracker. The clock starts immediately.
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            phase: RwLock::new(MigrationPhase::Analyzing),
            total_keys: AtomicU64::new(0),
            synced_keys: AtomicU64::new(0),
            verified_keys: AtomicU64::new(0),
            failed_keys: AtomicU64::new(0),
            bytes_transferred: AtomicU64::new(0),
            start_time: now,
            phase_start_time: RwLock::new(now),
            errors: RwLock::new(Vec::new()),
            throughput_samples: RwLock::new(VecDeque::with_capacity(THROUGHPUT_WINDOW)),
        }
    }

    /// Transition to a new phase.
    pub fn set_phase(&self, phase: MigrationPhase) {
        *self.phase.write() = phase;
        *self.phase_start_time.write() = Instant::now();
    }

    /// Set the total number of keys to migrate.
    pub fn set_total_keys(&self, total: u64) {
        self.total_keys.store(total, Ordering::Relaxed);
    }

    /// Record that `count` additional keys have been synced.
    pub fn increment_synced(&self, count: u64) {
        let new_val = self.synced_keys.fetch_add(count, Ordering::Relaxed) + count;
        let mut samples = self.throughput_samples.write();
        let now = Instant::now();
        samples.push_back((now, new_val));
        if samples.len() > THROUGHPUT_WINDOW {
            samples.pop_front();
        }
    }

    /// Record that `count` additional keys have been verified.
    pub fn increment_verified(&self, count: u64) {
        self.verified_keys.fetch_add(count, Ordering::Relaxed);
    }

    /// Record that `count` keys failed.
    pub fn increment_failed(&self, count: u64) {
        self.failed_keys.fetch_add(count, Ordering::Relaxed);
    }

    /// Record that `bytes` additional bytes have been transferred.
    pub fn add_bytes(&self, bytes: u64) {
        self.bytes_transferred.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record an error.
    pub fn add_error(&self, error: MigrationProgressError) {
        self.errors.write().push(error);
    }

    /// Calculate current throughput in keys per second.
    pub fn throughput_keys_per_sec(&self) -> f64 {
        let samples = self.throughput_samples.read();
        if samples.len() < 2 {
            return 0.0;
        }
        let (first, last) = match (samples.front(), samples.back()) {
            (Some(f), Some(l)) => (f, l),
            _ => return 0.0,
        };
        let elapsed = last.0.duration_since(first.0).as_secs_f64();
        if elapsed < f64::EPSILON {
            return 0.0;
        }
        (last.1 - first.1) as f64 / elapsed
    }

    /// Estimated time remaining based on current throughput.
    pub fn estimated_time_remaining(&self) -> Option<Duration> {
        let throughput = self.throughput_keys_per_sec();
        if throughput < f64::EPSILON {
            return None;
        }
        let total = self.total_keys.load(Ordering::Relaxed);
        let synced = self.synced_keys.load(Ordering::Relaxed);
        if synced >= total {
            return Some(Duration::ZERO);
        }
        let remaining = (total - synced) as f64;
        let secs = remaining / throughput;
        Some(Duration::from_secs_f64(secs))
    }

    /// Overall completion percentage (0.0–100.0).
    pub fn percentage_complete(&self) -> f64 {
        let total = self.total_keys.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        let synced = self.synced_keys.load(Ordering::Relaxed);
        (synced as f64 / total as f64) * 100.0
    }

    /// Take a point-in-time snapshot of all progress metrics.
    pub fn snapshot(&self) -> ProgressSnapshot {
        ProgressSnapshot {
            phase: *self.phase.read(),
            total_keys: self.total_keys.load(Ordering::Relaxed),
            synced_keys: self.synced_keys.load(Ordering::Relaxed),
            verified_keys: self.verified_keys.load(Ordering::Relaxed),
            failed_keys: self.failed_keys.load(Ordering::Relaxed),
            bytes_transferred: self.bytes_transferred.load(Ordering::Relaxed),
            elapsed: self.start_time.elapsed(),
            keys_per_sec: self.throughput_keys_per_sec(),
            eta: self.estimated_time_remaining(),
            percentage: self.percentage_complete(),
            error_count: self.errors.read().len(),
        }
    }
}

impl Default for MigrationProgressTracker {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Data verification
// ---------------------------------------------------------------------------

/// Result of verifying a single key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifyResult {
    /// Source and target values match.
    Match,
    /// Values differ.
    Mismatch {
        expected_hash: u32,
        actual_hash: u32,
    },
    /// Key exists in source but not in target.
    MissingInTarget,
    /// Key exists in target but not in source.
    MissingInSource,
}

/// Aggregated result of verifying a batch of keys.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BatchVerifyResult {
    /// Number of keys that matched
    pub matched: u64,
    /// Number of keys that differed
    pub mismatched: u64,
    /// Number of keys missing on one side
    pub missing: u64,
}

/// Final verification report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationReport {
    /// Total keys verified
    pub total_verified: u64,
    /// Keys that matched
    pub matches: u64,
    /// Keys that differed
    pub mismatches: u64,
    /// Keys missing on one side
    pub missing: u64,
    /// Confidence level (0.0–1.0) based on sample rate
    pub confidence_level: f64,
    /// How long verification took
    pub duration: Duration,
}

/// Data verifier for post-migration integrity checks.
///
/// Uses CRC32 hashing for fast value comparison. A `sample_rate` of 1.0
/// means every key is checked; lower values perform probabilistic sampling.
pub struct DataVerifier {
    /// Fraction of keys to verify (0.0–1.0)
    sample_rate: f64,
    /// Running counts
    matched: AtomicU64,
    mismatched: AtomicU64,
    missing: AtomicU64,
    /// When verification started
    start_time: RwLock<Instant>,
}

impl DataVerifier {
    /// Create a new verifier with the given sample rate.
    pub fn new(sample_rate: f64) -> Self {
        let rate = sample_rate.clamp(0.0, 1.0);
        Self {
            sample_rate: rate,
            matched: AtomicU64::new(0),
            mismatched: AtomicU64::new(0),
            missing: AtomicU64::new(0),
            start_time: RwLock::new(Instant::now()),
        }
    }

    /// Verify a single key's value between source and target.
    pub fn verify_key(&self, _key: &[u8], source_val: &[u8], target_val: &[u8]) -> VerifyResult {
        let expected_hash = crc32fast::hash(source_val);
        let actual_hash = crc32fast::hash(target_val);
        if expected_hash == actual_hash && source_val == target_val {
            self.matched.fetch_add(1, Ordering::Relaxed);
            VerifyResult::Match
        } else {
            self.mismatched.fetch_add(1, Ordering::Relaxed);
            VerifyResult::Mismatch {
                expected_hash,
                actual_hash,
            }
        }
    }

    /// Verify a batch of (key, source_value, target_value) triples.
    pub fn verify_batch(&self, pairs: &[(Vec<u8>, Vec<u8>, Vec<u8>)]) -> BatchVerifyResult {
        let mut result = BatchVerifyResult::default();
        for (key, source_val, target_val) in pairs {
            match self.verify_key(key, source_val, target_val) {
                VerifyResult::Match => result.matched += 1,
                VerifyResult::Mismatch { .. } => result.mismatched += 1,
                VerifyResult::MissingInTarget | VerifyResult::MissingInSource => {
                    result.missing += 1;
                }
            }
        }
        result
    }

    /// Generate a verification report from accumulated results.
    pub fn generate_report(&self) -> VerificationReport {
        let matches = self.matched.load(Ordering::Relaxed);
        let mismatches = self.mismatched.load(Ordering::Relaxed);
        let missing = self.missing.load(Ordering::Relaxed);
        let total = matches + mismatches + missing;
        VerificationReport {
            total_verified: total,
            matches,
            mismatches,
            missing,
            confidence_level: self.sample_rate,
            duration: self.start_time.read().elapsed(),
        }
    }
}

// ---------------------------------------------------------------------------
// Migration report
// ---------------------------------------------------------------------------

/// Outcome of a completed migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationOutcome {
    /// All keys migrated and verified
    Success,
    /// Migration completed with some failures
    PartialSuccess,
    /// Migration failed
    Failed,
    /// Migration was rolled back
    RolledBack,
}

impl std::fmt::Display for MigrationOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Success => write!(f, "success"),
            Self::PartialSuccess => write!(f, "partial_success"),
            Self::Failed => write!(f, "failed"),
            Self::RolledBack => write!(f, "rolled_back"),
        }
    }
}

/// Report for a single migration phase.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhaseReport {
    /// Which phase
    pub phase: MigrationPhase,
    /// How long the phase took
    pub duration: Duration,
    /// Keys processed in this phase
    pub keys_processed: u64,
}

/// Full migration report produced at the end of a migration run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationReport {
    /// Unique report identifier
    pub id: String,
    /// When the migration started
    pub started_at: SystemTime,
    /// When the migration completed (if it has)
    pub completed_at: Option<SystemTime>,
    /// Source Redis URL
    pub source_url: String,
    /// Target Ferrite URL
    pub target_url: String,
    /// Total keys in scope
    pub total_keys: u64,
    /// Keys successfully synced
    pub synced_keys: u64,
    /// Verification report, if verification was run
    pub verification: Option<VerificationReport>,
    /// Per-phase breakdown
    pub phases: Vec<PhaseReport>,
    /// Errors encountered during migration
    pub errors: Vec<MigrationProgressError>,
    /// Final outcome
    pub outcome: MigrationOutcome,
}

impl MigrationReport {
    /// Create a new report with a generated UUID.
    pub fn new(source_url: String, target_url: String) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            started_at: SystemTime::now(),
            completed_at: None,
            source_url,
            target_url,
            total_keys: 0,
            synced_keys: 0,
            verification: None,
            phases: Vec::new(),
            errors: Vec::new(),
            outcome: MigrationOutcome::Failed, // default until proven otherwise
        }
    }

    /// Produce a human-readable summary of the migration.
    pub fn summary(&self) -> String {
        let duration = self
            .completed_at
            .and_then(|end| end.duration_since(self.started_at).ok())
            .map(|d| format!("{:.1}s", d.as_secs_f64()))
            .unwrap_or_else(|| "in progress".to_string());

        let verification_line = match &self.verification {
            Some(v) => format!(
                "  Verified: {}/{} keys ({} mismatches, {} missing)\n",
                v.matches, v.total_verified, v.mismatches, v.missing
            ),
            None => String::new(),
        };

        format!(
            "Migration Report [{}]\n\
             Outcome: {}\n\
             Source:  {}\n\
             Target:  {}\n\
             Duration: {}\n\
             Keys:   {}/{} synced\n\
             {}\
             Errors: {}\n\
             Phases: {}",
            self.id,
            self.outcome,
            self.source_url,
            self.target_url,
            duration,
            self.synced_keys,
            self.total_keys,
            verification_line,
            self.errors.len(),
            self.phases.len(),
        )
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_tracker_new_defaults() {
        let tracker = MigrationProgressTracker::new();
        let snap = tracker.snapshot();
        assert_eq!(snap.phase, MigrationPhase::Analyzing);
        assert_eq!(snap.total_keys, 0);
        assert_eq!(snap.synced_keys, 0);
        assert_eq!(snap.percentage, 0.0);
    }

    #[test]
    fn test_set_phase() {
        let tracker = MigrationProgressTracker::new();
        tracker.set_phase(MigrationPhase::InitialSync);
        assert_eq!(tracker.snapshot().phase, MigrationPhase::InitialSync);
    }

    #[test]
    fn test_percentage_complete() {
        let tracker = MigrationProgressTracker::new();
        tracker.set_total_keys(200);
        tracker.increment_synced(50);
        assert!((tracker.percentage_complete() - 25.0).abs() < f64::EPSILON);
        tracker.increment_synced(150);
        assert!((tracker.percentage_complete() - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_percentage_zero_total() {
        let tracker = MigrationProgressTracker::new();
        assert!((tracker.percentage_complete() - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_throughput_and_eta() {
        let tracker = MigrationProgressTracker::new();
        tracker.set_total_keys(1000);

        // Simulate syncing 100 keys
        tracker.increment_synced(100);
        // Need at least two samples separated in time for throughput
        thread::sleep(Duration::from_millis(50));
        tracker.increment_synced(100);

        let throughput = tracker.throughput_keys_per_sec();
        assert!(throughput > 0.0, "throughput should be positive");

        let eta = tracker.estimated_time_remaining();
        assert!(eta.is_some(), "ETA should be present");
    }

    #[test]
    fn test_eta_none_when_no_samples() {
        let tracker = MigrationProgressTracker::new();
        tracker.set_total_keys(1000);
        assert!(tracker.estimated_time_remaining().is_none());
    }

    #[test]
    fn test_eta_zero_when_complete() {
        let tracker = MigrationProgressTracker::new();
        tracker.set_total_keys(100);
        tracker.increment_synced(50);
        thread::sleep(Duration::from_millis(10));
        tracker.increment_synced(50);
        let eta = tracker.estimated_time_remaining();
        assert_eq!(eta, Some(Duration::ZERO));
    }

    #[test]
    fn test_error_recording() {
        let tracker = MigrationProgressTracker::new();
        tracker.add_error(MigrationProgressError {
            timestamp: SystemTime::now(),
            phase: MigrationPhase::InitialSync,
            key: Some("user:1".to_string()),
            message: "connection lost".to_string(),
            recoverable: true,
        });
        assert_eq!(tracker.snapshot().error_count, 1);
    }

    #[test]
    fn test_bytes_tracking() {
        let tracker = MigrationProgressTracker::new();
        tracker.add_bytes(1024);
        tracker.add_bytes(2048);
        assert_eq!(tracker.snapshot().bytes_transferred, 3072);
    }

    #[test]
    fn test_verify_key_match() {
        let verifier = DataVerifier::new(1.0);
        let result = verifier.verify_key(b"key1", b"hello", b"hello");
        assert_eq!(result, VerifyResult::Match);
    }

    #[test]
    fn test_verify_key_mismatch() {
        let verifier = DataVerifier::new(1.0);
        let result = verifier.verify_key(b"key1", b"hello", b"world");
        assert!(matches!(result, VerifyResult::Mismatch { .. }));
    }

    #[test]
    fn test_verify_batch() {
        let verifier = DataVerifier::new(1.0);
        let pairs = vec![
            (b"k1".to_vec(), b"v1".to_vec(), b"v1".to_vec()),
            (b"k2".to_vec(), b"v2".to_vec(), b"v2".to_vec()),
            (b"k3".to_vec(), b"aaa".to_vec(), b"bbb".to_vec()),
        ];
        let result = verifier.verify_batch(&pairs);
        assert_eq!(result.matched, 2);
        assert_eq!(result.mismatched, 1);
        assert_eq!(result.missing, 0);
    }

    #[test]
    fn test_verification_report() {
        let verifier = DataVerifier::new(0.5);
        verifier.verify_key(b"k1", b"v", b"v");
        verifier.verify_key(b"k2", b"a", b"b");
        let report = verifier.generate_report();
        assert_eq!(report.total_verified, 2);
        assert_eq!(report.matches, 1);
        assert_eq!(report.mismatches, 1);
        assert!((report.confidence_level - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_sample_rate_clamping() {
        let v = DataVerifier::new(2.0);
        assert!((v.sample_rate - 1.0).abs() < f64::EPSILON);
        let v = DataVerifier::new(-1.0);
        assert!((v.sample_rate - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_migration_report_summary() {
        let mut report = MigrationReport::new(
            "redis://localhost:6379".to_string(),
            "ferrite://localhost:6380".to_string(),
        );
        report.total_keys = 1000;
        report.synced_keys = 1000;
        report.outcome = MigrationOutcome::Success;
        report.completed_at = Some(SystemTime::now());

        let summary = report.summary();
        assert!(summary.contains("success"));
        assert!(summary.contains("1000/1000"));
    }

    #[test]
    fn test_migration_outcome_display() {
        assert_eq!(MigrationOutcome::Success.to_string(), "success");
        assert_eq!(MigrationOutcome::RolledBack.to_string(), "rolled_back");
    }

    #[test]
    fn test_migration_phase_display() {
        assert_eq!(MigrationPhase::Analyzing.to_string(), "analyzing");
        assert_eq!(MigrationPhase::DualWrite.to_string(), "dual_write");
        assert_eq!(MigrationPhase::Complete.to_string(), "complete");
    }

    #[test]
    fn test_failed_keys_tracking() {
        let tracker = MigrationProgressTracker::new();
        tracker.increment_failed(3);
        tracker.increment_failed(2);
        assert_eq!(tracker.snapshot().failed_keys, 5);
    }

    #[test]
    fn test_verified_keys_tracking() {
        let tracker = MigrationProgressTracker::new();
        tracker.increment_verified(10);
        tracker.increment_verified(20);
        assert_eq!(tracker.snapshot().verified_keys, 30);
    }
}
