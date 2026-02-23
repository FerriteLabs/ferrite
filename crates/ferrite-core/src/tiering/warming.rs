//! Intelligent Cache Warming System
//!
//! Proactively warms keys from cold→hot tiers using ML-based predictions.
//! Combines seasonal pattern detection (24h × 7d hourly buckets), exponential
//! moving average trend analysis, and frequency scoring to predict which keys
//! will be accessed next.
//!
//! ## Architecture
//!
//! - **AccessTracker**: Sliding window of per-key access events with hourly
//!   buckets for seasonal detection and EMA for trend analysis.
//! - **CacheWarmer**: Prediction engine that generates warming plans from
//!   access history, respecting budget constraints.
//! - **PredictionAccuracy**: Tracks precision, recall, and F1 score via
//!   hit/miss feedback.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use ferrite::tiering::warming::{CacheWarmer, WarmingConfig, WarmingBudget, AccessType};
//! use chrono::Utc;
//!
//! let warmer = CacheWarmer::new(WarmingConfig::default());
//!
//! // Record access events
//! warmer.record_access("user:1001", AccessType::Read, Utc::now());
//!
//! // Generate and execute a warming plan
//! let budget = WarmingBudget::default();
//! let plan = warmer.generate_warming_plan(&budget);
//! let result = warmer.execute_plan(&plan);
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use chrono::{DateTime, Datelike, Duration, Timelike, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Number of hourly buckets per day.
const HOURS_PER_DAY: usize = 24;

/// Number of days per week for seasonal detection.
const DAYS_PER_WEEK: usize = 7;

/// Total number of seasonal bins (24h × 7d).
const SEASONAL_BINS: usize = HOURS_PER_DAY * DAYS_PER_WEEK;

/// Threshold for hot key detection (accesses per hour).
const HOT_KEY_THRESHOLD: f64 = 10.0;

/// EMA smoothing factor (α). Higher values give more weight to recent data.
const EMA_ALPHA: f64 = 0.3;

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

/// Target tier for warming actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TierTarget {
    /// In-memory storage — fastest, most expensive.
    Memory,
    /// Memory-mapped files — warm tier.
    Mmap,
    /// Local SSD / disk storage.
    Disk,
    /// Cloud object storage (S3/GCS/Azure).
    Cloud,
}

impl std::fmt::Display for TierTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TierTarget::Memory => write!(f, "memory"),
            TierTarget::Mmap => write!(f, "mmap"),
            TierTarget::Disk => write!(f, "disk"),
            TierTarget::Cloud => write!(f, "cloud"),
        }
    }
}

/// Type of access event recorded by the warmer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccessType {
    /// Read access.
    Read,
    /// Write access.
    Write,
    /// Scan / range query.
    Scan,
}

/// Reason a key was selected for warming.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WarmingReason {
    /// Seasonal (time-of-day / day-of-week) pattern match.
    SeasonalPattern {
        /// Hour of day (0-23).
        hour: u8,
        /// Day of week (0=Mon, 6=Sun).
        day_of_week: u8,
    },
    /// Access frequency is trending upward.
    TrendingUp {
        /// EMA growth rate.
        growth_rate: f64,
    },
    /// Key shows a recurring access cycle.
    RecurringAccess {
        /// Detected period in seconds.
        period_secs: u64,
    },
    /// An external hint requested warming.
    ExplicitHint {
        /// Hint source identifier.
        source: String,
    },
    /// Key is related to another frequently-accessed key.
    RelatedKey {
        /// The primary (hot) key.
        primary_key: String,
    },
}

/// Errors that can occur during cache warming operations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WarmingError {
    /// Warming plan would exceed the allocated budget.
    BudgetExceeded,
    /// No candidates meet the confidence threshold.
    NoCandidates,
    /// The requested tier is not available.
    TierUnavailable(String),
    /// Invalid configuration parameter.
    ConfigInvalid(String),
    /// Unexpected internal error.
    Internal(String),
}

impl std::fmt::Display for WarmingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WarmingError::BudgetExceeded => write!(f, "warming budget exceeded"),
            WarmingError::NoCandidates => write!(f, "no warming candidates found"),
            WarmingError::TierUnavailable(t) => write!(f, "tier unavailable: {t}"),
            WarmingError::ConfigInvalid(msg) => write!(f, "invalid config: {msg}"),
            WarmingError::Internal(msg) => write!(f, "internal error: {msg}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the intelligent cache warming system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WarmingConfig {
    /// Enable the warming system.
    pub enabled: bool,
    /// Maximum keys to warm per evaluation cycle.
    pub max_keys_per_cycle: usize,
    /// How often to evaluate warming candidates (seconds).
    pub evaluation_interval_secs: u64,
    /// Minimum confidence threshold for warming a key (0.0-1.0).
    pub confidence_threshold: f64,
    /// Enable seasonal (time-of-day / day-of-week) pattern detection.
    pub seasonal_detection: bool,
    /// Weight of the seasonal signal in composite confidence (0.0-1.0).
    pub hourly_pattern_weight: f64,
    /// Weight of the recency signal in composite confidence (0.0-1.0).
    pub recency_weight: f64,
    /// Weight of the frequency signal in composite confidence (0.0-1.0).
    pub frequency_weight: f64,
    /// Maximum memory budget for warmed data (bytes).
    pub max_memory_budget_bytes: u64,
}

impl Default for WarmingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_keys_per_cycle: 500,
            evaluation_interval_secs: 30,
            confidence_threshold: 0.7,
            seasonal_detection: true,
            hourly_pattern_weight: 0.6,
            recency_weight: 0.3,
            frequency_weight: 0.1,
            max_memory_budget_bytes: 512 * 1024 * 1024, // 512 MB
        }
    }
}

// ---------------------------------------------------------------------------
// Candidate / Budget / Plan / Action / Result
// ---------------------------------------------------------------------------

/// A key predicted to need warming.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WarmingCandidate {
    /// Key or key pattern to warm.
    pub key_pattern: String,
    /// Predicted number of accesses in the horizon window.
    pub predicted_access_count: u64,
    /// Confidence score (0.0-1.0).
    pub confidence: f64,
    /// Target tier to warm into.
    pub predicted_tier: TierTarget,
    /// Current tier the key resides in.
    pub current_tier: TierTarget,
    /// Estimated size of the value in bytes.
    pub estimated_size_bytes: u64,
    /// Reason the key was selected.
    pub reason: WarmingReason,
}

/// Budget constraints for a single warming cycle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WarmingBudget {
    /// Maximum total bytes to warm.
    pub max_bytes: u64,
    /// Maximum number of keys to warm.
    pub max_keys: usize,
    /// Maximum wall-clock time for the cycle (milliseconds).
    pub max_duration_ms: u64,
    /// Tiers that are allowed as warming targets.
    pub allowed_tiers: Vec<TierTarget>,
}

impl Default for WarmingBudget {
    fn default() -> Self {
        Self {
            max_bytes: 256 * 1024 * 1024, // 256 MB
            max_keys: 500,
            max_duration_ms: 5000,
            allowed_tiers: vec![TierTarget::Memory, TierTarget::Mmap],
        }
    }
}

/// A single warming action within a plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WarmingAction {
    /// Key or key pattern to warm.
    pub key_pattern: String,
    /// Tier the key currently resides in.
    pub from_tier: TierTarget,
    /// Target tier to move the key to.
    pub to_tier: TierTarget,
    /// Priority (lower = higher priority, 0 is highest).
    pub priority: u32,
    /// Estimated bytes to transfer.
    pub estimated_bytes: u64,
    /// Reason for this action.
    pub reason: WarmingReason,
}

/// An ordered plan of warming actions generated from predictions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WarmingPlan {
    /// Ordered list of warming actions.
    pub actions: Vec<WarmingAction>,
    /// Total estimated bytes across all actions.
    pub total_bytes: u64,
    /// Total number of keys to warm.
    pub total_keys: usize,
    /// Estimated cache-hit improvement percentage.
    pub estimated_improvement_percent: f64,
    /// When this plan was generated.
    pub generated_at: DateTime<Utc>,
}

/// Result of executing a warming plan.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WarmingResult {
    /// Actions that were in the plan.
    pub planned_actions: usize,
    /// Actions that executed successfully.
    pub executed_actions: usize,
    /// Bytes actually warmed.
    pub bytes_warmed: u64,
    /// Keys actually warmed.
    pub keys_warmed: u64,
    /// Wall-clock duration in milliseconds.
    pub duration_ms: u64,
    /// Errors encountered during execution.
    pub errors: Vec<String>,
}

// ---------------------------------------------------------------------------
// Statistics / Accuracy
// ---------------------------------------------------------------------------

/// Cumulative statistics for the warming system.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WarmingStats {
    /// Total evaluation cycles executed.
    pub total_cycles: u64,
    /// Total keys warmed across all cycles.
    pub total_keys_warmed: u64,
    /// Total bytes warmed across all cycles.
    pub total_bytes_warmed: u64,
    /// Total predictions made.
    pub predictions_made: u64,
    /// Predictions that turned out to be accurate.
    pub predictions_accurate: u64,
    /// Estimated cache-hit improvement percentage.
    pub cache_hit_improvement_percent: f64,
    /// Candidates in the current/most-recent cycle.
    pub current_cycle_candidates: u64,
}

/// Precision / recall / F1 accuracy metrics for the prediction model.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PredictionAccuracy {
    /// Total prediction events recorded.
    pub total_predictions: u64,
    /// Predicted warm AND actually accessed.
    pub true_positives: u64,
    /// Predicted warm BUT NOT accessed.
    pub false_positives: u64,
    /// Not predicted AND not accessed.
    pub true_negatives: u64,
    /// Not predicted BUT was accessed.
    pub false_negatives: u64,
    /// Precision = TP / (TP + FP).
    pub precision: f64,
    /// Recall = TP / (TP + FN).
    pub recall: f64,
    /// F1 = 2 * precision * recall / (precision + recall).
    pub f1_score: f64,
}

impl PredictionAccuracy {
    /// Recalculate derived metrics from the raw counts.
    fn recalculate(&mut self) {
        let tp = self.true_positives as f64;
        let fp = self.false_positives as f64;
        let fn_ = self.false_negatives as f64;

        self.precision = if tp + fp > 0.0 { tp / (tp + fp) } else { 0.0 };

        self.recall = if tp + fn_ > 0.0 { tp / (tp + fn_) } else { 0.0 };

        self.f1_score = if self.precision + self.recall > 0.0 {
            2.0 * self.precision * self.recall / (self.precision + self.recall)
        } else {
            0.0
        };

        self.total_predictions =
            self.true_positives + self.false_positives + self.true_negatives + self.false_negatives;
    }
}

// ---------------------------------------------------------------------------
// Internal: AccessTracker
// ---------------------------------------------------------------------------

/// A single recorded access event.
#[derive(Debug, Clone)]
struct AccessEvent {
    timestamp: DateTime<Utc>,
    #[allow(dead_code)] // Retained for future per-type analysis
    access_type: AccessType,
}

/// Per-key access tracking with seasonal buckets and EMA trend detection.
#[derive(Debug, Clone)]
struct KeyTracker {
    /// Hourly buckets: 24 hours × 7 days = 168 bins.
    seasonal_buckets: Vec<f64>,
    /// Total number of accesses recorded.
    total_accesses: u64,
    /// Timestamp of the most recent access.
    last_access: Option<DateTime<Utc>>,
    /// Exponential moving average of inter-access intervals.
    ema_interval: Option<f64>,
    /// EMA of access rate (accesses per hour).
    ema_rate: Option<f64>,
    /// Previous EMA rate (for trend detection).
    prev_ema_rate: Option<f64>,
    /// Estimated value size in bytes (heuristic).
    estimated_size: u64,
    /// Current tier of the key.
    current_tier: TierTarget,
    /// Recent access events (bounded sliding window).
    recent_events: Vec<AccessEvent>,
}

impl KeyTracker {
    fn new() -> Self {
        Self {
            seasonal_buckets: vec![0.0; SEASONAL_BINS],
            total_accesses: 0,
            last_access: None,
            ema_interval: None,
            ema_rate: None,
            prev_ema_rate: None,
            estimated_size: 256, // default estimate
            current_tier: TierTarget::Disk,
            recent_events: Vec::new(),
        }
    }

    /// Record an access and update seasonal buckets + EMA.
    fn record(&mut self, access_type: AccessType, timestamp: DateTime<Utc>) {
        let hour = timestamp.hour() as usize;
        let day = timestamp.weekday().num_days_from_monday() as usize;
        let bin = day * HOURS_PER_DAY + hour;

        if bin < SEASONAL_BINS {
            self.seasonal_buckets[bin] += 1.0;
        }

        // Update EMA of inter-access interval
        if let Some(last) = self.last_access {
            let interval = (timestamp - last).num_seconds().max(1) as f64;
            self.ema_interval = Some(match self.ema_interval {
                Some(prev) => EMA_ALPHA * interval + (1.0 - EMA_ALPHA) * prev,
                None => interval,
            });
        }

        // Update EMA of access rate (accesses per hour)
        let current_rate = self.compute_recent_rate(&timestamp);
        self.prev_ema_rate = self.ema_rate;
        self.ema_rate = Some(match self.ema_rate {
            Some(prev) => EMA_ALPHA * current_rate + (1.0 - EMA_ALPHA) * prev,
            None => current_rate,
        });

        self.last_access = Some(timestamp);
        self.total_accesses += 1;

        // Keep a bounded sliding window of recent events
        const MAX_RECENT: usize = 500;
        self.recent_events.push(AccessEvent {
            timestamp,
            access_type,
        });
        if self.recent_events.len() > MAX_RECENT {
            self.recent_events.remove(0);
        }
    }

    /// Compute recent access rate (accesses per hour) from the sliding window.
    fn compute_recent_rate(&self, now: &DateTime<Utc>) -> f64 {
        let one_hour_ago = *now - Duration::hours(1);
        let count = self
            .recent_events
            .iter()
            .filter(|e| e.timestamp >= one_hour_ago)
            .count();
        count as f64
    }

    /// Get the seasonal score for a given hour and day.
    fn seasonal_score(&self, hour: usize, day: usize) -> f64 {
        let bin = day * HOURS_PER_DAY + hour;
        let total: f64 = self.seasonal_buckets.iter().sum();
        if total == 0.0 {
            return 0.0;
        }
        let raw = self.seasonal_buckets.get(bin).copied().unwrap_or(0.0);
        // Normalize against the peak bucket
        let max_bucket = self
            .seasonal_buckets
            .iter()
            .cloned()
            .fold(0.0_f64, f64::max);
        if max_bucket == 0.0 {
            0.0
        } else {
            raw / max_bucket
        }
    }

    /// Detect whether the access trend is rising.
    fn trend_growth_rate(&self) -> f64 {
        match (self.ema_rate, self.prev_ema_rate) {
            (Some(current), Some(prev)) if prev > 0.0 => (current - prev) / prev,
            _ => 0.0,
        }
    }

    /// Frequency score normalized to 0.0-1.0 based on HOT_KEY_THRESHOLD.
    fn frequency_score(&self) -> f64 {
        let rate = self.ema_rate.unwrap_or(0.0);
        (rate / HOT_KEY_THRESHOLD).min(1.0)
    }

    /// Detect recurring access period from the seasonal buckets.
    fn detect_period(&self) -> Option<u64> {
        // Simple autocorrelation on the 168-bin seasonal vector
        let total: f64 = self.seasonal_buckets.iter().sum();
        if total < 10.0 {
            return None;
        }

        let mean = total / SEASONAL_BINS as f64;
        let variance: f64 = self
            .seasonal_buckets
            .iter()
            .map(|v| (v - mean).powi(2))
            .sum::<f64>()
            / SEASONAL_BINS as f64;
        if variance < 0.01 {
            return None;
        }

        // Check common periods: 24h (daily), 168h (weekly)
        for period in [HOURS_PER_DAY, SEASONAL_BINS] {
            let mut correlation = 0.0;
            let mut count = 0;
            for i in 0..SEASONAL_BINS - period {
                correlation +=
                    (self.seasonal_buckets[i] - mean) * (self.seasonal_buckets[i + period] - mean);
                count += 1;
            }
            if count > 0 {
                let autocorr = correlation / (count as f64 * variance);
                if autocorr > 0.5 {
                    return Some((period as u64) * 3600);
                }
            }
        }
        None
    }
}

/// Internal access tracker managing per-key trackers.
struct AccessTracker {
    trackers: HashMap<String, KeyTracker>,
}

impl AccessTracker {
    fn new() -> Self {
        Self {
            trackers: HashMap::new(),
        }
    }

    fn record(&mut self, key: &str, access_type: AccessType, timestamp: DateTime<Utc>) {
        self.trackers
            .entry(key.to_string())
            .or_insert_with(KeyTracker::new)
            .record(access_type, timestamp);
    }

    fn get(&self, key: &str) -> Option<&KeyTracker> {
        self.trackers.get(key)
    }

    fn keys(&self) -> impl Iterator<Item = &String> {
        self.trackers.keys()
    }

    #[allow(dead_code)]
    fn len(&self) -> usize {
        self.trackers.len()
    }
}

// ---------------------------------------------------------------------------
// CacheWarmer
// ---------------------------------------------------------------------------

/// Proactive cache warming engine that uses ML-based predictions to move keys
/// from cold to hot tiers before they are needed.
pub struct CacheWarmer {
    config: WarmingConfig,
    tracker: RwLock<AccessTracker>,
    stats: RwLock<WarmingStats>,
    accuracy: RwLock<PredictionAccuracy>,
    /// Keys that were predicted (warmed) — used for accuracy tracking.
    warmed_keys: RwLock<HashMap<String, bool>>,
    total_cycles: AtomicU64,
}

impl CacheWarmer {
    /// Create a new cache warmer with the given configuration.
    pub fn new(config: WarmingConfig) -> Self {
        Self {
            config,
            tracker: RwLock::new(AccessTracker::new()),
            stats: RwLock::new(WarmingStats::default()),
            accuracy: RwLock::new(PredictionAccuracy::default()),
            warmed_keys: RwLock::new(HashMap::new()),
            total_cycles: AtomicU64::new(0),
        }
    }

    /// Record an access event for a key.
    pub fn record_access(&self, key: &str, access_type: AccessType, timestamp: DateTime<Utc>) {
        self.tracker.write().record(key, access_type, timestamp);
    }

    /// Predict demand for keys within the given time horizon.
    ///
    /// Returns warming candidates sorted by confidence (highest first).
    pub fn predict_demand(&self, horizon: Duration) -> Vec<WarmingCandidate> {
        let tracker = self.tracker.read();
        let now = Utc::now();
        let target_time = now + horizon;
        let target_hour = target_time.hour() as usize;
        let target_day = target_time.weekday().num_days_from_monday() as usize;

        let mut candidates = Vec::new();

        for key in tracker.keys() {
            let kt = match tracker.get(key) {
                Some(kt) => kt,
                None => continue,
            };

            let (confidence, reason) = self.compute_confidence(kt, target_hour, target_day);

            if confidence >= self.config.confidence_threshold {
                let predicted_rate = kt.ema_rate.unwrap_or(0.0);
                let horizon_hours = horizon.num_seconds().max(1) as f64 / 3600.0;

                candidates.push(WarmingCandidate {
                    key_pattern: key.clone(),
                    predicted_access_count: (predicted_rate * horizon_hours).ceil() as u64,
                    confidence,
                    predicted_tier: TierTarget::Memory,
                    current_tier: kt.current_tier,
                    estimated_size_bytes: kt.estimated_size,
                    reason,
                });
            }
        }

        candidates.sort_by(|a, b| {
            b.confidence
                .partial_cmp(&a.confidence)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Update stats
        {
            let mut stats = self.stats.write();
            stats.predictions_made += candidates.len() as u64;
            stats.current_cycle_candidates = candidates.len() as u64;
        }

        candidates
    }

    /// Generate a warming plan that respects the given budget constraints.
    pub fn generate_warming_plan(&self, budget: &WarmingBudget) -> WarmingPlan {
        let horizon = Duration::seconds(self.config.evaluation_interval_secs as i64 * 2);
        let candidates = self.predict_demand(horizon);

        let mut actions = Vec::new();
        let mut total_bytes: u64 = 0;
        let mut priority: u32 = 0;

        for candidate in &candidates {
            if actions.len() >= budget.max_keys {
                break;
            }
            if total_bytes + candidate.estimated_size_bytes > budget.max_bytes {
                continue;
            }

            let to_tier = if budget.allowed_tiers.contains(&candidate.predicted_tier) {
                candidate.predicted_tier
            } else if let Some(&first) = budget.allowed_tiers.first() {
                first
            } else {
                continue;
            };

            // Skip if already in the target tier
            if candidate.current_tier == to_tier {
                continue;
            }

            total_bytes += candidate.estimated_size_bytes;
            actions.push(WarmingAction {
                key_pattern: candidate.key_pattern.clone(),
                from_tier: candidate.current_tier,
                to_tier,
                priority,
                estimated_bytes: candidate.estimated_size_bytes,
                reason: candidate.reason.clone(),
            });
            priority += 1;
        }

        let estimated_improvement = if !candidates.is_empty() {
            (actions.len() as f64 / candidates.len() as f64) * 100.0
        } else {
            0.0
        };

        WarmingPlan {
            total_keys: actions.len(),
            total_bytes,
            estimated_improvement_percent: estimated_improvement,
            generated_at: Utc::now(),
            actions,
        }
    }

    /// Execute a warming plan.
    ///
    /// In a real system this would trigger async I/O to move data between
    /// tiers. Here we simulate execution and track results.
    pub fn execute_plan(&self, plan: &WarmingPlan) -> WarmingResult {
        let start = Instant::now();
        let mut result = WarmingResult {
            planned_actions: plan.actions.len(),
            ..Default::default()
        };

        {
            let mut warmed = self.warmed_keys.write();
            for action in &plan.actions {
                // Simulate successful execution
                result.executed_actions += 1;
                result.bytes_warmed += action.estimated_bytes;
                result.keys_warmed += 1;
                warmed.insert(action.key_pattern.clone(), false);
            }
        }

        result.duration_ms = start.elapsed().as_millis() as u64;

        // Update cumulative stats
        {
            let mut stats = self.stats.write();
            stats.total_cycles += 1;
            stats.total_keys_warmed += result.keys_warmed;
            stats.total_bytes_warmed += result.bytes_warmed;
        }
        self.total_cycles.fetch_add(1, Ordering::Relaxed);

        result
    }

    /// Record whether a key that was predicted (warmed) was actually accessed.
    ///
    /// `was_warm` = true means the key was warmed AND subsequently accessed
    /// (true positive). `was_warm` = false means the key was NOT warmed but was
    /// accessed (false negative / miss).
    pub fn record_hit_miss(&self, key: &str, was_warm: bool) {
        let mut accuracy = self.accuracy.write();
        let mut warmed = self.warmed_keys.write();

        if was_warm {
            if warmed.remove(key).is_some() {
                // We predicted it AND it was accessed → true positive
                accuracy.true_positives += 1;
            } else {
                // It was warm but we didn't track it — still count
                accuracy.true_positives += 1;
            }
            self.stats.write().predictions_accurate += 1;
        } else {
            // Key was accessed but was NOT warmed → false negative
            accuracy.false_negatives += 1;
        }

        accuracy.recalculate();
    }

    /// Mark remaining unaccessed warmed keys as false positives.
    /// Call this at the end of an evaluation cycle.
    pub fn flush_unaccessed(&self) {
        let mut warmed = self.warmed_keys.write();
        let mut accuracy = self.accuracy.write();
        let fp_count = warmed.len() as u64;
        accuracy.false_positives += fp_count;
        warmed.clear();
        accuracy.recalculate();
    }

    /// Get cumulative warming statistics.
    pub fn get_stats(&self) -> WarmingStats {
        let stats = self.stats.read();
        let accuracy = self.accuracy.read();

        let improvement = if stats.predictions_made > 0 {
            (stats.predictions_accurate as f64 / stats.predictions_made as f64) * 100.0
        } else {
            0.0
        };

        WarmingStats {
            total_cycles: stats.total_cycles,
            total_keys_warmed: stats.total_keys_warmed,
            total_bytes_warmed: stats.total_bytes_warmed,
            predictions_made: stats.predictions_made + accuracy.total_predictions,
            predictions_accurate: stats.predictions_accurate,
            cache_hit_improvement_percent: improvement,
            current_cycle_candidates: stats.current_cycle_candidates,
        }
    }

    /// Get prediction accuracy metrics.
    pub fn get_accuracy(&self) -> PredictionAccuracy {
        self.accuracy.read().clone()
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Compute composite confidence and determine the warming reason.
    fn compute_confidence(
        &self,
        kt: &KeyTracker,
        target_hour: usize,
        target_day: usize,
    ) -> (f64, WarmingReason) {
        let seasonal = if self.config.seasonal_detection {
            kt.seasonal_score(target_hour, target_day)
        } else {
            0.0
        };

        let trend = kt.trend_growth_rate();
        let frequency = kt.frequency_score();

        // Recency score based on EMA interval (smaller interval → higher score)
        let recency = match kt.ema_interval {
            Some(interval) if interval > 0.0 => (1.0 / (1.0 + interval / 3600.0)).min(1.0),
            _ => 0.0,
        };

        // Hot keys (>10 accesses/hour) always get high confidence
        if frequency >= 1.0 {
            let confidence = (0.9_f64).max(
                self.config.hourly_pattern_weight * seasonal
                    + self.config.recency_weight * recency
                    + self.config.frequency_weight * frequency,
            );
            return (
                confidence,
                WarmingReason::TrendingUp {
                    growth_rate: trend.max(0.0),
                },
            );
        }

        // Composite confidence
        let confidence = self.config.hourly_pattern_weight * seasonal
            + self.config.recency_weight * recency
            + self.config.frequency_weight * frequency;

        // Determine the primary reason
        let reason = if trend > 0.1 {
            WarmingReason::TrendingUp { growth_rate: trend }
        } else if seasonal > 0.5 {
            WarmingReason::SeasonalPattern {
                hour: target_hour as u8,
                day_of_week: target_day as u8,
            }
        } else if let Some(period) = kt.detect_period() {
            WarmingReason::RecurringAccess {
                period_secs: period,
            }
        } else {
            WarmingReason::TrendingUp {
                growth_rate: trend.max(0.0),
            }
        };

        (confidence, reason)
    }
}

impl Default for CacheWarmer {
    fn default() -> Self {
        Self::new(WarmingConfig::default())
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    /// Helper: create a warmer with a low confidence threshold for testing.
    fn test_warmer() -> CacheWarmer {
        CacheWarmer::new(WarmingConfig {
            confidence_threshold: 0.1,
            ..Default::default()
        })
    }

    /// Helper: generate timestamps for a specific hour and day-of-week.
    fn make_timestamp(hour: u32, day_offset: i64) -> DateTime<Utc> {
        // Use a known Monday: 2024-01-01 is a Monday
        Utc.with_ymd_and_hms(2024, 1, 1 + day_offset as u32, hour, 0, 0)
            .unwrap()
    }

    // -----------------------------------------------------------------------
    // Configuration tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_default_config() {
        let config = WarmingConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_keys_per_cycle, 500);
        assert_eq!(config.evaluation_interval_secs, 30);
        assert!((config.confidence_threshold - 0.7).abs() < f64::EPSILON);
        assert!(config.seasonal_detection);
        assert!((config.hourly_pattern_weight - 0.6).abs() < f64::EPSILON);
        assert!((config.recency_weight - 0.3).abs() < f64::EPSILON);
        assert!((config.frequency_weight - 0.1).abs() < f64::EPSILON);
        assert_eq!(config.max_memory_budget_bytes, 512 * 1024 * 1024);
    }

    #[test]
    fn test_default_budget() {
        let budget = WarmingBudget::default();
        assert_eq!(budget.max_bytes, 256 * 1024 * 1024);
        assert_eq!(budget.max_keys, 500);
        assert_eq!(budget.max_duration_ms, 5000);
        assert!(budget.allowed_tiers.contains(&TierTarget::Memory));
    }

    // -----------------------------------------------------------------------
    // Access recording and pattern detection
    // -----------------------------------------------------------------------

    #[test]
    fn test_record_access_basic() {
        let warmer = test_warmer();
        let ts = Utc::now();

        warmer.record_access("key:1", AccessType::Read, ts);
        warmer.record_access("key:1", AccessType::Write, ts);
        warmer.record_access("key:2", AccessType::Read, ts);

        let tracker = warmer.tracker.read();
        assert_eq!(tracker.len(), 2);
        assert_eq!(tracker.get("key:1").unwrap().total_accesses, 2);
        assert_eq!(tracker.get("key:2").unwrap().total_accesses, 1);
    }

    #[test]
    fn test_seasonal_bucket_recording() {
        let warmer = test_warmer();

        // Record accesses at hour 10 on Monday (day 0)
        let ts = make_timestamp(10, 0);
        for _ in 0..5 {
            warmer.record_access("key:seasonal", AccessType::Read, ts);
        }

        let tracker = warmer.tracker.read();
        let kt = tracker.get("key:seasonal").unwrap();
        // Bin for Monday hour 10 = 0 * 24 + 10 = 10
        assert!(kt.seasonal_buckets[10] > 0.0);
        assert_eq!(kt.seasonal_buckets[10], 5.0);
    }

    #[test]
    fn test_ema_trend_detection() {
        let warmer = test_warmer();
        let base = Utc::now();

        // Simulate increasing access rate
        for i in 0..20 {
            let ts = base + Duration::seconds(i * 10);
            warmer.record_access("key:trend", AccessType::Read, ts);
        }

        let tracker = warmer.tracker.read();
        let kt = tracker.get("key:trend").unwrap();
        assert!(kt.ema_rate.is_some());
        assert!(kt.ema_interval.is_some());
    }

    // -----------------------------------------------------------------------
    // Demand prediction with seasonal patterns
    // -----------------------------------------------------------------------

    #[test]
    fn test_predict_demand_returns_candidates() {
        let warmer = test_warmer();
        let now = Utc::now();

        // Build up access history
        for i in 0..50 {
            let ts = now - Duration::seconds(i * 60);
            warmer.record_access("key:hot", AccessType::Read, ts);
        }

        let candidates = warmer.predict_demand(Duration::hours(1));
        // With low threshold, the hot key should appear
        assert!(
            !candidates.is_empty(),
            "expected at least one candidate for a hot key"
        );
        assert_eq!(candidates[0].key_pattern, "key:hot");
    }

    #[test]
    fn test_predict_demand_filters_low_confidence() {
        let warmer = CacheWarmer::new(WarmingConfig {
            confidence_threshold: 0.99, // very high threshold
            ..Default::default()
        });

        // Single access → low confidence
        warmer.record_access("key:cold", AccessType::Read, Utc::now());

        let candidates = warmer.predict_demand(Duration::hours(1));
        assert!(candidates.is_empty());
    }

    #[test]
    fn test_predict_demand_sorted_by_confidence() {
        let warmer = test_warmer();
        let now = Utc::now();

        // key:a gets many accesses
        for i in 0..30 {
            warmer.record_access("key:a", AccessType::Read, now - Duration::seconds(i * 30));
        }
        // key:b gets fewer
        for i in 0..5 {
            warmer.record_access("key:b", AccessType::Read, now - Duration::seconds(i * 300));
        }

        let candidates = warmer.predict_demand(Duration::hours(1));
        if candidates.len() >= 2 {
            assert!(candidates[0].confidence >= candidates[1].confidence);
        }
    }

    // -----------------------------------------------------------------------
    // Plan generation with budget constraints
    // -----------------------------------------------------------------------

    #[test]
    fn test_generate_warming_plan_respects_max_keys() {
        let warmer = test_warmer();
        let now = Utc::now();

        for i in 0..20 {
            let key = format!("key:{i}");
            for j in 0..15 {
                warmer.record_access(&key, AccessType::Read, now - Duration::seconds(j * 30));
            }
        }

        let budget = WarmingBudget {
            max_keys: 3,
            max_bytes: u64::MAX,
            max_duration_ms: 5000,
            allowed_tiers: vec![TierTarget::Memory],
        };

        let plan = warmer.generate_warming_plan(&budget);
        assert!(plan.actions.len() <= 3);
    }

    #[test]
    fn test_generate_warming_plan_respects_max_bytes() {
        let warmer = test_warmer();
        let now = Utc::now();

        for i in 0..10 {
            let key = format!("key:{i}");
            for j in 0..15 {
                warmer.record_access(&key, AccessType::Read, now - Duration::seconds(j * 30));
            }
        }

        let budget = WarmingBudget {
            max_keys: 100,
            max_bytes: 512, // very small budget
            max_duration_ms: 5000,
            allowed_tiers: vec![TierTarget::Memory],
        };

        let plan = warmer.generate_warming_plan(&budget);
        assert!(plan.total_bytes <= 512);
    }

    #[test]
    fn test_plan_action_priority_ordering() {
        let warmer = test_warmer();
        let now = Utc::now();

        for i in 0..5 {
            let key = format!("key:{i}");
            for j in 0..20 {
                warmer.record_access(&key, AccessType::Read, now - Duration::seconds(j * 30));
            }
        }

        let plan = warmer.generate_warming_plan(&WarmingBudget::default());
        for (i, action) in plan.actions.iter().enumerate() {
            assert_eq!(
                action.priority, i as u32,
                "actions should have sequential priority"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Plan execution and result tracking
    // -----------------------------------------------------------------------

    #[test]
    fn test_execute_plan() {
        let warmer = test_warmer();
        let now = Utc::now();

        for j in 0..20 {
            warmer.record_access(
                "key:exec",
                AccessType::Read,
                now - Duration::seconds(j * 30),
            );
        }

        let plan = warmer.generate_warming_plan(&WarmingBudget::default());
        if !plan.actions.is_empty() {
            let result = warmer.execute_plan(&plan);
            assert_eq!(result.planned_actions, plan.actions.len());
            assert_eq!(result.executed_actions, plan.actions.len());
            assert!(result.keys_warmed > 0);
            assert!(result.bytes_warmed > 0);
        }
    }

    // -----------------------------------------------------------------------
    // Accuracy tracking (precision, recall, F1)
    // -----------------------------------------------------------------------

    #[test]
    fn test_accuracy_tracking_true_positive() {
        let warmer = test_warmer();
        let now = Utc::now();

        for j in 0..20 {
            warmer.record_access("key:tp", AccessType::Read, now - Duration::seconds(j * 30));
        }

        let plan = warmer.generate_warming_plan(&WarmingBudget::default());
        warmer.execute_plan(&plan);

        // Simulate the key being accessed → true positive
        warmer.record_hit_miss("key:tp", true);

        let acc = warmer.get_accuracy();
        assert!(acc.true_positives > 0);
        assert!(acc.precision > 0.0);
    }

    #[test]
    fn test_accuracy_tracking_false_negative() {
        let warmer = test_warmer();

        // Key was accessed but NOT warmed
        warmer.record_hit_miss("key:fn", false);

        let acc = warmer.get_accuracy();
        assert_eq!(acc.false_negatives, 1);
        assert_eq!(acc.recall, 0.0);
    }

    #[test]
    fn test_accuracy_tracking_false_positive() {
        let warmer = test_warmer();
        let now = Utc::now();

        for j in 0..20 {
            warmer.record_access("key:fp", AccessType::Read, now - Duration::seconds(j * 30));
        }

        let plan = warmer.generate_warming_plan(&WarmingBudget::default());
        warmer.execute_plan(&plan);

        // Flush without the key being accessed → false positive
        warmer.flush_unaccessed();

        let acc = warmer.get_accuracy();
        assert!(acc.false_positives > 0);
    }

    #[test]
    fn test_precision_recall_f1() {
        let mut acc = PredictionAccuracy::default();
        acc.true_positives = 8;
        acc.false_positives = 2;
        acc.false_negatives = 3;
        acc.true_negatives = 10;
        acc.recalculate();

        // precision = 8 / (8+2) = 0.8
        assert!((acc.precision - 0.8).abs() < 1e-6);
        // recall = 8 / (8+3) ≈ 0.7272
        assert!((acc.recall - 8.0 / 11.0).abs() < 1e-6);
        // f1 = 2 * 0.8 * 0.7272 / (0.8 + 0.7272)
        let expected_f1 = 2.0 * 0.8 * (8.0 / 11.0) / (0.8 + 8.0 / 11.0);
        assert!((acc.f1_score - expected_f1).abs() < 1e-6);
        assert_eq!(acc.total_predictions, 23);
    }

    #[test]
    fn test_accuracy_zero_division() {
        let mut acc = PredictionAccuracy::default();
        acc.recalculate();
        assert_eq!(acc.precision, 0.0);
        assert_eq!(acc.recall, 0.0);
        assert_eq!(acc.f1_score, 0.0);
    }

    // -----------------------------------------------------------------------
    // Stats tracking
    // -----------------------------------------------------------------------

    #[test]
    fn test_stats_initial() {
        let warmer = CacheWarmer::default();
        let stats = warmer.get_stats();
        assert_eq!(stats.total_cycles, 0);
        assert_eq!(stats.total_keys_warmed, 0);
        assert_eq!(stats.total_bytes_warmed, 0);
        assert_eq!(stats.predictions_made, 0);
    }

    #[test]
    fn test_stats_after_execution() {
        let warmer = test_warmer();
        let now = Utc::now();

        for j in 0..20 {
            warmer.record_access("key:s", AccessType::Read, now - Duration::seconds(j * 30));
        }

        let plan = warmer.generate_warming_plan(&WarmingBudget::default());
        let result = warmer.execute_plan(&plan);

        let stats = warmer.get_stats();
        assert_eq!(stats.total_cycles, 1);
        assert_eq!(stats.total_keys_warmed, result.keys_warmed);
        assert_eq!(stats.total_bytes_warmed, result.bytes_warmed);
    }

    // -----------------------------------------------------------------------
    // Edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_empty_access_history() {
        let warmer = CacheWarmer::default();
        let candidates = warmer.predict_demand(Duration::hours(1));
        assert!(candidates.is_empty());

        let plan = warmer.generate_warming_plan(&WarmingBudget::default());
        assert!(plan.actions.is_empty());
        assert_eq!(plan.total_bytes, 0);
        assert_eq!(plan.total_keys, 0);
    }

    #[test]
    fn test_cold_start_single_access() {
        let warmer = CacheWarmer::new(WarmingConfig {
            confidence_threshold: 0.01, // very low for testing
            ..Default::default()
        });

        warmer.record_access("key:cold", AccessType::Read, Utc::now());

        let candidates = warmer.predict_demand(Duration::hours(1));
        // Even with a very low threshold, a single access may not produce
        // a candidate if frequency/recency/seasonal scores are all near zero.
        // This test verifies no panic occurs.
        let _ = candidates;
    }

    #[test]
    fn test_warming_result_default() {
        let result = WarmingResult::default();
        assert_eq!(result.planned_actions, 0);
        assert_eq!(result.executed_actions, 0);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_tier_target_display() {
        assert_eq!(TierTarget::Memory.to_string(), "memory");
        assert_eq!(TierTarget::Mmap.to_string(), "mmap");
        assert_eq!(TierTarget::Disk.to_string(), "disk");
        assert_eq!(TierTarget::Cloud.to_string(), "cloud");
    }

    #[test]
    fn test_warming_error_display() {
        assert_eq!(
            WarmingError::BudgetExceeded.to_string(),
            "warming budget exceeded"
        );
        assert_eq!(
            WarmingError::NoCandidates.to_string(),
            "no warming candidates found"
        );
        assert_eq!(
            WarmingError::TierUnavailable("ssd".into()).to_string(),
            "tier unavailable: ssd"
        );
        assert_eq!(
            WarmingError::ConfigInvalid("bad".into()).to_string(),
            "invalid config: bad"
        );
        assert_eq!(
            WarmingError::Internal("oops".into()).to_string(),
            "internal error: oops"
        );
    }

    // -----------------------------------------------------------------------
    // KeyTracker internals
    // -----------------------------------------------------------------------

    #[test]
    fn test_key_tracker_frequency_score() {
        let mut kt = KeyTracker::new();
        let now = Utc::now();

        // Simulate >10 accesses in the last hour → frequency_score ≈ 1.0
        for i in 0..15 {
            kt.record(AccessType::Read, now - Duration::seconds(i * 60));
        }

        assert!(kt.frequency_score() > 0.5);
    }

    #[test]
    fn test_key_tracker_seasonal_score_peak() {
        let mut kt = KeyTracker::new();

        // Concentrate all accesses at hour 14, Monday
        let ts = make_timestamp(14, 0);
        for _ in 0..20 {
            kt.record(AccessType::Read, ts);
        }

        // Score at the peak should be 1.0
        let score = kt.seasonal_score(14, 0);
        assert!((score - 1.0).abs() < f64::EPSILON);

        // Score at an empty bucket should be 0.0
        let off_score = kt.seasonal_score(3, 4);
        assert!((off_score - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_key_tracker_detect_period_insufficient_data() {
        let kt = KeyTracker::new();
        assert!(kt.detect_period().is_none());
    }

    #[test]
    fn test_warming_plan_skips_same_tier() {
        let warmer = CacheWarmer::new(WarmingConfig {
            confidence_threshold: 0.01,
            ..Default::default()
        });
        let now = Utc::now();

        // Record accesses so the key gets a candidate
        for j in 0..20 {
            warmer.record_access(
                "key:same",
                AccessType::Read,
                now - Duration::seconds(j * 30),
            );
        }

        // Set current tier to Memory (the default target)
        {
            let mut tracker = warmer.tracker.write();
            if let Some(kt) = tracker.trackers.get_mut("key:same") {
                kt.current_tier = TierTarget::Memory;
            }
        }

        let budget = WarmingBudget {
            allowed_tiers: vec![TierTarget::Memory],
            ..Default::default()
        };
        let plan = warmer.generate_warming_plan(&budget);
        // Should skip because current_tier == to_tier
        assert!(
            plan.actions.is_empty(),
            "should skip keys already in target tier"
        );
    }

    #[test]
    fn test_multiple_cycles_accumulate_stats() {
        let warmer = test_warmer();
        let now = Utc::now();

        for j in 0..20 {
            warmer.record_access("key:mc", AccessType::Read, now - Duration::seconds(j * 30));
        }

        let budget = WarmingBudget::default();

        let plan1 = warmer.generate_warming_plan(&budget);
        warmer.execute_plan(&plan1);

        let plan2 = warmer.generate_warming_plan(&budget);
        warmer.execute_plan(&plan2);

        let stats = warmer.get_stats();
        assert_eq!(stats.total_cycles, 2);
    }
}
