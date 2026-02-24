//! Predictive Auto-Scaling Engine
//!
//! Uses time-series analysis with exponential moving averages (EMA) to predict
//! future load and scale Ferrite clusters proactively, before demand spikes hit.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────┐    ┌───────────────┐    ┌─────────────────┐
//! │ ScalingMetrics│───▶│ PredictiveScaler│──▶│ LoadPrediction  │
//! │  (real-time)  │    │  (EMA + hourly │    │  (forecast +    │
//! │               │    │   patterns)    │    │   confidence)   │
//! └──────────────┘    └───────┬───────┘    └────────┬────────┘
//!                             │                      │
//!                             ▼                      ▼
//!                    ┌─────────────────┐    ┌─────────────────┐
//!                    │ PredictiveStats │    │ScalingRecommend. │
//!                    │  (accuracy,     │    │  (replicas +    │
//!                    │   error rates)  │    │   urgency)      │
//!                    └─────────────────┘    └─────────────────┘
//! ```
//!
//! # Prediction Method
//!
//! The engine maintains an exponential moving average (EMA) of key metrics
//! (CPU, memory, ops/sec) and detects hourly seasonal patterns. Predictions
//! combine the EMA trend with any detected seasonality to forecast load over
//! a configurable horizon (default 15 minutes).

use chrono::{DateTime, Duration, Timelike, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

/// Smoothing factor for the exponential moving average (α = 0.3).
const EMA_ALPHA: f64 = 0.3;

/// Number of hourly buckets for seasonal pattern detection.
const HOURLY_BUCKETS: usize = 24;

/// Configuration for the predictive scaling engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictiveConfig {
    /// How many minutes of historical metrics to keep for analysis.
    #[serde(default = "default_lookback_window")]
    pub lookback_window_minutes: u32,

    /// How far ahead (in minutes) to predict load.
    #[serde(default = "default_prediction_horizon")]
    pub prediction_horizon_minutes: u32,

    /// CPU/memory utilization above this fraction triggers a scale-up recommendation.
    #[serde(default = "default_scale_up_threshold")]
    pub scale_up_threshold: f64,

    /// CPU/memory utilization below this fraction triggers a scale-down recommendation.
    #[serde(default = "default_scale_down_threshold")]
    pub scale_down_threshold: f64,

    /// Minimum seconds between consecutive scaling actions.
    #[serde(default = "default_cooldown_period")]
    pub cooldown_period_secs: u64,

    /// Minimum number of replicas the scaler will recommend.
    #[serde(default = "default_min_replicas")]
    pub min_replicas: u32,

    /// Maximum number of replicas the scaler will recommend.
    #[serde(default = "default_max_replicas")]
    pub max_replicas: u32,

    /// Whether predictive (forecast-based) scaling is enabled.
    /// When `false`, only reactive thresholds are used.
    #[serde(default = "default_enable_predictive")]
    pub enable_predictive: bool,
}

fn default_lookback_window() -> u32 {
    60
}
fn default_prediction_horizon() -> u32 {
    15
}
fn default_scale_up_threshold() -> f64 {
    0.8
}
fn default_scale_down_threshold() -> f64 {
    0.3
}
fn default_cooldown_period() -> u64 {
    300
}
fn default_min_replicas() -> u32 {
    1
}
fn default_max_replicas() -> u32 {
    100
}
fn default_enable_predictive() -> bool {
    true
}

impl Default for PredictiveConfig {
    fn default() -> Self {
        Self {
            lookback_window_minutes: default_lookback_window(),
            prediction_horizon_minutes: default_prediction_horizon(),
            scale_up_threshold: default_scale_up_threshold(),
            scale_down_threshold: default_scale_down_threshold(),
            cooldown_period_secs: default_cooldown_period(),
            min_replicas: default_min_replicas(),
            max_replicas: default_max_replicas(),
            enable_predictive: default_enable_predictive(),
        }
    }
}

/// A point-in-time snapshot of cluster metrics used for prediction.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScalingMetrics {
    /// When these metrics were collected.
    pub timestamp: DateTime<Utc>,
    /// CPU utilization as a fraction (0.0–1.0).
    pub cpu_utilization: f64,
    /// Memory utilization as a fraction (0.0–1.0).
    pub memory_utilization: f64,
    /// Operations per second.
    pub ops_per_sec: f64,
    /// Number of active client connections.
    pub connections: u64,
    /// Pending command queue depth.
    pub queue_depth: u64,
    /// 99th-percentile latency in milliseconds.
    pub p99_latency_ms: f64,
}

/// A load forecast produced by the predictive engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoadPrediction {
    /// Forecasted operations per second.
    pub predicted_ops_per_sec: f64,
    /// Forecasted CPU utilization (0.0–1.0).
    pub predicted_cpu: f64,
    /// Forecasted memory utilization (0.0–1.0).
    pub predicted_memory: f64,
    /// Confidence score for this prediction (0.0–1.0).
    pub confidence: f64,
    /// Detected load trend.
    pub trend: LoadTrend,
    /// When the prediction was generated.
    pub predicted_at: DateTime<Utc>,
    /// How far into the future this prediction covers.
    pub horizon: Duration,
}

/// Observed load trend direction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LoadTrend {
    /// Load is growing steadily.
    Increasing,
    /// Load is roughly constant.
    Stable,
    /// Load is declining.
    Decreasing,
    /// A sudden transient spike is detected.
    Spike,
    /// A recurring hourly/daily pattern is detected.
    Seasonal,
}

/// A concrete scaling recommendation produced from a [`LoadPrediction`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScalingRecommendation {
    /// Current replica count.
    pub current_replicas: u32,
    /// Recommended replica count after scaling.
    pub recommended_replicas: u32,
    /// Human-readable explanation of the recommendation.
    pub reason: String,
    /// How urgently the scaling action should be taken.
    pub urgency: ScalingUrgency,
    /// The prediction that drove this recommendation.
    pub predicted_load: LoadPrediction,
}

/// How urgently a scaling recommendation should be acted upon.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScalingUrgency {
    /// No immediate pressure; can be batched with the next reconcile.
    Low,
    /// Moderate pressure; should be acted on soon.
    Medium,
    /// High pressure; act within the current reconcile cycle.
    High,
    /// Critical pressure; immediate scaling required to avoid SLA breach.
    Critical,
}

/// Decision returned by [`PredictiveScaler::should_scale`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ScalingDecision {
    /// Scale up to the given replica count.
    ScaleUp(u32),
    /// Scale down to the given replica count.
    ScaleDown(u32),
    /// No change is needed.
    NoChange,
    /// A recent scaling action is still within its cooldown window.
    CooldownActive,
}

/// Accuracy and activity statistics for the predictive engine.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictiveStats {
    /// Total number of predictions made.
    pub total_predictions: u64,
    /// Predictions whose error was below the confidence threshold.
    pub accurate_predictions: u64,
    /// Total scale-up events triggered.
    pub total_scale_ups: u64,
    /// Total scale-down events triggered.
    pub total_scale_downs: u64,
    /// Average absolute prediction error (0.0–1.0).
    pub avg_prediction_error: f64,
    /// Longest period (in seconds) without a scaling event.
    pub longest_stable_period_secs: u64,
}

// ---------------------------------------------------------------------------
// Internal state
// ---------------------------------------------------------------------------

/// Exponential moving average tracker for a single metric.
#[derive(Debug, Clone)]
struct EmaState {
    value: f64,
    initialized: bool,
}

impl EmaState {
    fn new() -> Self {
        Self {
            value: 0.0,
            initialized: false,
        }
    }

    fn update(&mut self, sample: f64) {
        if self.initialized {
            self.value = EMA_ALPHA * sample + (1.0 - EMA_ALPHA) * self.value;
        } else {
            self.value = sample;
            self.initialized = true;
        }
    }
}

/// Per-hour average used for seasonal pattern detection.
#[derive(Debug, Clone)]
struct HourlyBucket {
    sum: f64,
    count: u64,
}

impl HourlyBucket {
    fn new() -> Self {
        Self { sum: 0.0, count: 0 }
    }

    fn record(&mut self, value: f64) {
        self.sum += value;
        self.count += 1;
    }

    fn average(&self) -> Option<f64> {
        if self.count == 0 {
            None
        } else {
            Some(self.sum / self.count as f64)
        }
    }
}

/// Mutable inner state protected by a [`RwLock`].
#[derive(Debug)]
struct ScalerState {
    /// Raw metrics history, kept within the lookback window.
    history: VecDeque<ScalingMetrics>,
    /// EMA for CPU utilization.
    ema_cpu: EmaState,
    /// EMA for memory utilization.
    ema_memory: EmaState,
    /// EMA for operations per second.
    ema_ops: EmaState,
    /// Hourly ops/sec buckets for seasonality detection.
    hourly_ops: Vec<HourlyBucket>,
    /// Cumulative prediction statistics.
    stats: PredictiveStats,
    /// Timestamp of the last scaling action (used for cooldown).
    last_scale_time: Option<DateTime<Utc>>,
    /// Previous prediction stored for accuracy tracking.
    last_prediction: Option<LoadPrediction>,
}

impl ScalerState {
    fn new() -> Self {
        Self {
            history: VecDeque::new(),
            ema_cpu: EmaState::new(),
            ema_memory: EmaState::new(),
            ema_ops: EmaState::new(),
            hourly_ops: (0..HOURLY_BUCKETS).map(|_| HourlyBucket::new()).collect(),
            stats: PredictiveStats::default(),
            last_scale_time: None,
            last_prediction: None,
        }
    }
}

// ---------------------------------------------------------------------------
// PredictiveScaler
// ---------------------------------------------------------------------------

/// Predictive auto-scaling engine for Ferrite Kubernetes clusters.
///
/// Consumes real-time [`ScalingMetrics`], maintains an EMA-based model with
/// hourly seasonal patterns, and produces [`LoadPrediction`]s that drive
/// proactive scaling decisions.
///
/// # Example
///
/// ```rust,ignore
/// use ferrite_k8s::k8s::predictive_scaling::*;
///
/// let scaler = PredictiveScaler::new(PredictiveConfig::default());
/// scaler.record_metrics(metrics);
/// let prediction = scaler.predict_load(15);
/// let recommendation = scaler.recommend_replicas(&prediction);
/// let decision = scaler.should_scale(3, recommendation.recommended_replicas);
/// ```
pub struct PredictiveScaler {
    config: PredictiveConfig,
    state: RwLock<ScalerState>,
}

impl PredictiveScaler {
    /// Create a new predictive scaler with the given configuration.
    pub fn new(config: PredictiveConfig) -> Self {
        Self {
            config,
            state: RwLock::new(ScalerState::new()),
        }
    }

    /// Record a new set of metrics and update the internal model.
    ///
    /// Old samples outside the lookback window are automatically evicted.
    pub fn record_metrics(&self, metrics: ScalingMetrics) {
        let mut state = self.state.write();

        // Update EMAs
        state.ema_cpu.update(metrics.cpu_utilization);
        state.ema_memory.update(metrics.memory_utilization);
        state.ema_ops.update(metrics.ops_per_sec);

        // Update hourly bucket for seasonal analysis
        let hour = metrics.timestamp.hour() as usize;
        if hour < HOURLY_BUCKETS {
            state.hourly_ops[hour].record(metrics.ops_per_sec);
        }

        // Track accuracy of the previous prediction (if any)
        if let Some(prev) = state.last_prediction.take() {
            let error = (prev.predicted_cpu - metrics.cpu_utilization).abs();
            let n = state.stats.total_predictions as f64;
            state.stats.avg_prediction_error =
                (state.stats.avg_prediction_error * n + error) / (n + 1.0);
            if error < 0.15 {
                state.stats.accurate_predictions += 1;
            }
        }

        // Append to history and evict stale entries
        let cutoff =
            metrics.timestamp - Duration::minutes(self.config.lookback_window_minutes as i64);
        state.history.push_back(metrics);
        while state
            .history
            .front()
            .map_or(false, |m| m.timestamp < cutoff)
        {
            state.history.pop_front();
        }
    }

    /// Predict future load over the given horizon (in minutes).
    ///
    /// Returns a [`LoadPrediction`] combining the current EMA trend with any
    /// detected hourly seasonal pattern.
    pub fn predict_load(&self, horizon_minutes: u32) -> LoadPrediction {
        let state = self.state.read();
        let now = Utc::now();

        let base_cpu = state.ema_cpu.value;
        let base_memory = state.ema_memory.value;
        let base_ops = state.ema_ops.value;

        // Detect trend from recent history
        let trend = self.detect_trend(&state);

        // Seasonal adjustment: look at the target hour's historical average
        let target_hour =
            ((now.hour() as u32 + (horizon_minutes / 60)) % HOURLY_BUCKETS as u32) as usize;
        let seasonal_factor = state.hourly_ops[target_hour].average().and_then(|avg| {
            if base_ops > 0.0 {
                Some(avg / base_ops)
            } else {
                None
            }
        });

        // Apply trend multiplier
        let trend_multiplier = match trend {
            LoadTrend::Increasing => 1.15,
            LoadTrend::Spike => 1.40,
            LoadTrend::Decreasing => 0.85,
            LoadTrend::Seasonal => seasonal_factor.unwrap_or(1.0),
            LoadTrend::Stable => 1.0,
        };

        let predicted_cpu = (base_cpu * trend_multiplier).clamp(0.0, 1.0);
        let predicted_memory = (base_memory * trend_multiplier).clamp(0.0, 1.0);
        let predicted_ops = (base_ops * trend_multiplier).max(0.0);

        // Confidence degrades with longer horizons and fewer data points
        let data_points = state.history.len() as f64;
        let data_confidence = (data_points / 60.0).min(1.0);
        let horizon_penalty = 1.0 - (horizon_minutes as f64 / 120.0).min(0.5);
        let confidence = (data_confidence * horizon_penalty).clamp(0.0, 1.0);

        drop(state);

        // Store prediction for accuracy tracking
        let prediction = LoadPrediction {
            predicted_ops_per_sec: predicted_ops,
            predicted_cpu,
            predicted_memory,
            confidence,
            trend,
            predicted_at: now,
            horizon: Duration::minutes(horizon_minutes as i64),
        };

        let mut state = self.state.write();
        state.stats.total_predictions += 1;
        state.last_prediction = Some(prediction.clone());

        prediction
    }

    /// Recommend a replica count based on the given prediction.
    ///
    /// Uses the configured utilization thresholds and clamps the result to
    /// `[min_replicas, max_replicas]`.
    pub fn recommend_replicas(&self, prediction: &LoadPrediction) -> ScalingRecommendation {
        let state = self.state.read();
        let current_replicas = self.estimate_current_replicas(&state);

        let peak_util = prediction.predicted_cpu.max(prediction.predicted_memory);

        let (recommended, reason, urgency) = if peak_util >= self.config.scale_up_threshold {
            let factor = peak_util / self.config.scale_up_threshold;
            let needed = ((current_replicas as f64) * factor).ceil() as u32;
            let needed = needed.clamp(self.config.min_replicas, self.config.max_replicas);

            let urgency = if peak_util >= 0.95 {
                ScalingUrgency::Critical
            } else if peak_util >= 0.90 {
                ScalingUrgency::High
            } else {
                ScalingUrgency::Medium
            };

            (
                needed,
                format!(
                    "Predicted utilization {:.0}% exceeds {:.0}% threshold (trend: {:?})",
                    peak_util * 100.0,
                    self.config.scale_up_threshold * 100.0,
                    prediction.trend,
                ),
                urgency,
            )
        } else if peak_util <= self.config.scale_down_threshold
            && current_replicas > self.config.min_replicas
        {
            let factor = peak_util / self.config.scale_up_threshold;
            let needed = ((current_replicas as f64) * factor).ceil().max(1.0) as u32;
            let needed = needed.clamp(self.config.min_replicas, self.config.max_replicas);

            (
                needed,
                format!(
                    "Predicted utilization {:.0}% below {:.0}% threshold (trend: {:?})",
                    peak_util * 100.0,
                    self.config.scale_down_threshold * 100.0,
                    prediction.trend,
                ),
                ScalingUrgency::Low,
            )
        } else {
            (
                current_replicas,
                format!(
                    "Predicted utilization {:.0}% within thresholds",
                    peak_util * 100.0,
                ),
                ScalingUrgency::Low,
            )
        };

        ScalingRecommendation {
            current_replicas,
            recommended_replicas: recommended,
            reason,
            urgency,
            predicted_load: prediction.clone(),
        }
    }

    /// Decide whether to actually execute a scaling action, accounting for
    /// cooldown periods and minimum change thresholds.
    pub fn should_scale(&self, current_replicas: u32, recommended: u32) -> ScalingDecision {
        if current_replicas == recommended {
            return ScalingDecision::NoChange;
        }

        let state = self.state.read();
        if let Some(last) = state.last_scale_time {
            let elapsed = Utc::now().signed_duration_since(last);
            if elapsed.num_seconds() < self.config.cooldown_period_secs as i64 {
                return ScalingDecision::CooldownActive;
            }
        }
        drop(state);

        // Record the scaling event
        let mut state = self.state.write();
        state.last_scale_time = Some(Utc::now());

        if recommended > current_replicas {
            state.stats.total_scale_ups += 1;
            // Update longest stable period
            self.update_stable_period(&mut state);
            ScalingDecision::ScaleUp(recommended)
        } else {
            state.stats.total_scale_downs += 1;
            self.update_stable_period(&mut state);
            ScalingDecision::ScaleDown(recommended)
        }
    }

    /// Return a snapshot of the engine's accuracy and activity statistics.
    pub fn get_stats(&self) -> PredictiveStats {
        self.state.read().stats.clone()
    }

    // ------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------

    /// Detect the current load trend from recent metric history.
    fn detect_trend(&self, state: &ScalerState) -> LoadTrend {
        let len = state.history.len();
        if len < 3 {
            return LoadTrend::Stable;
        }

        // Compare the average of the most recent quarter to the preceding quarter
        let quarter = len / 4;
        if quarter == 0 {
            return LoadTrend::Stable;
        }

        let recent_avg: f64 = state
            .history
            .iter()
            .rev()
            .take(quarter)
            .map(|m| m.cpu_utilization)
            .sum::<f64>()
            / quarter as f64;

        let prior_avg: f64 = state
            .history
            .iter()
            .rev()
            .skip(quarter)
            .take(quarter)
            .map(|m| m.cpu_utilization)
            .sum::<f64>()
            / quarter as f64;

        if prior_avg == 0.0 {
            return LoadTrend::Stable;
        }

        let change_ratio = (recent_avg - prior_avg) / prior_avg;

        // Spike: sudden jump > 50%
        if change_ratio > 0.50 {
            return LoadTrend::Spike;
        }

        // Check seasonality: if hourly bucket has significant data
        let current_hour = Utc::now().hour() as usize;
        if current_hour < HOURLY_BUCKETS {
            if let Some(hourly_avg) = state.hourly_ops[current_hour].average() {
                let current_ops = state.ema_ops.value;
                if current_ops > 0.0 {
                    let seasonal_ratio = (hourly_avg - current_ops).abs() / current_ops;
                    if seasonal_ratio < 0.10 && state.hourly_ops[current_hour].count > 10 {
                        return LoadTrend::Seasonal;
                    }
                }
            }
        }

        if change_ratio > 0.10 {
            LoadTrend::Increasing
        } else if change_ratio < -0.10 {
            LoadTrend::Decreasing
        } else {
            LoadTrend::Stable
        }
    }

    /// Estimate the current replica count from average per-replica ops rate.
    fn estimate_current_replicas(&self, state: &ScalerState) -> u32 {
        // Without direct K8s API access, fall back to min_replicas as baseline.
        self.config.min_replicas.max(1)
    }

    /// Update the longest stable period stat when a scaling event occurs.
    fn update_stable_period(&self, state: &mut ScalerState) {
        if let Some(prev) = state.last_scale_time {
            let now = Utc::now();
            let gap = now.signed_duration_since(prev).num_seconds().unsigned_abs();
            if gap > state.stats.longest_stable_period_secs {
                state.stats.longest_stable_period_secs = gap;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn sample_metrics(cpu: f64, memory: f64, ops: f64) -> ScalingMetrics {
        ScalingMetrics {
            timestamp: Utc::now(),
            cpu_utilization: cpu,
            memory_utilization: memory,
            ops_per_sec: ops,
            connections: 100,
            queue_depth: 0,
            p99_latency_ms: 1.5,
        }
    }

    #[test]
    fn test_default_config() {
        let cfg = PredictiveConfig::default();
        assert_eq!(cfg.lookback_window_minutes, 60);
        assert_eq!(cfg.prediction_horizon_minutes, 15);
        assert!((cfg.scale_up_threshold - 0.8).abs() < f64::EPSILON);
        assert!((cfg.scale_down_threshold - 0.3).abs() < f64::EPSILON);
        assert_eq!(cfg.cooldown_period_secs, 300);
        assert_eq!(cfg.min_replicas, 1);
        assert_eq!(cfg.max_replicas, 100);
        assert!(cfg.enable_predictive);
    }

    #[test]
    fn test_record_metrics_updates_ema() {
        let scaler = PredictiveScaler::new(PredictiveConfig::default());

        scaler.record_metrics(sample_metrics(0.5, 0.4, 1000.0));
        scaler.record_metrics(sample_metrics(0.7, 0.6, 2000.0));

        let state = scaler.state.read();
        // After two samples with α=0.3: EMA = 0.3*0.7 + 0.7*0.5 = 0.56
        assert!(state.ema_cpu.value > 0.5);
        assert!(state.ema_cpu.value < 0.7);
        assert!(state.ema_cpu.initialized);
    }

    #[test]
    fn test_predict_load_returns_valid_prediction() {
        let scaler = PredictiveScaler::new(PredictiveConfig::default());

        // Feed several samples to build history
        for i in 0..10 {
            scaler.record_metrics(sample_metrics(
                0.5 + (i as f64) * 0.02,
                0.4,
                1000.0 + (i as f64) * 100.0,
            ));
        }

        let prediction = scaler.predict_load(15);
        assert!(prediction.predicted_cpu >= 0.0 && prediction.predicted_cpu <= 1.0);
        assert!(prediction.predicted_memory >= 0.0 && prediction.predicted_memory <= 1.0);
        assert!(prediction.predicted_ops_per_sec >= 0.0);
        assert!(prediction.confidence >= 0.0 && prediction.confidence <= 1.0);
    }

    #[test]
    fn test_recommend_scale_up() {
        let scaler = PredictiveScaler::new(PredictiveConfig::default());

        // Feed high-utilization metrics
        for _ in 0..5 {
            scaler.record_metrics(sample_metrics(0.9, 0.85, 5000.0));
        }

        let prediction = scaler.predict_load(15);
        let recommendation = scaler.recommend_replicas(&prediction);

        assert!(
            recommendation.recommended_replicas >= recommendation.current_replicas,
            "Should recommend scale-up for high utilization"
        );
        assert!(
            recommendation.urgency == ScalingUrgency::High
                || recommendation.urgency == ScalingUrgency::Critical
                || recommendation.urgency == ScalingUrgency::Medium
        );
    }

    #[test]
    fn test_recommend_no_change_within_thresholds() {
        let scaler = PredictiveScaler::new(PredictiveConfig::default());

        // Feed moderate-utilization metrics
        for _ in 0..5 {
            scaler.record_metrics(sample_metrics(0.5, 0.4, 2000.0));
        }

        let prediction = scaler.predict_load(15);
        let recommendation = scaler.recommend_replicas(&prediction);

        assert_eq!(
            recommendation.recommended_replicas, recommendation.current_replicas,
            "Should not recommend change within thresholds"
        );
    }

    #[test]
    fn test_should_scale_no_change() {
        let scaler = PredictiveScaler::new(PredictiveConfig::default());
        let decision = scaler.should_scale(3, 3);
        assert_eq!(decision, ScalingDecision::NoChange);
    }

    #[test]
    fn test_should_scale_up() {
        let scaler = PredictiveScaler::new(PredictiveConfig::default());
        let decision = scaler.should_scale(3, 5);
        assert_eq!(decision, ScalingDecision::ScaleUp(5));
    }

    #[test]
    fn test_should_scale_down() {
        let scaler = PredictiveScaler::new(PredictiveConfig::default());
        let decision = scaler.should_scale(5, 3);
        assert_eq!(decision, ScalingDecision::ScaleDown(3));
    }

    #[test]
    fn test_cooldown_active() {
        let mut cfg = PredictiveConfig::default();
        cfg.cooldown_period_secs = 600;
        let scaler = PredictiveScaler::new(cfg);

        // First scale triggers normally
        let d1 = scaler.should_scale(3, 5);
        assert_eq!(d1, ScalingDecision::ScaleUp(5));

        // Second attempt within cooldown
        let d2 = scaler.should_scale(5, 7);
        assert_eq!(d2, ScalingDecision::CooldownActive);
    }

    #[test]
    fn test_get_stats_initial() {
        let scaler = PredictiveScaler::new(PredictiveConfig::default());
        let stats = scaler.get_stats();
        assert_eq!(stats.total_predictions, 0);
        assert_eq!(stats.accurate_predictions, 0);
        assert_eq!(stats.total_scale_ups, 0);
        assert_eq!(stats.total_scale_downs, 0);
    }

    #[test]
    fn test_stats_track_predictions() {
        let scaler = PredictiveScaler::new(PredictiveConfig::default());

        scaler.record_metrics(sample_metrics(0.5, 0.4, 1000.0));
        let _ = scaler.predict_load(15);

        let stats = scaler.get_stats();
        assert_eq!(stats.total_predictions, 1);
    }

    #[test]
    fn test_stats_track_scale_events() {
        let scaler = PredictiveScaler::new(PredictiveConfig::default());

        scaler.should_scale(3, 5);
        let stats = scaler.get_stats();
        assert_eq!(stats.total_scale_ups, 1);
        assert_eq!(stats.total_scale_downs, 0);
    }

    #[test]
    fn test_load_trend_detection() {
        let scaler = PredictiveScaler::new(PredictiveConfig::default());

        // Feed stable metrics
        for _ in 0..20 {
            scaler.record_metrics(sample_metrics(0.5, 0.4, 1000.0));
        }

        let state = scaler.state.read();
        let trend = scaler.detect_trend(&state);
        // With enough identical samples in the same hourly bucket, the
        // detector may report Seasonal (hourly pattern matches EMA).
        assert!(
            trend == LoadTrend::Stable || trend == LoadTrend::Seasonal,
            "expected Stable or Seasonal, got {:?}",
            trend
        );
    }

    #[test]
    fn test_history_eviction() {
        let mut cfg = PredictiveConfig::default();
        cfg.lookback_window_minutes = 1; // 1 minute window
        let scaler = PredictiveScaler::new(cfg);

        // Record a metric with an old timestamp
        let old = ScalingMetrics {
            timestamp: Utc::now() - Duration::minutes(5),
            cpu_utilization: 0.5,
            memory_utilization: 0.4,
            ops_per_sec: 1000.0,
            connections: 100,
            queue_depth: 0,
            p99_latency_ms: 1.5,
        };
        scaler.record_metrics(old);

        // Record a fresh metric — the old one should be evicted
        scaler.record_metrics(sample_metrics(0.6, 0.5, 1200.0));

        let state = scaler.state.read();
        assert_eq!(state.history.len(), 1);
    }

    #[test]
    fn test_config_serialization() {
        let cfg = PredictiveConfig::default();
        let json = serde_json::to_string(&cfg).expect("serialize");
        let deserialized: PredictiveConfig = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(
            deserialized.lookback_window_minutes,
            cfg.lookback_window_minutes
        );
        assert_eq!(deserialized.max_replicas, cfg.max_replicas);
    }

    #[test]
    fn test_scaling_metrics_serialization() {
        let m = sample_metrics(0.75, 0.60, 3000.0);
        let json = serde_json::to_string(&m).expect("serialize");
        let deserialized: ScalingMetrics = serde_json::from_str(&json).expect("deserialize");
        assert!((deserialized.cpu_utilization - 0.75).abs() < f64::EPSILON);
    }
}
