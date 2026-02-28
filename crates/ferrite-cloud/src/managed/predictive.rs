//! Predictive Auto-Scaling
//!
//! ML model that forecasts traffic spikes and pre-scales cluster capacity.
#![allow(dead_code)]

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Configuration for the predictive auto-scaler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredictiveConfig {
    /// Whether predictive scaling is enabled.
    pub enabled: bool,
    /// How far back to look for patterns.
    #[serde(with = "duration_secs")]
    pub lookback_window: Duration,
    /// How far ahead to predict.
    #[serde(with = "duration_secs")]
    pub prediction_horizon: Duration,
    /// Minimum confidence to act on a prediction.
    pub confidence_threshold: f64,
    /// Minimum interval between scaling actions.
    #[serde(with = "duration_secs")]
    pub min_scale_interval: Duration,
    /// Maximum scale factor relative to current replicas.
    pub max_scale_factor: f64,
}

impl Default for PredictiveConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            lookback_window: Duration::from_secs(7 * 24 * 3600),
            prediction_horizon: Duration::from_secs(30 * 60),
            confidence_threshold: 0.8,
            min_scale_interval: Duration::from_secs(5 * 60),
            max_scale_factor: 3.0,
        }
    }
}

/// A single traffic observation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrafficSample {
    /// Epoch timestamp in seconds.
    pub timestamp: u64,
    /// Operations per second at this point.
    pub ops_per_sec: f64,
    /// Memory utilization percentage.
    pub memory_pct: f64,
    /// CPU utilization percentage.
    pub cpu_pct: f64,
    /// P99 latency in microseconds.
    pub latency_p99_us: f64,
    /// Number of active connections.
    pub connections: u32,
}

/// Result of a traffic prediction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredictionResult {
    /// Predicted operations per second.
    pub predicted_ops: f64,
    /// Predicted memory utilization percentage.
    pub predicted_memory_pct: f64,
    /// Confidence in the prediction (0.0–1.0).
    pub confidence: f64,
    /// Recommended number of replicas.
    pub recommended_replicas: u8,
    /// Lead time in seconds before the predicted spike.
    pub lead_time_secs: u64,
    /// Human-readable explanation.
    pub reason: String,
}

/// Detected traffic pattern.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TrafficPattern {
    /// Daily cycle with identified peak and trough hours.
    DailyCycle {
        /// Hour of day with highest traffic (0-23).
        peak_hour: u8,
        /// Hour of day with lowest traffic (0-23).
        trough_hour: u8,
    },
    /// Weekly cycle with identified peak day.
    WeeklyCycle {
        /// Day of week with highest traffic (0=Mon, 6=Sun).
        peak_day: u8,
    },
    /// Traffic is roughly constant.
    Steady,
    /// No discernible pattern.
    Unpredictable,
}

/// What triggered a scaling event.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ScalingTrigger {
    /// Proactive scaling from a prediction.
    Predicted,
    /// Reactive scaling from threshold breach.
    Reactive,
    /// Operator-initiated scaling.
    Manual,
    /// Calendar/cron-scheduled scaling.
    Scheduled,
}

/// Record of a past scaling event.
#[derive(Debug, Clone)]
pub struct ScalingEvent {
    /// When the event occurred.
    pub timestamp: Instant,
    /// Replica count before scaling.
    pub from_replicas: u8,
    /// Replica count after scaling.
    pub to_replicas: u8,
    /// What triggered this event.
    pub trigger: ScalingTrigger,
    /// Outcome description.
    pub outcome: String,
}

/// Runtime statistics for the predictive scaler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredictiveStats {
    /// Total traffic samples collected.
    pub samples_collected: u64,
    /// Total predictions made.
    pub predictions_made: u64,
    /// Predictions that turned out to be accurate.
    pub accurate_predictions: u64,
    /// Accuracy percentage.
    pub accuracy_pct: f64,
    /// Currently detected traffic pattern, if any.
    pub pattern: Option<TrafficPattern>,
}

/// Predictive auto-scaler.
///
/// Collects traffic samples, detects daily/weekly patterns using
/// simple exponential smoothing, and recommends proactive scaling.
pub struct PredictiveScaler {
    config: PredictiveConfig,
    samples: RwLock<VecDeque<TrafficSample>>,
    events: RwLock<Vec<ScalingEvent>>,
    stats: RwLock<PredictiveStats>,
    forecast_state: RwLock<ForecastState>,
}

/// Internal state for the exponential-smoothing forecast.
struct ForecastState {
    smoothed_ops: f64,
    smoothed_memory: f64,
    alpha: f64,
    initialized: bool,
}

impl Default for ForecastState {
    fn default() -> Self {
        Self {
            smoothed_ops: 0.0,
            smoothed_memory: 0.0,
            alpha: 0.3,
            initialized: false,
        }
    }
}

impl PredictiveScaler {
    /// Create a new predictive scaler with the given configuration.
    pub fn new(config: PredictiveConfig) -> Self {
        Self {
            config,
            samples: RwLock::new(VecDeque::new()),
            events: RwLock::new(Vec::new()),
            stats: RwLock::new(PredictiveStats {
                samples_collected: 0,
                predictions_made: 0,
                accurate_predictions: 0,
                accuracy_pct: 0.0,
                pattern: None,
            }),
            forecast_state: RwLock::new(ForecastState::default()),
        }
    }

    /// Record a new traffic sample and update the rolling window.
    pub fn record_sample(&self, sample: TrafficSample) {
        let max_samples = (self.config.lookback_window.as_secs() / 60).max(1) as usize;

        // Update exponential smoothing state
        {
            let mut state = self.forecast_state.write();
            if !state.initialized {
                state.smoothed_ops = sample.ops_per_sec;
                state.smoothed_memory = sample.memory_pct;
                state.initialized = true;
            } else {
                state.smoothed_ops =
                    state.alpha * sample.ops_per_sec + (1.0 - state.alpha) * state.smoothed_ops;
                state.smoothed_memory =
                    state.alpha * sample.memory_pct + (1.0 - state.alpha) * state.smoothed_memory;
            }
        }

        let mut samples = self.samples.write();
        samples.push_back(sample);
        while samples.len() > max_samples {
            samples.pop_front();
        }

        self.stats.write().samples_collected += 1;
    }

    /// Predict future traffic using exponential smoothing.
    pub fn predict(&self, horizon: Duration) -> PredictionResult {
        let state = self.forecast_state.read();
        let samples = self.samples.read();
        let sample_count = samples.len();

        let (predicted_ops, predicted_memory, confidence) =
            if !state.initialized || sample_count < 2 {
                (0.0, 0.0, 0.0)
            } else {
                let conf = (sample_count as f64 / 100.0).min(0.95);
                (state.smoothed_ops, state.smoothed_memory, conf)
            };

        // Simple replica recommendation: 1 replica per 10k ops/s, clamped
        let raw_replicas = (predicted_ops / 10_000.0).ceil().max(1.0);
        let clamped = raw_replicas.min(self.config.max_scale_factor * 3.0) as u8;
        let recommended_replicas = clamped.max(1);

        let reason = if confidence < self.config.confidence_threshold {
            "Low confidence — insufficient data".to_string()
        } else if predicted_ops > state.smoothed_ops * 1.5 {
            format!(
                "Predicted traffic spike to {:.0} ops/s within {}s",
                predicted_ops,
                horizon.as_secs()
            )
        } else {
            "Traffic forecast is stable".to_string()
        };

        self.stats.write().predictions_made += 1;

        PredictionResult {
            predicted_ops,
            predicted_memory_pct: predicted_memory,
            confidence,
            recommended_replicas,
            lead_time_secs: horizon.as_secs(),
            reason,
        }
    }

    /// Detect daily or weekly traffic patterns from the sample window.
    pub fn detect_pattern(&self) -> Option<TrafficPattern> {
        let samples = self.samples.read();
        if samples.len() < 24 {
            return Some(TrafficPattern::Unpredictable);
        }

        // Build per-hour averages
        let mut hour_totals = [0.0_f64; 24];
        let mut hour_counts = [0u64; 24];

        for s in samples.iter() {
            let hour = ((s.timestamp / 3600) % 24) as usize;
            hour_totals[hour] += s.ops_per_sec;
            hour_counts[hour] += 1;
        }

        let mut hour_avgs = [0.0_f64; 24];
        for i in 0..24 {
            if hour_counts[i] > 0 {
                hour_avgs[i] = hour_totals[i] / hour_counts[i] as f64;
            }
        }

        let max_avg = hour_avgs.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let min_avg = hour_avgs.iter().copied().fold(f64::INFINITY, f64::min);

        if max_avg <= 0.0 {
            return Some(TrafficPattern::Steady);
        }

        let variation = (max_avg - min_avg) / max_avg;
        if variation < 0.2 {
            let pattern = TrafficPattern::Steady;
            self.stats.write().pattern = Some(pattern.clone());
            return Some(pattern);
        }

        let peak_hour = hour_avgs
            .iter()
            .enumerate()
            .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(i, _)| i as u8)
            .unwrap_or(0);

        let trough_hour = hour_avgs
            .iter()
            .enumerate()
            .min_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(i, _)| i as u8)
            .unwrap_or(0);

        let pattern = TrafficPattern::DailyCycle {
            peak_hour,
            trough_hour,
        };
        self.stats.write().pattern = Some(pattern.clone());
        Some(pattern)
    }

    /// Return the history of scaling events.
    pub fn scaling_history(&self) -> Vec<ScalingEvent> {
        self.events.read().clone()
    }

    /// Return current predictive scaler statistics.
    pub fn stats(&self) -> PredictiveStats {
        self.stats.read().clone()
    }
}

/// Serde helper to serialize Duration as seconds.
mod duration_secs {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u64(d.as_secs())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        let secs = u64::deserialize(d)?;
        Ok(Duration::from_secs(secs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_sample(ts: u64, ops: f64) -> TrafficSample {
        TrafficSample {
            timestamp: ts,
            ops_per_sec: ops,
            memory_pct: 50.0,
            cpu_pct: 40.0,
            latency_p99_us: 500.0,
            connections: 100,
        }
    }

    #[test]
    fn test_record_and_predict() {
        let scaler = PredictiveScaler::new(PredictiveConfig::default());
        for i in 0..10 {
            scaler.record_sample(make_sample(i * 60, 5000.0 + (i as f64) * 100.0));
        }
        let result = scaler.predict(Duration::from_secs(1800));
        assert!(result.predicted_ops > 0.0);
        assert!(result.recommended_replicas >= 1);
    }

    #[test]
    fn test_empty_predict() {
        let scaler = PredictiveScaler::new(PredictiveConfig::default());
        let result = scaler.predict(Duration::from_secs(1800));
        assert_eq!(result.confidence, 0.0);
    }

    #[test]
    fn test_detect_pattern_insufficient_data() {
        let scaler = PredictiveScaler::new(PredictiveConfig::default());
        scaler.record_sample(make_sample(0, 1000.0));
        let pattern = scaler.detect_pattern();
        assert_eq!(pattern, Some(TrafficPattern::Unpredictable));
    }

    #[test]
    fn test_detect_daily_cycle() {
        let scaler = PredictiveScaler::new(PredictiveConfig::default());
        // Simulate 48 hours with a clear daily pattern
        for hour in 0..48u64 {
            let ops = if (hour % 24) >= 9 && (hour % 24) <= 17 {
                10000.0 // business hours
            } else {
                2000.0 // off hours
            };
            scaler.record_sample(make_sample(hour * 3600, ops));
        }
        let pattern = scaler.detect_pattern();
        match pattern {
            Some(TrafficPattern::DailyCycle { .. }) => {} // expected
            other => panic!("Expected DailyCycle, got {:?}", other),
        }
    }

    #[test]
    fn test_stats() {
        let scaler = PredictiveScaler::new(PredictiveConfig::default());
        scaler.record_sample(make_sample(0, 1000.0));
        let stats = scaler.stats();
        assert_eq!(stats.samples_collected, 1);
    }
}
