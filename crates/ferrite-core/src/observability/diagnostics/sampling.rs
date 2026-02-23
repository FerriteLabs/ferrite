//! Adaptive Sampling Engine
//!
//! Dynamically adjusts trace/metric sampling rates based on observed latency.
//! Uses z-score anomaly detection to automatically increase sampling when
//! latency patterns deviate from the baseline.

#![forbid(unsafe_code)]

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

/// Sampling state reflecting current system behaviour.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SamplingState {
    /// Operating within normal latency bounds.
    Normal,
    /// Latency slightly elevated — sampling rate increased.
    Elevated,
    /// Significant anomaly detected — sampling at maximum.
    AnomalyDetected,
}

/// Configuration for the adaptive sampler.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SamplerConfig {
    /// Maximum number of latency measurements to retain in the sliding window.
    pub window_size: usize,
    /// Minimum measurements required before anomaly detection activates.
    pub min_samples: usize,
}

impl Default for SamplerConfig {
    fn default() -> Self {
        Self {
            window_size: 1_000,
            min_samples: 30,
        }
    }
}

/// Snapshot of current sampler status.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SamplerStatus {
    /// Current effective sampling rate.
    pub current_rate: f64,
    /// Base (normal) sampling rate.
    pub base_rate: f64,
    /// Rate used during anomaly detection.
    pub anomaly_rate: f64,
    /// Current state.
    pub state: SamplingState,
    /// Number of latency measurements in the window.
    pub measurements: usize,
    /// Mean latency of current window (µs).
    pub mean_latency_us: f64,
    /// Standard deviation of current window (µs).
    pub stddev_latency_us: f64,
}

/// Adaptive sampling engine with z-score–based anomaly detection.
pub struct AdaptiveSampler {
    base_rate: f64,
    anomaly_rate: f64,
    inner: RwLock<SamplerInner>,
    config: SamplerConfig,
}

struct SamplerInner {
    current_rate: f64,
    latency_window: VecDeque<u64>,
    anomaly_threshold: f64,
    state: SamplingState,
    // Running statistics for efficient z-score computation
    sum: f64,
    sum_sq: f64,
}

impl AdaptiveSampler {
    /// Create a new adaptive sampler.
    ///
    /// * `base_rate`        – sampling rate under normal conditions (0.0–1.0).
    /// * `anomaly_rate`     – higher rate when an anomaly is detected.
    /// * `anomaly_threshold` – z-score threshold for anomaly detection (e.g. 2.5).
    pub fn new(base_rate: f64, anomaly_rate: f64, anomaly_threshold: f64) -> Self {
        Self::with_config(
            base_rate,
            anomaly_rate,
            anomaly_threshold,
            SamplerConfig::default(),
        )
    }

    /// Create with explicit configuration.
    pub fn with_config(
        base_rate: f64,
        anomaly_rate: f64,
        anomaly_threshold: f64,
        config: SamplerConfig,
    ) -> Self {
        Self {
            base_rate: base_rate.clamp(0.0, 1.0),
            anomaly_rate: anomaly_rate.clamp(0.0, 1.0),
            config,
            inner: RwLock::new(SamplerInner {
                current_rate: base_rate.clamp(0.0, 1.0),
                latency_window: VecDeque::with_capacity(1_000),
                anomaly_threshold,
                state: SamplingState::Normal,
                sum: 0.0,
                sum_sq: 0.0,
            }),
        }
    }

    /// Decide whether the current request should be sampled.
    pub fn should_sample(&self) -> bool {
        let rate = self.inner.read().current_rate;
        if rate >= 1.0 {
            return true;
        }
        if rate <= 0.0 {
            return false;
        }
        rand::random::<f64>() < rate
    }

    /// Feed a latency measurement (µs) and update anomaly state.
    pub fn record_latency(&self, duration_us: u64) {
        let mut inner = self.inner.write();
        let val = duration_us as f64;

        // Maintain sliding window
        if inner.latency_window.len() >= self.config.window_size {
            if let Some(old) = inner.latency_window.pop_front() {
                let old_f = old as f64;
                inner.sum -= old_f;
                inner.sum_sq -= old_f * old_f;
            }
        }
        inner.latency_window.push_back(duration_us);
        inner.sum += val;
        inner.sum_sq += val * val;

        // Update state
        let n = inner.latency_window.len();
        if n < self.config.min_samples {
            return;
        }

        let mean = inner.sum / n as f64;
        let variance = (inner.sum_sq / n as f64) - (mean * mean);
        let stddev = if variance > 0.0 {
            variance.sqrt()
        } else {
            0.0
        };

        if stddev < f64::EPSILON {
            inner.state = SamplingState::Normal;
            inner.current_rate = self.base_rate;
            return;
        }

        let z_score = (val - mean) / stddev;

        if z_score > inner.anomaly_threshold * 1.5 {
            inner.state = SamplingState::AnomalyDetected;
            inner.current_rate = self.anomaly_rate;
        } else if z_score > inner.anomaly_threshold {
            inner.state = SamplingState::Elevated;
            // Interpolate between base and anomaly rate
            let factor =
                (z_score - inner.anomaly_threshold) / (inner.anomaly_threshold * 0.5);
            inner.current_rate =
                self.base_rate + (self.anomaly_rate - self.base_rate) * factor.clamp(0.0, 1.0);
        } else {
            inner.state = SamplingState::Normal;
            inner.current_rate = self.base_rate;
        }
    }

    /// Check if the most recent measurement is anomalous.
    pub fn detect_anomaly(&self) -> bool {
        let inner = self.inner.read();
        inner.state == SamplingState::AnomalyDetected
    }

    /// Get the current sampler status.
    pub fn status(&self) -> SamplerStatus {
        let inner = self.inner.read();
        let n = inner.latency_window.len();
        let (mean, stddev) = if n > 0 {
            let m = inner.sum / n as f64;
            let v = (inner.sum_sq / n as f64) - (m * m);
            (m, if v > 0.0 { v.sqrt() } else { 0.0 })
        } else {
            (0.0, 0.0)
        };
        SamplerStatus {
            current_rate: inner.current_rate,
            base_rate: self.base_rate,
            anomaly_rate: self.anomaly_rate,
            state: inner.state,
            measurements: n,
            mean_latency_us: mean,
            stddev_latency_us: stddev,
        }
    }

    /// Update the base sampling rate.
    pub fn set_base_rate(&self, rate: f64) {
        let clamped = rate.clamp(0.0, 1.0);
        let mut inner = self.inner.write();
        if inner.state == SamplingState::Normal {
            inner.current_rate = clamped;
        }
    }

    /// Return the base rate.
    pub fn base_rate(&self) -> f64 {
        self.base_rate
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normal_state_initial() {
        let sampler = AdaptiveSampler::new(0.1, 1.0, 2.5);
        let status = sampler.status();
        assert_eq!(status.state, SamplingState::Normal);
        assert!((status.current_rate - 0.1).abs() < f64::EPSILON);
    }

    #[test]
    fn test_should_sample_always_at_1() {
        let sampler = AdaptiveSampler::new(1.0, 1.0, 2.5);
        for _ in 0..100 {
            assert!(sampler.should_sample());
        }
    }

    #[test]
    fn test_should_sample_never_at_0() {
        let sampler = AdaptiveSampler::new(0.0, 0.0, 2.5);
        for _ in 0..100 {
            assert!(!sampler.should_sample());
        }
    }

    #[test]
    fn test_anomaly_detection_with_spike() {
        let config = SamplerConfig {
            window_size: 100,
            min_samples: 10,
        };
        let sampler = AdaptiveSampler::with_config(0.01, 1.0, 2.0, config);

        // Feed stable baseline
        for _ in 0..50 {
            sampler.record_latency(100);
        }
        assert_eq!(sampler.status().state, SamplingState::Normal);

        // Inject a huge spike
        sampler.record_latency(100_000);
        assert_ne!(sampler.status().state, SamplingState::Normal);
    }

    #[test]
    fn test_returns_to_normal_after_spike() {
        let config = SamplerConfig {
            window_size: 50,
            min_samples: 10,
        };
        let sampler = AdaptiveSampler::with_config(0.01, 1.0, 2.0, config);

        for _ in 0..30 {
            sampler.record_latency(100);
        }
        sampler.record_latency(100_000);
        assert_ne!(sampler.status().state, SamplingState::Normal);

        // Feed more stable values to recover
        for _ in 0..30 {
            sampler.record_latency(100);
        }
        assert_eq!(sampler.status().state, SamplingState::Normal);
    }

    #[test]
    fn test_z_score_calculation() {
        // With constant values the stddev is 0 → should stay Normal
        let config = SamplerConfig {
            window_size: 100,
            min_samples: 5,
        };
        let sampler = AdaptiveSampler::with_config(0.1, 1.0, 2.0, config);
        for _ in 0..20 {
            sampler.record_latency(500);
        }
        let status = sampler.status();
        assert_eq!(status.state, SamplingState::Normal);
        assert!((status.mean_latency_us - 500.0).abs() < 1.0);
    }

    #[test]
    fn test_set_base_rate() {
        let sampler = AdaptiveSampler::new(0.1, 1.0, 2.5);
        sampler.set_base_rate(0.5);
        let status = sampler.status();
        // current_rate is updated since we're in Normal state
        assert!((status.current_rate - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_clamping() {
        let sampler = AdaptiveSampler::new(2.0, -0.5, 2.5);
        let status = sampler.status();
        assert!((status.current_rate - 1.0).abs() < f64::EPSILON);
        assert!((status.anomaly_rate - 0.0).abs() < f64::EPSILON);
    }
}
