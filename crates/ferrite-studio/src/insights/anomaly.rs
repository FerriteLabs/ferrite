//! Anomaly detection for Ferrite Insights.
//!
//! Detects unusual patterns in access rates, latency, memory usage,
//! and error rates using statistical analysis.

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

/// Configuration for anomaly detection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnomalyDetectorConfig {
    /// Number of standard deviations for anomaly threshold.
    pub sensitivity: f64,
    /// Minimum data points before anomaly detection activates.
    pub min_samples: usize,
    /// Rolling window size for baseline calculation.
    pub window_size: usize,
}

impl Default for AnomalyDetectorConfig {
    fn default() -> Self {
        Self {
            sensitivity: 3.0,
            min_samples: 30,
            window_size: 100,
        }
    }
}

/// Statistical anomaly detector using z-score analysis.
pub struct AnomalyDetector {
    config: AnomalyDetectorConfig,
    observations: VecDeque<f64>,
}

impl AnomalyDetector {
    pub fn new(config: AnomalyDetectorConfig) -> Self {
        let capacity = config.window_size;
        Self {
            config,
            observations: VecDeque::with_capacity(capacity),
        }
    }

    /// Records an observation and returns whether it's anomalous.
    pub fn observe(&mut self, value: f64) -> Option<AnomalyResult> {
        if self.observations.len() >= self.config.window_size {
            self.observations.pop_front();
        }
        self.observations.push_back(value);

        if self.observations.len() < self.config.min_samples {
            return None;
        }

        let mean = self.mean();
        let stddev = self.stddev(mean);

        if stddev < f64::EPSILON {
            return None;
        }

        let z_score = (value - mean) / stddev;

        if z_score.abs() > self.config.sensitivity {
            Some(AnomalyResult {
                value,
                mean,
                stddev,
                z_score,
                is_anomaly: true,
            })
        } else {
            None
        }
    }

    /// Returns the current mean of observations.
    pub fn mean(&self) -> f64 {
        if self.observations.is_empty() {
            return 0.0;
        }
        self.observations.iter().sum::<f64>() / self.observations.len() as f64
    }

    /// Returns the standard deviation.
    pub fn stddev(&self, mean: f64) -> f64 {
        if self.observations.len() < 2 {
            return 0.0;
        }
        let variance = self
            .observations
            .iter()
            .map(|x| (x - mean).powi(2))
            .sum::<f64>()
            / (self.observations.len() - 1) as f64;
        variance.sqrt()
    }

    /// Returns the number of observations recorded.
    pub fn observation_count(&self) -> usize {
        self.observations.len()
    }
}

/// Result of an anomaly check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnomalyResult {
    pub value: f64,
    pub mean: f64,
    pub stddev: f64,
    pub z_score: f64,
    pub is_anomaly: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_anomaly_with_insufficient_data() {
        let mut detector = AnomalyDetector::new(AnomalyDetectorConfig {
            min_samples: 10,
            ..Default::default()
        });

        for i in 0..5 {
            assert!(detector.observe(i as f64).is_none());
        }
    }

    #[test]
    fn test_detects_anomaly() {
        let mut detector = AnomalyDetector::new(AnomalyDetectorConfig {
            sensitivity: 2.0,
            min_samples: 10,
            window_size: 100,
        });

        // Normal baseline
        for _ in 0..50 {
            detector.observe(100.0);
        }

        // Anomalous spike
        let result = detector.observe(500.0);
        assert!(result.is_some());
        assert!(result.unwrap().z_score > 2.0);
    }

    #[test]
    fn test_no_false_positives_normal_data() {
        let mut detector = AnomalyDetector::new(AnomalyDetectorConfig {
            sensitivity: 3.0,
            min_samples: 30,
            window_size: 100,
        });

        // Slightly varying normal data
        for i in 0..100 {
            let value = 100.0 + (i % 10) as f64;
            let result = detector.observe(value);
            if detector.observation_count() >= 30 {
                assert!(result.is_none(), "False positive at i={i}, value={value}");
            }
        }
    }

    #[test]
    fn test_mean_calculation() {
        let mut detector = AnomalyDetector::new(AnomalyDetectorConfig::default());
        detector.observe(10.0);
        detector.observe(20.0);
        detector.observe(30.0);
        assert!((detector.mean() - 20.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_window_size_limit() {
        let mut detector = AnomalyDetector::new(AnomalyDetectorConfig {
            window_size: 5,
            ..Default::default()
        });

        for i in 0..10 {
            detector.observe(i as f64);
        }
        assert_eq!(detector.observation_count(), 5);
    }

    #[test]
    fn test_stddev_calculation() {
        let mut detector = AnomalyDetector::new(AnomalyDetectorConfig::default());
        detector.observe(10.0);
        detector.observe(10.0);
        detector.observe(10.0);
        let mean = detector.mean();
        let stddev = detector.stddev(mean);
        assert!(stddev < f64::EPSILON); // All same values → 0 stddev
    }

    #[test]
    fn test_stddev_with_variance() {
        let mut detector = AnomalyDetector::new(AnomalyDetectorConfig::default());
        detector.observe(10.0);
        detector.observe(20.0);
        let mean = detector.mean();
        let stddev = detector.stddev(mean);
        assert!(stddev > 0.0);
    }

    #[test]
    fn test_empty_detector() {
        let detector = AnomalyDetector::new(AnomalyDetectorConfig::default());
        assert_eq!(detector.observation_count(), 0);
        assert_eq!(detector.mean(), 0.0);
    }

    #[test]
    fn test_single_observation_stddev() {
        let mut detector = AnomalyDetector::new(AnomalyDetectorConfig::default());
        detector.observe(42.0);
        let mean = detector.mean();
        let stddev = detector.stddev(mean);
        assert_eq!(stddev, 0.0); // Need >= 2 samples
    }

    #[test]
    fn test_anomaly_result_fields() {
        let mut detector = AnomalyDetector::new(AnomalyDetectorConfig {
            sensitivity: 2.0,
            min_samples: 5,
            window_size: 100,
        });

        for _ in 0..20 {
            detector.observe(100.0);
        }

        let result = detector.observe(500.0);
        assert!(result.is_some());
        let r = result.unwrap();
        assert!(r.is_anomaly);
        assert!(r.z_score > 2.0);
        assert!((r.mean - 100.0).abs() < 50.0); // Mean is close to 100
    }

    #[test]
    fn test_default_config() {
        let config = AnomalyDetectorConfig::default();
        assert_eq!(config.sensitivity, 3.0);
        assert_eq!(config.min_samples, 30);
        assert_eq!(config.window_size, 100);
    }

    #[test]
    fn test_detects_drop() {
        let mut detector = AnomalyDetector::new(AnomalyDetectorConfig {
            sensitivity: 2.0,
            min_samples: 10,
            window_size: 100,
        });

        // Normal baseline
        for _ in 0..50 {
            detector.observe(1000.0);
        }

        // Sudden drop
        let result = detector.observe(0.0);
        assert!(result.is_some());
        let r = result.unwrap();
        assert!(r.z_score < -2.0); // Negative z-score for drop
    }

    #[test]
    fn test_gradual_increase_no_anomaly() {
        let mut detector = AnomalyDetector::new(AnomalyDetectorConfig {
            sensitivity: 3.0,
            min_samples: 10,
            window_size: 50,
        });

        // Gradually increasing data - should not trigger anomaly
        for i in 0..50 {
            let value = 100.0 + i as f64 * 2.0;
            let _result = detector.observe(value);
        }
        // The last observation is part of the gradual trend
        let result = detector.observe(200.0);
        // Gradual increase shouldn't cause anomaly with normal trend
        assert!(result.is_none());
    }

    #[test]
    fn test_anomaly_config_custom() {
        let config = AnomalyDetectorConfig {
            sensitivity: 1.5,
            min_samples: 5,
            window_size: 20,
        };
        assert_eq!(config.sensitivity, 1.5);
        assert_eq!(config.min_samples, 5);
        assert_eq!(config.window_size, 20);
    }

    #[test]
    fn test_anomaly_result_serialization() {
        let result = AnomalyResult {
            value: 500.0,
            mean: 100.0,
            stddev: 10.0,
            z_score: 40.0,
            is_anomaly: true,
        };
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("500"));
        let deserialized: AnomalyResult = serde_json::from_str(&json).unwrap();
        assert!(deserialized.is_anomaly);
    }

    #[test]
    fn test_detector_rolling_window() {
        let mut detector = AnomalyDetector::new(AnomalyDetectorConfig {
            sensitivity: 3.0,
            min_samples: 5,
            window_size: 10,
        });

        // Fill window
        for i in 0..10 {
            detector.observe(100.0 + i as f64);
        }
        assert_eq!(detector.observation_count(), 10);

        // Add more - should not exceed window size
        for _ in 0..5 {
            detector.observe(100.0);
        }
        assert_eq!(detector.observation_count(), 10);
    }
}
