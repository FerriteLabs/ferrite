//! Load Prediction
//!
//! ML-based load prediction for proactive optimization.

use super::MetricsSample;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::time::Duration;

/// Load predictor for forecasting workload patterns
pub struct LoadPredictor {
    /// Historical samples
    samples: VecDeque<MetricsSample>,
    /// Maximum samples to keep
    max_samples: usize,
    /// Model state
    model: PredictionModel,
    /// Last prediction
    last_prediction: Option<PredictionResult>,
}

impl LoadPredictor {
    /// Create a new predictor
    pub fn new(max_samples: usize) -> Self {
        Self {
            samples: VecDeque::with_capacity(max_samples),
            max_samples,
            model: PredictionModel::new(),
            last_prediction: None,
        }
    }

    /// Add a sample for analysis
    pub fn add_sample(&mut self, sample: MetricsSample) {
        if self.samples.len() >= self.max_samples {
            self.samples.pop_front();
        }
        self.samples.push_back(sample);

        // Update model with new data
        self.model.update(&self.samples);
    }

    /// Predict load for the given horizon
    pub fn predict(&mut self, horizon: Duration) -> Option<PredictionResult> {
        if self.samples.len() < 30 {
            return None;
        }

        let prediction = self.model.predict(&self.samples, horizon);
        self.last_prediction = Some(prediction.clone());
        Some(prediction)
    }

    /// Get the last prediction
    pub fn last_prediction(&self) -> Option<&PredictionResult> {
        self.last_prediction.as_ref()
    }

    /// Get detected traffic pattern
    pub fn traffic_pattern(&self) -> TrafficPattern {
        if self.samples.len() < 20 {
            return TrafficPattern::Unknown;
        }

        let cv = self.coefficient_of_variation();

        if cv < 0.1 {
            TrafficPattern::Steady
        } else if cv > 0.5 {
            if self.has_periodicity() {
                TrafficPattern::Periodic
            } else {
                TrafficPattern::Bursty
            }
        } else if self.has_trend() {
            if self.trend_direction() > 0.0 {
                TrafficPattern::Growing
            } else {
                TrafficPattern::Declining
            }
        } else {
            TrafficPattern::Variable
        }
    }

    /// Calculate coefficient of variation
    fn coefficient_of_variation(&self) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }

        let ops: Vec<f64> = self.samples.iter().map(|s| s.ops_per_second).collect();
        let mean = ops.iter().sum::<f64>() / ops.len() as f64;

        if mean == 0.0 {
            return 0.0;
        }

        let variance = ops.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / ops.len() as f64;
        variance.sqrt() / mean
    }

    /// Check for periodicity in the data
    fn has_periodicity(&self) -> bool {
        if self.samples.len() < 24 {
            return false;
        }

        // Simple autocorrelation check
        let ops: Vec<f64> = self.samples.iter().map(|s| s.ops_per_second).collect();
        let mean = ops.iter().sum::<f64>() / ops.len() as f64;

        // Check for daily pattern (if we have enough samples)
        let lag = ops.len() / 4; // Check at quarter period
        if lag < 5 {
            return false;
        }

        let mut correlation = 0.0;
        for i in 0..(ops.len() - lag) {
            correlation += (ops[i] - mean) * (ops[i + lag] - mean);
        }

        let variance: f64 = ops.iter().map(|x| (x - mean).powi(2)).sum();
        if variance == 0.0 {
            return false;
        }

        correlation /= variance;
        correlation > 0.3
    }

    /// Check for trend in the data
    fn has_trend(&self) -> bool {
        self.trend_direction().abs() > 0.1
    }

    /// Get trend direction (positive = growing, negative = declining)
    fn trend_direction(&self) -> f64 {
        if self.samples.len() < 10 {
            return 0.0;
        }

        // Simple linear regression slope
        let n = self.samples.len() as f64;
        let ops: Vec<f64> = self.samples.iter().map(|s| s.ops_per_second).collect();

        let sum_x: f64 = (0..self.samples.len()).map(|i| i as f64).sum();
        let sum_y: f64 = ops.iter().sum();
        let sum_xy: f64 = ops.iter().enumerate().map(|(i, y)| i as f64 * y).sum();
        let sum_x2: f64 = (0..self.samples.len()).map(|i| (i as f64).powi(2)).sum();

        let denominator = n * sum_x2 - sum_x.powi(2);
        if denominator == 0.0 {
            return 0.0;
        }

        let slope = (n * sum_xy - sum_x * sum_y) / denominator;

        // Normalize by mean
        let mean = sum_y / n;
        if mean == 0.0 {
            return 0.0;
        }

        slope / mean
    }

    /// Clear all samples
    pub fn clear(&mut self) {
        self.samples.clear();
        self.model = PredictionModel::new();
        self.last_prediction = None;
    }
}

impl Default for LoadPredictor {
    fn default() -> Self {
        Self::new(1000)
    }
}

/// Prediction model using exponential smoothing
struct PredictionModel {
    /// Level component
    level: f64,
    /// Trend component
    trend: f64,
    /// Seasonal components (if detected)
    seasonal: Vec<f64>,
    /// Smoothing parameter for level
    alpha: f64,
    /// Smoothing parameter for trend
    beta: f64,
    /// Smoothing parameter for seasonal
    gamma: f64,
    /// Whether the model is initialized
    initialized: bool,
}

impl PredictionModel {
    /// Create a new model
    fn new() -> Self {
        Self {
            level: 0.0,
            trend: 0.0,
            seasonal: vec![1.0; 24], // Hourly seasonality
            alpha: 0.3,
            beta: 0.1,
            gamma: 0.1,
            initialized: false,
        }
    }

    /// Update the model with new data
    fn update(&mut self, samples: &VecDeque<MetricsSample>) {
        if samples.len() < 2 {
            return;
        }

        let latest = match samples.back() {
            Some(s) => s.ops_per_second,
            None => return,
        };

        if !self.initialized {
            self.level = latest;
            self.initialized = true;
            return;
        }

        // Holt-Winters exponential smoothing
        let old_level = self.level;

        // Update level
        self.level = self.alpha * latest + (1.0 - self.alpha) * (self.level + self.trend);

        // Update trend
        self.trend = self.beta * (self.level - old_level) + (1.0 - self.beta) * self.trend;
    }

    /// Predict future load
    fn predict(&self, samples: &VecDeque<MetricsSample>, horizon: Duration) -> PredictionResult {
        if !self.initialized || samples.is_empty() {
            return PredictionResult::default();
        }

        let steps = (horizon.as_secs() / 60).max(1) as usize; // Steps in minutes
        let predicted_ops = (self.level + self.trend * steps as f64).max(0.0);

        // Calculate confidence based on model stability
        let recent_error = self.calculate_recent_error(samples);
        let confidence = (1.0 - recent_error.min(1.0)).max(0.0);

        // Predict other metrics based on correlations
        let current = match samples.back() {
            Some(s) => s,
            None => return PredictionResult::default(),
        };
        let ops_ratio = if current.ops_per_second > 0.0 {
            predicted_ops / current.ops_per_second
        } else {
            1.0
        };

        // Memory prediction (tends to correlate with ops)
        let predicted_memory = (current.memory_used as f64 * ops_ratio.sqrt()) as u64;

        // Latency prediction (may increase under load)
        let predicted_latency = if ops_ratio > 1.0 {
            current.avg_latency_us * (1.0 + (ops_ratio - 1.0) * 0.5)
        } else {
            current.avg_latency_us
        };

        PredictionResult {
            horizon,
            predicted_ops_per_second: predicted_ops,
            predicted_memory_bytes: predicted_memory,
            predicted_latency_us: predicted_latency,
            predicted_connections: (current.connections as f64 * ops_ratio.sqrt()) as u64,
            confidence,
            trend: if self.trend > 0.0 {
                TrendDirection::Increasing
            } else if self.trend < 0.0 {
                TrendDirection::Decreasing
            } else {
                TrendDirection::Stable
            },
            anomaly_probability: self.calculate_anomaly_probability(samples),
        }
    }

    /// Calculate recent prediction error
    fn calculate_recent_error(&self, samples: &VecDeque<MetricsSample>) -> f64 {
        if samples.len() < 5 {
            return 0.5;
        }

        let recent: Vec<f64> = samples
            .iter()
            .rev()
            .take(5)
            .map(|s| s.ops_per_second)
            .collect();

        let mean = recent.iter().sum::<f64>() / recent.len() as f64;
        if mean == 0.0 {
            return 0.0;
        }

        let error: f64 = recent
            .iter()
            .map(|x| ((x - self.level) / mean).abs())
            .sum::<f64>()
            / recent.len() as f64;

        error.min(1.0)
    }

    /// Calculate probability of anomaly
    fn calculate_anomaly_probability(&self, samples: &VecDeque<MetricsSample>) -> f64 {
        if samples.len() < 10 {
            return 0.0;
        }

        let ops: Vec<f64> = samples.iter().map(|s| s.ops_per_second).collect();
        let mean = ops.iter().sum::<f64>() / ops.len() as f64;
        let std_dev =
            (ops.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / ops.len() as f64).sqrt();

        if std_dev == 0.0 {
            return 0.0;
        }

        let latest = match ops.last() {
            Some(v) => v,
            None => return 0.0,
        };
        let z_score = ((latest - mean) / std_dev).abs();

        // Convert z-score to probability
        // z > 3 is typically considered anomalous
        (z_score / 3.0).min(1.0)
    }
}

/// Result of a load prediction
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PredictionResult {
    /// Prediction horizon
    #[serde(with = "duration_serde")]
    pub horizon: Duration,
    /// Predicted operations per second
    pub predicted_ops_per_second: f64,
    /// Predicted memory usage
    pub predicted_memory_bytes: u64,
    /// Predicted latency
    pub predicted_latency_us: f64,
    /// Predicted connections
    pub predicted_connections: u64,
    /// Confidence in the prediction (0.0 to 1.0)
    pub confidence: f64,
    /// Trend direction
    pub trend: TrendDirection,
    /// Probability of anomaly
    pub anomaly_probability: f64,
}

impl PredictionResult {
    /// Check if load increase is predicted
    pub fn predicts_increase(&self) -> bool {
        matches!(self.trend, TrendDirection::Increasing) && self.confidence > 0.5
    }

    /// Check if the prediction suggests taking action
    pub fn requires_action(&self) -> bool {
        (self.predicts_increase() && self.confidence > 0.7) || self.anomaly_probability > 0.8
    }
}

/// Trend direction
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum TrendDirection {
    /// Load is increasing
    Increasing,
    /// Load is decreasing
    Decreasing,
    /// Load is stable
    #[default]
    Stable,
}

/// Traffic pattern classification
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TrafficPattern {
    /// Steady traffic
    Steady,
    /// Variable traffic
    Variable,
    /// Periodic pattern (daily/hourly)
    Periodic,
    /// Bursty traffic
    Bursty,
    /// Growing trend
    Growing,
    /// Declining trend
    Declining,
    /// Unknown (insufficient data)
    Unknown,
}

/// Serde support for Duration
mod duration_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        duration.as_secs().serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let secs = u64::deserialize(deserializer)?;
        Ok(Duration::from_secs(secs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_sample(ops: f64) -> MetricsSample {
        let mut sample = MetricsSample::new();
        sample.ops_per_second = ops;
        sample.memory_used = 1000;
        sample.avg_latency_us = 100.0;
        sample.connections = 10;
        sample
    }

    #[test]
    fn test_predictor_creation() {
        let predictor = LoadPredictor::new(100);
        assert!(predictor.last_prediction().is_none());
    }

    #[test]
    fn test_insufficient_data() {
        let mut predictor = LoadPredictor::new(100);

        // Add only a few samples
        for i in 0..10 {
            predictor.add_sample(create_sample(1000.0 + i as f64));
        }

        // Should return None with insufficient data
        let result = predictor.predict(Duration::from_secs(300));
        assert!(result.is_none());
    }

    #[test]
    fn test_steady_traffic_detection() {
        let mut predictor = LoadPredictor::new(100);

        // Add steady samples
        for _ in 0..30 {
            predictor.add_sample(create_sample(1000.0));
        }

        assert_eq!(predictor.traffic_pattern(), TrafficPattern::Steady);
    }

    #[test]
    fn test_prediction_with_data() {
        let mut predictor = LoadPredictor::new(100);

        // Add enough samples
        for i in 0..50 {
            predictor.add_sample(create_sample(1000.0 + (i as f64 * 10.0)));
        }

        let result = predictor.predict(Duration::from_secs(300));
        assert!(result.is_some());

        let prediction = result.unwrap();
        assert!(prediction.predicted_ops_per_second > 0.0);
        assert!(prediction.confidence > 0.0);
    }

    #[test]
    fn test_trend_detection() {
        let mut predictor = LoadPredictor::new(100);

        // Add increasing samples
        for i in 0..50 {
            predictor.add_sample(create_sample(1000.0 + (i as f64 * 50.0)));
        }

        let pattern = predictor.traffic_pattern();
        assert!(matches!(
            pattern,
            TrafficPattern::Growing | TrafficPattern::Variable
        ));
    }
}
