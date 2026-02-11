//! Access Pattern Predictor
//!
//! Uses ML-based analysis to predict future access patterns and trends,
//! helping the auto-indexing engine make proactive decisions.

use super::collector::QueryPattern;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};

/// Result of access pattern prediction
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PredictionResult {
    /// Pattern being predicted
    pub pattern: String,
    /// Predicted access count for next period
    pub predicted_access_count: u64,
    /// Predicted read/write ratio
    pub predicted_rw_ratio: f64,
    /// Trend direction
    pub trend: TrendAnalysis,
    /// Confidence in prediction (0.0-1.0)
    pub confidence: f64,
    /// Recommended action
    pub recommendation: PredictionRecommendation,
}

/// Trend analysis for access patterns
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TrendAnalysis {
    /// Trend direction
    pub direction: TrendDirection,
    /// Trend strength (0.0-1.0)
    pub strength: f64,
    /// Estimated growth/decline rate per hour
    pub rate_per_hour: f64,
    /// Seasonality detected
    pub seasonality: Option<Seasonality>,
}

/// Trend direction
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TrendDirection {
    /// Access increasing
    Increasing,
    /// Access stable
    Stable,
    /// Access decreasing
    Decreasing,
    /// Too much variance to determine
    Volatile,
}

/// Seasonality pattern
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Seasonality {
    /// Period in seconds
    pub period_secs: u64,
    /// Peak time offset from period start
    pub peak_offset_secs: u64,
    /// Amplitude (ratio of peak to trough)
    pub amplitude: f64,
}

/// Recommended action based on prediction
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PredictionRecommendation {
    /// Create index now
    CreateIndex,
    /// Wait and monitor
    Monitor,
    /// Consider removing index
    ConsiderRemoval,
    /// No action needed
    NoAction,
}

/// Access pattern predictor using simple ML techniques
pub struct AccessPredictor {
    /// Whether ML prediction is enabled
    enabled: bool,
    /// Historical data per pattern
    history: RwLock<HashMap<String, PatternHistory>>,
    /// Model weights (simple linear model)
    weights: RwLock<ModelWeights>,
    /// Prediction horizon in seconds
    horizon_secs: u64,
}

/// Historical data for a pattern
struct PatternHistory {
    /// Access counts per time bucket (hourly)
    hourly_counts: VecDeque<u64>,
    /// Read/write ratios per bucket
    rw_ratios: VecDeque<f64>,
    /// Last update timestamp
    last_update: u64,
}

/// Simple linear model weights
struct ModelWeights {
    /// Trend weight
    trend_weight: f64,
    /// Momentum weight
    momentum_weight: f64,
    /// Seasonality weight
    seasonality_weight: f64,
}

impl Default for ModelWeights {
    fn default() -> Self {
        Self {
            trend_weight: 0.4,
            momentum_weight: 0.3,
            seasonality_weight: 0.3,
        }
    }
}

impl AccessPredictor {
    /// Create a new access predictor
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            history: RwLock::new(HashMap::new()),
            weights: RwLock::new(ModelWeights::default()),
            horizon_secs: 3600, // 1 hour prediction horizon
        }
    }

    /// Create a disabled predictor
    pub fn disabled() -> Self {
        Self::new(false)
    }

    /// Predict future access pattern
    pub fn predict(&self, pattern: &QueryPattern) -> PredictionResult {
        if !self.enabled {
            return self.default_prediction(pattern);
        }

        // Update history
        self.update_history(pattern);

        // Get trend analysis
        let trend = self.analyze_trend(pattern);

        // Predict access count
        let predicted_count = self.predict_access_count(pattern, &trend);

        // Predict read/write ratio
        let predicted_rw_ratio = self.predict_rw_ratio(pattern);

        // Calculate confidence
        let confidence = self.calculate_confidence(pattern);

        // Generate recommendation
        let recommendation = self.generate_recommendation(pattern, &trend, confidence);

        PredictionResult {
            pattern: pattern.pattern.clone(),
            predicted_access_count: predicted_count,
            predicted_rw_ratio,
            trend,
            confidence,
            recommendation,
        }
    }

    /// Update historical data for a pattern
    fn update_history(&self, pattern: &QueryPattern) {
        let mut history = self.history.write();

        let entry = history
            .entry(pattern.pattern.clone())
            .or_insert_with(|| PatternHistory {
                hourly_counts: VecDeque::with_capacity(168), // 1 week
                rw_ratios: VecDeque::with_capacity(168),
                last_update: 0,
            });

        // Check if we need to add a new bucket
        let current_hour = pattern.last_seen / 3600;
        let last_hour = entry.last_update / 3600;

        if current_hour > last_hour {
            entry.hourly_counts.push_back(pattern.access_count);
            entry
                .rw_ratios
                .push_back(pattern.read_write_ratio().min(100.0));
            entry.last_update = pattern.last_seen;

            // Keep only last week of data
            while entry.hourly_counts.len() > 168 {
                entry.hourly_counts.pop_front();
                entry.rw_ratios.pop_front();
            }
        }
    }

    /// Analyze trend for a pattern
    fn analyze_trend(&self, pattern: &QueryPattern) -> TrendAnalysis {
        let history = self.history.read();

        if let Some(entry) = history.get(&pattern.pattern) {
            if entry.hourly_counts.len() < 3 {
                return self.default_trend();
            }

            // Calculate trend using simple linear regression
            let counts: Vec<f64> = entry.hourly_counts.iter().map(|&c| c as f64).collect();
            let (slope, _intercept, r_squared) = self.linear_regression(&counts);

            // Determine direction
            let direction = if slope.abs() < 0.01 {
                TrendDirection::Stable
            } else if r_squared < 0.3 {
                TrendDirection::Volatile
            } else if slope > 0.0 {
                TrendDirection::Increasing
            } else {
                TrendDirection::Decreasing
            };

            // Calculate strength
            let strength = r_squared.min(1.0);

            // Detect seasonality
            let seasonality = self.detect_seasonality(&counts);

            TrendAnalysis {
                direction,
                strength,
                rate_per_hour: slope,
                seasonality,
            }
        } else {
            self.default_trend()
        }
    }

    /// Simple linear regression
    fn linear_regression(&self, values: &[f64]) -> (f64, f64, f64) {
        let n = values.len() as f64;
        if n < 2.0 {
            return (0.0, 0.0, 0.0);
        }

        let x_mean = (n - 1.0) / 2.0;
        let y_mean: f64 = values.iter().sum::<f64>() / n;

        let mut numerator = 0.0;
        let mut denominator = 0.0;
        let mut ss_tot = 0.0;

        for (i, &y) in values.iter().enumerate() {
            let x = i as f64;
            numerator += (x - x_mean) * (y - y_mean);
            denominator += (x - x_mean) * (x - x_mean);
            ss_tot += (y - y_mean) * (y - y_mean);
        }

        if denominator == 0.0 || ss_tot == 0.0 {
            return (0.0, y_mean, 0.0);
        }

        let slope = numerator / denominator;
        let intercept = y_mean - slope * x_mean;

        // Calculate R-squared
        let mut ss_res = 0.0;
        for (i, &y) in values.iter().enumerate() {
            let y_pred = slope * i as f64 + intercept;
            ss_res += (y - y_pred) * (y - y_pred);
        }

        let r_squared = 1.0 - (ss_res / ss_tot);

        (slope, intercept, r_squared.max(0.0))
    }

    /// Detect seasonality using autocorrelation
    fn detect_seasonality(&self, values: &[f64]) -> Option<Seasonality> {
        if values.len() < 48 {
            // Need at least 2 days of data
            return None;
        }

        // Check for daily seasonality (24 hour period)
        let autocorr_24 = self.autocorrelation(values, 24);
        if autocorr_24 > 0.5 {
            // Find peak within period
            let peak_offset = self.find_peak_offset(values, 24);
            return Some(Seasonality {
                period_secs: 24 * 3600,
                peak_offset_secs: peak_offset * 3600,
                amplitude: autocorr_24,
            });
        }

        // Check for weekly seasonality (168 hour period)
        if values.len() >= 336 {
            let autocorr_168 = self.autocorrelation(values, 168);
            if autocorr_168 > 0.5 {
                let peak_offset = self.find_peak_offset(values, 168);
                return Some(Seasonality {
                    period_secs: 168 * 3600,
                    peak_offset_secs: peak_offset * 3600,
                    amplitude: autocorr_168,
                });
            }
        }

        None
    }

    /// Calculate autocorrelation at a given lag
    fn autocorrelation(&self, values: &[f64], lag: usize) -> f64 {
        if values.len() <= lag {
            return 0.0;
        }

        let mean: f64 = values.iter().sum::<f64>() / values.len() as f64;
        let variance: f64 =
            values.iter().map(|&v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;

        if variance == 0.0 {
            return 0.0;
        }

        let n = values.len() - lag;
        let covariance: f64 = (0..n)
            .map(|i| (values[i] - mean) * (values[i + lag] - mean))
            .sum::<f64>()
            / n as f64;

        covariance / variance
    }

    /// Find peak offset within a period
    fn find_peak_offset(&self, values: &[f64], period: usize) -> u64 {
        let mut period_sums = vec![0.0; period];
        let mut period_counts = vec![0usize; period];

        for (i, &v) in values.iter().enumerate() {
            let bucket = i % period;
            period_sums[bucket] += v;
            period_counts[bucket] += 1;
        }

        let mut max_avg = 0.0;
        let mut max_offset = 0;

        for (i, (sum, count)) in period_sums.iter().zip(period_counts.iter()).enumerate() {
            if *count > 0 {
                let avg = sum / *count as f64;
                if avg > max_avg {
                    max_avg = avg;
                    max_offset = i;
                }
            }
        }

        max_offset as u64
    }

    /// Predict access count for next period
    fn predict_access_count(&self, pattern: &QueryPattern, trend: &TrendAnalysis) -> u64 {
        let base_count = pattern.access_count;
        let weights = self.weights.read();

        // Apply trend
        let trend_adjustment = match trend.direction {
            TrendDirection::Increasing => 1.0 + (trend.rate_per_hour * weights.trend_weight),
            TrendDirection::Decreasing => 1.0 - (trend.rate_per_hour.abs() * weights.trend_weight),
            _ => 1.0,
        };

        // Apply seasonality
        let seasonality_adjustment = if let Some(ref s) = trend.seasonality {
            1.0 + (s.amplitude * weights.seasonality_weight * 0.5)
        } else {
            1.0
        };

        let predicted = base_count as f64 * trend_adjustment * seasonality_adjustment;
        predicted.max(0.0) as u64
    }

    /// Predict read/write ratio
    fn predict_rw_ratio(&self, pattern: &QueryPattern) -> f64 {
        let history = self.history.read();

        if let Some(entry) = history.get(&pattern.pattern) {
            if entry.rw_ratios.len() >= 3 {
                // Use moving average
                let recent: Vec<f64> = entry.rw_ratios.iter().rev().take(5).copied().collect();
                return recent.iter().sum::<f64>() / recent.len() as f64;
            }
        }

        pattern.read_write_ratio().min(100.0)
    }

    /// Calculate prediction confidence
    fn calculate_confidence(&self, pattern: &QueryPattern) -> f64 {
        let history = self.history.read();

        if let Some(entry) = history.get(&pattern.pattern) {
            // More data = higher confidence
            let data_confidence = (entry.hourly_counts.len() as f64 / 168.0).min(1.0);

            // Less variance = higher confidence
            let counts: Vec<f64> = entry.hourly_counts.iter().map(|&c| c as f64).collect();
            let variance_confidence = if counts.len() > 1 {
                let mean = counts.iter().sum::<f64>() / counts.len() as f64;
                let variance =
                    counts.iter().map(|&c| (c - mean).powi(2)).sum::<f64>() / counts.len() as f64;
                let cv = variance.sqrt() / mean.max(1.0); // Coefficient of variation
                (1.0 - cv.min(1.0)).max(0.0)
            } else {
                0.5
            };

            data_confidence * 0.6 + variance_confidence * 0.4
        } else {
            0.3 // Low confidence without history
        }
    }

    /// Generate recommendation based on prediction
    fn generate_recommendation(
        &self,
        pattern: &QueryPattern,
        trend: &TrendAnalysis,
        confidence: f64,
    ) -> PredictionRecommendation {
        // Low confidence = monitor
        if confidence < 0.5 {
            return PredictionRecommendation::Monitor;
        }

        // Read-heavy + increasing = create index
        if pattern.is_read_heavy() && trend.direction == TrendDirection::Increasing {
            return PredictionRecommendation::CreateIndex;
        }

        // Decreasing trend = consider removal
        if trend.direction == TrendDirection::Decreasing && trend.strength > 0.7 {
            return PredictionRecommendation::ConsiderRemoval;
        }

        // Stable + read-heavy + high frequency = create index
        if pattern.is_read_heavy()
            && pattern.frequency_per_minute > 10.0
            && trend.direction == TrendDirection::Stable
        {
            return PredictionRecommendation::CreateIndex;
        }

        PredictionRecommendation::Monitor
    }

    /// Generate default prediction when ML is disabled
    fn default_prediction(&self, pattern: &QueryPattern) -> PredictionResult {
        PredictionResult {
            pattern: pattern.pattern.clone(),
            predicted_access_count: pattern.access_count,
            predicted_rw_ratio: pattern.read_write_ratio().min(100.0),
            trend: self.default_trend(),
            confidence: 0.5,
            recommendation: if pattern.is_read_heavy() && pattern.frequency_per_minute >= 10.0 {
                PredictionRecommendation::CreateIndex
            } else {
                PredictionRecommendation::Monitor
            },
        }
    }

    /// Generate default trend
    fn default_trend(&self) -> TrendAnalysis {
        TrendAnalysis {
            direction: TrendDirection::Stable,
            strength: 0.0,
            rate_per_hour: 0.0,
            seasonality: None,
        }
    }

    /// Clear history for a pattern
    pub fn clear_history(&self, pattern: &str) {
        self.history.write().remove(pattern);
    }

    /// Clear all history
    pub fn clear_all(&self) {
        self.history.write().clear();
    }

    /// Check if predictor is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::autoindex::collector::AccessType;
    use std::time::Duration;

    fn create_test_pattern(access_count: u64, read_count: u64, write_count: u64) -> QueryPattern {
        QueryPattern {
            pattern: "test:*".to_string(),
            access_count,
            read_count,
            write_count,
            scan_count: 0,
            range_count: 0,
            avg_latency_us: 100,
            p50_latency_us: 80,
            p99_latency_us: 500,
            hot_fields: vec![],
            first_seen: 0,
            last_seen: 3600,
            frequency_per_minute: 10.0,
        }
    }

    #[test]
    fn test_predictor_disabled() {
        let predictor = AccessPredictor::disabled();
        let pattern = create_test_pattern(100, 80, 20);

        let result = predictor.predict(&pattern);
        assert_eq!(result.confidence, 0.5);
        assert_eq!(result.trend.direction, TrendDirection::Stable);
    }

    #[test]
    fn test_predictor_enabled() {
        let predictor = AccessPredictor::new(true);
        let pattern = create_test_pattern(100, 80, 20);

        let result = predictor.predict(&pattern);
        // With no history, confidence should be low
        assert!(result.confidence <= 0.5);
    }

    #[test]
    fn test_linear_regression() {
        let predictor = AccessPredictor::new(true);

        // Linear increasing data
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let (slope, _intercept, r_squared) = predictor.linear_regression(&values);

        assert!((slope - 1.0).abs() < 0.01);
        assert!((r_squared - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_trend_directions() {
        assert_eq!(TrendDirection::Increasing, TrendDirection::Increasing);
        assert_ne!(TrendDirection::Increasing, TrendDirection::Decreasing);
    }

    #[test]
    fn test_recommendation_read_heavy() {
        let pattern = create_test_pattern(100, 90, 10);
        assert!(pattern.is_read_heavy());

        let predictor = AccessPredictor::disabled();
        let result = predictor.predict(&pattern);

        // High read ratio + high frequency should recommend index
        assert_eq!(result.recommendation, PredictionRecommendation::CreateIndex);
    }

    #[test]
    fn test_autocorrelation() {
        let predictor = AccessPredictor::new(true);

        // Constant data should have autocorrelation of 1
        let constant = vec![5.0; 100];
        // Note: autocorrelation of constant data is 0 due to zero variance
        let autocorr = predictor.autocorrelation(&constant, 10);
        assert!(autocorr.is_nan() || autocorr == 0.0);

        // Alternating data
        let alternating: Vec<f64> = (0..100)
            .map(|i| if i % 2 == 0 { 1.0 } else { 0.0 })
            .collect();
        let autocorr_1 = predictor.autocorrelation(&alternating, 1);
        assert!(autocorr_1 < 0.0); // Negative correlation at lag 1

        let autocorr_2 = predictor.autocorrelation(&alternating, 2);
        assert!(autocorr_2 > 0.0); // Positive correlation at lag 2
    }

    #[test]
    fn test_clear_history() {
        let predictor = AccessPredictor::new(true);
        let pattern = create_test_pattern(100, 80, 20);

        predictor.predict(&pattern);
        predictor.clear_history("test:*");

        // History should be cleared
        let history = predictor.history.read();
        assert!(!history.contains_key("test:*"));
    }
}
