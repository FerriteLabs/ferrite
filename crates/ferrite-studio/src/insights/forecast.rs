//! Capacity forecasting for Ferrite Insights.
//!
//! Uses linear regression on historical metrics to project future
//! memory, key count, and storage growth.

use serde::{Deserialize, Serialize};

/// A data point for time-series forecasting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataPoint {
    /// Timestamp (seconds since epoch).
    pub timestamp: u64,
    /// Measured value.
    pub value: f64,
}

/// Forecast result from linear regression.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForecastResult {
    /// Slope (rate of change per second).
    pub slope: f64,
    /// Y-intercept.
    pub intercept: f64,
    /// R² goodness of fit (0.0-1.0).
    pub r_squared: f64,
    /// Predicted value at the target timestamp.
    pub predicted_value: f64,
    /// Target timestamp for prediction.
    pub target_timestamp: u64,
}

/// Simple linear regression for capacity forecasting.
pub fn linear_forecast(data: &[DataPoint], target_timestamp: u64) -> Option<ForecastResult> {
    if data.len() < 2 {
        return None;
    }

    let n = data.len() as f64;
    let sum_x: f64 = data.iter().map(|p| p.timestamp as f64).sum();
    let sum_y: f64 = data.iter().map(|p| p.value).sum();
    let sum_xy: f64 = data.iter().map(|p| p.timestamp as f64 * p.value).sum();
    let sum_xx: f64 = data.iter().map(|p| (p.timestamp as f64).powi(2)).sum();

    let denominator = n * sum_xx - sum_x * sum_x;
    if denominator.abs() < f64::EPSILON {
        return None;
    }

    let slope = (n * sum_xy - sum_x * sum_y) / denominator;
    let intercept = (sum_y - slope * sum_x) / n;

    // R² calculation
    let mean_y = sum_y / n;
    let ss_tot: f64 = data.iter().map(|p| (p.value - mean_y).powi(2)).sum();
    let ss_res: f64 = data
        .iter()
        .map(|p| {
            let predicted = slope * p.timestamp as f64 + intercept;
            (p.value - predicted).powi(2)
        })
        .sum();

    let r_squared = if ss_tot > f64::EPSILON {
        1.0 - (ss_res / ss_tot)
    } else {
        1.0
    };

    let predicted_value = slope * target_timestamp as f64 + intercept;

    Some(ForecastResult {
        slope,
        intercept,
        r_squared: r_squared.clamp(0.0, 1.0),
        predicted_value: predicted_value.max(0.0),
        target_timestamp,
    })
}

/// Estimates days until a threshold is reached based on current growth trend.
pub fn days_until_threshold(
    data: &[DataPoint],
    threshold: f64,
    current_timestamp: u64,
) -> Option<u32> {
    let forecast = linear_forecast(data, current_timestamp)?;

    let current_value = forecast.predicted_value;
    if current_value >= threshold {
        return Some(0);
    }

    if forecast.slope <= 0.0 {
        return None; // Not growing
    }

    let remaining = threshold - current_value;
    let seconds_remaining = remaining / forecast.slope;
    let days = (seconds_remaining / 86400.0).ceil() as u32;

    Some(days)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_linear_forecast() {
        let data: Vec<DataPoint> = (0..10)
            .map(|i| DataPoint {
                timestamp: i * 3600,
                value: 100.0 + i as f64 * 10.0,
            })
            .collect();

        let result = linear_forecast(&data, 10 * 3600).unwrap();
        assert!(result.slope > 0.0);
        assert!(result.r_squared > 0.99);
        assert!((result.predicted_value - 200.0).abs() < 1.0);
    }

    #[test]
    fn test_forecast_insufficient_data() {
        let data = vec![DataPoint {
            timestamp: 0,
            value: 100.0,
        }];
        assert!(linear_forecast(&data, 3600).is_none());
    }

    #[test]
    fn test_days_until_threshold() {
        let data: Vec<DataPoint> = (0..10)
            .map(|i| DataPoint {
                timestamp: i * 86400,           // One per day
                value: 100.0 + i as f64 * 10.0, // +10 per day
            })
            .collect();

        let current = 9 * 86400;
        let days = days_until_threshold(&data, 300.0, current);
        assert!(days.is_some());
        // Currently at ~190, need 300, growing +10/day → ~11 days
        assert!(days.unwrap() > 5 && days.unwrap() < 20);
    }

    #[test]
    fn test_days_threshold_already_exceeded() {
        let data: Vec<DataPoint> = (0..5)
            .map(|i| DataPoint {
                timestamp: i * 3600,
                value: 500.0,
            })
            .collect();

        let days = days_until_threshold(&data, 100.0, 5 * 3600);
        assert_eq!(days, Some(0));
    }

    #[test]
    fn test_days_threshold_not_growing() {
        let data: Vec<DataPoint> = (0..5)
            .map(|i| DataPoint {
                timestamp: i * 3600,
                value: 100.0 - i as f64,
            })
            .collect();

        let days = days_until_threshold(&data, 200.0, 5 * 3600);
        assert!(days.is_none()); // Shrinking, never reaches threshold
    }

    #[test]
    fn test_forecast_empty_data() {
        let data: Vec<DataPoint> = vec![];
        assert!(linear_forecast(&data, 3600).is_none());
    }

    #[test]
    fn test_forecast_flat_data() {
        let data: Vec<DataPoint> = (0..10)
            .map(|i| DataPoint {
                timestamp: i * 3600,
                value: 100.0,
            })
            .collect();

        let result = linear_forecast(&data, 20 * 3600).unwrap();
        assert!(result.slope.abs() < 0.001); // No growth
        assert!((result.predicted_value - 100.0).abs() < 1.0);
    }

    #[test]
    fn test_forecast_negative_prediction_clamped() {
        // Decreasing data with a future prediction that would go negative
        let data: Vec<DataPoint> = (0..5)
            .map(|i| DataPoint {
                timestamp: i * 3600,
                value: 10.0 - i as f64 * 5.0,
            })
            .collect();

        let result = linear_forecast(&data, 100 * 3600).unwrap();
        assert!(result.predicted_value >= 0.0); // Clamped to 0
    }

    #[test]
    fn test_forecast_r_squared_perfect_fit() {
        let data: Vec<DataPoint> = (0..10)
            .map(|i| DataPoint {
                timestamp: i * 3600,
                value: 50.0 + i as f64 * 20.0,
            })
            .collect();

        let result = linear_forecast(&data, 10 * 3600).unwrap();
        assert!(result.r_squared > 0.99);
    }

    #[test]
    fn test_forecast_result_fields() {
        let data: Vec<DataPoint> = (0..5)
            .map(|i| DataPoint {
                timestamp: i * 3600,
                value: 100.0 + i as f64 * 10.0,
            })
            .collect();

        let target = 10 * 3600;
        let result = linear_forecast(&data, target).unwrap();
        assert_eq!(result.target_timestamp, target);
        assert!(result.slope > 0.0);
    }

    #[test]
    fn test_days_until_threshold_with_fast_growth() {
        let data: Vec<DataPoint> = (0..10)
            .map(|i| DataPoint {
                timestamp: i * 86400,
                value: i as f64 * 100.0,
            })
            .collect();

        let current = 9 * 86400;
        let days = days_until_threshold(&data, 2000.0, current);
        assert!(days.is_some());
        assert!(days.unwrap() > 0);
    }

    #[test]
    fn test_forecast_two_data_points() {
        let data = vec![
            DataPoint {
                timestamp: 0,
                value: 100.0,
            },
            DataPoint {
                timestamp: 3600,
                value: 200.0,
            },
        ];
        let result = linear_forecast(&data, 7200).unwrap();
        assert!(result.slope > 0.0);
        assert!((result.predicted_value - 300.0).abs() < 1.0);
        assert!(result.r_squared > 0.99);
    }

    #[test]
    fn test_forecast_noisy_data() {
        let data: Vec<DataPoint> = (0..20)
            .map(|i| DataPoint {
                timestamp: i * 3600,
                // Linear trend with noise
                value: 100.0 + i as f64 * 10.0 + if i % 2 == 0 { 5.0 } else { -5.0 },
            })
            .collect();

        let result = linear_forecast(&data, 20 * 3600).unwrap();
        assert!(result.slope > 0.0);
        // R² should be high but not perfect due to noise
        assert!(result.r_squared > 0.9);
    }

    #[test]
    fn test_days_until_threshold_insufficient_data() {
        let data = vec![DataPoint {
            timestamp: 0,
            value: 100.0,
        }];
        assert!(days_until_threshold(&data, 200.0, 3600).is_none());
    }

    #[test]
    fn test_forecast_decreasing_data() {
        let data: Vec<DataPoint> = (0..10)
            .map(|i| DataPoint {
                timestamp: i * 3600,
                value: 1000.0 - i as f64 * 50.0,
            })
            .collect();

        let result = linear_forecast(&data, 20 * 3600).unwrap();
        assert!(result.slope < 0.0);
    }

    #[test]
    fn test_data_point_serialization() {
        let dp = DataPoint {
            timestamp: 1000,
            value: 42.5,
        };
        let json = serde_json::to_string(&dp).unwrap();
        let deserialized: DataPoint = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.timestamp, 1000);
        assert!((deserialized.value - 42.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_forecast_result_serialization() {
        let result = ForecastResult {
            slope: 0.5,
            intercept: 100.0,
            r_squared: 0.95,
            predicted_value: 150.0,
            target_timestamp: 10000,
        };
        let json = serde_json::to_string(&result).unwrap();
        let deserialized: ForecastResult = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.target_timestamp, 10000);
        assert!((deserialized.slope - 0.5).abs() < f64::EPSILON);
    }
}
