//! Workload Pattern Detection
//!
//! Detects and classifies workload patterns for optimization decisions.

use super::MetricsSample;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

/// Pattern detector for workload analysis
pub struct PatternDetector {
    /// Historical samples
    samples: VecDeque<MetricsSample>,
    /// Maximum samples to keep
    max_samples: usize,
    /// Detected pattern
    current_pattern: Option<WorkloadPattern>,
}

impl PatternDetector {
    /// Create a new pattern detector
    pub fn new(max_samples: usize) -> Self {
        Self {
            samples: VecDeque::with_capacity(max_samples),
            max_samples,
            current_pattern: None,
        }
    }

    /// Add a sample for analysis
    pub fn add_sample(&mut self, sample: MetricsSample) {
        if self.samples.len() >= self.max_samples {
            self.samples.pop_front();
        }
        self.samples.push_back(sample);
    }

    /// Detect the current workload pattern
    pub fn detect(&mut self) -> Option<WorkloadPattern> {
        if self.samples.len() < 10 {
            return None;
        }

        let workload_type = self.classify_workload();
        let access_pattern = self.classify_access();
        let traffic_pattern = self.classify_traffic();

        let pattern = WorkloadPattern {
            workload_type,
            access_pattern,
            traffic_pattern,
            read_ratio: self.average_read_ratio(),
            avg_ops_per_second: self.average_ops(),
            avg_latency_us: self.average_latency(),
            cache_hit_rate: self.average_hit_rate(),
            confidence: self.calculate_confidence(),
        };

        self.current_pattern = Some(pattern.clone());
        Some(pattern)
    }

    /// Get the current pattern
    pub fn current_pattern(&self) -> Option<&WorkloadPattern> {
        self.current_pattern.as_ref()
    }

    /// Classify the workload type
    fn classify_workload(&self) -> WorkloadType {
        let read_ratio = self.average_read_ratio();
        let ops = self.average_ops();

        if read_ratio > 0.95 {
            WorkloadType::ReadHeavy
        } else if read_ratio < 0.2 {
            WorkloadType::WriteHeavy
        } else if ops > 100_000.0 {
            WorkloadType::HighThroughput
        } else if self.average_latency() > 10_000.0 {
            WorkloadType::LatencySensitive
        } else {
            WorkloadType::Mixed
        }
    }

    /// Classify the access pattern
    fn classify_access(&self) -> AccessPattern {
        // Simplified classification based on cache hit rate
        let hit_rate = self.average_hit_rate();

        if hit_rate > 0.95 {
            AccessPattern::HotSpot // High cache hits indicate hot data
        } else if hit_rate < 0.5 {
            AccessPattern::Uniform // Low cache hits indicate uniform access
        } else if self.has_key_skew() {
            AccessPattern::Zipfian
        } else {
            AccessPattern::Random
        }
    }

    /// Classify the traffic pattern
    fn classify_traffic(&self) -> TrafficClassification {
        if self.samples.len() < 10 {
            return TrafficClassification::Unknown;
        }

        let ops_variance = self.ops_variance();
        let avg_ops = self.average_ops();

        // Calculate coefficient of variation
        let cv = if avg_ops > 0.0 {
            (ops_variance.sqrt() / avg_ops) * 100.0
        } else {
            0.0
        };

        if cv < 10.0 {
            TrafficClassification::Steady
        } else if cv > 50.0 {
            if self.has_periodic_pattern() {
                TrafficClassification::Periodic
            } else {
                TrafficClassification::Bursty
            }
        } else {
            TrafficClassification::Variable
        }
    }

    /// Calculate average read ratio
    fn average_read_ratio(&self) -> f64 {
        if self.samples.is_empty() {
            return 0.5;
        }
        let sum: f64 = self.samples.iter().map(|s| s.read_ratio).sum();
        sum / self.samples.len() as f64
    }

    /// Calculate average operations per second
    fn average_ops(&self) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        let sum: f64 = self.samples.iter().map(|s| s.ops_per_second).sum();
        sum / self.samples.len() as f64
    }

    /// Calculate average latency
    fn average_latency(&self) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        let sum: f64 = self.samples.iter().map(|s| s.avg_latency_us).sum();
        sum / self.samples.len() as f64
    }

    /// Calculate average cache hit rate
    fn average_hit_rate(&self) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        let sum: f64 = self.samples.iter().map(|s| s.cache_hit_rate).sum();
        sum / self.samples.len() as f64
    }

    /// Calculate ops variance
    fn ops_variance(&self) -> f64 {
        if self.samples.len() < 2 {
            return 0.0;
        }

        let mean = self.average_ops();
        let sum_sq: f64 = self
            .samples
            .iter()
            .map(|s| (s.ops_per_second - mean).powi(2))
            .sum();

        sum_sq / (self.samples.len() - 1) as f64
    }

    /// Check for key skew in access pattern
    fn has_key_skew(&self) -> bool {
        // Simplified: high variance in hit rate indicates skew
        if self.samples.len() < 5 {
            return false;
        }

        let mean = self.average_hit_rate();
        let variance: f64 = self
            .samples
            .iter()
            .map(|s| (s.cache_hit_rate - mean).powi(2))
            .sum::<f64>()
            / self.samples.len() as f64;

        variance > 0.01 // 1% variance threshold
    }

    /// Check for periodic pattern in traffic
    fn has_periodic_pattern(&self) -> bool {
        if self.samples.len() < 20 {
            return false;
        }

        // Simple autocorrelation check at common intervals
        let ops: Vec<f64> = self.samples.iter().map(|s| s.ops_per_second).collect();
        let mean = self.average_ops();

        // Check for autocorrelation at lag = samples.len() / 2
        let lag = ops.len() / 2;
        if lag < 5 {
            return false;
        }

        let mut correlation = 0.0;
        for i in 0..(ops.len() - lag) {
            correlation += (ops[i] - mean) * (ops[i + lag] - mean);
        }

        let variance = self.ops_variance();
        if variance > 0.0 {
            correlation /= (ops.len() - lag) as f64 * variance;
            correlation > 0.5 // Strong positive correlation indicates periodicity
        } else {
            false
        }
    }

    /// Calculate pattern detection confidence
    fn calculate_confidence(&self) -> f64 {
        // Base confidence on sample count
        let sample_factor = (self.samples.len() as f64 / self.max_samples as f64).min(1.0);

        // Stability factor based on variance
        let variance = self.ops_variance();
        let avg = self.average_ops();
        let cv = if avg > 0.0 {
            variance.sqrt() / avg
        } else {
            0.0
        };
        let stability_factor = (1.0 - cv.min(1.0)).max(0.0);

        // Combined confidence
        (sample_factor * 0.6 + stability_factor * 0.4).min(1.0)
    }

    /// Clear all samples
    pub fn clear(&mut self) {
        self.samples.clear();
        self.current_pattern = None;
    }
}

impl Default for PatternDetector {
    fn default() -> Self {
        Self::new(1000)
    }
}

/// Detected workload pattern
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkloadPattern {
    /// Workload type
    pub workload_type: WorkloadType,
    /// Access pattern
    pub access_pattern: AccessPattern,
    /// Traffic pattern
    pub traffic_pattern: TrafficClassification,
    /// Read ratio (0.0 to 1.0)
    pub read_ratio: f64,
    /// Average operations per second
    pub avg_ops_per_second: f64,
    /// Average latency in microseconds
    pub avg_latency_us: f64,
    /// Cache hit rate
    pub cache_hit_rate: f64,
    /// Detection confidence (0.0 to 1.0)
    pub confidence: f64,
}

/// Workload type classification
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkloadType {
    /// Mostly reads (>95%)
    ReadHeavy,
    /// Mostly writes (>80%)
    WriteHeavy,
    /// Mixed read/write
    Mixed,
    /// High throughput focus
    HighThroughput,
    /// Latency sensitive
    LatencySensitive,
    /// Batch processing
    Batch,
    /// Real-time
    RealTime,
}

/// Access pattern classification
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccessPattern {
    /// Random access
    Random,
    /// Zipfian distribution (power law)
    Zipfian,
    /// Hot spot (few keys dominate)
    HotSpot,
    /// Sequential access
    Sequential,
    /// Uniform distribution
    Uniform,
    /// Time-based (recent data accessed more)
    Temporal,
}

/// Traffic pattern classification
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TrafficClassification {
    /// Steady traffic
    Steady,
    /// Variable but predictable
    Variable,
    /// Periodic (daily/hourly patterns)
    Periodic,
    /// Bursty traffic
    Bursty,
    /// Unknown/insufficient data
    Unknown,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_sample(ops: f64, read_ratio: f64, hit_rate: f64) -> MetricsSample {
        let mut sample = MetricsSample::new();
        sample.ops_per_second = ops;
        sample.read_ratio = read_ratio;
        sample.cache_hit_rate = hit_rate;
        sample.avg_latency_us = 100.0;
        sample
    }

    #[test]
    fn test_detector_creation() {
        let detector = PatternDetector::new(100);
        assert!(detector.current_pattern().is_none());
    }

    #[test]
    fn test_read_heavy_detection() {
        let mut detector = PatternDetector::new(100);

        // Add samples with high read ratio
        for _ in 0..20 {
            detector.add_sample(create_sample(1000.0, 0.98, 0.8));
        }

        let pattern = detector.detect().unwrap();
        assert_eq!(pattern.workload_type, WorkloadType::ReadHeavy);
    }

    #[test]
    fn test_write_heavy_detection() {
        let mut detector = PatternDetector::new(100);

        // Add samples with low read ratio
        for _ in 0..20 {
            detector.add_sample(create_sample(1000.0, 0.1, 0.3));
        }

        let pattern = detector.detect().unwrap();
        assert_eq!(pattern.workload_type, WorkloadType::WriteHeavy);
    }

    #[test]
    fn test_confidence_increases_with_samples() {
        let mut detector = PatternDetector::new(100);

        // Add few samples
        for _ in 0..10 {
            detector.add_sample(create_sample(1000.0, 0.5, 0.8));
        }
        let confidence1 = detector.detect().unwrap().confidence;

        // Add more samples
        for _ in 0..40 {
            detector.add_sample(create_sample(1000.0, 0.5, 0.8));
        }
        let confidence2 = detector.detect().unwrap().confidence;

        assert!(confidence2 >= confidence1);
    }

    #[test]
    fn test_insufficient_data() {
        let mut detector = PatternDetector::new(100);

        // Add only 5 samples (less than minimum 10)
        for _ in 0..5 {
            detector.add_sample(create_sample(1000.0, 0.5, 0.8));
        }

        assert!(detector.detect().is_none());
    }
}
