//! Bottleneck Analysis
//!
//! Collects system-level resource samples (CPU, memory, I/O, connections)
//! and identifies the most likely performance bottleneck with a confidence
//! score and actionable recommendation.

#![forbid(unsafe_code)]

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

/// Classification of the dominant bottleneck.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BottleneckType {
    /// CPU-bound workload.
    Cpu,
    /// Memory pressure.
    Memory,
    /// Disk / I/O wait.
    Io,
    /// Network / connection saturation.
    Network,
    /// No clear bottleneck detected.
    None,
}

impl std::fmt::Display for BottleneckType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cpu => write!(f, "CPU"),
            Self::Memory => write!(f, "Memory"),
            Self::Io => write!(f, "I/O"),
            Self::Network => write!(f, "Network"),
            Self::None => write!(f, "None"),
        }
    }
}

/// Result of a bottleneck analysis run.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BottleneckReport {
    /// Identified bottleneck area.
    pub bottleneck: BottleneckType,
    /// Confidence in the classification (0.0–1.0).
    pub confidence: f64,
    /// Human-readable recommendation.
    pub recommendation: String,
    /// Average CPU utilisation across the window (0.0–1.0).
    pub avg_cpu: f64,
    /// Average memory utilisation across the window (0.0–1.0).
    pub avg_memory: f64,
    /// Average I/O wait ratio across the window (0.0–1.0).
    pub avg_io: f64,
    /// Average connection count.
    pub avg_connections: f64,
    /// Number of samples used.
    pub sample_count: usize,
}

/// Collects resource samples and analyses for bottlenecks.
pub struct BottleneckAnalyzer {
    inner: RwLock<AnalyzerInner>,
    max_samples: usize,
}

struct AnalyzerInner {
    cpu_samples: VecDeque<f64>,
    memory_samples: VecDeque<f64>,
    io_samples: VecDeque<f64>,
    connection_samples: VecDeque<u64>,
}

impl BottleneckAnalyzer {
    /// Create a new analyzer retaining at most `max_samples`.
    pub fn new(max_samples: usize) -> Self {
        Self {
            inner: RwLock::new(AnalyzerInner {
                cpu_samples: VecDeque::with_capacity(max_samples),
                memory_samples: VecDeque::with_capacity(max_samples),
                io_samples: VecDeque::with_capacity(max_samples),
                connection_samples: VecDeque::with_capacity(max_samples),
            }),
            max_samples,
        }
    }

    /// Feed a resource sample.
    ///
    /// * `cpu` – CPU utilisation ratio (0.0–1.0).
    /// * `memory` – memory utilisation ratio (0.0–1.0).
    /// * `io_wait` – I/O wait ratio (0.0–1.0).
    /// * `connections` – active connection count.
    pub fn record_sample(&self, cpu: f64, memory: f64, io_wait: f64, connections: u64) {
        let mut inner = self.inner.write();
        push_bounded(&mut inner.cpu_samples, cpu, self.max_samples);
        push_bounded(&mut inner.memory_samples, memory, self.max_samples);
        push_bounded(&mut inner.io_samples, io_wait, self.max_samples);
        push_bounded_u64(&mut inner.connection_samples, connections, self.max_samples);
    }

    /// Analyze current samples and produce a report.
    pub fn analyze(&self) -> BottleneckReport {
        let inner = self.inner.read();
        let n = inner.cpu_samples.len();

        if n == 0 {
            return BottleneckReport {
                bottleneck: BottleneckType::None,
                confidence: 0.0,
                recommendation: "No samples collected yet. Record system metrics first."
                    .to_string(),
                avg_cpu: 0.0,
                avg_memory: 0.0,
                avg_io: 0.0,
                avg_connections: 0.0,
                sample_count: 0,
            };
        }

        let avg_cpu = avg_f64(&inner.cpu_samples);
        let avg_memory = avg_f64(&inner.memory_samples);
        let avg_io = avg_f64(&inner.io_samples);
        let avg_connections = avg_u64(&inner.connection_samples);

        // Scoring: higher score → more likely bottleneck
        let cpu_score = avg_cpu;
        let mem_score = avg_memory;
        let io_score = avg_io;
        // Normalise connections: assume 10k as "saturation"
        let net_score = (avg_connections / 10_000.0).min(1.0);

        let scores = [
            (BottleneckType::Cpu, cpu_score),
            (BottleneckType::Memory, mem_score),
            (BottleneckType::Io, io_score),
            (BottleneckType::Network, net_score),
        ];

        let (bottleneck, max_score) = scores
            .iter()
            .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
            .map(|&(t, s)| (t, s))
            .unwrap_or((BottleneckType::None, 0.0));

        // Confidence is relative dominance of the top score
        let total: f64 = scores.iter().map(|(_, s)| s).sum();
        let confidence = if total > 0.0 {
            (max_score / total).clamp(0.0, 1.0)
        } else {
            0.0
        };

        let (bottleneck, confidence) = if max_score < 0.3 {
            (BottleneckType::None, 0.0)
        } else {
            (bottleneck, confidence)
        };

        let recommendation = match bottleneck {
            BottleneckType::Cpu => format!(
                "CPU utilisation averages {:.0}%. Consider optimising hot command paths, \
                 enabling pipelining, or scaling horizontally.",
                avg_cpu * 100.0
            ),
            BottleneckType::Memory => format!(
                "Memory utilisation averages {:.0}%. Consider tuning maxmemory, \
                 enabling tiered storage, or reducing key count.",
                avg_memory * 100.0
            ),
            BottleneckType::Io => format!(
                "I/O wait averages {:.0}%. Consider enabling io_uring, \
                 reducing AOF fsync frequency, or using faster storage.",
                avg_io * 100.0
            ),
            BottleneckType::Network => format!(
                "Average connections: {:.0}. Consider connection pooling, \
                 reducing client count, or scaling replicas.",
                avg_connections
            ),
            BottleneckType::None => "No significant bottleneck detected.".to_string(),
        };

        BottleneckReport {
            bottleneck,
            confidence,
            recommendation,
            avg_cpu,
            avg_memory,
            avg_io,
            avg_connections,
            sample_count: n,
        }
    }

    /// Number of samples currently buffered.
    pub fn sample_count(&self) -> usize {
        self.inner.read().cpu_samples.len()
    }

    /// Clear all collected samples.
    pub fn reset(&self) {
        let mut inner = self.inner.write();
        inner.cpu_samples.clear();
        inner.memory_samples.clear();
        inner.io_samples.clear();
        inner.connection_samples.clear();
    }
}

fn push_bounded(buf: &mut VecDeque<f64>, val: f64, max: usize) {
    if buf.len() >= max {
        buf.pop_front();
    }
    buf.push_back(val);
}

fn push_bounded_u64(buf: &mut VecDeque<u64>, val: u64, max: usize) {
    if buf.len() >= max {
        buf.pop_front();
    }
    buf.push_back(val);
}

fn avg_f64(buf: &VecDeque<f64>) -> f64 {
    if buf.is_empty() {
        return 0.0;
    }
    buf.iter().sum::<f64>() / buf.len() as f64
}

fn avg_u64(buf: &VecDeque<u64>) -> f64 {
    if buf.is_empty() {
        return 0.0;
    }
    buf.iter().sum::<u64>() as f64 / buf.len() as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_samples() {
        let analyzer = BottleneckAnalyzer::new(100);
        let report = analyzer.analyze();
        assert_eq!(report.bottleneck, BottleneckType::None);
        assert_eq!(report.sample_count, 0);
    }

    #[test]
    fn test_cpu_bottleneck() {
        let analyzer = BottleneckAnalyzer::new(100);
        for _ in 0..20 {
            analyzer.record_sample(0.95, 0.2, 0.1, 100);
        }
        let report = analyzer.analyze();
        assert_eq!(report.bottleneck, BottleneckType::Cpu);
        assert!(report.confidence > 0.5);
        assert!(report.recommendation.contains("CPU"));
    }

    #[test]
    fn test_memory_bottleneck() {
        let analyzer = BottleneckAnalyzer::new(100);
        for _ in 0..20 {
            analyzer.record_sample(0.1, 0.92, 0.05, 50);
        }
        let report = analyzer.analyze();
        assert_eq!(report.bottleneck, BottleneckType::Memory);
        assert!(report.recommendation.contains("Memory"));
    }

    #[test]
    fn test_io_bottleneck() {
        let analyzer = BottleneckAnalyzer::new(100);
        for _ in 0..20 {
            analyzer.record_sample(0.1, 0.1, 0.85, 50);
        }
        let report = analyzer.analyze();
        assert_eq!(report.bottleneck, BottleneckType::Io);
    }

    #[test]
    fn test_network_bottleneck() {
        let analyzer = BottleneckAnalyzer::new(100);
        for _ in 0..20 {
            analyzer.record_sample(0.1, 0.1, 0.05, 9_500);
        }
        let report = analyzer.analyze();
        assert_eq!(report.bottleneck, BottleneckType::Network);
    }

    #[test]
    fn test_no_bottleneck_low_values() {
        let analyzer = BottleneckAnalyzer::new(100);
        for _ in 0..20 {
            analyzer.record_sample(0.1, 0.1, 0.1, 50);
        }
        let report = analyzer.analyze();
        assert_eq!(report.bottleneck, BottleneckType::None);
    }

    #[test]
    fn test_ring_buffer_eviction() {
        let analyzer = BottleneckAnalyzer::new(5);
        for _ in 0..10 {
            analyzer.record_sample(0.5, 0.5, 0.5, 100);
        }
        assert_eq!(analyzer.sample_count(), 5);
    }

    #[test]
    fn test_reset() {
        let analyzer = BottleneckAnalyzer::new(100);
        analyzer.record_sample(0.5, 0.5, 0.5, 100);
        assert_eq!(analyzer.sample_count(), 1);
        analyzer.reset();
        assert_eq!(analyzer.sample_count(), 0);
    }

    #[test]
    fn test_bottleneck_type_display() {
        assert_eq!(BottleneckType::Cpu.to_string(), "CPU");
        assert_eq!(BottleneckType::Memory.to_string(), "Memory");
        assert_eq!(BottleneckType::Io.to_string(), "I/O");
        assert_eq!(BottleneckType::Network.to_string(), "Network");
        assert_eq!(BottleneckType::None.to_string(), "None");
    }
}
