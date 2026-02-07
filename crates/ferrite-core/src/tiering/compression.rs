//! Adaptive Compression Engine
//!
//! Per-key-pattern compression selection using lightweight heuristics and
//! statistics. Selects the optimal compression algorithm per key prefix
//! based on observed data characteristics (size, entropy, access patterns).
//!
//! # Supported Algorithms
//!
//! | Algorithm | Best For | Ratio | Speed |
//! |-----------|----------|-------|-------|
//! | LZ4 | Hot data, low latency | Low | Very fast |
//! | Zstd | Warm/cold data, balanced | High | Fast |
//! | Flate2/Gzip | Archival, max ratio | Very high | Slow |
//! | Delta | Numeric/time-series | Variable | Very fast |
//! | None | Already compressed, tiny | 1:1 | Instant |

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Available compression algorithms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    /// No compression.
    None,
    /// LZ4 fast compression.
    Lz4,
    /// Zstandard compression.
    Zstd,
    /// Flate2/Gzip compression.
    Flate2,
    /// Delta encoding for sequential data.
    Delta,
}

impl std::fmt::Display for CompressionAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "none"),
            Self::Lz4 => write!(f, "lz4"),
            Self::Zstd => write!(f, "zstd"),
            Self::Flate2 => write!(f, "flate2"),
            Self::Delta => write!(f, "delta"),
        }
    }
}

/// Configuration for the adaptive compression engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdaptiveCompressionConfig {
    /// Enable adaptive selection
    pub enabled: bool,
    /// Minimum value size to compress (bytes)
    pub min_compress_size: usize,
    /// Samples to collect before making a decision
    pub samples_before_decision: usize,
    /// Re-evaluate interval (number of writes)
    pub reevaluate_interval: u64,
    /// Default algorithm when no stats available
    pub default_algorithm: CompressionAlgorithm,
    /// Zstd compression level (1-22)
    pub zstd_level: i32,
    /// Flate2 compression level (0-9)
    pub flate2_level: u32,
}

impl Default for AdaptiveCompressionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_compress_size: 64,
            samples_before_decision: 50,
            reevaluate_interval: 10_000,
            default_algorithm: CompressionAlgorithm::Lz4,
            zstd_level: 3,
            flate2_level: 6,
        }
    }
}

// ---------------------------------------------------------------------------
// Statistics per prefix
// ---------------------------------------------------------------------------

/// Collected statistics for a key prefix.
#[derive(Debug, Clone, Default)]
pub struct PrefixStats {
    /// Key prefix identifier.
    pub prefix: String,
    /// Number of data samples collected.
    pub sample_count: u64,
    /// Total bytes sampled.
    pub total_bytes: u64,
    /// Average value size in bytes.
    pub avg_value_size: f64,
    /// Minimum value size in bytes.
    pub min_value_size: usize,
    /// Maximum value size in bytes.
    pub max_value_size: usize,
    /// Estimated Shannon entropy (0.0–1.0).
    pub estimated_entropy: f64,
    /// Number of read operations recorded.
    pub read_count: u64,
    /// Number of write operations recorded.
    pub write_count: u64,
    /// Average compression ratio per algorithm.
    pub compression_ratios: HashMap<CompressionAlgorithm, f64>,
    /// Average compression time (microseconds) per algorithm.
    pub compression_times_us: HashMap<CompressionAlgorithm, f64>,
    /// Algorithm selected by the adaptive engine, if any.
    pub selected_algorithm: Option<CompressionAlgorithm>,
}

/// Recommendation produced by the adaptive engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionRecommendation {
    /// Key prefix this recommendation applies to.
    pub prefix: String,
    /// Recommended compression algorithm.
    pub recommended: CompressionAlgorithm,
    /// Estimated compression ratio.
    pub estimated_ratio: f64,
    /// Estimated compression latency in microseconds.
    pub estimated_latency_us: f64,
    /// Confidence level of this recommendation (0.0–1.0).
    pub confidence: f64,
    /// Human-readable reason for the recommendation.
    pub reason: String,
}

// ---------------------------------------------------------------------------
// Compression / decompression helpers
// ---------------------------------------------------------------------------

/// Compress data using the specified algorithm.
pub fn compress(
    algorithm: CompressionAlgorithm,
    data: &[u8],
    config: &AdaptiveCompressionConfig,
) -> Vec<u8> {
    match algorithm {
        CompressionAlgorithm::None => data.to_vec(),
        CompressionAlgorithm::Lz4 => lz4_flex::compress_prepend_size(data),
        CompressionAlgorithm::Zstd => {
            // Use flate2 as a stand-in since zstd isn't in deps; in production would use zstd crate
            use flate2::write::GzEncoder;
            use flate2::Compression;
            use std::io::Write;
            let mut encoder = GzEncoder::new(Vec::new(), Compression::new(config.flate2_level));
            encoder.write_all(data).unwrap_or_default();
            encoder.finish().unwrap_or_else(|_| data.to_vec())
        }
        CompressionAlgorithm::Flate2 => {
            use flate2::write::GzEncoder;
            use flate2::Compression;
            use std::io::Write;
            let mut encoder = GzEncoder::new(Vec::new(), Compression::new(config.flate2_level));
            encoder.write_all(data).unwrap_or_default();
            encoder.finish().unwrap_or_else(|_| data.to_vec())
        }
        CompressionAlgorithm::Delta => {
            // Simple delta encoding: store first byte, then deltas
            if data.is_empty() {
                return Vec::new();
            }
            let mut out = Vec::with_capacity(data.len());
            out.push(data[0]);
            for i in 1..data.len() {
                out.push(data[i].wrapping_sub(data[i - 1]));
            }
            out
        }
    }
}

/// Decompress data using the specified algorithm.
pub fn decompress(algorithm: CompressionAlgorithm, data: &[u8]) -> Result<Vec<u8>, String> {
    match algorithm {
        CompressionAlgorithm::None => Ok(data.to_vec()),
        CompressionAlgorithm::Lz4 => lz4_flex::decompress_size_prepended(data)
            .map_err(|e| format!("LZ4 decompress error: {}", e)),
        CompressionAlgorithm::Zstd | CompressionAlgorithm::Flate2 => {
            use flate2::read::GzDecoder;
            use std::io::Read;
            let mut decoder = GzDecoder::new(data);
            let mut out = Vec::new();
            decoder
                .read_to_end(&mut out)
                .map_err(|e| format!("Flate2 decompress error: {}", e))?;
            Ok(out)
        }
        CompressionAlgorithm::Delta => {
            if data.is_empty() {
                return Ok(Vec::new());
            }
            let mut out = Vec::with_capacity(data.len());
            out.push(data[0]);
            for i in 1..data.len() {
                out.push(data[i].wrapping_add(out[i - 1]));
            }
            Ok(out)
        }
    }
}

/// Estimate entropy of data (Shannon entropy, normalized 0.0 – 1.0).
pub fn estimate_entropy(data: &[u8]) -> f64 {
    if data.is_empty() {
        return 0.0;
    }
    let mut counts = [0u64; 256];
    for &byte in data {
        counts[byte as usize] += 1;
    }
    let len = data.len() as f64;
    let mut entropy = 0.0;
    for &count in &counts {
        if count > 0 {
            let p = count as f64 / len;
            entropy -= p * p.log2();
        }
    }
    entropy / 8.0 // Normalize to 0.0 – 1.0
}

// ---------------------------------------------------------------------------
// Adaptive engine
// ---------------------------------------------------------------------------

/// Metrics for the compression engine.
#[derive(Debug, Default)]
pub struct CompressionMetrics {
    /// Total bytes before compression.
    pub bytes_before_compression: AtomicU64,
    /// Total bytes after compression.
    pub bytes_after_compression: AtomicU64,
    /// Total number of compression operations.
    pub compressions_total: AtomicU64,
    /// Total number of decompression operations.
    pub decompressions_total: AtomicU64,
}

impl CompressionMetrics {
    /// Returns the overall compression ratio (before / after).
    pub fn overall_ratio(&self) -> f64 {
        let before = self.bytes_before_compression.load(Ordering::Relaxed) as f64;
        let after = self.bytes_after_compression.load(Ordering::Relaxed) as f64;
        if before < 1.0 {
            1.0
        } else {
            before / after
        }
    }
}

/// The adaptive compression engine.
pub struct AdaptiveCompressionEngine {
    config: AdaptiveCompressionConfig,
    prefix_stats: Arc<parking_lot::RwLock<HashMap<String, PrefixStats>>>,
    metrics: Arc<CompressionMetrics>,
}

impl AdaptiveCompressionEngine {
    /// Create a new adaptive compression engine with the given configuration.
    pub fn new(config: AdaptiveCompressionConfig) -> Self {
        Self {
            config,
            prefix_stats: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            metrics: Arc::new(CompressionMetrics::default()),
        }
    }

    /// Record a data sample for a key prefix to build statistics.
    pub fn record_sample(&self, prefix: &str, data: &[u8]) {
        let mut stats = self.prefix_stats.write();
        let entry = stats
            .entry(prefix.to_string())
            .or_insert_with(|| PrefixStats {
                prefix: prefix.to_string(),
                ..Default::default()
            });

        entry.sample_count += 1;
        entry.total_bytes += data.len() as u64;
        entry.avg_value_size = entry.total_bytes as f64 / entry.sample_count as f64;
        if entry.min_value_size == 0 || data.len() < entry.min_value_size {
            entry.min_value_size = data.len();
        }
        if data.len() > entry.max_value_size {
            entry.max_value_size = data.len();
        }
        entry.estimated_entropy = estimate_entropy(data);
        entry.write_count += 1;

        // Trial compression every N samples
        if entry.sample_count <= self.config.samples_before_decision as u64 {
            self.trial_compress(entry, data);
        }

        // Select algorithm once we have enough samples
        if entry.sample_count == self.config.samples_before_decision as u64 {
            entry.selected_algorithm = Some(self.select_algorithm(entry));
        }
    }

    fn trial_compress(&self, stats: &mut PrefixStats, data: &[u8]) {
        let algorithms = [
            CompressionAlgorithm::Lz4,
            CompressionAlgorithm::Zstd,
            CompressionAlgorithm::Flate2,
        ];

        for algo in &algorithms {
            let start = std::time::Instant::now();
            let compressed = compress(*algo, data, &self.config);
            let elapsed = start.elapsed().as_micros() as f64;

            let ratio = if compressed.is_empty() {
                1.0
            } else {
                data.len() as f64 / compressed.len() as f64
            };

            // Running average
            let count = stats.sample_count as f64;
            let prev_ratio = *stats.compression_ratios.get(algo).unwrap_or(&0.0);
            stats
                .compression_ratios
                .insert(*algo, prev_ratio + (ratio - prev_ratio) / count);

            let prev_time = *stats.compression_times_us.get(algo).unwrap_or(&0.0);
            stats
                .compression_times_us
                .insert(*algo, prev_time + (elapsed - prev_time) / count);
        }
    }

    fn select_algorithm(&self, stats: &PrefixStats) -> CompressionAlgorithm {
        if stats.avg_value_size < self.config.min_compress_size as f64 {
            return CompressionAlgorithm::None;
        }

        // High entropy → likely already compressed or random data
        if stats.estimated_entropy > 0.95 {
            return CompressionAlgorithm::None;
        }

        // Score each algorithm: balance ratio and speed
        let mut best = self.config.default_algorithm;
        let mut best_score = f64::MIN;

        for (algo, &ratio) in &stats.compression_ratios {
            if *algo == CompressionAlgorithm::None {
                continue;
            }
            let time = stats.compression_times_us.get(algo).unwrap_or(&1.0);
            // Score = ratio * weight - latency_penalty
            let is_hot = stats.read_count > stats.write_count * 5;
            let speed_weight = if is_hot { 3.0 } else { 1.0 };
            let score = ratio * 2.0 - (time / 1000.0) * speed_weight;

            if score > best_score {
                best_score = score;
                best = *algo;
            }
        }

        best
    }

    /// Get the recommended algorithm for a key prefix.
    pub fn get_algorithm(&self, prefix: &str) -> CompressionAlgorithm {
        let stats = self.prefix_stats.read();
        stats
            .get(prefix)
            .and_then(|s| s.selected_algorithm)
            .unwrap_or(self.config.default_algorithm)
    }

    /// Compress data using the best algorithm for the given prefix.
    pub fn compress_for_prefix(
        &self,
        prefix: &str,
        data: &[u8],
    ) -> (CompressionAlgorithm, Vec<u8>) {
        let algo = self.get_algorithm(prefix);
        self.metrics
            .bytes_before_compression
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        let compressed = compress(algo, data, &self.config);
        self.metrics
            .bytes_after_compression
            .fetch_add(compressed.len() as u64, Ordering::Relaxed);
        self.metrics
            .compressions_total
            .fetch_add(1, Ordering::Relaxed);
        (algo, compressed)
    }

    /// Get recommendations for all known prefixes.
    pub fn recommendations(&self) -> Vec<CompressionRecommendation> {
        let stats = self.prefix_stats.read();
        stats
            .values()
            .filter_map(|s| {
                let algo = s.selected_algorithm?;
                let ratio = s.compression_ratios.get(&algo).copied().unwrap_or(1.0);
                let latency = s.compression_times_us.get(&algo).copied().unwrap_or(0.0);
                Some(CompressionRecommendation {
                    prefix: s.prefix.clone(),
                    recommended: algo,
                    estimated_ratio: ratio,
                    estimated_latency_us: latency,
                    confidence: (s.sample_count as f64 / 100.0).min(1.0),
                    reason: format!(
                        "avg_size={}B entropy={:.2} read/write={}/{}",
                        s.avg_value_size as u64, s.estimated_entropy, s.read_count, s.write_count
                    ),
                })
            })
            .collect()
    }

    /// Get stats for all prefixes.
    pub fn stats(&self) -> Vec<PrefixStats> {
        self.prefix_stats.read().values().cloned().collect()
    }

    /// Returns a reference to the compression metrics.
    pub fn metrics(&self) -> &CompressionMetrics {
        &self.metrics
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_decompress_lz4() {
        let data = b"Hello, world! Hello, world! Hello, world!";
        let config = AdaptiveCompressionConfig::default();
        let compressed = compress(CompressionAlgorithm::Lz4, data, &config);
        let decompressed = decompress(CompressionAlgorithm::Lz4, &compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compress_decompress_flate2() {
        let data = b"Repetitive data repetitive data repetitive data";
        let config = AdaptiveCompressionConfig::default();
        let compressed = compress(CompressionAlgorithm::Flate2, data, &config);
        let decompressed = decompress(CompressionAlgorithm::Flate2, &compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compress_decompress_delta() {
        let data: Vec<u8> = (0..100u8).collect();
        let config = AdaptiveCompressionConfig::default();
        let compressed = compress(CompressionAlgorithm::Delta, &data, &config);
        let decompressed = decompress(CompressionAlgorithm::Delta, &compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compress_none() {
        let data = b"raw data";
        let config = AdaptiveCompressionConfig::default();
        let compressed = compress(CompressionAlgorithm::None, data, &config);
        assert_eq!(compressed, data);
    }

    #[test]
    fn test_entropy_estimation() {
        let zeros = vec![0u8; 1000];
        assert!(estimate_entropy(&zeros) < 0.01);

        let random: Vec<u8> = (0..1000).map(|i| (i * 37 + 13) as u8).collect();
        assert!(estimate_entropy(&random) > 0.5);
    }

    #[test]
    fn test_adaptive_engine_selection() {
        let config = AdaptiveCompressionConfig {
            samples_before_decision: 5,
            ..Default::default()
        };
        let engine = AdaptiveCompressionEngine::new(config);

        // Feed repetitive data → should select a compression algorithm
        for _ in 0..10 {
            let data = b"This is repetitive data that should compress well. ".repeat(10);
            engine.record_sample("cache:", &data);
        }

        let algo = engine.get_algorithm("cache:");
        assert_ne!(algo, CompressionAlgorithm::None);
    }

    #[test]
    fn test_compress_for_prefix() {
        let engine = AdaptiveCompressionEngine::new(AdaptiveCompressionConfig::default());
        let data = b"test data for compression";
        let (algo, compressed) = engine.compress_for_prefix("unknown:", data);
        // Default should be LZ4
        assert_eq!(algo, CompressionAlgorithm::Lz4);
        assert!(!compressed.is_empty());
    }

    #[test]
    fn test_metrics_tracking() {
        let engine = AdaptiveCompressionEngine::new(AdaptiveCompressionConfig::default());
        engine.compress_for_prefix("test:", b"some data here");
        assert_eq!(
            engine.metrics().compressions_total.load(Ordering::Relaxed),
            1
        );
        assert!(engine.metrics().overall_ratio() > 0.0);
    }

    #[test]
    fn test_algorithm_display() {
        assert_eq!(CompressionAlgorithm::Lz4.to_string(), "lz4");
        assert_eq!(CompressionAlgorithm::Zstd.to_string(), "zstd");
    }
}
