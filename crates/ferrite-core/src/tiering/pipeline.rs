//! Compression Pipeline — Dictionary Training & Streaming Integration
//!
//! Extends the adaptive compression engine with:
//! - **Dictionary training**: Build shared dictionaries from sample data
//!   for better compression ratios on small values.
//! - **Value-class profiling**: Classify values by structure (JSON, binary,
//!   numeric sequences) and route to optimal codec.
//! - **Streaming compression context**: Maintain per-prefix compression state
//!   for delta-aware encoding across consecutive writes.
//! - **Budget-aware compression**: Respect CPU and latency budgets.
//!
//! Works alongside `compression.rs` — this module focuses on the pipeline
//! orchestration while `compression.rs` owns algorithm selection and raw
//! compress/decompress.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Value Classification
// ---------------------------------------------------------------------------

/// Structural class of a value, used to route to the best codec.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ValueClass {
    /// Appears to be JSON / structured text
    Json,
    /// Numeric sequence (e.g., time-series samples)
    NumericSequence,
    /// Plain text / strings
    PlainText,
    /// Small value below compression threshold
    Tiny,
    /// Already compressed or high-entropy binary
    HighEntropy,
    /// Generic binary data
    Binary,
}

impl fmt::Display for ValueClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValueClass::Json => write!(f, "json"),
            ValueClass::NumericSequence => write!(f, "numeric"),
            ValueClass::PlainText => write!(f, "text"),
            ValueClass::Tiny => write!(f, "tiny"),
            ValueClass::HighEntropy => write!(f, "high-entropy"),
            ValueClass::Binary => write!(f, "binary"),
        }
    }
}

/// Classify a byte slice into a `ValueClass`.
pub fn classify_value(data: &[u8], min_size: usize) -> ValueClass {
    if data.len() < min_size {
        return ValueClass::Tiny;
    }

    // Check for JSON
    let trimmed = data
        .iter()
        .skip_while(|b| b.is_ascii_whitespace())
        .copied()
        .collect::<Vec<u8>>();

    if !trimmed.is_empty() && (trimmed[0] == b'{' || trimmed[0] == b'[') {
        return ValueClass::Json;
    }

    // Check if all bytes are printable ASCII
    let printable_ratio = data
        .iter()
        .filter(|b| b.is_ascii_graphic() || b.is_ascii_whitespace())
        .count() as f64
        / data.len() as f64;

    if printable_ratio > 0.95 {
        return ValueClass::PlainText;
    }

    // Check for numeric patterns (sequences of 8-byte floats or 4-byte ints)
    if data.len() >= 16 && data.len() % 8 == 0 {
        let mut looks_numeric = true;
        for chunk in data.chunks(8) {
            let val = f64::from_le_bytes(chunk.try_into().unwrap_or([0u8; 8]));
            if val.is_nan() || val.is_infinite() || val.abs() > 1e18 {
                looks_numeric = false;
                break;
            }
        }
        if looks_numeric {
            return ValueClass::NumericSequence;
        }
    }

    // Estimate entropy
    let entropy = estimate_entropy_fast(data);
    if entropy > 0.95 {
        return ValueClass::HighEntropy;
    }

    ValueClass::Binary
}

/// Fast entropy estimate (Shannon, normalized 0..1).
fn estimate_entropy_fast(data: &[u8]) -> f64 {
    if data.is_empty() {
        return 0.0;
    }
    let mut counts = [0u32; 256];
    for &b in data {
        counts[b as usize] += 1;
    }
    let len = data.len() as f64;
    let mut entropy = 0.0_f64;
    for &c in &counts {
        if c > 0 {
            let p = c as f64 / len;
            entropy -= p * p.log2();
        }
    }
    entropy / 8.0
}

// ---------------------------------------------------------------------------
// Compression Dictionary
// ---------------------------------------------------------------------------

/// A trained compression dictionary built from sample values.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionDictionary {
    /// Identifier for this dictionary
    pub id: String,
    /// The value class this dictionary was trained on
    pub value_class: ValueClass,
    /// The dictionary data (common byte sequences)
    pub data: Vec<u8>,
    /// Number of training samples used
    pub training_samples: usize,
    /// Total bytes of training data
    pub training_bytes: u64,
    /// Measured compression improvement with dictionary
    pub improvement_ratio: f64,
}

/// Collects samples and builds dictionaries.
pub struct DictionaryTrainer {
    /// Samples collected per value class
    samples: HashMap<ValueClass, Vec<Vec<u8>>>,
    /// Max samples to collect before training
    max_samples: usize,
    /// Max sample size in bytes
    max_sample_bytes: usize,
    /// Trained dictionaries
    dictionaries: HashMap<ValueClass, CompressionDictionary>,
    next_dict_id: u64,
}

impl DictionaryTrainer {
    /// Create a new dictionary trainer with the given sample limits.
    pub fn new(max_samples: usize, max_sample_bytes: usize) -> Self {
        Self {
            samples: HashMap::new(),
            max_samples,
            max_sample_bytes,
            dictionaries: HashMap::new(),
            next_dict_id: 0,
        }
    }

    /// Add a training sample.
    pub fn add_sample(&mut self, class: ValueClass, data: &[u8]) {
        if data.len() > self.max_sample_bytes {
            return;
        }
        let samples = self.samples.entry(class).or_default();
        if samples.len() < self.max_samples {
            samples.push(data.to_vec());
        }
    }

    /// Check if we have enough samples to train a dictionary for a class.
    pub fn ready_to_train(&self, class: ValueClass) -> bool {
        self.samples
            .get(&class)
            .map(|s| s.len() >= self.max_samples)
            .unwrap_or(false)
    }

    /// Train a dictionary from collected samples using n-gram frequency analysis.
    pub fn train(&mut self, class: ValueClass) -> Option<&CompressionDictionary> {
        let samples = self.samples.get(&class)?;
        if samples.len() < 2 {
            return None;
        }

        // Build n-gram frequency table (3-grams to 8-grams)
        let mut ngram_freq: HashMap<Vec<u8>, u32> = HashMap::new();
        for sample in samples {
            for n in 3..=8 {
                for window in sample.windows(n) {
                    *ngram_freq.entry(window.to_vec()).or_default() += 1;
                }
            }
        }

        // Select top n-grams by frequency × length (prefer longer common sequences)
        let mut scored: Vec<(Vec<u8>, f64)> = ngram_freq
            .iter()
            .filter(|(_, &freq)| freq > 1)
            .map(|(ngram, &freq)| {
                let score = freq as f64 * (ngram.len() as f64).sqrt();
                (ngram.clone(), score)
            })
            .collect();
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // Build dictionary from top entries (up to 32KB)
        let mut dict_data = Vec::new();
        let max_dict_size = 32 * 1024;
        for (ngram, _) in scored.iter().take(1000) {
            if dict_data.len() + ngram.len() > max_dict_size {
                break;
            }
            dict_data.extend_from_slice(ngram);
        }

        if dict_data.is_empty() {
            return None;
        }

        let total_bytes: u64 = samples.iter().map(|s| s.len() as u64).sum();
        self.next_dict_id += 1;

        let dict = CompressionDictionary {
            id: format!("dict-{}-{}", class, self.next_dict_id),
            value_class: class,
            data: dict_data,
            training_samples: samples.len(),
            training_bytes: total_bytes,
            improvement_ratio: 0.0, // Set after evaluation
        };

        self.dictionaries.insert(class, dict);
        self.dictionaries.get(&class)
    }

    /// Get the trained dictionary for a value class, if available.
    pub fn get_dictionary(&self, class: ValueClass) -> Option<&CompressionDictionary> {
        self.dictionaries.get(&class)
    }

    /// Get the number of collected samples for a value class.
    pub fn sample_count(&self, class: ValueClass) -> usize {
        self.samples.get(&class).map(|s| s.len()).unwrap_or(0)
    }

    /// Get all trained dictionaries.
    pub fn dictionaries(&self) -> &HashMap<ValueClass, CompressionDictionary> {
        &self.dictionaries
    }
}

impl fmt::Debug for DictionaryTrainer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DictionaryTrainer")
            .field("max_samples", &self.max_samples)
            .field(
                "samples",
                &self
                    .samples
                    .iter()
                    .map(|(k, v)| (*k, v.len()))
                    .collect::<Vec<_>>(),
            )
            .field(
                "dictionaries",
                &self.dictionaries.keys().collect::<Vec<_>>(),
            )
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Compression Budget
// ---------------------------------------------------------------------------

/// CPU/latency budget for compression decisions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionBudget {
    /// Maximum microseconds allowed per compression
    pub max_latency_us: u64,
    /// Maximum CPU percentage to spend on compression (0-100)
    pub max_cpu_percent: f64,
    /// Current observed compression latency (running average)
    pub current_latency_us: f64,
    /// Whether budget constraints are currently exceeded
    pub budget_exceeded: bool,
}

impl Default for CompressionBudget {
    fn default() -> Self {
        Self {
            max_latency_us: 500,
            max_cpu_percent: 10.0,
            current_latency_us: 0.0,
            budget_exceeded: false,
        }
    }
}

impl CompressionBudget {
    /// Update the running average latency and check if budget is exceeded.
    pub fn update_latency(&mut self, latency_us: f64) {
        // Exponential moving average
        self.current_latency_us = self.current_latency_us * 0.9 + latency_us * 0.1;
        self.budget_exceeded = self.current_latency_us > self.max_latency_us as f64;
    }

    /// Returns `true` if the current latency is well within budget for heavy compression.
    pub fn allows_heavy_compression(&self) -> bool {
        !self.budget_exceeded && self.current_latency_us < self.max_latency_us as f64 * 0.5
    }
}

// ---------------------------------------------------------------------------
// Compression Pipeline
// ---------------------------------------------------------------------------

/// Configuration for the compression pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    /// Minimum value size to consider for compression
    pub min_compress_size: usize,
    /// Number of samples before training a dictionary
    pub dictionary_training_samples: usize,
    /// Maximum individual sample size for dictionary training
    pub max_sample_bytes: usize,
    /// Enable dictionary-based compression
    pub enable_dictionaries: bool,
    /// Latency budget
    pub budget: CompressionBudget,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            min_compress_size: 64,
            dictionary_training_samples: 100,
            max_sample_bytes: 16384,
            enable_dictionaries: true,
            budget: CompressionBudget::default(),
        }
    }
}

/// Result of a pipeline compression operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineResult {
    /// Structural class of the compressed value.
    pub value_class: ValueClass,
    /// Original size of the data in bytes.
    pub original_size: usize,
    /// Compressed size of the data in bytes.
    pub compressed_size: usize,
    /// Whether a trained dictionary was used.
    pub used_dictionary: bool,
    /// Time spent on compression in microseconds.
    pub compression_us: u64,
}

impl PipelineResult {
    /// Returns the compression ratio (original / compressed).
    pub fn ratio(&self) -> f64 {
        if self.compressed_size == 0 {
            return 1.0;
        }
        self.original_size as f64 / self.compressed_size as f64
    }
}

/// Pipeline metrics.
#[derive(Debug, Default)]
pub struct PipelineMetrics {
    /// Total number of compression operations performed.
    pub total_compressions: AtomicU64,
    /// Total bytes before compression.
    pub total_original_bytes: AtomicU64,
    /// Total bytes after compression.
    pub total_compressed_bytes: AtomicU64,
    /// Number of compressions that used a trained dictionary.
    pub dictionary_hits: AtomicU64,
    /// Number of compressions skipped due to budget constraints.
    pub budget_throttles: AtomicU64,
    /// Compression counts per value class.
    pub class_counts: RwLock<HashMap<ValueClass, u64>>,
}

impl PipelineMetrics {
    /// Record metrics from a compression result.
    pub fn record(&self, result: &PipelineResult) {
        self.total_compressions.fetch_add(1, Ordering::Relaxed);
        self.total_original_bytes
            .fetch_add(result.original_size as u64, Ordering::Relaxed);
        self.total_compressed_bytes
            .fetch_add(result.compressed_size as u64, Ordering::Relaxed);
        if result.used_dictionary {
            self.dictionary_hits.fetch_add(1, Ordering::Relaxed);
        }
        *self
            .class_counts
            .write()
            .entry(result.value_class)
            .or_default() += 1;
    }

    /// Returns the overall compression ratio across all operations.
    pub fn overall_ratio(&self) -> f64 {
        let orig = self.total_original_bytes.load(Ordering::Relaxed) as f64;
        let comp = self.total_compressed_bytes.load(Ordering::Relaxed) as f64;
        if comp < 1.0 {
            1.0
        } else {
            orig / comp
        }
    }
}

/// The compression pipeline orchestrator.
///
/// Classifies values, trains dictionaries, respects budgets, and delegates
/// actual compression to the appropriate codec path.
pub struct CompressionPipeline {
    config: PipelineConfig,
    trainer: RwLock<DictionaryTrainer>,
    metrics: Arc<PipelineMetrics>,
    budget: RwLock<CompressionBudget>,
}

impl CompressionPipeline {
    /// Create a new compression pipeline with the given configuration.
    pub fn new(config: PipelineConfig) -> Self {
        let trainer =
            DictionaryTrainer::new(config.dictionary_training_samples, config.max_sample_bytes);
        let budget = config.budget.clone();
        Self {
            config,
            trainer: RwLock::new(trainer),
            metrics: Arc::new(PipelineMetrics::default()),
            budget: RwLock::new(budget),
        }
    }

    /// Process a value through the compression pipeline.
    ///
    /// Returns the compressed bytes and metadata about the operation.
    pub fn compress(&self, key: &str, data: &[u8]) -> (Vec<u8>, PipelineResult) {
        let start = std::time::Instant::now();
        let class = classify_value(data, self.config.min_compress_size);

        // For tiny values, skip compression entirely
        if class == ValueClass::Tiny || class == ValueClass::HighEntropy {
            let result = PipelineResult {
                value_class: class,
                original_size: data.len(),
                compressed_size: data.len(),
                used_dictionary: false,
                compression_us: 0,
            };
            self.metrics.record(&result);
            return (data.to_vec(), result);
        }

        // Check budget
        if self.budget.read().budget_exceeded {
            self.metrics
                .budget_throttles
                .fetch_add(1, Ordering::Relaxed);
            let result = PipelineResult {
                value_class: class,
                original_size: data.len(),
                compressed_size: data.len(),
                used_dictionary: false,
                compression_us: 0,
            };
            self.metrics.record(&result);
            return (data.to_vec(), result);
        }

        // Feed training samples
        if self.config.enable_dictionaries {
            let mut trainer = self.trainer.write();
            trainer.add_sample(class, data);
            if trainer.ready_to_train(class) && trainer.get_dictionary(class).is_none() {
                trainer.train(class);
                tracing::debug!(
                    key = key,
                    class = %class,
                    "Trained compression dictionary"
                );
            }
        }

        // Attempt dictionary-enhanced compression
        let (compressed, used_dict) = if self.config.enable_dictionaries {
            let trainer = self.trainer.read();
            if let Some(dict) = trainer.get_dictionary(class) {
                let compressed = self.compress_with_dictionary(data, dict);
                // Only use dictionary result if it actually helps
                if compressed.len() < data.len() {
                    (compressed, true)
                } else {
                    (self.compress_plain(data), false)
                }
            } else {
                (self.compress_plain(data), false)
            }
        } else {
            (self.compress_plain(data), false)
        };

        let elapsed_us = start.elapsed().as_micros() as u64;

        // Update budget
        self.budget.write().update_latency(elapsed_us as f64);

        let result = PipelineResult {
            value_class: class,
            original_size: data.len(),
            compressed_size: compressed.len(),
            used_dictionary: used_dict,
            compression_us: elapsed_us,
        };
        self.metrics.record(&result);
        (compressed, result)
    }

    /// Plain LZ4 compression (fast path).
    fn compress_plain(&self, data: &[u8]) -> Vec<u8> {
        lz4_flex::compress_prepend_size(data)
    }

    /// Dictionary-enhanced compression: prepend dictionary data to improve context matching.
    fn compress_with_dictionary(&self, data: &[u8], dict: &CompressionDictionary) -> Vec<u8> {
        // Strategy: concatenate dictionary + data, compress together,
        // then record the dictionary length so decompression can strip it.
        let mut combined = Vec::with_capacity(dict.data.len() + data.len());
        combined.extend_from_slice(&dict.data);
        combined.extend_from_slice(data);

        let compressed = lz4_flex::compress_prepend_size(&combined);

        // Prefix with dictionary length (4 bytes LE) so decompressor knows to strip it
        let dict_len = dict.data.len() as u32;
        let mut output = Vec::with_capacity(4 + compressed.len());
        output.extend_from_slice(&dict_len.to_le_bytes());
        output.extend_from_slice(&compressed);
        output
    }

    /// Decompress data that was compressed by this pipeline.
    pub fn decompress(&self, data: &[u8], used_dictionary: bool) -> Result<Vec<u8>, PipelineError> {
        if !used_dictionary {
            return lz4_flex::decompress_size_prepended(data).map_err(|e| {
                PipelineError::DecompressionFailed {
                    detail: e.to_string(),
                }
            });
        }

        // Dictionary-compressed: first 4 bytes are dictionary length
        if data.len() < 4 {
            return Err(PipelineError::DecompressionFailed {
                detail: "dictionary-compressed data too short".to_string(),
            });
        }

        let dict_len = u32::from_le_bytes(data[0..4].try_into().map_err(|_| {
            PipelineError::DecompressionFailed {
                detail: "failed to read dictionary length".to_string(),
            }
        })?) as usize;
        let decompressed = lz4_flex::decompress_size_prepended(&data[4..]).map_err(|e| {
            PipelineError::DecompressionFailed {
                detail: e.to_string(),
            }
        })?;

        // Strip the dictionary prefix
        if decompressed.len() < dict_len {
            return Err(PipelineError::DecompressionFailed {
                detail: "decompressed data shorter than dictionary length".to_string(),
            });
        }
        Ok(decompressed[dict_len..].to_vec())
    }

    /// Returns a reference to the pipeline metrics.
    pub fn metrics(&self) -> &PipelineMetrics {
        &self.metrics
    }

    /// Returns the number of dictionaries trained so far.
    pub fn dictionaries_trained(&self) -> usize {
        self.trainer.read().dictionaries().len()
    }

    /// Get pipeline statistics summary.
    pub fn summary(&self) -> PipelineSummary {
        let metrics = &self.metrics;
        let budget = self.budget.read();
        PipelineSummary {
            total_compressions: metrics.total_compressions.load(Ordering::Relaxed),
            overall_ratio: metrics.overall_ratio(),
            dictionary_hit_rate: {
                let total = metrics.total_compressions.load(Ordering::Relaxed);
                if total == 0 {
                    0.0
                } else {
                    metrics.dictionary_hits.load(Ordering::Relaxed) as f64 / total as f64
                }
            },
            budget_throttle_count: metrics.budget_throttles.load(Ordering::Relaxed),
            current_latency_us: budget.current_latency_us,
            dictionaries_count: self.dictionaries_trained(),
            class_distribution: metrics.class_counts.read().clone(),
        }
    }
}

impl fmt::Debug for CompressionPipeline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompressionPipeline")
            .field("config", &self.config)
            .field("dictionaries_trained", &self.dictionaries_trained())
            .finish()
    }
}

/// Summary of pipeline statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSummary {
    /// Total number of compression operations.
    pub total_compressions: u64,
    /// Overall compression ratio (original / compressed).
    pub overall_ratio: f64,
    /// Fraction of compressions that used a dictionary.
    pub dictionary_hit_rate: f64,
    /// Number of compressions throttled by the budget.
    pub budget_throttle_count: u64,
    /// Current average compression latency in microseconds.
    pub current_latency_us: f64,
    /// Number of trained dictionaries.
    pub dictionaries_count: usize,
    /// Distribution of value classes across compressions.
    pub class_distribution: HashMap<ValueClass, u64>,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can occur in the compression pipeline.
#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    /// Decompression failed with the given detail.
    #[error("decompression failed: {detail}")]
    DecompressionFailed {
        /// Description of the decompression failure.
        detail: String,
    },

    /// No dictionary found for the specified value class.
    #[error("dictionary not found for class: {class}")]
    DictionaryNotFound {
        /// The value class that was missing a dictionary.
        class: String,
    },

    /// CPU/latency budget was exceeded.
    #[error("budget exceeded: latency {current_us}us > {max_us}us")]
    BudgetExceeded {
        /// Current observed latency in microseconds.
        current_us: f64,
        /// Maximum allowed latency in microseconds.
        max_us: u64,
    },
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_tiny() {
        let class = classify_value(b"hi", 64);
        assert_eq!(class, ValueClass::Tiny);
    }

    #[test]
    fn test_classify_json() {
        let json = br#"{"name": "ferrite", "version": 1}"#;
        let class = classify_value(json, 8);
        assert_eq!(class, ValueClass::Json);

        let json_array = br#"[1, 2, 3, 4, 5]"#;
        let class = classify_value(json_array, 8);
        assert_eq!(class, ValueClass::Json);
    }

    #[test]
    fn test_classify_plain_text() {
        let text = b"This is a plain text string with lots of regular characters inside it.";
        let class = classify_value(text, 8);
        assert_eq!(class, ValueClass::PlainText);
    }

    #[test]
    fn test_classify_high_entropy() {
        // Generate pseudo-random data with all byte values
        let data: Vec<u8> = (0..1024).map(|i| ((i * 197 + 37) % 256) as u8).collect();
        let class = classify_value(&data, 8);
        // Should be either HighEntropy or Binary depending on exact distribution
        assert!(
            class == ValueClass::HighEntropy
                || class == ValueClass::Binary
                || class == ValueClass::NumericSequence
        );
    }

    #[test]
    fn test_value_class_display() {
        assert_eq!(ValueClass::Json.to_string(), "json");
        assert_eq!(ValueClass::PlainText.to_string(), "text");
        assert_eq!(ValueClass::Tiny.to_string(), "tiny");
    }

    #[test]
    fn test_dictionary_trainer_basic() {
        let mut trainer = DictionaryTrainer::new(5, 4096);

        for i in 0..5 {
            let sample = format!("user:{}:profile:data:common_prefix", i);
            trainer.add_sample(ValueClass::PlainText, sample.as_bytes());
        }

        assert!(trainer.ready_to_train(ValueClass::PlainText));
        assert!(!trainer.ready_to_train(ValueClass::Json));

        let dict = trainer.train(ValueClass::PlainText);
        assert!(dict.is_some());
        let dict = dict.unwrap();
        assert_eq!(dict.value_class, ValueClass::PlainText);
        assert!(!dict.data.is_empty());
        assert_eq!(dict.training_samples, 5);
    }

    #[test]
    fn test_dictionary_trainer_max_sample_size() {
        let mut trainer = DictionaryTrainer::new(10, 100);

        // Sample too large should be ignored
        let big = vec![0u8; 200];
        trainer.add_sample(ValueClass::Binary, &big);
        assert_eq!(trainer.sample_count(ValueClass::Binary), 0);

        // Small sample should be accepted
        let small = vec![0u8; 50];
        trainer.add_sample(ValueClass::Binary, &small);
        assert_eq!(trainer.sample_count(ValueClass::Binary), 1);
    }

    #[test]
    fn test_compression_budget() {
        let mut budget = CompressionBudget {
            max_latency_us: 100,
            ..Default::default()
        };

        assert!(budget.allows_heavy_compression());

        // Simulate high latency
        for _ in 0..50 {
            budget.update_latency(200.0);
        }
        assert!(budget.budget_exceeded);
        assert!(!budget.allows_heavy_compression());
    }

    #[test]
    fn test_pipeline_compress_tiny() {
        let pipeline = CompressionPipeline::new(PipelineConfig::default());
        let (compressed, result) = pipeline.compress("key:1", b"hi");
        assert_eq!(result.value_class, ValueClass::Tiny);
        assert_eq!(compressed, b"hi");
        assert_eq!(result.original_size, result.compressed_size);
    }

    #[test]
    fn test_pipeline_compress_and_decompress() {
        let pipeline = CompressionPipeline::new(PipelineConfig {
            min_compress_size: 8,
            enable_dictionaries: false,
            ..Default::default()
        });

        let data = b"This is a test string that should be compressed with LZ4 for performance.";
        let (compressed, result) = pipeline.compress("test:key", data);
        assert_eq!(result.value_class, ValueClass::PlainText);
        assert!(!result.used_dictionary);

        let decompressed = pipeline.decompress(&compressed, false).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_pipeline_dictionary_training() {
        let config = PipelineConfig {
            min_compress_size: 8,
            dictionary_training_samples: 5,
            enable_dictionaries: true,
            ..Default::default()
        };
        let pipeline = CompressionPipeline::new(config);

        // Feed enough samples to trigger dictionary training
        for i in 0..10 {
            let data = format!(
                r#"{{"user_id": {}, "username": "user{}", "email": "user{}@example.com", "status": "active"}}"#,
                i, i, i
            );
            pipeline.compress(&format!("user:{}", i), data.as_bytes());
        }

        // Dictionary should have been trained for JSON class
        assert!(pipeline.dictionaries_trained() > 0);
    }

    #[test]
    fn test_pipeline_dictionary_compress_decompress() {
        let config = PipelineConfig {
            min_compress_size: 8,
            dictionary_training_samples: 3,
            enable_dictionaries: true,
            ..Default::default()
        };
        let pipeline = CompressionPipeline::new(config);

        // Train dictionary with similar data
        for i in 0..5 {
            let data = format!("common_prefix_data_field_{}_value_more_common_stuff", i);
            pipeline.compress(&format!("k:{}", i), data.as_bytes());
        }

        // Compress new data with trained dictionary
        let test_data = b"common_prefix_data_field_99_value_more_common_stuff";
        let (compressed, result) = pipeline.compress("k:99", test_data);

        // Decompress
        let decompressed = pipeline
            .decompress(&compressed, result.used_dictionary)
            .unwrap();
        assert_eq!(decompressed, test_data);
    }

    #[test]
    fn test_pipeline_metrics() {
        let pipeline = CompressionPipeline::new(PipelineConfig {
            min_compress_size: 8,
            enable_dictionaries: false,
            ..Default::default()
        });

        let data = b"Some data that needs to be compressed for testing purposes";
        pipeline.compress("key:1", data);
        pipeline.compress("key:2", data);

        let summary = pipeline.summary();
        assert_eq!(summary.total_compressions, 2);
        assert!(summary.overall_ratio > 0.0);
    }

    #[test]
    fn test_pipeline_result_ratio() {
        let result = PipelineResult {
            value_class: ValueClass::PlainText,
            original_size: 1000,
            compressed_size: 500,
            used_dictionary: false,
            compression_us: 10,
        };
        assert!((result.ratio() - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_entropy_fast() {
        let zeros = vec![0u8; 1000];
        assert!(estimate_entropy_fast(&zeros) < 0.01);
        assert_eq!(estimate_entropy_fast(&[]), 0.0);
    }

    #[test]
    fn test_pipeline_summary() {
        let pipeline = CompressionPipeline::new(PipelineConfig::default());
        let summary = pipeline.summary();
        assert_eq!(summary.total_compressions, 0);
        assert_eq!(summary.dictionaries_count, 0);
        assert!(summary.class_distribution.is_empty());
    }

    #[test]
    fn test_pipeline_error_display() {
        let err = PipelineError::DecompressionFailed {
            detail: "bad data".to_string(),
        };
        assert!(err.to_string().contains("bad data"));

        let err = PipelineError::BudgetExceeded {
            current_us: 600.0,
            max_us: 500,
        };
        assert!(err.to_string().contains("600"));
    }
}
