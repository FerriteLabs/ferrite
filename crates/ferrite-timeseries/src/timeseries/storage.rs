//! Time Series Storage Backend
//!
//! Provides persistent and in-memory storage for time series data.

use super::compression::{CompressedBlock, Compression, CompressionCodec, GorillaCompressor};
use super::labels::Labels;
use super::query::{QueryResult, TimeSeriesQuery};
use super::sample::Sample;
use super::series::{TimeSeries, TimeSeriesId, TimeSeriesMetadata, TimeSeriesSet};
use super::*;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// Storage configuration
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Maximum samples to keep in memory per series
    pub max_samples_in_memory: usize,
    /// Enable compression
    pub compression_enabled: bool,
    /// Compression codec
    pub compression_codec: CompressionCodec,
    /// Data directory for persistence
    pub data_dir: Option<PathBuf>,
    /// Flush threshold (samples) - flush when this many samples are in memory
    pub flush_threshold: usize,
    /// Compaction threshold (blocks) - compact when this many blocks exist
    pub compaction_threshold: usize,
    /// Block size target for compaction
    pub target_block_size: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            max_samples_in_memory: 100_000,
            compression_enabled: true,
            compression_codec: CompressionCodec::Gorilla,
            data_dir: None,
            flush_threshold: 10_000,
            compaction_threshold: 100,
            target_block_size: 1024 * 1024, // 1MB
        }
    }
}

/// Time series storage engine
pub struct TimeSeriesStorage {
    /// Configuration
    config: StorageConfig,
    /// Series sets by metric name
    series_sets: RwLock<HashMap<String, Arc<TimeSeriesSet>>>,
    /// Series by ID
    series_by_id: RwLock<HashMap<TimeSeriesId, Arc<TimeSeries>>>,
    /// Compressed blocks (for cold data)
    compressed_blocks: RwLock<Vec<CompressedBlock>>,
    /// Storage metrics
    metrics: StorageMetrics,
}

/// Storage metrics
#[derive(Debug, Default)]
pub struct StorageMetrics {
    /// Total series count
    pub series_count: std::sync::atomic::AtomicU64,
    /// Total samples in memory
    pub samples_in_memory: std::sync::atomic::AtomicU64,
    /// Total samples on disk
    pub samples_on_disk: std::sync::atomic::AtomicU64,
    /// Bytes used in memory
    pub bytes_in_memory: std::sync::atomic::AtomicU64,
    /// Bytes on disk
    pub bytes_on_disk: std::sync::atomic::AtomicU64,
}

impl TimeSeriesStorage {
    /// Create new storage
    pub fn new(config: StorageConfig) -> Result<Self> {
        Ok(Self {
            config,
            series_sets: RwLock::new(HashMap::new()),
            series_by_id: RwLock::new(HashMap::new()),
            compressed_blocks: RwLock::new(Vec::new()),
            metrics: StorageMetrics::default(),
        })
    }

    /// Add a sample to a series
    pub fn add_sample(&self, id: &TimeSeriesId, sample: Sample) -> Result<()> {
        let series = self.get_or_create_series(id, None)?;
        series.add_sample(sample);

        self.metrics
            .samples_in_memory
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        Ok(())
    }

    /// Add a sample to a series with associated labels
    pub fn add_sample_with_labels(
        &self,
        id: &TimeSeriesId,
        sample: Sample,
        labels: &Labels,
    ) -> Result<()> {
        let series = self.get_or_create_series(id, Some(labels))?;
        series.add_sample(sample);

        self.metrics
            .samples_in_memory
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        Ok(())
    }

    /// Get or create a series
    fn get_or_create_series(
        &self,
        id: &TimeSeriesId,
        labels: Option<&Labels>,
    ) -> Result<Arc<TimeSeries>> {
        // Check if series exists
        if let Some(series) = self.series_by_id.read().get(id) {
            return Ok(Arc::clone(series));
        }

        // Create new series
        let mut series_sets = self.series_sets.write();
        let mut series_by_id = self.series_by_id.write();

        // Double-check after acquiring write lock
        if let Some(series) = series_by_id.get(id) {
            return Ok(Arc::clone(series));
        }

        // Get or create series set
        let series_set = series_sets
            .entry(id.metric.clone())
            .or_insert_with(|| Arc::new(TimeSeriesSet::new(&id.metric)));

        // Create series with provided labels or empty
        let series_labels = labels.cloned().unwrap_or_else(Labels::empty);
        let series = series_set.get_or_create(id, series_labels);
        series_by_id.insert(id.clone(), Arc::clone(&series));

        self.metrics
            .series_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        Ok(series)
    }

    /// Get a series by ID
    pub fn get_series(&self, id: &TimeSeriesId) -> Option<Arc<TimeSeries>> {
        self.series_by_id.read().get(id).cloned()
    }

    /// Get all series
    pub fn all_series(&self) -> Vec<(TimeSeriesId, Arc<TimeSeries>)> {
        self.series_by_id
            .read()
            .iter()
            .map(|(id, series)| (id.clone(), Arc::clone(series)))
            .collect()
    }

    /// Update samples_in_memory metric after cleanup
    pub fn update_samples_metric(&self, removed: u64) {
        self.metrics
            .samples_in_memory
            .fetch_sub(removed, std::sync::atomic::Ordering::Relaxed);
    }

    /// List all metrics
    pub fn list_metrics(&self) -> Result<Vec<String>> {
        Ok(self.series_sets.read().keys().cloned().collect())
    }

    /// Get metadata for a metric
    pub fn get_metadata(&self, metric: &str) -> Result<TimeSeriesMetadata> {
        let series_sets = self.series_sets.read();
        if let Some(set) = series_sets.get(metric) {
            if let Some(series) = set.all().first() {
                return Ok(series.metadata());
            }
        }
        Err(TimeSeriesError::MetricNotFound(metric.to_string()))
    }

    /// Delete a metric
    pub fn delete_metric(&self, metric: &str) -> Result<()> {
        let mut series_sets = self.series_sets.write();
        let mut series_by_id = self.series_by_id.write();

        if let Some(set) = series_sets.remove(metric) {
            for series in set.all() {
                series_by_id.remove(series.id());
            }
        }

        Ok(())
    }

    /// Execute a query
    pub fn execute_query(&self, query: TimeSeriesQuery) -> Result<QueryResult> {
        let mut results = Vec::new();

        let series_sets = self.series_sets.read();

        // Find matching series
        for (metric_name, series_set) in series_sets.iter() {
            if !query.matches_metric(metric_name) {
                continue;
            }

            for series in series_set.all() {
                if query.matches_labels(series.labels()) {
                    // Get samples in time range
                    let samples = if let Some((start, end)) = query.time_range() {
                        series.get_samples(start, end)
                    } else {
                        series.all_samples()
                    };

                    // Apply aggregation if specified
                    let processed_samples = if let Some(agg) = query.aggregation() {
                        if let Some(interval) = query.step_interval() {
                            series.downsample(interval, agg.to_statistic())
                        } else {
                            samples
                        }
                    } else {
                        samples
                    };

                    results.push(SeriesResult {
                        id: series.id().clone(),
                        labels: series.labels().clone(),
                        samples: processed_samples,
                    });
                }
            }
        }

        // Apply limit if specified
        if let Some(limit) = query.limit() {
            results.truncate(limit);
        }

        let total_samples = results.iter().map(|r| r.samples.len()).sum();
        Ok(QueryResult {
            series: results,
            total_samples,
            query_time_ms: 0, // Would be set by actual timing
        })
    }

    /// Flush data to disk
    ///
    /// This method compresses in-memory data and writes it to disk as compressed blocks.
    /// The block format is:
    /// - Magic bytes (4): "TSDB"
    /// - Version (1): 1
    /// - Block count (4): u32
    /// - For each block:
    ///   - Series ID length (4): u32
    ///   - Series ID (variable)
    ///   - Codec (1): u8
    ///   - Start time (8): i64
    ///   - End time (8): i64
    ///   - Sample count (4): u32
    ///   - Data length (4): u32
    ///   - Compressed data (variable)
    pub fn flush(&self) -> Result<()> {
        let data_dir = match &self.config.data_dir {
            Some(dir) => dir.clone(),
            None => {
                tracing::debug!("No data directory configured, skipping flush");
                return Ok(());
            }
        };

        // Ensure data directory exists
        std::fs::create_dir_all(&data_dir).map_err(|e| {
            TimeSeriesError::StorageError(format!("Failed to create data dir: {}", e))
        })?;

        tracing::info!("Flushing time series storage to disk");

        let compressor = GorillaCompressor::new();
        let mut blocks_written = 0u32;
        let mut total_samples_flushed = 0u64;

        // Collect all series data
        let all_series: Vec<_> = self.all_series();

        // Group samples by series and compress
        let mut compressed_data: Vec<(TimeSeriesId, CompressedBlock)> = Vec::new();

        for (id, series) in &all_series {
            let samples = series.all_samples();
            if samples.is_empty() {
                continue;
            }

            // Compress the samples
            let block = if self.config.compression_enabled {
                compressor.compress(&samples)?
            } else {
                use super::compression::NoCompression;
                NoCompression.compress(&samples)?
            };

            total_samples_flushed += block.sample_count as u64;
            compressed_data.push((id.clone(), block));
        }

        if compressed_data.is_empty() {
            tracing::debug!("No data to flush");
            return Ok(());
        }

        // Write to file
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let filename = format!("tsdb_{}.dat", timestamp);
        let filepath = data_dir.join(&filename);

        let mut file = std::fs::File::create(&filepath)
            .map_err(|e| TimeSeriesError::StorageError(format!("Failed to create file: {}", e)))?;

        // Write header
        file.write_all(b"TSDB")
            .map_err(|e| TimeSeriesError::StorageError(format!("Failed to write magic: {}", e)))?;
        file.write_all(&[1u8]).map_err(|e| {
            TimeSeriesError::StorageError(format!("Failed to write version: {}", e))
        })?; // Version 1
        file.write_all(&(compressed_data.len() as u32).to_le_bytes())
            .map_err(|e| {
                TimeSeriesError::StorageError(format!("Failed to write block count: {}", e))
            })?;

        // Write each block
        for (id, block) in &compressed_data {
            // Series ID
            let id_bytes = id.to_string().into_bytes();
            file.write_all(&(id_bytes.len() as u32).to_le_bytes())
                .map_err(|e| {
                    TimeSeriesError::StorageError(format!("Failed to write ID length: {}", e))
                })?;
            file.write_all(&id_bytes)
                .map_err(|e| TimeSeriesError::StorageError(format!("Failed to write ID: {}", e)))?;

            // Codec
            let codec_byte: u8 = match block.codec {
                CompressionCodec::None => 0,
                CompressionCodec::Gorilla => 1,
                CompressionCodec::DoD => 2,
                CompressionCodec::Simple8b => 3,
                CompressionCodec::Lz4 => 4,
                CompressionCodec::Zstd => 5,
                CompressionCodec::Custom(_) => 255,
            };
            file.write_all(&[codec_byte]).map_err(|e| {
                TimeSeriesError::StorageError(format!("Failed to write codec: {}", e))
            })?;

            // Timestamps
            file.write_all(&block.start_time.as_nanos().to_le_bytes())
                .map_err(|e| {
                    TimeSeriesError::StorageError(format!("Failed to write start time: {}", e))
                })?;
            file.write_all(&block.end_time.as_nanos().to_le_bytes())
                .map_err(|e| {
                    TimeSeriesError::StorageError(format!("Failed to write end time: {}", e))
                })?;

            // Sample count
            file.write_all(&(block.sample_count as u32).to_le_bytes())
                .map_err(|e| {
                    TimeSeriesError::StorageError(format!("Failed to write sample count: {}", e))
                })?;

            // Data
            file.write_all(&(block.data.len() as u32).to_le_bytes())
                .map_err(|e| {
                    TimeSeriesError::StorageError(format!("Failed to write data length: {}", e))
                })?;
            file.write_all(&block.data).map_err(|e| {
                TimeSeriesError::StorageError(format!("Failed to write data: {}", e))
            })?;

            blocks_written += 1;
        }

        file.flush()
            .map_err(|e| TimeSeriesError::StorageError(format!("Failed to flush file: {}", e)))?;

        // Update metrics
        self.metrics
            .samples_on_disk
            .fetch_add(total_samples_flushed, Ordering::Relaxed);
        self.metrics.bytes_on_disk.fetch_add(
            std::fs::metadata(&filepath).map(|m| m.len()).unwrap_or(0),
            Ordering::Relaxed,
        );

        // Add to compressed blocks
        {
            let mut blocks = self.compressed_blocks.write();
            for (_, block) in compressed_data {
                let mut block = block;
                block.id = blocks.len() as u64;
                blocks.push(block);
            }
        }

        tracing::info!(
            "Flushed {} blocks ({} samples) to {}",
            blocks_written,
            total_samples_flushed,
            filename
        );

        Ok(())
    }

    /// Compact storage
    ///
    /// This method performs the following optimizations:
    /// 1. Merges small blocks into larger ones
    /// 2. Removes expired/duplicate data
    /// 3. Re-compresses data for better compression ratios
    pub fn compact(&self) -> Result<()> {
        let data_dir = match &self.config.data_dir {
            Some(dir) => dir.clone(),
            None => {
                tracing::debug!("No data directory configured, skipping compaction");
                return Ok(());
            }
        };

        tracing::info!("Starting time series storage compaction");

        let mut blocks = self.compressed_blocks.write();

        // Skip if not enough blocks to compact
        if blocks.len() < self.config.compaction_threshold {
            tracing::debug!(
                "Only {} blocks, skipping compaction (threshold: {})",
                blocks.len(),
                self.config.compaction_threshold
            );
            return Ok(());
        }

        // Group blocks by series (blocks with overlapping or adjacent time ranges)
        // For simplicity, we'll merge all blocks into larger ones based on total size

        let compressor = GorillaCompressor::new();
        let mut merged_blocks: Vec<CompressedBlock> = Vec::new();
        let mut current_samples: Vec<Sample> = Vec::new();
        let mut bytes_before: usize = 0;
        let mut bytes_after: usize = 0;

        // Sort blocks by start time
        blocks.sort_by(|a, b| a.start_time.as_nanos().cmp(&b.start_time.as_nanos()));

        for block in blocks.iter() {
            bytes_before += block.data.len();

            // Decompress block
            let samples = compressor.decompress(block)?;
            current_samples.extend(samples);

            // If we've accumulated enough samples, create a new block
            let estimated_size = current_samples.len() * 16; // Rough estimate
            if estimated_size >= self.config.target_block_size {
                // Sort by timestamp and remove duplicates
                current_samples.sort_by(|a, b| a.timestamp.as_nanos().cmp(&b.timestamp.as_nanos()));
                current_samples.dedup_by(|a, b| a.timestamp == b.timestamp);

                // Compress
                let new_block = compressor.compress(&current_samples)?;
                bytes_after += new_block.data.len();
                merged_blocks.push(new_block);
                current_samples.clear();
            }
        }

        // Don't forget remaining samples
        if !current_samples.is_empty() {
            current_samples.sort_by(|a, b| a.timestamp.as_nanos().cmp(&b.timestamp.as_nanos()));
            current_samples.dedup_by(|a, b| a.timestamp == b.timestamp);
            let new_block = compressor.compress(&current_samples)?;
            bytes_after += new_block.data.len();
            merged_blocks.push(new_block);
        }

        // Assign new IDs
        for (i, block) in merged_blocks.iter_mut().enumerate() {
            block.id = i as u64;
        }

        let blocks_before = blocks.len();
        let blocks_after = merged_blocks.len();

        // Replace old blocks with merged ones
        *blocks = merged_blocks;

        // Write compacted data to new file
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let filename = format!("tsdb_compact_{}.dat", timestamp);
        let filepath = data_dir.join(&filename);

        let mut file = std::fs::File::create(&filepath).map_err(|e| {
            TimeSeriesError::StorageError(format!("Failed to create compacted file: {}", e))
        })?;

        // Write header
        file.write_all(b"TSDB")
            .map_err(|e| TimeSeriesError::StorageError(format!("Failed to write magic: {}", e)))?;
        file.write_all(&[1u8]).map_err(|e| {
            TimeSeriesError::StorageError(format!("Failed to write version: {}", e))
        })?;
        file.write_all(&(blocks.len() as u32).to_le_bytes())
            .map_err(|e| {
                TimeSeriesError::StorageError(format!("Failed to write block count: {}", e))
            })?;

        // Write blocks (simplified - just data for now)
        for block in blocks.iter() {
            file.write_all(&[1u8]).map_err(|e| {
                TimeSeriesError::StorageError(format!("Failed to write codec: {}", e))
            })?; // Gorilla
            file.write_all(&block.start_time.as_nanos().to_le_bytes())
                .map_err(|e| {
                    TimeSeriesError::StorageError(format!("Failed to write start time: {}", e))
                })?;
            file.write_all(&block.end_time.as_nanos().to_le_bytes())
                .map_err(|e| {
                    TimeSeriesError::StorageError(format!("Failed to write end time: {}", e))
                })?;
            file.write_all(&(block.sample_count as u32).to_le_bytes())
                .map_err(|e| {
                    TimeSeriesError::StorageError(format!("Failed to write sample count: {}", e))
                })?;
            file.write_all(&(block.data.len() as u32).to_le_bytes())
                .map_err(|e| {
                    TimeSeriesError::StorageError(format!("Failed to write data length: {}", e))
                })?;
            file.write_all(&block.data).map_err(|e| {
                TimeSeriesError::StorageError(format!("Failed to write data: {}", e))
            })?;
        }

        file.flush()
            .map_err(|e| TimeSeriesError::StorageError(format!("Failed to flush file: {}", e)))?;

        let compression_ratio = if bytes_after > 0 {
            bytes_before as f64 / bytes_after as f64
        } else {
            1.0
        };

        tracing::info!(
            "Compaction complete: {} -> {} blocks, {:.1}x compression ratio",
            blocks_before,
            blocks_after,
            compression_ratio
        );

        Ok(())
    }

    /// Load data from disk
    ///
    /// Reads previously flushed data files and loads them into memory.
    pub fn load(&self) -> Result<usize> {
        let data_dir = match &self.config.data_dir {
            Some(dir) => dir.clone(),
            None => {
                tracing::debug!("No data directory configured, skipping load");
                return Ok(0);
            }
        };

        if !data_dir.exists() {
            tracing::debug!("Data directory does not exist, nothing to load");
            return Ok(0);
        }

        let compressor = GorillaCompressor::new();
        let mut total_blocks_loaded = 0usize;
        let mut total_samples_loaded = 0usize;

        // Find all TSDB files
        let entries = std::fs::read_dir(&data_dir).map_err(|e| {
            TimeSeriesError::StorageError(format!("Failed to read data dir: {}", e))
        })?;

        for entry in entries {
            let entry = entry.map_err(|e| {
                TimeSeriesError::StorageError(format!("Failed to read entry: {}", e))
            })?;
            let path = entry.path();

            if !path.is_file() {
                continue;
            }
            let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if !filename.starts_with("tsdb_") || !path.extension().is_some_and(|e| e.eq_ignore_ascii_case("dat")) {
                continue;
            }

            tracing::debug!("Loading file: {:?}", path);

            let mut file = std::fs::File::open(&path).map_err(|e| {
                TimeSeriesError::StorageError(format!("Failed to open file: {}", e))
            })?;

            // Read header
            let mut magic = [0u8; 4];
            file.read_exact(&mut magic).map_err(|e| {
                TimeSeriesError::StorageError(format!("Failed to read magic: {}", e))
            })?;
            if &magic != b"TSDB" {
                tracing::warn!("Invalid magic bytes in {:?}, skipping", path);
                continue;
            }

            let mut version = [0u8; 1];
            file.read_exact(&mut version).map_err(|e| {
                TimeSeriesError::StorageError(format!("Failed to read version: {}", e))
            })?;
            if version[0] != 1 {
                tracing::warn!("Unsupported version {} in {:?}, skipping", version[0], path);
                continue;
            }

            let mut block_count_bytes = [0u8; 4];
            file.read_exact(&mut block_count_bytes).map_err(|e| {
                TimeSeriesError::StorageError(format!("Failed to read block count: {}", e))
            })?;
            let block_count = u32::from_le_bytes(block_count_bytes);

            // Read blocks
            for _ in 0..block_count {
                // Series ID
                let mut id_len_bytes = [0u8; 4];
                file.read_exact(&mut id_len_bytes).map_err(|e| {
                    TimeSeriesError::StorageError(format!("Failed to read ID length: {}", e))
                })?;
                let id_len = u32::from_le_bytes(id_len_bytes) as usize;
                let mut id_bytes = vec![0u8; id_len];
                file.read_exact(&mut id_bytes).map_err(|e| {
                    TimeSeriesError::StorageError(format!("Failed to read ID: {}", e))
                })?;
                let id_str = String::from_utf8_lossy(&id_bytes);
                let series_id = TimeSeriesId::from_metric(&id_str);

                // Codec
                let mut codec_byte = [0u8; 1];
                file.read_exact(&mut codec_byte).map_err(|e| {
                    TimeSeriesError::StorageError(format!("Failed to read codec: {}", e))
                })?;

                // Timestamps
                let mut start_time_bytes = [0u8; 8];
                file.read_exact(&mut start_time_bytes).map_err(|e| {
                    TimeSeriesError::StorageError(format!("Failed to read start time: {}", e))
                })?;
                let start_time = Timestamp::from_nanos(i64::from_le_bytes(start_time_bytes));

                let mut end_time_bytes = [0u8; 8];
                file.read_exact(&mut end_time_bytes).map_err(|e| {
                    TimeSeriesError::StorageError(format!("Failed to read end time: {}", e))
                })?;
                let end_time = Timestamp::from_nanos(i64::from_le_bytes(end_time_bytes));

                // Sample count
                let mut sample_count_bytes = [0u8; 4];
                file.read_exact(&mut sample_count_bytes).map_err(|e| {
                    TimeSeriesError::StorageError(format!("Failed to read sample count: {}", e))
                })?;
                let sample_count = u32::from_le_bytes(sample_count_bytes) as usize;

                // Data
                let mut data_len_bytes = [0u8; 4];
                file.read_exact(&mut data_len_bytes).map_err(|e| {
                    TimeSeriesError::StorageError(format!("Failed to read data length: {}", e))
                })?;
                let data_len = u32::from_le_bytes(data_len_bytes) as usize;
                let mut data = vec![0u8; data_len];
                file.read_exact(&mut data).map_err(|e| {
                    TimeSeriesError::StorageError(format!("Failed to read data: {}", e))
                })?;

                // Create compressed block
                let block = CompressedBlock {
                    id: total_blocks_loaded as u64,
                    codec: CompressionCodec::Gorilla,
                    start_time,
                    end_time,
                    sample_count,
                    data,
                    original_size: sample_count * 16,
                };

                // Decompress and add to storage
                let samples = compressor.decompress(&block)?;
                for sample in &samples {
                    self.add_sample(&series_id, sample.clone())?;
                }

                // Store compressed block
                self.compressed_blocks.write().push(block);

                total_blocks_loaded += 1;
                total_samples_loaded += sample_count;
            }
        }

        tracing::info!(
            "Loaded {} blocks ({} samples) from disk",
            total_blocks_loaded,
            total_samples_loaded
        );

        Ok(total_blocks_loaded)
    }

    /// Get storage statistics
    pub fn stats(&self) -> StorageStats {
        StorageStats {
            series_count: self
                .metrics
                .series_count
                .load(std::sync::atomic::Ordering::Relaxed),
            samples_in_memory: self
                .metrics
                .samples_in_memory
                .load(std::sync::atomic::Ordering::Relaxed),
            samples_on_disk: self
                .metrics
                .samples_on_disk
                .load(std::sync::atomic::Ordering::Relaxed),
            bytes_in_memory: self
                .metrics
                .bytes_in_memory
                .load(std::sync::atomic::Ordering::Relaxed),
            bytes_on_disk: self
                .metrics
                .bytes_on_disk
                .load(std::sync::atomic::Ordering::Relaxed),
            compressed_blocks: self.compressed_blocks.read().len(),
        }
    }
}

/// Result from a series query
#[derive(Debug, Clone)]
pub struct SeriesResult {
    /// Series ID
    pub id: TimeSeriesId,
    /// Labels
    pub labels: Labels,
    /// Samples
    pub samples: Vec<Sample>,
}

/// Storage statistics
#[derive(Debug, Clone)]
pub struct StorageStats {
    /// Number of series
    pub series_count: u64,
    /// Samples in memory
    pub samples_in_memory: u64,
    /// Samples on disk
    pub samples_on_disk: u64,
    /// Memory bytes used
    pub bytes_in_memory: u64,
    /// Disk bytes used
    pub bytes_on_disk: u64,
    /// Number of compressed blocks
    pub compressed_blocks: usize,
}

/// Storage tier for tiered storage
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StorageTier {
    /// Hot tier - recent data in memory
    #[default]
    Hot,
    /// Warm tier - older data, possibly compressed
    Warm,
    /// Cold tier - old data on disk
    Cold,
    /// Archive tier - very old data, highly compressed
    Archive,
}



/// Storage block containing samples
#[derive(Debug, Clone)]
pub struct StorageBlock {
    /// Block ID
    pub id: u64,
    /// Start timestamp
    pub start_time: Timestamp,
    /// End timestamp
    pub end_time: Timestamp,
    /// Storage tier
    pub tier: StorageTier,
    /// Sample count
    pub sample_count: usize,
    /// Block size in bytes
    pub size_bytes: usize,
    /// Is compressed
    pub compressed: bool,
}

impl StorageBlock {
    /// Create a new storage block
    pub fn new(id: u64, start_time: Timestamp, end_time: Timestamp) -> Self {
        Self {
            id,
            start_time,
            end_time,
            tier: StorageTier::Hot,
            sample_count: 0,
            size_bytes: 0,
            compressed: false,
        }
    }

    /// Get block duration
    pub fn duration(&self) -> std::time::Duration {
        self.end_time.diff(&self.start_time)
    }

    /// Check if block contains timestamp
    pub fn contains(&self, ts: Timestamp) -> bool {
        ts.as_nanos() >= self.start_time.as_nanos() && ts.as_nanos() <= self.end_time.as_nanos()
    }

    /// Check if block overlaps with time range
    pub fn overlaps(&self, start: Timestamp, end: Timestamp) -> bool {
        self.start_time.as_nanos() <= end.as_nanos() && self.end_time.as_nanos() >= start.as_nanos()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_creation() {
        let config = StorageConfig::default();
        let storage = TimeSeriesStorage::new(config).unwrap();

        let stats = storage.stats();
        assert_eq!(stats.series_count, 0);
    }

    #[test]
    fn test_add_sample() {
        let config = StorageConfig::default();
        let storage = TimeSeriesStorage::new(config).unwrap();

        let id = TimeSeriesId::from_metric("cpu.usage");
        let sample = Sample::now(42.0);

        storage.add_sample(&id, sample).unwrap();

        let stats = storage.stats();
        assert_eq!(stats.series_count, 1);
        assert_eq!(stats.samples_in_memory, 1);
    }

    #[test]
    fn test_get_series() {
        let config = StorageConfig::default();
        let storage = TimeSeriesStorage::new(config).unwrap();

        let id = TimeSeriesId::from_metric("memory.usage");
        let sample = Sample::now(100.0);

        storage.add_sample(&id, sample).unwrap();

        let series = storage.get_series(&id);
        assert!(series.is_some());
        assert_eq!(series.unwrap().len(), 1);
    }

    #[test]
    fn test_list_metrics() {
        let config = StorageConfig::default();
        let storage = TimeSeriesStorage::new(config).unwrap();

        storage
            .add_sample(&TimeSeriesId::from_metric("cpu"), Sample::now(50.0))
            .unwrap();
        storage
            .add_sample(&TimeSeriesId::from_metric("memory"), Sample::now(60.0))
            .unwrap();

        let metrics = storage.list_metrics().unwrap();
        assert_eq!(metrics.len(), 2);
        assert!(metrics.contains(&"cpu".to_string()));
        assert!(metrics.contains(&"memory".to_string()));
    }

    #[test]
    fn test_delete_metric() {
        let config = StorageConfig::default();
        let storage = TimeSeriesStorage::new(config).unwrap();

        let id = TimeSeriesId::from_metric("test");
        storage.add_sample(&id, Sample::now(42.0)).unwrap();

        assert!(storage.get_series(&id).is_some());

        storage.delete_metric("test").unwrap();

        assert!(storage.get_series(&id).is_none());
    }

    #[test]
    fn test_storage_block() {
        let block = StorageBlock::new(1, Timestamp::from_secs(1000), Timestamp::from_secs(2000));

        assert!(block.contains(Timestamp::from_secs(1500)));
        assert!(!block.contains(Timestamp::from_secs(500)));

        assert!(block.overlaps(Timestamp::from_secs(1500), Timestamp::from_secs(2500),));
    }
}
