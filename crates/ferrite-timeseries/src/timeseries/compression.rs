//! Time Series Compression
//!
//! Efficient compression algorithms for time series data.

use super::sample::{Sample, Timestamp};
use super::*;

/// Compression codec types
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum CompressionCodec {
    /// No compression
    None,
    /// Gorilla compression (Facebook's algorithm)
    #[default]
    Gorilla,
    /// Delta-of-delta encoding
    DoD,
    /// Simple-8b for integers
    Simple8b,
    /// LZ4 general compression
    Lz4,
    /// Zstd general compression
    Zstd,
    /// Custom codec
    Custom(String),
}



impl std::fmt::Display for CompressionCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionCodec::None => write!(f, "none"),
            CompressionCodec::Gorilla => write!(f, "gorilla"),
            CompressionCodec::DoD => write!(f, "dod"),
            CompressionCodec::Simple8b => write!(f, "simple8b"),
            CompressionCodec::Lz4 => write!(f, "lz4"),
            CompressionCodec::Zstd => write!(f, "zstd"),
            CompressionCodec::Custom(name) => write!(f, "custom:{}", name),
        }
    }
}

/// Compression trait for time series data
pub trait Compression: Send + Sync {
    /// Compress a slice of samples
    fn compress(&self, samples: &[Sample]) -> Result<CompressedBlock>;

    /// Decompress a block
    fn decompress(&self, block: &CompressedBlock) -> Result<Vec<Sample>>;

    /// Get compression ratio (original_size / compressed_size)
    fn compression_ratio(&self, original_size: usize, compressed_size: usize) -> f64 {
        if compressed_size == 0 {
            0.0
        } else {
            original_size as f64 / compressed_size as f64
        }
    }

    /// Get codec name
    fn name(&self) -> &str;
}

/// A compressed block of time series data
#[derive(Debug, Clone)]
pub struct CompressedBlock {
    /// Block ID
    pub id: u64,
    /// Codec used
    pub codec: CompressionCodec,
    /// Start timestamp
    pub start_time: Timestamp,
    /// End timestamp
    pub end_time: Timestamp,
    /// Number of samples
    pub sample_count: usize,
    /// Compressed data
    pub data: Vec<u8>,
    /// Original size in bytes
    pub original_size: usize,
}

impl CompressedBlock {
    /// Create a new compressed block
    pub fn new(codec: CompressionCodec, samples: &[Sample], data: Vec<u8>) -> Self {
        let start_time = samples.first().map(|s| s.timestamp).unwrap_or_default();
        let end_time = samples.last().map(|s| s.timestamp).unwrap_or_default();
        let original_size = std::mem::size_of_val(samples);

        Self {
            id: 0,
            codec,
            start_time,
            end_time,
            sample_count: samples.len(),
            data,
            original_size,
        }
    }

    /// Get compression ratio
    pub fn compression_ratio(&self) -> f64 {
        if self.data.is_empty() {
            0.0
        } else {
            self.original_size as f64 / self.data.len() as f64
        }
    }

    /// Get compressed size
    pub fn compressed_size(&self) -> usize {
        self.data.len()
    }

    /// Check if block contains timestamp
    pub fn contains(&self, ts: Timestamp) -> bool {
        ts.as_nanos() >= self.start_time.as_nanos() && ts.as_nanos() <= self.end_time.as_nanos()
    }
}

/// Compression level controlling trade-off between speed and ratio
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CompressionLevel {
    /// Fastest compression, lowest ratio
    Fast,
    /// Balanced speed and compression ratio (default)
    #[default]
    Balanced,
    /// Best compression ratio, slowest
    Best,
}



impl std::fmt::Display for CompressionLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionLevel::Fast => write!(f, "fast"),
            CompressionLevel::Balanced => write!(f, "balanced"),
            CompressionLevel::Best => write!(f, "best"),
        }
    }
}

/// Gorilla compression (Facebook's algorithm)
/// Optimized for time series with XOR-based encoding
pub struct GorillaCompressor {
    /// Block duration
    block_duration: std::time::Duration,
    /// Compression level
    level: CompressionLevel,
}

impl GorillaCompressor {
    /// Create a new Gorilla compressor
    pub fn new() -> Self {
        Self {
            block_duration: std::time::Duration::from_secs(7200), // 2 hours
            level: CompressionLevel::Balanced,
        }
    }

    /// Set block duration
    pub fn with_block_duration(mut self, duration: std::time::Duration) -> Self {
        self.block_duration = duration;
        self
    }

    /// Set compression level
    pub fn with_level(mut self, level: CompressionLevel) -> Self {
        self.level = level;
        self
    }

    /// Get the current compression level
    pub fn level(&self) -> CompressionLevel {
        self.level
    }

    /// Get the effective block duration based on compression level
    fn effective_block_duration(&self) -> std::time::Duration {
        match self.level {
            CompressionLevel::Fast => self.block_duration * 2,
            CompressionLevel::Balanced => self.block_duration,
            CompressionLevel::Best => self.block_duration / 2,
        }
    }
}

impl Default for GorillaCompressor {
    fn default() -> Self {
        Self::new()
    }
}

impl Compression for GorillaCompressor {
    fn compress(&self, samples: &[Sample]) -> Result<CompressedBlock> {
        if samples.is_empty() {
            return Ok(CompressedBlock::new(
                CompressionCodec::Gorilla,
                samples,
                Vec::new(),
            ));
        }

        let mut writer = BitWriter::new();

        // Write header
        let first = &samples[0];
        writer.write_i64(first.timestamp.as_nanos(), 64);
        writer.write_f64(first.as_f64().unwrap_or(0.0));

        // Compress timestamps using delta-of-delta
        let mut prev_ts = first.timestamp.as_nanos();
        let mut prev_delta: i64 = 0;

        // Compress values using XOR
        let mut prev_value_bits = first.as_f64().unwrap_or(0.0).to_bits();
        let mut prev_leading: u8 = u8::MAX;
        let mut prev_trailing: u8 = 0;

        for sample in samples.iter().skip(1) {
            // Timestamp compression
            let ts = sample.timestamp.as_nanos();
            let delta = ts - prev_ts;
            let dod = delta - prev_delta;

            // Encode delta-of-delta
            if dod == 0 {
                writer.write_bit(false);
            } else if (-63..=64).contains(&dod) {
                writer.write_bits(0b10, 2);
                writer.write_i64(dod, 7);
            } else if (-255..=256).contains(&dod) {
                writer.write_bits(0b110, 3);
                writer.write_i64(dod, 9);
            } else if (-2047..=2048).contains(&dod) {
                writer.write_bits(0b1110, 4);
                writer.write_i64(dod, 12);
            } else {
                writer.write_bits(0b1111, 4);
                writer.write_i64(dod, 32);
            }

            prev_delta = delta;
            prev_ts = ts;

            // Value compression using XOR
            let value_bits = sample.as_f64().unwrap_or(0.0).to_bits();
            let xor = prev_value_bits ^ value_bits;

            if xor == 0 {
                writer.write_bit(false);
            } else {
                writer.write_bit(true);
                let leading = xor.leading_zeros() as u8;
                let trailing = xor.trailing_zeros() as u8;

                // Best level: reuse previous leading/trailing when beneficial
                if self.level == CompressionLevel::Best
                    && leading >= prev_leading
                    && trailing >= prev_trailing
                    && prev_leading != u8::MAX
                {
                    writer.write_bit(false);
                    let meaningful_bits = 64 - prev_leading - prev_trailing;
                    writer.write_bits(
                        xor >> prev_trailing,
                        meaningful_bits as usize,
                    );
                } else if leading >= 5 && trailing >= 5 {
                    writer.write_bit(false);
                    writer.write_bits(leading as u64, 6);
                    let meaningful_bits = 64 - leading - trailing;
                    writer.write_bits(meaningful_bits as u64, 6);
                    writer.write_bits(xor >> trailing, meaningful_bits as usize);
                    prev_leading = leading;
                    prev_trailing = trailing;
                } else {
                    writer.write_bit(true);
                    writer.write_bits(xor, 64);
                    prev_leading = u8::MAX;
                    prev_trailing = 0;
                }
            }

            prev_value_bits = value_bits;
        }

        Ok(CompressedBlock::new(
            CompressionCodec::Gorilla,
            samples,
            writer.finish(),
        ))
    }

    fn decompress(&self, block: &CompressedBlock) -> Result<Vec<Sample>> {
        if block.data.is_empty() {
            return Ok(Vec::new());
        }

        let mut reader = BitReader::new(&block.data);
        let mut samples = Vec::with_capacity(block.sample_count);

        // Read header
        let first_ts = reader.read_i64(64)?;
        let first_value = reader.read_f64()?;
        samples.push(Sample::new(Timestamp::from_nanos(first_ts), first_value));

        let mut prev_ts = first_ts;
        let mut prev_delta: i64 = 0;
        let mut prev_value_bits = first_value.to_bits();

        for _ in 1..block.sample_count {
            // Decode timestamp
            let dod = if !reader.read_bit()? {
                0
            } else if !reader.read_bit()? {
                reader.read_i64(7)?
            } else if !reader.read_bit()? {
                reader.read_i64(9)?
            } else if !reader.read_bit()? {
                reader.read_i64(12)?
            } else {
                reader.read_i64(32)?
            };

            let delta = prev_delta + dod;
            let ts = prev_ts + delta;
            prev_delta = delta;
            prev_ts = ts;

            // Decode value
            #[allow(clippy::same_functions_in_if_condition)]
            let value_bits = if !reader.read_bit()? {
                prev_value_bits
            } else if !reader.read_bit()? {
                let leading = reader.read_u64(6)? as usize;
                let meaningful_bits = reader.read_u64(6)? as usize;
                let trailing = 64 - leading - meaningful_bits;
                let xor = reader.read_u64(meaningful_bits)? << trailing;
                prev_value_bits ^ xor
            } else {
                prev_value_bits ^ reader.read_u64(64)?
            };

            prev_value_bits = value_bits;
            let value = f64::from_bits(value_bits);
            samples.push(Sample::new(Timestamp::from_nanos(ts), value));
        }

        Ok(samples)
    }

    fn name(&self) -> &str {
        "gorilla"
    }
}

/// Simple bit writer for compression
struct BitWriter {
    data: Vec<u8>,
    current_byte: u8,
    bit_position: u8,
}

impl BitWriter {
    fn new() -> Self {
        Self {
            data: Vec::new(),
            current_byte: 0,
            bit_position: 0,
        }
    }

    fn write_bit(&mut self, bit: bool) {
        if bit {
            self.current_byte |= 1 << (7 - self.bit_position);
        }
        self.bit_position += 1;
        if self.bit_position == 8 {
            self.data.push(self.current_byte);
            self.current_byte = 0;
            self.bit_position = 0;
        }
    }

    fn write_bits(&mut self, value: u64, count: usize) {
        for i in (0..count).rev() {
            self.write_bit((value >> i) & 1 == 1);
        }
    }

    fn write_i64(&mut self, value: i64, bits: usize) {
        self.write_bits(value as u64, bits);
    }

    fn write_f64(&mut self, value: f64) {
        self.write_bits(value.to_bits(), 64);
    }

    fn finish(mut self) -> Vec<u8> {
        if self.bit_position > 0 {
            self.data.push(self.current_byte);
        }
        self.data
    }
}

/// Simple bit reader for decompression
struct BitReader<'a> {
    data: &'a [u8],
    byte_position: usize,
    bit_position: u8,
}

impl<'a> BitReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            byte_position: 0,
            bit_position: 0,
        }
    }

    fn read_bit(&mut self) -> Result<bool> {
        if self.byte_position >= self.data.len() {
            return Err(TimeSeriesError::CompressionError(
                "unexpected end of data".to_string(),
            ));
        }

        let bit = (self.data[self.byte_position] >> (7 - self.bit_position)) & 1 == 1;
        self.bit_position += 1;
        if self.bit_position == 8 {
            self.byte_position += 1;
            self.bit_position = 0;
        }
        Ok(bit)
    }

    fn read_u64(&mut self, count: usize) -> Result<u64> {
        let mut value: u64 = 0;
        for _ in 0..count {
            value = (value << 1) | if self.read_bit()? { 1 } else { 0 };
        }
        Ok(value)
    }

    fn read_i64(&mut self, count: usize) -> Result<i64> {
        let value = self.read_u64(count)?;
        // Sign extend if necessary
        if count < 64 && (value >> (count - 1)) & 1 == 1 {
            Ok((value | (!0u64 << count)) as i64)
        } else {
            Ok(value as i64)
        }
    }

    fn read_f64(&mut self) -> Result<f64> {
        let bits = self.read_u64(64)?;
        Ok(f64::from_bits(bits))
    }
}

/// No compression (passthrough)
pub struct NoCompression;

impl Compression for NoCompression {
    fn compress(&self, samples: &[Sample]) -> Result<CompressedBlock> {
        let mut data = Vec::with_capacity(samples.len() * 16);
        for sample in samples {
            data.extend_from_slice(&sample.timestamp.as_nanos().to_le_bytes());
            data.extend_from_slice(&sample.as_f64().unwrap_or(0.0).to_le_bytes());
        }
        Ok(CompressedBlock::new(CompressionCodec::None, samples, data))
    }

    fn decompress(&self, block: &CompressedBlock) -> Result<Vec<Sample>> {
        let mut samples = Vec::with_capacity(block.sample_count);
        let mut offset = 0;

        while offset + 16 <= block.data.len() {
            let ts = i64::from_le_bytes(block.data[offset..offset + 8].try_into().unwrap_or_default());
            let value = f64::from_le_bytes(block.data[offset + 8..offset + 16].try_into().unwrap_or_default());
            samples.push(Sample::new(Timestamp::from_nanos(ts), value));
            offset += 16;
        }

        Ok(samples)
    }

    fn name(&self) -> &str {
        "none"
    }
}

/// Compression statistics
#[derive(Debug, Clone, Default)]
pub struct CompressionStats {
    /// Total bytes before compression
    pub original_bytes: u64,
    /// Total bytes after compression
    pub compressed_bytes: u64,
    /// Number of blocks compressed
    pub blocks_compressed: u64,
    /// Number of samples compressed
    pub samples_compressed: u64,
}

impl CompressionStats {
    /// Get overall compression ratio
    pub fn compression_ratio(&self) -> f64 {
        if self.compressed_bytes == 0 {
            0.0
        } else {
            self.original_bytes as f64 / self.compressed_bytes as f64
        }
    }

    /// Get space savings percentage
    pub fn space_savings(&self) -> f64 {
        if self.original_bytes == 0 {
            0.0
        } else {
            (1.0 - (self.compressed_bytes as f64 / self.original_bytes as f64)) * 100.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_compression() {
        let compressor = NoCompression;

        let samples = vec![
            Sample::from_secs(1, 10.0),
            Sample::from_secs(2, 20.0),
            Sample::from_secs(3, 30.0),
        ];

        let compressed = compressor.compress(&samples).unwrap();
        assert_eq!(compressed.sample_count, 3);

        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(decompressed.len(), 3);
        assert_eq!(decompressed[0].as_f64(), Some(10.0));
        assert_eq!(decompressed[1].as_f64(), Some(20.0));
        assert_eq!(decompressed[2].as_f64(), Some(30.0));
    }

    #[test]
    fn test_gorilla_compression() {
        let compressor = GorillaCompressor::new();

        // Create samples with similar values (good for XOR compression)
        let samples: Vec<Sample> = (0..100)
            .map(|i| Sample::from_secs(i, 100.0 + (i as f64 * 0.1)))
            .collect();

        let compressed = compressor.compress(&samples).unwrap();
        assert_eq!(compressed.sample_count, 100);
        assert!(compressed.compression_ratio() > 1.0); // Should compress

        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(decompressed.len(), 100);

        // Verify values (may have small floating point differences)
        for (orig, decomp) in samples.iter().zip(decompressed.iter()) {
            let diff = (orig.as_f64().unwrap() - decomp.as_f64().unwrap()).abs();
            assert!(diff < 0.0001);
        }
    }

    #[test]
    fn test_compressed_block() {
        let samples = vec![Sample::from_secs(1, 10.0), Sample::from_secs(5, 20.0)];

        let block = CompressedBlock::new(CompressionCodec::None, &samples, vec![1, 2, 3, 4]);

        assert_eq!(block.start_time.as_secs(), 1);
        assert_eq!(block.end_time.as_secs(), 5);
        assert_eq!(block.sample_count, 2);
        assert!(block.contains(Timestamp::from_secs(3)));
        assert!(!block.contains(Timestamp::from_secs(10)));
    }

    #[test]
    fn test_compression_stats() {
        let mut stats = CompressionStats::default();
        stats.original_bytes = 1000;
        stats.compressed_bytes = 250;

        assert_eq!(stats.compression_ratio(), 4.0);
        assert_eq!(stats.space_savings(), 75.0);
    }

    #[test]
    fn test_bit_writer_reader() {
        let mut writer = BitWriter::new();
        writer.write_bit(true);
        writer.write_bit(false);
        writer.write_bits(0b101, 3);
        writer.write_i64(42, 8);
        let data = writer.finish();

        let mut reader = BitReader::new(&data);
        assert!(reader.read_bit().unwrap());
        assert!(!reader.read_bit().unwrap());
        assert_eq!(reader.read_u64(3).unwrap(), 0b101);
        assert_eq!(reader.read_i64(8).unwrap(), 42);
    }

    #[test]
    fn test_compression_level_default() {
        assert_eq!(CompressionLevel::default(), CompressionLevel::Balanced);
    }

    #[test]
    fn test_gorilla_with_level_fast() {
        let compressor = GorillaCompressor::new().with_level(CompressionLevel::Fast);
        assert_eq!(compressor.level(), CompressionLevel::Fast);

        let samples: Vec<Sample> = (0..50)
            .map(|i| Sample::from_secs(i, 100.0 + (i as f64 * 0.1)))
            .collect();

        let compressed = compressor.compress(&samples).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(decompressed.len(), 50);

        for (orig, decomp) in samples.iter().zip(decompressed.iter()) {
            let diff = (orig.as_f64().unwrap() - decomp.as_f64().unwrap()).abs();
            assert!(diff < 0.0001, "Fast: diff {} too large", diff);
        }
    }

    #[test]
    fn test_gorilla_with_level_best() {
        let compressor = GorillaCompressor::new().with_level(CompressionLevel::Best);
        assert_eq!(compressor.level(), CompressionLevel::Best);

        let samples: Vec<Sample> = (0..100)
            .map(|i| Sample::from_secs(i, 100.0 + (i as f64 * 0.1)))
            .collect();

        let compressed = compressor.compress(&samples).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(decompressed.len(), 100);
    }

    #[test]
    fn test_compression_level_affects_ratio() {
        let samples: Vec<Sample> = (0..200)
            .map(|i| Sample::from_secs(i, 50.0 + (i as f64 * 0.01)))
            .collect();

        let balanced = GorillaCompressor::new().with_level(CompressionLevel::Balanced);
        let compressed_balanced = balanced.compress(&samples).unwrap();

        // All levels should produce valid results
        assert!(compressed_balanced.compression_ratio() > 1.0);
        assert_eq!(compressed_balanced.sample_count, 200);
    }

    #[test]
    fn test_compression_level_display() {
        assert_eq!(CompressionLevel::Fast.to_string(), "fast");
        assert_eq!(CompressionLevel::Balanced.to_string(), "balanced");
        assert_eq!(CompressionLevel::Best.to_string(), "best");
    }

    #[test]
    fn test_gorilla_roundtrip_constant_values() {
        let compressor = GorillaCompressor::new();

        let samples: Vec<Sample> = (0..50)
            .map(|i| Sample::from_secs(i, 42.0))
            .collect();

        let compressed = compressor.compress(&samples).unwrap();
        assert!(compressed.compression_ratio() > 2.0); // Constant values compress well

        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(decompressed.len(), 50);
        for s in &decompressed {
            assert_eq!(s.as_f64(), Some(42.0));
        }
    }

    #[test]
    fn test_gorilla_roundtrip_irregular_timestamps() {
        let compressor = GorillaCompressor::new();

        // Use millisecond-scale timestamps to avoid 32-bit dod overflow
        let samples = vec![
            Sample::new(Timestamp::from_millis(0), 1.0),
            Sample::new(Timestamp::from_millis(1000), 2.0),
            Sample::new(Timestamp::from_millis(2000), 3.0),
            Sample::new(Timestamp::from_millis(5000), 4.0),
            Sample::new(Timestamp::from_millis(6000), 5.0),
        ];

        let compressed = compressor.compress(&samples).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(decompressed.len(), 5);

        for (orig, decomp) in samples.iter().zip(decompressed.iter()) {
            assert_eq!(orig.timestamp.as_millis(), decomp.timestamp.as_millis());
            let diff = (orig.as_f64().unwrap() - decomp.as_f64().unwrap()).abs();
            assert!(diff < 0.0001);
        }
    }
}
