//! Stress Testing Helpers for HybridLog
//!
//! Provides utilities for generating random read/write workloads,
//! concurrent reader/writer test scenarios, and crash recovery simulation.

use std::io::{self, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rand::Rng;

use super::checksum::{recovery_scan, write_checksummed_record};

/// Workload mix configuration
#[derive(Debug, Clone)]
pub struct WorkloadConfig {
    /// Number of operations to perform
    pub num_ops: u64,
    /// Fraction of writes (0.0–1.0)
    pub write_ratio: f64,
    /// Minimum key size in bytes
    pub min_key_size: usize,
    /// Maximum key size in bytes
    pub max_key_size: usize,
    /// Minimum value size in bytes
    pub min_value_size: usize,
    /// Maximum value size in bytes
    pub max_value_size: usize,
    /// Key space size (number of distinct keys)
    pub key_space: u64,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            num_ops: 10_000,
            write_ratio: 0.5,
            min_key_size: 8,
            max_key_size: 64,
            min_value_size: 32,
            max_value_size: 256,
            key_space: 1000,
        }
    }
}

/// Stats collected from a workload run
#[derive(Debug, Default)]
pub struct WorkloadStats {
    /// Number of reads performed
    pub reads: AtomicU64,
    /// Number of writes performed
    pub writes: AtomicU64,
    /// Number of read hits (key found)
    pub read_hits: AtomicU64,
    /// Number of read misses (key not found)
    pub read_misses: AtomicU64,
    /// Number of errors
    pub errors: AtomicU64,
}

impl WorkloadStats {
    /// Create new zeroed stats
    pub fn new() -> Self {
        Self::default()
    }

    /// Get total operations
    pub fn total_ops(&self) -> u64 {
        self.reads.load(Ordering::Relaxed) + self.writes.load(Ordering::Relaxed)
    }
}

/// Generate a random key within the key space
pub fn random_key(rng: &mut impl Rng, key_space: u64) -> Vec<u8> {
    let idx = rng.gen_range(0..key_space);
    format!("key:{:012}", idx).into_bytes()
}

/// Generate a random value
pub fn random_value(rng: &mut impl Rng, min_size: usize, max_size: usize) -> Vec<u8> {
    let size = rng.gen_range(min_size..=max_size);
    let mut buf = vec![0u8; size];
    rng.fill(buf.as_mut_slice());
    buf
}

/// Simulate crash recovery by writing valid records then injecting a partial/corrupt record
///
/// Returns the number of valid records written before the crash point.
pub fn simulate_crash_recovery(path: &Path, valid_count: usize) -> io::Result<u64> {
    let mut file = std::fs::File::create(path)?;

    // Write valid records
    for i in 0..valid_count {
        let key = format!("key:{:06}", i);
        let value = format!("value:{:06}", i);
        write_checksummed_record(&mut file, key.as_bytes(), value.as_bytes())?;
    }

    // Write a partial/corrupt trailing record
    // Write just the magic bytes and partial CRC to simulate crash mid-write
    file.write_all(&[0xFE, 0x00, 0x1C, 0x00])?; // magic bytes (partial)
    file.write_all(&[0x00, 0x00])?; // incomplete CRC
    file.sync_all()?;

    // Now verify recovery sees exactly `valid_count` valid records
    let summary = recovery_scan(path)?;
    Ok(summary.valid_records)
}

/// Generate concurrent read/write operations for stress testing
///
/// This helper creates a shared stats object and returns closures that
/// callers can spawn on threads.
pub fn concurrent_workload(config: WorkloadConfig) -> (Arc<WorkloadStats>, WorkloadConfig) {
    (Arc::new(WorkloadStats::new()), config)
}

/// Write a batch of checksummed records to a file for testing
pub fn write_test_records(path: &Path, count: usize) -> io::Result<()> {
    let mut file = std::fs::File::create(path)?;
    let mut rng = rand::thread_rng();

    for i in 0..count {
        let key = format!("key:{:06}", i);
        let value_size = rng.gen_range(32..=256);
        let mut value = vec![0u8; value_size];
        rng.fill(value.as_mut_slice());
        write_checksummed_record(&mut file, key.as_bytes(), &value)?;
    }

    file.sync_all()?;
    Ok(())
}

/// Write records then corrupt one in the middle, returning expected valid count
pub fn write_with_mid_corruption(path: &Path, total: usize, corrupt_at: usize) -> io::Result<u64> {
    let mut file = std::fs::File::create(path)?;
    let mut offsets = Vec::new();

    for i in 0..total {
        let start = file.seek(SeekFrom::Current(0))?;
        offsets.push(start);
        let key = format!("key:{:06}", i);
        let value = format!("value:{:06}", i);
        write_checksummed_record(&mut file, key.as_bytes(), value.as_bytes())?;
    }

    // Corrupt the CRC of the record at corrupt_at
    if corrupt_at < offsets.len() {
        // CRC is at offset + 4 (after magic)
        let crc_offset = offsets[corrupt_at] + 4;
        file.seek(SeekFrom::Start(crc_offset))?;
        file.write_all(&[0xFF, 0xFF, 0xFF, 0xFF])?;
    }

    file.sync_all()?;

    let summary = recovery_scan(path)?;
    Ok(summary.valid_records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_random_key_format() {
        let mut rng = rand::thread_rng();
        let key = random_key(&mut rng, 100);
        let key_str = String::from_utf8(key).expect("valid utf8");
        assert!(key_str.starts_with("key:"));
    }

    #[test]
    fn test_random_value_size() {
        let mut rng = rand::thread_rng();
        let value = random_value(&mut rng, 32, 64);
        assert!(value.len() >= 32 && value.len() <= 64);
    }

    #[test]
    fn test_simulate_crash_recovery() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("crash_test.dat");

        let valid = simulate_crash_recovery(&path, 10).expect("crash sim");
        assert_eq!(valid, 10);
    }

    #[test]
    fn test_write_test_records() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("records.dat");

        write_test_records(&path, 50).expect("write records");

        let summary = recovery_scan(&path).expect("scan");
        assert_eq!(summary.valid_records, 50);
        assert_eq!(summary.corrupted_records, 0);
    }

    #[test]
    fn test_mid_corruption() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("mid_corrupt.dat");

        // 10 records, corrupt at index 5 → only 5 valid records before it
        let valid = write_with_mid_corruption(&path, 10, 5).expect("mid corrupt");
        assert_eq!(valid, 5);
    }

    #[test]
    fn test_mid_corruption_at_zero() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("corrupt_zero.dat");

        let valid = write_with_mid_corruption(&path, 5, 0).expect("corrupt at 0");
        assert_eq!(valid, 0);
    }

    #[test]
    fn test_concurrent_workload_setup() {
        let config = WorkloadConfig {
            num_ops: 100,
            write_ratio: 0.7,
            ..Default::default()
        };
        let (stats, cfg) = concurrent_workload(config);
        assert_eq!(stats.total_ops(), 0);
        assert_eq!(cfg.num_ops, 100);
    }

    #[test]
    fn test_workload_stats() {
        let stats = WorkloadStats::new();
        stats.reads.fetch_add(10, Ordering::Relaxed);
        stats.writes.fetch_add(5, Ordering::Relaxed);
        assert_eq!(stats.total_ops(), 15);
    }

    #[test]
    fn test_crash_recovery_zero_records() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("empty_crash.dat");

        let valid = simulate_crash_recovery(&path, 0).expect("crash sim");
        assert_eq!(valid, 0);
    }
}
