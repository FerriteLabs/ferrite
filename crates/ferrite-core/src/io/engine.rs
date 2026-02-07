//! I/O Engine Trait and Types
//!
//! Defines the unified interface for async I/O operations across
//! different backends (io_uring, tokio::fs, etc.)

use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use super::buffer::IoBuffer;

/// Result of a read operation
#[derive(Debug)]
pub struct ReadResult {
    /// Bytes read
    pub bytes_read: usize,
    /// The buffer containing data
    pub buffer: IoBuffer,
    /// File offset where read occurred
    pub offset: u64,
}

/// Result of a write operation
#[derive(Debug)]
pub struct WriteResult {
    /// Bytes written
    pub bytes_written: usize,
    /// File offset where write occurred
    pub offset: u64,
}

/// Generic I/O result type
pub type IoResult<T> = io::Result<T>;

/// An I/O operation to be submitted
#[derive(Debug, Clone)]
pub enum IoOperation {
    /// Read from file at offset
    Read {
        /// File path
        path: PathBuf,
        /// Offset in file
        offset: u64,
        /// Number of bytes to read
        len: usize,
    },
    /// Write to file at offset
    Write {
        /// File path
        path: PathBuf,
        /// Offset in file
        offset: u64,
        /// Data to write
        data: Bytes,
    },
    /// Append to file
    Append {
        /// File path
        path: PathBuf,
        /// Data to append
        data: Bytes,
    },
    /// Sync file to disk
    Fsync {
        /// File path
        path: PathBuf,
    },
    /// Sync file data (no metadata)
    Fdatasync {
        /// File path
        path: PathBuf,
    },
    /// Open a file
    Open {
        /// File path
        path: PathBuf,
        /// Create if not exists
        create: bool,
        /// Open for writing
        write: bool,
        /// Truncate existing file
        truncate: bool,
    },
    /// Close a file
    Close {
        /// File path
        path: PathBuf,
    },
}

/// I/O Engine configuration
#[derive(Debug, Clone)]
pub struct IoEngineConfig {
    /// Queue depth for io_uring (entries in submission queue)
    pub queue_depth: u32,
    /// Number of registered buffers
    pub buffer_count: usize,
    /// Buffer size for each registered buffer
    pub buffer_size: usize,
    /// Enable direct I/O (O_DIRECT)
    pub direct_io: bool,
    /// Enable file descriptor registration
    pub register_files: bool,
    /// Completion queue polling (IORING_SETUP_SQPOLL)
    pub kernel_poll: bool,
    /// Maximum inflight operations
    pub max_inflight: usize,
    /// Batch size for submissions
    pub batch_size: usize,
}

impl Default for IoEngineConfig {
    fn default() -> Self {
        Self {
            queue_depth: 256,
            buffer_count: 32,
            buffer_size: 64 * 1024, // 64KB
            direct_io: false,
            register_files: false,
            kernel_poll: false,
            max_inflight: 1024,
            batch_size: 16,
        }
    }
}

impl IoEngineConfig {
    /// Create a high-performance configuration
    pub fn high_performance() -> Self {
        Self {
            queue_depth: 4096,
            buffer_count: 128,
            buffer_size: 128 * 1024, // 128KB
            direct_io: true,
            register_files: true,
            kernel_poll: true,
            max_inflight: 8192,
            batch_size: 64,
        }
    }

    /// Create a low-latency configuration
    pub fn low_latency() -> Self {
        Self {
            queue_depth: 128,
            buffer_count: 16,
            buffer_size: 4 * 1024, // 4KB
            direct_io: true,
            register_files: true,
            kernel_poll: true,
            max_inflight: 256,
            batch_size: 4,
        }
    }

    /// Create a balanced configuration
    pub fn balanced() -> Self {
        Self::default()
    }
}

/// I/O statistics
#[derive(Debug, Default)]
pub struct IoStats {
    /// Total read operations
    pub reads: AtomicU64,
    /// Total write operations
    pub writes: AtomicU64,
    /// Total bytes read
    pub bytes_read: AtomicU64,
    /// Total bytes written
    pub bytes_written: AtomicU64,
    /// Total fsync operations
    pub fsyncs: AtomicU64,
    /// Read latency sum (nanoseconds)
    pub read_latency_ns: AtomicU64,
    /// Write latency sum (nanoseconds)
    pub write_latency_ns: AtomicU64,
    /// Batched submissions
    pub batched_submissions: AtomicU64,
    /// Total operations in batches
    pub batched_ops: AtomicU64,
    /// Submission queue entries used
    pub sq_entries_used: AtomicU64,
    /// Completion queue entries received
    pub cq_entries_received: AtomicU64,
}

impl IoStats {
    /// Record a read operation
    pub fn record_read(&self, bytes: usize, latency_ns: u64) {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.bytes_read.fetch_add(bytes as u64, Ordering::Relaxed);
        self.read_latency_ns
            .fetch_add(latency_ns, Ordering::Relaxed);
    }

    /// Record a write operation
    pub fn record_write(&self, bytes: usize, latency_ns: u64) {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.bytes_written
            .fetch_add(bytes as u64, Ordering::Relaxed);
        self.write_latency_ns
            .fetch_add(latency_ns, Ordering::Relaxed);
    }

    /// Record a batch submission
    pub fn record_batch(&self, ops: usize) {
        self.batched_submissions.fetch_add(1, Ordering::Relaxed);
        self.batched_ops.fetch_add(ops as u64, Ordering::Relaxed);
    }

    /// Get average read latency in nanoseconds
    pub fn avg_read_latency_ns(&self) -> u64 {
        let reads = self.reads.load(Ordering::Relaxed);
        if reads == 0 {
            return 0;
        }
        self.read_latency_ns.load(Ordering::Relaxed) / reads
    }

    /// Get average write latency in nanoseconds
    pub fn avg_write_latency_ns(&self) -> u64 {
        let writes = self.writes.load(Ordering::Relaxed);
        if writes == 0 {
            return 0;
        }
        self.write_latency_ns.load(Ordering::Relaxed) / writes
    }

    /// Get total throughput in bytes
    pub fn total_throughput(&self) -> u64 {
        self.bytes_read.load(Ordering::Relaxed) + self.bytes_written.load(Ordering::Relaxed)
    }

    /// Reset all statistics
    pub fn reset(&self) {
        self.reads.store(0, Ordering::Release);
        self.writes.store(0, Ordering::Release);
        self.bytes_read.store(0, Ordering::Release);
        self.bytes_written.store(0, Ordering::Release);
        self.fsyncs.store(0, Ordering::Release);
        self.read_latency_ns.store(0, Ordering::Release);
        self.write_latency_ns.store(0, Ordering::Release);
        self.batched_submissions.store(0, Ordering::Release);
        self.batched_ops.store(0, Ordering::Release);
        self.sq_entries_used.store(0, Ordering::Release);
        self.cq_entries_received.store(0, Ordering::Release);
    }
}

/// Async boxed future type
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The core I/O engine trait
///
/// Provides a unified interface for async I/O operations that can be
/// implemented by different backends (io_uring, tokio::fs, etc.)
pub trait IoEngine: Send + Sync {
    /// Get the engine name
    fn name(&self) -> &'static str;

    /// Read from file at offset
    fn read<'a>(
        &'a self,
        path: &'a Path,
        offset: u64,
        len: usize,
    ) -> BoxFuture<'a, IoResult<ReadResult>>;

    /// Write to file at offset
    fn write<'a>(
        &'a self,
        path: &'a Path,
        offset: u64,
        data: &'a [u8],
    ) -> BoxFuture<'a, IoResult<WriteResult>>;

    /// Append data to file
    fn append<'a>(&'a self, path: &'a Path, data: &'a [u8])
        -> BoxFuture<'a, IoResult<WriteResult>>;

    /// Read entire file
    fn read_all<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, IoResult<Bytes>>;

    /// Write entire file
    fn write_all<'a>(&'a self, path: &'a Path, data: &'a [u8]) -> BoxFuture<'a, IoResult<()>>;

    /// Sync file to disk (fsync)
    fn sync<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, IoResult<()>>;

    /// Sync file data only (fdatasync)
    fn datasync<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, IoResult<()>>;

    /// Get file size
    fn file_size<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, IoResult<u64>>;

    /// Check if file exists
    fn exists<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, IoResult<bool>>;

    /// Delete file
    fn delete<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, IoResult<()>>;

    /// Get I/O statistics
    fn stats(&self) -> &IoStats;

    /// Get engine configuration
    fn config(&self) -> &IoEngineConfig;

    /// Check if engine supports io_uring features
    fn supports_uring(&self) -> bool {
        false
    }

    /// Check if engine supports zero-copy operations
    fn supports_zero_copy(&self) -> bool {
        false
    }

    /// Check if engine supports registered buffers
    fn supports_registered_buffers(&self) -> bool {
        false
    }
}

/// Extension trait for batch operations
pub trait BatchIoEngine: IoEngine {
    /// Submit a batch of operations
    fn submit_batch<'a>(
        &'a self,
        ops: Vec<IoOperation>,
    ) -> BoxFuture<'a, IoResult<Vec<IoResult<usize>>>>;

    /// Get maximum batch size
    fn max_batch_size(&self) -> usize {
        16
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config = IoEngineConfig::default();
        assert_eq!(config.queue_depth, 256);
        assert!(!config.direct_io);
        assert!(!config.kernel_poll);
    }

    #[test]
    fn test_config_high_performance() {
        let config = IoEngineConfig::high_performance();
        assert_eq!(config.queue_depth, 4096);
        assert!(config.direct_io);
        assert!(config.kernel_poll);
    }

    #[test]
    fn test_io_stats() {
        let stats = IoStats::default();

        stats.record_read(1024, 1000);
        stats.record_read(2048, 2000);
        stats.record_write(512, 500);

        assert_eq!(stats.reads.load(Ordering::Relaxed), 2);
        assert_eq!(stats.bytes_read.load(Ordering::Relaxed), 3072);
        assert_eq!(stats.avg_read_latency_ns(), 1500);
        assert_eq!(stats.writes.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_io_stats_reset() {
        let stats = IoStats::default();
        stats.record_read(1024, 1000);
        stats.record_write(512, 500);

        stats.reset();

        assert_eq!(stats.reads.load(Ordering::Relaxed), 0);
        assert_eq!(stats.writes.load(Ordering::Relaxed), 0);
    }
}
