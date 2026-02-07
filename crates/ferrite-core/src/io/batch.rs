//! Batched I/O Operations
//!
//! Provides batched I/O for improved throughput by reducing
//! syscall overhead and enabling io_uring's batch submissions.

use std::collections::VecDeque;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use super::buffer::{IoBuffer, IoBufferPool};
use super::engine::{IoEngine, IoOperation, IoResult, WriteResult};

/// A batch of I/O operations waiting to be submitted
#[derive(Debug, Default)]
pub struct IoBatch {
    /// Pending operations
    operations: Vec<PendingOp>,
    /// Maximum batch size
    max_size: usize,
    /// Created timestamp
    created_at: Option<Instant>,
}

/// A pending operation in a batch
#[derive(Debug)]
struct PendingOp {
    /// The operation
    op: IoOperation,
    /// Completion callback
    callback: oneshot::Sender<IoResult<usize>>,
}

impl IoBatch {
    /// Create a new batch with default size
    pub fn new() -> Self {
        Self::with_capacity(16)
    }

    /// Create a new batch with specific capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            operations: Vec::with_capacity(capacity),
            max_size: capacity,
            created_at: None,
        }
    }

    /// Add an operation to the batch
    pub fn add(&mut self, op: IoOperation) -> oneshot::Receiver<IoResult<usize>> {
        if self.created_at.is_none() {
            self.created_at = Some(Instant::now());
        }

        let (tx, rx) = oneshot::channel();
        self.operations.push(PendingOp { op, callback: tx });
        rx
    }

    /// Check if batch is full
    pub fn is_full(&self) -> bool {
        self.operations.len() >= self.max_size
    }

    /// Check if batch is empty
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    /// Get batch size
    pub fn len(&self) -> usize {
        self.operations.len()
    }

    /// Get batch age
    pub fn age(&self) -> Duration {
        self.created_at
            .map(|t| t.elapsed())
            .unwrap_or(Duration::ZERO)
    }

    /// Take all operations from the batch
    pub fn take(&mut self) -> Vec<(IoOperation, oneshot::Sender<IoResult<usize>>)> {
        self.created_at = None;
        self.operations
            .drain(..)
            .map(|p| (p.op, p.callback))
            .collect()
    }
}

/// Configuration for batched writer
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Maximum batch size
    pub max_batch_size: usize,
    /// Maximum wait time before flushing
    pub max_wait: Duration,
    /// Buffer pool size
    pub buffer_pool_size: usize,
    /// Enable automatic flushing
    pub auto_flush: bool,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 16,
            max_wait: Duration::from_millis(1),
            buffer_pool_size: 32,
            auto_flush: true,
        }
    }
}

/// Batched writer that accumulates writes before flushing
pub struct BatchedWriter {
    /// File path
    path: PathBuf,
    /// I/O engine
    engine: Arc<dyn IoEngine>,
    /// Current batch buffer
    buffer: IoBuffer,
    /// Batch configuration
    config: BatchConfig,
    /// Current file offset
    offset: u64,
    /// Pending writes
    pending: VecDeque<PendingWrite>,
    /// Buffer pool (retained for reuse across flushes)
    _pool: Arc<IoBufferPool>,
    /// Last flush time
    last_flush: Instant,
    /// Write queue sender (retained for async submission)
    _queue_tx: Option<mpsc::Sender<WriteRequest>>,
    /// Statistics
    stats: BatchedWriterStats,
}

/// A pending write operation
struct PendingWrite {
    /// Data offset in buffer
    _offset: usize,
    /// Data length
    _len: usize,
    /// Completion signal
    _complete: Option<oneshot::Sender<IoResult<WriteResult>>>,
}

/// Write request for async queue
struct WriteRequest {
    /// Data to write
    _data: Bytes,
    /// Completion signal
    _complete: oneshot::Sender<IoResult<WriteResult>>,
}

/// Statistics for batched writer
#[derive(Debug, Default)]
pub struct BatchedWriterStats {
    /// Total writes submitted
    pub writes: u64,
    /// Total bytes written
    pub bytes_written: u64,
    /// Number of flushes
    pub flushes: u64,
    /// Average batch size
    pub avg_batch_size: f64,
}

impl BatchedWriter {
    /// Create a new batched writer
    pub fn new(path: impl AsRef<Path>, engine: Arc<dyn IoEngine>, config: BatchConfig) -> Self {
        let pool = Arc::new(IoBufferPool::with_preallocated(
            64 * 1024,
            config.buffer_pool_size,
        ));

        Self {
            path: path.as_ref().to_path_buf(),
            engine,
            buffer: pool.acquire(),
            config,
            offset: 0,
            pending: VecDeque::new(),
            _pool: pool,
            last_flush: Instant::now(),
            _queue_tx: None,
            stats: BatchedWriterStats::default(),
        }
    }

    /// Set the current offset
    pub fn set_offset(&mut self, offset: u64) {
        self.offset = offset;
    }

    /// Write data to the batch
    pub fn write(&mut self, data: &[u8]) -> IoResult<usize> {
        // Check if we need to flush
        if self.buffer.remaining() < data.len() {
            // For sync path, we can't await
            // Buffer is full, need to handle differently
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "Buffer full, call flush first",
            ));
        }

        let start_offset = self.buffer.len();
        let written = self.buffer.copy_from(data);

        self.pending.push_back(PendingWrite {
            _offset: start_offset,
            _len: written,
            _complete: None,
        });

        self.stats.writes += 1;
        self.stats.bytes_written += written as u64;

        Ok(written)
    }

    /// Write data asynchronously with completion notification
    pub async fn write_async(&mut self, data: &[u8]) -> IoResult<WriteResult> {
        // Flush if needed
        if self.buffer.remaining() < data.len() {
            self.flush().await?;
        }

        let start_offset = self.buffer.len();
        let written = self.buffer.copy_from(data);
        let file_offset = self.offset;

        self.pending.push_back(PendingWrite {
            _offset: start_offset,
            _len: written,
            _complete: None,
        });

        self.stats.writes += 1;
        self.stats.bytes_written += written as u64;

        // Check if we should auto-flush
        if self.config.auto_flush && self.should_flush() {
            self.flush().await?;
        }

        Ok(WriteResult {
            bytes_written: written,
            offset: file_offset,
        })
    }

    /// Check if we should flush
    fn should_flush(&self) -> bool {
        self.pending.len() >= self.config.max_batch_size
            || self.last_flush.elapsed() >= self.config.max_wait
            || self.buffer.remaining() < 1024 // Low buffer space
    }

    /// Flush all pending writes
    pub async fn flush(&mut self) -> IoResult<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        // Write buffer to file
        let result = self
            .engine
            .append(&self.path, self.buffer.as_slice())
            .await?;

        self.offset += result.bytes_written as u64;
        self.stats.flushes += 1;

        // Update average batch size
        let batch_size = self.pending.len() as f64;
        self.stats.avg_batch_size = (self.stats.avg_batch_size * (self.stats.flushes - 1) as f64
            + batch_size)
            / self.stats.flushes as f64;

        // Clear pending and reset buffer
        self.pending.clear();
        self.buffer.reset();
        self.last_flush = Instant::now();

        Ok(())
    }

    /// Sync file to disk
    pub async fn sync(&self) -> IoResult<()> {
        self.engine.sync(&self.path).await
    }

    /// Flush and sync
    pub async fn flush_sync(&mut self) -> IoResult<()> {
        self.flush().await?;
        self.sync().await
    }

    /// Get current offset
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Get statistics
    pub fn stats(&self) -> &BatchedWriterStats {
        &self.stats
    }

    /// Get pending count
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }
}

/// Batched reader for efficient sequential reads
pub struct BatchedReader {
    /// File path
    path: PathBuf,
    /// I/O engine
    engine: Arc<dyn IoEngine>,
    /// Read buffer
    buffer: IoBuffer,
    /// Buffer pool (retained for reuse)
    _pool: Arc<IoBufferPool>,
    /// Current file offset
    file_offset: u64,
    /// Buffer read position
    buffer_pos: usize,
    /// Valid data in buffer
    buffer_len: usize,
    /// File size (cached)
    file_size: u64,
    /// Prefetch size
    prefetch_size: usize,
    /// Statistics
    stats: BatchedReaderStats,
}

/// Statistics for batched reader
#[derive(Debug, Default)]
pub struct BatchedReaderStats {
    /// Total reads
    pub reads: u64,
    /// Total bytes read
    pub bytes_read: u64,
    /// Buffer hits
    pub buffer_hits: u64,
    /// Buffer misses
    pub buffer_misses: u64,
    /// Prefetch reads
    pub prefetches: u64,
}

impl BatchedReader {
    /// Create a new batched reader
    pub async fn new(
        path: impl AsRef<Path>,
        engine: Arc<dyn IoEngine>,
        prefetch_size: usize,
    ) -> IoResult<Self> {
        let pool = Arc::new(IoBufferPool::with_preallocated(prefetch_size, 4));
        let file_size = engine.file_size(path.as_ref()).await?;

        Ok(Self {
            path: path.as_ref().to_path_buf(),
            engine,
            buffer: pool.acquire(),
            _pool: pool,
            file_offset: 0,
            buffer_pos: 0,
            buffer_len: 0,
            file_size,
            prefetch_size,
            stats: BatchedReaderStats::default(),
        })
    }

    /// Read data from current position
    pub async fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        let mut total_read = 0;

        while total_read < buf.len() {
            // Check if we have buffered data
            let available = self.buffer_len - self.buffer_pos;
            if available > 0 {
                let to_copy = (buf.len() - total_read).min(available);
                buf[total_read..total_read + to_copy].copy_from_slice(
                    &self.buffer.as_slice()[self.buffer_pos..self.buffer_pos + to_copy],
                );
                self.buffer_pos += to_copy;
                total_read += to_copy;
                self.stats.buffer_hits += 1;
            } else {
                // Need to refill buffer
                if self.file_offset >= self.file_size {
                    break; // EOF
                }

                self.refill().await?;
                self.stats.buffer_misses += 1;
            }
        }

        self.stats.reads += 1;
        self.stats.bytes_read += total_read as u64;
        Ok(total_read)
    }

    /// Refill the buffer from file
    async fn refill(&mut self) -> IoResult<()> {
        let to_read = self
            .prefetch_size
            .min((self.file_size - self.file_offset) as usize);
        if to_read == 0 {
            return Ok(());
        }

        let result = self
            .engine
            .read(&self.path, self.file_offset, to_read)
            .await?;

        self.buffer = result.buffer;
        self.buffer_pos = 0;
        self.buffer_len = result.bytes_read;
        self.file_offset += result.bytes_read as u64;
        self.stats.prefetches += 1;

        Ok(())
    }

    /// Seek to a position
    pub async fn seek(&mut self, pos: u64) -> IoResult<()> {
        if pos > self.file_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Seek beyond file size",
            ));
        }

        // Check if position is within current buffer
        let buffer_start = self.file_offset - self.buffer_len as u64;
        if pos >= buffer_start && pos < self.file_offset {
            // Position is in buffer
            self.buffer_pos = (pos - buffer_start) as usize;
        } else {
            // Need to refill from new position
            self.file_offset = pos;
            self.buffer_pos = 0;
            self.buffer_len = 0;
        }

        Ok(())
    }

    /// Get current position
    pub fn position(&self) -> u64 {
        self.file_offset - (self.buffer_len - self.buffer_pos) as u64
    }

    /// Get file size
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Get statistics
    pub fn stats(&self) -> &BatchedReaderStats {
        &self.stats
    }

    /// Get buffer hit rate
    pub fn hit_rate(&self) -> f64 {
        let total = self.stats.buffer_hits + self.stats.buffer_misses;
        if total == 0 {
            return 0.0;
        }
        self.stats.buffer_hits as f64 / total as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_creation() {
        let batch = IoBatch::new();
        assert!(batch.is_empty());
        assert!(!batch.is_full());
    }

    #[test]
    fn test_batch_add() {
        let mut batch = IoBatch::with_capacity(2);

        let _rx1 = batch.add(IoOperation::Fsync {
            path: PathBuf::from("/test"),
        });
        assert_eq!(batch.len(), 1);
        assert!(!batch.is_full());

        let _rx2 = batch.add(IoOperation::Fsync {
            path: PathBuf::from("/test2"),
        });
        assert_eq!(batch.len(), 2);
        assert!(batch.is_full());
    }

    #[test]
    fn test_batch_take() {
        let mut batch = IoBatch::with_capacity(4);

        batch.add(IoOperation::Fsync {
            path: PathBuf::from("/test1"),
        });
        batch.add(IoOperation::Fsync {
            path: PathBuf::from("/test2"),
        });

        let ops = batch.take();
        assert_eq!(ops.len(), 2);
        assert!(batch.is_empty());
    }

    #[test]
    fn test_batch_config_default() {
        let config = BatchConfig::default();
        assert_eq!(config.max_batch_size, 16);
        assert!(config.auto_flush);
    }

    #[test]
    fn test_batched_writer_stats() {
        let stats = BatchedWriterStats::default();
        assert_eq!(stats.writes, 0);
        assert_eq!(stats.flushes, 0);
    }

    #[test]
    fn test_batched_reader_stats() {
        let stats = BatchedReaderStats::default();
        assert_eq!(stats.reads, 0);
        assert_eq!(stats.buffer_hits, 0);
    }
}
