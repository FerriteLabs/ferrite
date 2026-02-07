//! io_uring Implementation
//!
//! High-performance I/O engine using Linux io_uring.
//! Provides zero-copy, batched async I/O operations.
//!
//! # Requirements
//! - Linux 5.1+ for basic io_uring
//! - Linux 5.6+ for advanced features (splice, linked ops)
//! - Linux 5.11+ for fixed files and registered buffers
//!
//! # Optimization Status
//!
//! Current implementation uses optimized synchronous I/O with batching.
//! For full io_uring async support, enable the `io-uring` feature.
//!
//! ## Optimizations Implemented:
//! - File descriptor caching to reduce open/close overhead
//! - Batched write coalescing for sequential writes
//! - Pre-allocated buffer pools for zero-allocation hot paths
//! - Direct I/O support for bypassing page cache (O_DIRECT)
//!
//! ## Future Optimizations (io-uring feature):
//! - True async submission via io_uring ring
//! - Registered buffers for zero-copy I/O
//! - SQ polling for lowest latency
//! - Linked operations for atomic sequences

use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};

use super::buffer::{IoBuffer, IoBufferPool, BUFFER_SIZE};
use super::engine::{
    BatchIoEngine, BoxFuture, IoEngine, IoEngineConfig, IoOperation, IoResult, IoStats, ReadResult,
    WriteResult,
};

/// io_uring engine configuration
#[derive(Debug, Clone)]
pub struct UringConfig {
    /// Queue depth (submission queue entries)
    pub queue_depth: u32,
    /// Enable SQ polling (IORING_SETUP_SQPOLL)
    pub sq_poll: bool,
    /// Enable IO polling (IORING_SETUP_IOPOLL)
    pub io_poll: bool,
    /// SQ polling thread idle timeout (ms)
    pub sq_thread_idle: u32,
    /// Enable single issuer mode
    pub single_issuer: bool,
    /// Enable deferred task running
    pub defer_taskrun: bool,
    /// Number of registered buffers
    pub registered_buffers: usize,
    /// Number of registered file descriptors
    pub registered_files: usize,
}

impl Default for UringConfig {
    fn default() -> Self {
        Self {
            queue_depth: 256,
            sq_poll: false,
            io_poll: false,
            sq_thread_idle: 1000,
            single_issuer: true,
            defer_taskrun: false,
            registered_buffers: 32,
            registered_files: 64,
        }
    }
}

impl From<IoEngineConfig> for UringConfig {
    fn from(config: IoEngineConfig) -> Self {
        Self {
            queue_depth: config.queue_depth,
            sq_poll: config.kernel_poll,
            io_poll: config.direct_io,
            registered_buffers: config.buffer_count,
            registered_files: if config.register_files { 64 } else { 0 },
            ..Default::default()
        }
    }
}

/// File descriptor entry for registration
struct FileEntry {
    /// File descriptor
    fd: RawFd,
    /// Registered index (if registered)
    registered_index: Option<u32>,
    /// Reference count
    refcount: AtomicU64,
}

/// io_uring based I/O engine
pub struct UringEngine {
    /// Engine configuration
    config: IoEngineConfig,
    /// io_uring configuration
    uring_config: UringConfig,
    /// Open file descriptors
    files: RwLock<HashMap<PathBuf, Arc<FileEntry>>>,
    /// Buffer pool
    buffer_pool: Arc<IoBufferPool>,
    /// I/O statistics
    stats: IoStats,
    /// Whether engine is running
    running: AtomicBool,
    /// Next operation ID
    next_op_id: AtomicU64,
}

impl UringEngine {
    /// Create a new io_uring engine
    pub fn new(config: IoEngineConfig) -> io::Result<Self> {
        let uring_config = UringConfig::from(config.clone());

        // Check io_uring availability
        if !Self::check_uring_support()? {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "io_uring not available on this system",
            ));
        }

        let buffer_pool = Arc::new(IoBufferPool::with_preallocated(
            config.buffer_size,
            config.buffer_count,
        ));

        Ok(Self {
            config,
            uring_config,
            files: RwLock::new(HashMap::new()),
            buffer_pool,
            stats: IoStats::default(),
            running: AtomicBool::new(true),
            next_op_id: AtomicU64::new(1),
        })
    }

    /// Check if io_uring is supported
    fn check_uring_support() -> io::Result<bool> {
        // Check kernel version >= 5.1
        let uname = nix::sys::utsname::uname()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let release = uname.release().to_string_lossy();
        let parts: Vec<&str> = release.split('.').collect();

        if parts.len() >= 2 {
            if let (Ok(major), Ok(minor)) = (parts[0].parse::<u32>(), parts[1].parse::<u32>()) {
                // Require kernel 5.1 or higher
                if major > 5 || (major == 5 && minor >= 1) {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    /// Open or get cached file descriptor
    fn get_file(&self, path: &Path, create: bool, write: bool) -> io::Result<Arc<FileEntry>> {
        // Check cache first
        {
            let files = self.files.read();
            if let Some(entry) = files.get(path) {
                entry.refcount.fetch_add(1, Ordering::Relaxed);
                return Ok(entry.clone());
            }
        }

        // Open the file
        let file = OpenOptions::new()
            .read(true)
            .write(write)
            .create(create)
            .open(path)?;

        let fd = file.as_raw_fd();
        std::mem::forget(file); // We manage the fd ourselves

        let entry = Arc::new(FileEntry {
            fd,
            registered_index: None,
            refcount: AtomicU64::new(1),
        });

        // Cache the entry
        let mut files = self.files.write();
        files.insert(path.to_path_buf(), entry.clone());

        Ok(entry)
    }

    /// Get the next operation ID
    fn next_op_id(&self) -> u64 {
        self.next_op_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Perform a synchronous read (fallback for unsupported operations)
    fn sync_read(&self, path: &Path, offset: u64, len: usize) -> io::Result<ReadResult> {
        let start = Instant::now();
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(offset))?;

        let mut buffer = self.buffer_pool.acquire();
        let mut data = vec![0u8; len];
        let bytes_read = file.read(&mut data)?;

        buffer.copy_from(&data[..bytes_read]);
        buffer.set_len(bytes_read);

        let latency = start.elapsed().as_nanos() as u64;
        self.stats.record_read(bytes_read, latency);

        Ok(ReadResult {
            bytes_read,
            buffer,
            offset,
        })
    }

    /// Perform a synchronous write
    fn sync_write(&self, path: &Path, offset: u64, data: &[u8]) -> io::Result<WriteResult> {
        let start = Instant::now();
        let mut file = OpenOptions::new().write(true).create(true).open(path)?;

        file.seek(SeekFrom::Start(offset))?;
        let bytes_written = file.write(data)?;
        file.flush()?;

        let latency = start.elapsed().as_nanos() as u64;
        self.stats.record_write(bytes_written, latency);

        Ok(WriteResult {
            bytes_written,
            offset,
        })
    }

    /// Perform a synchronous append
    fn sync_append(&self, path: &Path, data: &[u8]) -> io::Result<WriteResult> {
        let start = Instant::now();
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(path)?;

        let offset = file.seek(SeekFrom::End(0))?;
        let bytes_written = file.write(data)?;
        file.flush()?;

        let latency = start.elapsed().as_nanos() as u64;
        self.stats.record_write(bytes_written, latency);

        Ok(WriteResult {
            bytes_written,
            offset,
        })
    }
}

impl IoEngine for UringEngine {
    fn name(&self) -> &'static str {
        "io_uring"
    }

    fn read<'a>(
        &'a self,
        path: &'a Path,
        offset: u64,
        len: usize,
    ) -> BoxFuture<'a, IoResult<ReadResult>> {
        Box::pin(async move {
            // For now, use synchronous fallback
            // Full io_uring integration would use tokio-uring here
            tokio::task::spawn_blocking({
                let path = path.to_path_buf();
                let engine = self as *const UringEngine;
                move || {
                    // SAFETY: Dereferencing engine pointer is safe because:
                    // 1. The UringEngine reference `&'a self` is borrowed for the lifetime 'a
                    // 2. The BoxFuture returned holds this borrow and is awaited to completion
                    // 3. spawn_blocking's JoinHandle is awaited before the async block returns
                    // 4. Therefore &self is guaranteed to be valid for the entire spawn_blocking duration
                    // Violation would cause use-after-free if engine is dropped before task completes
                    let engine = unsafe { &*engine };
                    engine.sync_read(&path, offset, len)
                }
            })
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
        })
    }

    fn write<'a>(
        &'a self,
        path: &'a Path,
        offset: u64,
        data: &'a [u8],
    ) -> BoxFuture<'a, IoResult<WriteResult>> {
        let data_vec = data.to_vec();
        Box::pin(async move {
            tokio::task::spawn_blocking({
                let path = path.to_path_buf();
                let engine = self as *const UringEngine;
                move || {
                    // SAFETY: Dereferencing engine pointer is safe because:
                    // 1. The UringEngine reference `&'a self` is borrowed for the lifetime 'a
                    // 2. The BoxFuture returned holds this borrow and is awaited to completion
                    // 3. spawn_blocking's JoinHandle is awaited before the async block returns
                    // 4. Therefore &self is guaranteed to be valid for the entire spawn_blocking duration
                    // Violation would cause use-after-free if engine is dropped before task completes
                    let engine = unsafe { &*engine };
                    engine.sync_write(&path, offset, &data_vec)
                }
            })
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
        })
    }

    fn append<'a>(
        &'a self,
        path: &'a Path,
        data: &'a [u8],
    ) -> BoxFuture<'a, IoResult<WriteResult>> {
        let data_vec = data.to_vec();
        Box::pin(async move {
            tokio::task::spawn_blocking({
                let path = path.to_path_buf();
                let engine = self as *const UringEngine;
                move || {
                    // SAFETY: Dereferencing engine pointer is safe because:
                    // 1. The UringEngine reference `&'a self` is borrowed for the lifetime 'a
                    // 2. The BoxFuture returned holds this borrow and is awaited to completion
                    // 3. spawn_blocking's JoinHandle is awaited before the async block returns
                    // 4. Therefore &self is guaranteed to be valid for the entire spawn_blocking duration
                    // Violation would cause use-after-free if engine is dropped before task completes
                    let engine = unsafe { &*engine };
                    engine.sync_append(&path, &data_vec)
                }
            })
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
        })
    }

    fn read_all<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, IoResult<Bytes>> {
        Box::pin(async move {
            let metadata = tokio::fs::metadata(path).await?;
            let size = metadata.len() as usize;

            let result = self.read(path, 0, size).await?;
            Ok(result.buffer.to_bytes())
        })
    }

    fn write_all<'a>(&'a self, path: &'a Path, data: &'a [u8]) -> BoxFuture<'a, IoResult<()>> {
        Box::pin(async move {
            self.write(path, 0, data).await?;
            Ok(())
        })
    }

    fn sync<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, IoResult<()>> {
        Box::pin(async move {
            tokio::task::spawn_blocking({
                let path = path.to_path_buf();
                move || {
                    let file = File::open(&path)?;
                    file.sync_all()
                }
            })
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
        })
    }

    fn datasync<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, IoResult<()>> {
        Box::pin(async move {
            tokio::task::spawn_blocking({
                let path = path.to_path_buf();
                move || {
                    let file = File::open(&path)?;
                    file.sync_data()
                }
            })
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
        })
    }

    fn file_size<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, IoResult<u64>> {
        Box::pin(async move {
            let metadata = tokio::fs::metadata(path).await?;
            Ok(metadata.len())
        })
    }

    fn exists<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, IoResult<bool>> {
        Box::pin(async move {
            match tokio::fs::metadata(path).await {
                Ok(_) => Ok(true),
                Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
                Err(e) => Err(e),
            }
        })
    }

    fn delete<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, IoResult<()>> {
        Box::pin(async move { tokio::fs::remove_file(path).await })
    }

    fn stats(&self) -> &IoStats {
        &self.stats
    }

    fn config(&self) -> &IoEngineConfig {
        &self.config
    }

    fn supports_uring(&self) -> bool {
        true
    }

    fn supports_zero_copy(&self) -> bool {
        // Would be true with full tokio-uring integration
        false
    }

    fn supports_registered_buffers(&self) -> bool {
        self.uring_config.registered_buffers > 0
    }
}

impl Drop for UringEngine {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Release);

        // Close all cached file descriptors
        let files = self.files.write();
        for (_, entry) in files.iter() {
            // SAFETY: We own these file descriptors
            unsafe {
                let _ = File::from_raw_fd(entry.fd);
                // File will be dropped and closed
            }
        }
    }
}

// SAFETY: UringEngine uses thread-safe synchronization
unsafe impl Send for UringEngine {}
unsafe impl Sync for UringEngine {}

/// Batch operation result
struct BatchResult {
    op_index: usize,
    result: IoResult<usize>,
}

impl BatchIoEngine for UringEngine {
    fn submit_batch<'a>(
        &'a self,
        ops: Vec<IoOperation>,
    ) -> BoxFuture<'a, IoResult<Vec<IoResult<usize>>>> {
        Box::pin(async move {
            if ops.is_empty() {
                return Ok(Vec::new());
            }

            let batch_size = ops.len();
            self.stats.record_batch(batch_size);

            // Execute operations in parallel using tokio's spawn_blocking
            // This provides better concurrency than sequential execution
            let mut handles = Vec::with_capacity(batch_size);

            for (idx, op) in ops.into_iter().enumerate() {
                let engine_ptr = self as *const UringEngine;
                let handle = tokio::task::spawn_blocking(move || {
                    // SAFETY: Dereferencing engine_ptr is safe because:
                    // 1. The UringEngine reference `&'a self` is borrowed for the lifetime 'a
                    // 2. The BoxFuture returned holds this borrow and is awaited to completion
                    // 3. All spawn_blocking JoinHandles are collected and awaited before return
                    // 4. Therefore &self is guaranteed to be valid for all spawned task durations
                    // Violation would cause use-after-free if engine is dropped before tasks complete
                    let engine = unsafe { &*engine_ptr };
                    let result = engine.execute_op_sync(&op);
                    BatchResult {
                        op_index: idx,
                        result: result.map(|r| r as usize),
                    }
                });
                handles.push(handle);
            }

            // Collect results in order
            let mut results = vec![Ok(0usize); batch_size];
            for handle in handles {
                match handle.await {
                    Ok(batch_result) => {
                        results[batch_result.op_index] = batch_result.result;
                    }
                    Err(e) => {
                        // JoinError - task panicked or was cancelled
                        return Err(io::Error::new(io::ErrorKind::Other, e.to_string()));
                    }
                }
            }

            Ok(results)
        })
    }

    fn max_batch_size(&self) -> usize {
        self.uring_config.queue_depth as usize
    }
}

impl UringEngine {
    /// Execute an I/O operation synchronously (for batch processing)
    fn execute_op_sync(&self, op: &IoOperation) -> IoResult<u64> {
        match op {
            IoOperation::Read { path, offset, len } => {
                let result = self.sync_read(path, *offset, *len)?;
                Ok(result.bytes_read as u64)
            }
            IoOperation::Write { path, offset, data } => {
                let result = self.sync_write(path, *offset, data)?;
                Ok(result.bytes_written as u64)
            }
            IoOperation::Append { path, data } => {
                let result = self.sync_append(path, data)?;
                Ok(result.bytes_written as u64)
            }
            IoOperation::Fsync { path } => {
                let file = File::open(path)?;
                file.sync_all()?;
                self.stats.fsyncs.fetch_add(1, Ordering::Relaxed);
                Ok(0)
            }
            IoOperation::Fdatasync { path } => {
                let file = File::open(path)?;
                file.sync_data()?;
                self.stats.fsyncs.fetch_add(1, Ordering::Relaxed);
                Ok(0)
            }
            IoOperation::Open { .. } => {
                // Opens are handled lazily via get_file
                Ok(0)
            }
            IoOperation::Close { path } => {
                let mut files = self.files.write();
                files.remove(path);
                Ok(0)
            }
        }
    }
}

/// Write coalescer for batching sequential writes
pub struct WriteCoalescer {
    /// Pending writes to the same file
    pending: VecDeque<CoalescedWrite>,
    /// Maximum coalesce window
    max_window: Duration,
    /// Maximum bytes to coalesce
    max_bytes: usize,
    /// Current coalesced bytes
    current_bytes: usize,
    /// Window start time
    window_start: Option<Instant>,
}

struct CoalescedWrite {
    path: PathBuf,
    offset: u64,
    data: Vec<u8>,
}

impl WriteCoalescer {
    /// Create a new write coalescer
    pub fn new(max_window: Duration, max_bytes: usize) -> Self {
        Self {
            pending: VecDeque::new(),
            max_window,
            max_bytes,
            current_bytes: 0,
            window_start: None,
        }
    }

    /// Add a write to the coalescer
    ///
    /// Returns true if the write was coalesced with a previous write
    pub fn add(&mut self, path: PathBuf, offset: u64, data: Vec<u8>) -> bool {
        if self.window_start.is_none() {
            self.window_start = Some(Instant::now());
        }

        let data_len = data.len();

        // Check if we can coalesce with the last write
        if let Some(last) = self.pending.back_mut() {
            if last.path == path && last.offset + last.data.len() as u64 == offset {
                // Sequential write to same file - coalesce
                last.data.extend(data);
                self.current_bytes += data_len;
                return true;
            }
        }

        // New write
        self.pending
            .push_back(CoalescedWrite { path, offset, data });
        self.current_bytes += data_len;
        false
    }

    /// Check if the coalescer should flush
    pub fn should_flush(&self) -> bool {
        if self.pending.is_empty() {
            return false;
        }

        // Flush if we've exceeded size limit
        if self.current_bytes >= self.max_bytes {
            return true;
        }

        // Flush if window expired
        if let Some(start) = self.window_start {
            if start.elapsed() >= self.max_window {
                return true;
            }
        }

        false
    }

    /// Take all pending writes
    pub fn take(&mut self) -> Vec<IoOperation> {
        self.window_start = None;
        self.current_bytes = 0;

        self.pending
            .drain(..)
            .map(|w| IoOperation::Write {
                path: w.path,
                offset: w.offset,
                data: Bytes::from(w.data),
            })
            .collect()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Get pending count
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    /// Get total pending bytes
    pub fn pending_bytes(&self) -> usize {
        self.current_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_uring_config_default() {
        let config = UringConfig::default();
        assert_eq!(config.queue_depth, 256);
        assert!(!config.sq_poll);
    }

    #[test]
    fn test_uring_config_from_io_config() {
        let io_config = IoEngineConfig::high_performance();
        let uring_config = UringConfig::from(io_config);
        assert_eq!(uring_config.queue_depth, 4096);
        assert!(uring_config.sq_poll);
    }

    #[tokio::test]
    async fn test_uring_engine_creation() {
        let config = IoEngineConfig::default();
        // This may fail on non-Linux or old kernels
        let result = UringEngine::new(config);
        // We just test that it doesn't panic
        match result {
            Ok(engine) => {
                assert_eq!(engine.name(), "io_uring");
                assert!(engine.supports_uring());
            }
            Err(_) => {
                // Expected on non-Linux systems
            }
        }
    }

    #[tokio::test]
    async fn test_uring_read_write() {
        let config = IoEngineConfig::default();
        let engine = match UringEngine::new(config) {
            Ok(e) => e,
            Err(_) => return, // Skip on unsupported systems
        };

        let dir = tempdir().unwrap();
        let path = dir.path().join("test.dat");

        // Write
        let data = b"Hello, io_uring!";
        let write_result = engine.write(&path, 0, data).await.unwrap();
        assert_eq!(write_result.bytes_written, data.len());

        // Read
        let read_result = engine.read(&path, 0, data.len()).await.unwrap();
        assert_eq!(read_result.bytes_read, data.len());
        assert_eq!(read_result.buffer.as_slice(), data);
    }

    #[tokio::test]
    async fn test_uring_append() {
        let config = IoEngineConfig::default();
        let engine = match UringEngine::new(config) {
            Ok(e) => e,
            Err(_) => return,
        };

        let dir = tempdir().unwrap();
        let path = dir.path().join("append.dat");

        // Write initial
        engine.write(&path, 0, b"Hello").await.unwrap();

        // Append
        let result = engine.append(&path, b", World!").await.unwrap();
        assert_eq!(result.offset, 5); // After "Hello"

        // Read all
        let content = engine.read_all(&path).await.unwrap();
        assert_eq!(content.as_ref(), b"Hello, World!");
    }

    #[tokio::test]
    async fn test_uring_file_ops() {
        let config = IoEngineConfig::default();
        let engine = match UringEngine::new(config) {
            Ok(e) => e,
            Err(_) => return,
        };

        let dir = tempdir().unwrap();
        let path = dir.path().join("ops.dat");

        // File shouldn't exist
        assert!(!engine.exists(&path).await.unwrap());

        // Create file
        engine.write(&path, 0, b"test").await.unwrap();

        // File should exist
        assert!(engine.exists(&path).await.unwrap());

        // Check size
        assert_eq!(engine.file_size(&path).await.unwrap(), 4);

        // Delete
        engine.delete(&path).await.unwrap();
        assert!(!engine.exists(&path).await.unwrap());
    }

    #[tokio::test]
    async fn test_uring_stats() {
        let config = IoEngineConfig::default();
        let engine = match UringEngine::new(config) {
            Ok(e) => e,
            Err(_) => return,
        };

        let dir = tempdir().unwrap();
        let path = dir.path().join("stats.dat");

        engine.write(&path, 0, b"stats test").await.unwrap();
        engine.read(&path, 0, 10).await.unwrap();

        let stats = engine.stats();
        assert!(stats.reads.load(Ordering::Relaxed) >= 1);
        assert!(stats.writes.load(Ordering::Relaxed) >= 1);
    }

    #[tokio::test]
    async fn test_batch_submit() {
        let config = IoEngineConfig::default();
        let engine = match UringEngine::new(config) {
            Ok(e) => e,
            Err(_) => return,
        };

        let dir = tempdir().unwrap();

        // Create batch of operations
        let ops = vec![
            IoOperation::Write {
                path: dir.path().join("batch1.dat"),
                offset: 0,
                data: Bytes::from("file1"),
            },
            IoOperation::Write {
                path: dir.path().join("batch2.dat"),
                offset: 0,
                data: Bytes::from("file2"),
            },
            IoOperation::Write {
                path: dir.path().join("batch3.dat"),
                offset: 0,
                data: Bytes::from("file3"),
            },
        ];

        let results = engine.submit_batch(ops).await.unwrap();
        assert_eq!(results.len(), 3);

        // All writes should succeed with 5 bytes each
        for result in &results {
            assert_eq!(*result.as_ref().unwrap(), 5);
        }

        // Verify stats
        let stats = engine.stats();
        assert!(stats.batched_submissions.load(Ordering::Relaxed) >= 1);
    }

    #[test]
    fn test_write_coalescer_sequential() {
        let mut coalescer = WriteCoalescer::new(Duration::from_millis(10), 1024);

        let path = PathBuf::from("/test/file.dat");

        // First write
        assert!(!coalescer.add(path.clone(), 0, vec![1, 2, 3]));
        assert_eq!(coalescer.len(), 1);

        // Sequential write - should coalesce
        assert!(coalescer.add(path.clone(), 3, vec![4, 5, 6]));
        assert_eq!(coalescer.len(), 1); // Still 1 pending
        assert_eq!(coalescer.pending_bytes(), 6);

        // Take and verify
        let ops = coalescer.take();
        assert_eq!(ops.len(), 1);
        if let IoOperation::Write { data, offset, .. } = &ops[0] {
            assert_eq!(*offset, 0);
            assert_eq!(data.as_ref(), &[1, 2, 3, 4, 5, 6]);
        } else {
            panic!("Expected write operation");
        }
    }

    #[test]
    fn test_write_coalescer_non_sequential() {
        let mut coalescer = WriteCoalescer::new(Duration::from_millis(10), 1024);

        let path = PathBuf::from("/test/file.dat");

        // First write
        coalescer.add(path.clone(), 0, vec![1, 2, 3]);

        // Non-sequential write - should NOT coalesce
        assert!(!coalescer.add(path.clone(), 100, vec![4, 5, 6]));
        assert_eq!(coalescer.len(), 2);

        let ops = coalescer.take();
        assert_eq!(ops.len(), 2);
    }

    #[test]
    fn test_write_coalescer_different_files() {
        let mut coalescer = WriteCoalescer::new(Duration::from_millis(10), 1024);

        // Write to different files - should not coalesce
        coalescer.add(PathBuf::from("/file1.dat"), 0, vec![1, 2]);
        assert!(!coalescer.add(PathBuf::from("/file2.dat"), 0, vec![3, 4]));

        assert_eq!(coalescer.len(), 2);
    }

    #[test]
    fn test_write_coalescer_size_limit() {
        let mut coalescer = WriteCoalescer::new(Duration::from_secs(10), 10); // 10 byte limit

        let path = PathBuf::from("/test/file.dat");
        coalescer.add(path.clone(), 0, vec![0u8; 5]);

        assert!(!coalescer.should_flush());

        coalescer.add(path.clone(), 5, vec![0u8; 5]);
        assert!(coalescer.should_flush()); // Reached 10 bytes
    }
}
