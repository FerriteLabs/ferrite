//! Fallback I/O Engine
//!
//! Provides I/O operations using tokio::fs for platforms
//! that don't support io_uring (macOS, Windows, older Linux).

use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use super::buffer::{IoBuffer, IoBufferPool, BUFFER_SIZE};
use super::engine::{
    BoxFuture, IoEngine, IoEngineConfig, IoResult, IoStats, ReadResult, WriteResult,
};

/// Fallback engine configuration
#[derive(Debug, Clone)]
pub struct FallbackConfig {
    /// Buffer size for I/O operations
    pub buffer_size: usize,
    /// Number of buffers in pool
    pub buffer_count: usize,
}

impl Default for FallbackConfig {
    fn default() -> Self {
        Self {
            buffer_size: BUFFER_SIZE,
            buffer_count: 32,
        }
    }
}

impl From<IoEngineConfig> for FallbackConfig {
    fn from(config: IoEngineConfig) -> Self {
        Self {
            buffer_size: config.buffer_size,
            buffer_count: config.buffer_count,
        }
    }
}

/// Fallback I/O engine using tokio::fs
pub struct FallbackEngine {
    /// Engine configuration
    config: IoEngineConfig,
    /// Buffer pool
    buffer_pool: Arc<IoBufferPool>,
    /// I/O statistics
    stats: IoStats,
}

impl FallbackEngine {
    /// Create a new fallback engine
    pub fn new(config: IoEngineConfig) -> io::Result<Self> {
        let buffer_pool = Arc::new(IoBufferPool::with_preallocated(
            config.buffer_size,
            config.buffer_count,
        ));

        Ok(Self {
            config,
            buffer_pool,
            stats: IoStats::default(),
        })
    }

    /// Get a buffer from the pool
    fn acquire_buffer(&self) -> IoBuffer {
        self.buffer_pool.acquire()
    }
}

impl IoEngine for FallbackEngine {
    fn name(&self) -> &'static str {
        "fallback"
    }

    fn read<'a>(
        &'a self,
        path: &'a Path,
        offset: u64,
        len: usize,
    ) -> BoxFuture<'a, IoResult<ReadResult>> {
        Box::pin(async move {
            let start = Instant::now();

            let mut file = File::open(path).await?;
            file.seek(std::io::SeekFrom::Start(offset)).await?;

            let mut buffer = self.acquire_buffer();
            let mut data = vec![0u8; len];
            let bytes_read = file.read(&mut data).await?;

            buffer.copy_from(&data[..bytes_read]);
            buffer.set_len(bytes_read);

            let latency = start.elapsed().as_nanos() as u64;
            self.stats.record_read(bytes_read, latency);

            Ok(ReadResult {
                bytes_read,
                buffer,
                offset,
            })
        })
    }

    fn write<'a>(
        &'a self,
        path: &'a Path,
        offset: u64,
        data: &'a [u8],
    ) -> BoxFuture<'a, IoResult<WriteResult>> {
        Box::pin(async move {
            let start = Instant::now();

            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)
                .await?;

            file.seek(std::io::SeekFrom::Start(offset)).await?;
            let bytes_written = file.write(data).await?;
            file.flush().await?;

            let latency = start.elapsed().as_nanos() as u64;
            self.stats.record_write(bytes_written, latency);

            Ok(WriteResult {
                bytes_written,
                offset,
            })
        })
    }

    fn append<'a>(
        &'a self,
        path: &'a Path,
        data: &'a [u8],
    ) -> BoxFuture<'a, IoResult<WriteResult>> {
        Box::pin(async move {
            let start = Instant::now();

            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .append(true)
                .open(path)
                .await?;

            let offset = file.seek(std::io::SeekFrom::End(0)).await?;
            let bytes_written = file.write(data).await?;
            file.flush().await?;

            let latency = start.elapsed().as_nanos() as u64;
            self.stats.record_write(bytes_written, latency);

            Ok(WriteResult {
                bytes_written,
                offset,
            })
        })
    }

    fn read_all<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, IoResult<Bytes>> {
        Box::pin(async move {
            let start = Instant::now();

            let data = fs::read(path).await?;
            let bytes = Bytes::from(data);

            let latency = start.elapsed().as_nanos() as u64;
            self.stats.record_read(bytes.len(), latency);

            Ok(bytes)
        })
    }

    fn write_all<'a>(&'a self, path: &'a Path, data: &'a [u8]) -> BoxFuture<'a, IoResult<()>> {
        Box::pin(async move {
            let start = Instant::now();

            fs::write(path, data).await?;

            let latency = start.elapsed().as_nanos() as u64;
            self.stats.record_write(data.len(), latency);

            Ok(())
        })
    }

    fn sync<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, IoResult<()>> {
        Box::pin(async move {
            let file = File::open(path).await?;
            file.sync_all().await?;
            self.stats
                .fsyncs
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        })
    }

    fn datasync<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, IoResult<()>> {
        Box::pin(async move {
            let file = File::open(path).await?;
            file.sync_data().await?;
            self.stats
                .fsyncs
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        })
    }

    fn file_size<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, IoResult<u64>> {
        Box::pin(async move {
            let metadata = fs::metadata(path).await?;
            Ok(metadata.len())
        })
    }

    fn exists<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, IoResult<bool>> {
        Box::pin(async move {
            match fs::metadata(path).await {
                Ok(_) => Ok(true),
                Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
                Err(e) => Err(e),
            }
        })
    }

    fn delete<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, IoResult<()>> {
        Box::pin(async move { fs::remove_file(path).await })
    }

    fn stats(&self) -> &IoStats {
        &self.stats
    }

    fn config(&self) -> &IoEngineConfig {
        &self.config
    }

    fn supports_uring(&self) -> bool {
        false
    }

    fn supports_zero_copy(&self) -> bool {
        false
    }

    fn supports_registered_buffers(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_fallback_engine_creation() {
        let config = IoEngineConfig::default();
        let engine = FallbackEngine::new(config).unwrap();
        assert_eq!(engine.name(), "fallback");
        assert!(!engine.supports_uring());
    }

    #[tokio::test]
    async fn test_fallback_read_write() {
        let config = IoEngineConfig::default();
        let engine = FallbackEngine::new(config).unwrap();

        let dir = tempdir().unwrap();
        let path = dir.path().join("test.dat");

        // Write
        let data = b"Hello, fallback!";
        let write_result = engine.write(&path, 0, data).await.unwrap();
        assert_eq!(write_result.bytes_written, data.len());

        // Read
        let read_result = engine.read(&path, 0, data.len()).await.unwrap();
        assert_eq!(read_result.bytes_read, data.len());
        assert_eq!(read_result.buffer.as_slice(), data);
    }

    #[tokio::test]
    async fn test_fallback_append() {
        let config = IoEngineConfig::default();
        let engine = FallbackEngine::new(config).unwrap();

        let dir = tempdir().unwrap();
        let path = dir.path().join("append.dat");

        // Write initial
        engine.write(&path, 0, b"Hello").await.unwrap();

        // Append
        let result = engine.append(&path, b", World!").await.unwrap();
        assert_eq!(result.offset, 5);

        // Read all
        let content = engine.read_all(&path).await.unwrap();
        assert_eq!(content.as_ref(), b"Hello, World!");
    }

    #[tokio::test]
    async fn test_fallback_file_ops() {
        let config = IoEngineConfig::default();
        let engine = FallbackEngine::new(config).unwrap();

        let dir = tempdir().unwrap();
        let path = dir.path().join("ops.dat");

        // File shouldn't exist
        assert!(!engine.exists(&path).await.unwrap());

        // Create file
        engine.write_all(&path, b"test").await.unwrap();

        // File should exist
        assert!(engine.exists(&path).await.unwrap());

        // Check size
        assert_eq!(engine.file_size(&path).await.unwrap(), 4);

        // Sync
        engine.sync(&path).await.unwrap();
        engine.datasync(&path).await.unwrap();

        // Delete
        engine.delete(&path).await.unwrap();
        assert!(!engine.exists(&path).await.unwrap());
    }

    #[tokio::test]
    async fn test_fallback_partial_read() {
        let config = IoEngineConfig::default();
        let engine = FallbackEngine::new(config).unwrap();

        let dir = tempdir().unwrap();
        let path = dir.path().join("partial.dat");

        // Write data
        engine.write_all(&path, b"0123456789").await.unwrap();

        // Read middle portion
        let result = engine.read(&path, 3, 4).await.unwrap();
        assert_eq!(result.bytes_read, 4);
        assert_eq!(result.buffer.as_slice(), b"3456");
        assert_eq!(result.offset, 3);
    }

    #[tokio::test]
    async fn test_fallback_stats() {
        use std::sync::atomic::Ordering;

        let config = IoEngineConfig::default();
        let engine = FallbackEngine::new(config).unwrap();

        let dir = tempdir().unwrap();
        let path = dir.path().join("stats.dat");

        // Perform operations
        engine.write_all(&path, b"stats test").await.unwrap();
        engine.read_all(&path).await.unwrap();
        engine.sync(&path).await.unwrap();

        let stats = engine.stats();
        assert!(stats.reads.load(Ordering::Relaxed) >= 1);
        assert!(stats.writes.load(Ordering::Relaxed) >= 1);
        assert!(stats.fsyncs.load(Ordering::Relaxed) >= 1);
    }

    #[tokio::test]
    async fn test_fallback_large_file() {
        let config = IoEngineConfig::default();
        let engine = FallbackEngine::new(config).unwrap();

        let dir = tempdir().unwrap();
        let path = dir.path().join("large.dat");

        // Write 1MB of data
        let data = vec![0u8; 1024 * 1024];
        engine.write_all(&path, &data).await.unwrap();

        // Verify size
        assert_eq!(engine.file_size(&path).await.unwrap(), 1024 * 1024);

        // Read back
        let content = engine.read_all(&path).await.unwrap();
        assert_eq!(content.len(), 1024 * 1024);
    }

    #[tokio::test]
    async fn test_fallback_concurrent_writes() {
        use tokio::task::JoinSet;

        let config = IoEngineConfig::default();
        let engine = Arc::new(FallbackEngine::new(config).unwrap());

        let dir = tempdir().unwrap();
        let mut tasks = JoinSet::new();

        // Spawn concurrent writes to different files
        for i in 0..10 {
            let engine = engine.clone();
            let path = dir.path().join(format!("file_{}.dat", i));
            tasks.spawn(async move {
                engine
                    .write_all(&path, format!("data {}", i).as_bytes())
                    .await
            });
        }

        // Wait for all writes
        while let Some(result) = tasks.join_next().await {
            result.unwrap().unwrap();
        }

        // Verify all files exist
        for i in 0..10 {
            let path = dir.path().join(format!("file_{}.dat", i));
            assert!(engine.exists(&path).await.unwrap());
        }
    }
}
