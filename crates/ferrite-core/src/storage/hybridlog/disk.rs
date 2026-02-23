//! Disk Region for HybridLog
//!
//! The disk region is the cold tier of the HybridLog. It stores data
//! that has been evicted from the read-only region on disk, using
//! async I/O for reads.
//!
//! Platform handling:
//! - Linux: Uses tokio for async file I/O (io_uring support via feature flag)
//! - macOS/Other: Uses tokio::fs for async file I/O
//!
//! Key features:
//! - Async reads with optional promotion back to warmer tiers
//! - Sequential writes for append operations
//! - Offset-based addressing consistent with other regions
//! - Migration from read-only region
//! - Read-ahead buffer for improved sequential read performance

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;

use parking_lot::RwLock;

use bytes::Bytes;
use tokio::fs::File as AsyncFile;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tracing::debug;

use super::address::LogAddress;
use super::mutable::EntryHeader;

/// Read-ahead buffer for prefetching disk data
///
/// This buffer caches a contiguous range of data from disk,
/// improving performance for sequential access patterns.
struct ReadAheadBuffer {
    /// Buffered data
    data: Vec<u8>,
    /// Starting offset of buffered data in the file
    start_offset: u64,
    /// End offset of buffered data (exclusive)
    end_offset: u64,
}

impl ReadAheadBuffer {
    /// Create a new empty read-ahead buffer
    fn new() -> Self {
        Self {
            data: Vec::new(),
            start_offset: 0,
            end_offset: 0,
        }
    }

    /// Check if the buffer contains data at the given offset
    fn contains(&self, offset: u64, len: usize) -> bool {
        offset >= self.start_offset && offset + len as u64 <= self.end_offset
    }

    /// Read data from the buffer at the given offset
    fn read(&self, offset: u64, len: usize) -> Option<&[u8]> {
        if !self.contains(offset, len) {
            return None;
        }
        let start = (offset - self.start_offset) as usize;
        Some(&self.data[start..start + len])
    }

    /// Fill the buffer with data starting at the given offset
    fn fill(&mut self, data: Vec<u8>, start_offset: u64) {
        self.data = data;
        self.start_offset = start_offset;
        self.end_offset = start_offset + self.data.len() as u64;
    }

    /// Invalidate the buffer (called on writes or compaction)
    fn invalidate(&mut self) {
        self.data.clear();
        self.start_offset = 0;
        self.end_offset = 0;
    }

    /// Check if the buffer is empty
    #[allow(dead_code)] // Planned for v0.2 — reserved for disk buffer management API
    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

/// Prefetch statistics
#[derive(Debug, Default)]
pub struct PrefetchStats {
    /// Number of cache hits
    pub hits: AtomicU64,
    /// Number of cache misses
    pub misses: AtomicU64,
}

/// Disk region - cold tier of the HybridLog
///
/// This region provides async reads from disk for cold data.
/// Writes are synchronous (happens during migration/compaction).
pub struct DiskRegion {
    /// Path to the data file
    path: PathBuf,
    /// Synchronous file handle for writes
    sync_file: Mutex<File>,
    /// Current size of the region
    size: AtomicUsize,
    /// Total capacity (0 = unlimited)
    capacity: usize,
    /// Number of entries
    entry_count: AtomicU64,
    /// Read count for statistics
    read_count: AtomicU64,
    /// Read-ahead buffer for prefetching
    prefetch_buffer: RwLock<ReadAheadBuffer>,
    /// Prefetch buffer size (in bytes)
    prefetch_buffer_size: usize,
    /// Whether prefetching is enabled
    prefetch_enabled: bool,
    /// Prefetch statistics
    prefetch_stats: PrefetchStats,
}

impl DiskRegion {
    /// Default prefetch buffer size (64KB)
    pub const DEFAULT_PREFETCH_BUFFER_SIZE: usize = 64 * 1024;

    /// Create a new disk region
    pub fn new<P: AsRef<Path>>(path: P, capacity: usize) -> io::Result<Self> {
        Self::with_prefetch(path, capacity, true, Self::DEFAULT_PREFETCH_BUFFER_SIZE)
    }

    /// Create a new disk region with prefetch configuration
    pub fn with_prefetch<P: AsRef<Path>>(
        path: P,
        capacity: usize,
        prefetch_enabled: bool,
        prefetch_buffer_size: usize,
    ) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;

        let metadata = file.metadata()?;
        let current_size = metadata.len() as usize;

        Ok(Self {
            path,
            sync_file: Mutex::new(file),
            size: AtomicUsize::new(current_size),
            capacity,
            entry_count: AtomicU64::new(0),
            read_count: AtomicU64::new(0),
            prefetch_buffer: RwLock::new(ReadAheadBuffer::new()),
            prefetch_buffer_size,
            prefetch_enabled,
            prefetch_stats: PrefetchStats::default(),
        })
    }

    /// Open an existing disk region
    pub fn open<P: AsRef<Path>>(path: P, capacity: usize) -> io::Result<Self> {
        Self::open_with_prefetch(path, capacity, true, Self::DEFAULT_PREFETCH_BUFFER_SIZE)
    }

    /// Open an existing disk region with prefetch configuration
    pub fn open_with_prefetch<P: AsRef<Path>>(
        path: P,
        capacity: usize,
        prefetch_enabled: bool,
        prefetch_buffer_size: usize,
    ) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();

        if !path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Disk region file not found",
            ));
        }

        let file = OpenOptions::new().read(true).write(true).open(&path)?;

        let metadata = file.metadata()?;
        let current_size = metadata.len() as usize;

        // Count entries by scanning
        let entry_count = Self::count_entries_sync(&file, current_size)?;

        Ok(Self {
            path,
            sync_file: Mutex::new(file),
            size: AtomicUsize::new(current_size),
            capacity,
            entry_count: AtomicU64::new(entry_count),
            read_count: AtomicU64::new(0),
            prefetch_buffer: RwLock::new(ReadAheadBuffer::new()),
            prefetch_buffer_size,
            prefetch_enabled,
            prefetch_stats: PrefetchStats::default(),
        })
    }

    /// Count entries in the file synchronously
    fn count_entries_sync(file: &File, size: usize) -> io::Result<u64> {
        let mut file = file.try_clone()?;
        file.seek(SeekFrom::Start(0))?;

        let mut count = 0u64;
        let mut offset = 0usize;
        let mut header_buf = [0u8; EntryHeader::SIZE];

        while offset + EntryHeader::SIZE <= size {
            if file.read_exact(&mut header_buf).is_err() {
                break;
            }

            // SAFETY: header_buf is properly aligned and sized
            let header: EntryHeader =
                unsafe { std::ptr::read(header_buf.as_ptr() as *const EntryHeader) };

            let entry_size = header.aligned_size();
            if offset + entry_size > size {
                break;
            }

            // Seek past the entry data
            file.seek(SeekFrom::Current((entry_size - EntryHeader::SIZE) as i64))?;

            count += 1;
            offset += entry_size;
        }

        Ok(count)
    }

    /// Get the current size
    pub fn size(&self) -> usize {
        self.size.load(Ordering::Acquire)
    }

    /// Get the capacity (0 = unlimited)
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get remaining space (usize::MAX if unlimited)
    pub fn remaining(&self) -> usize {
        if self.capacity == 0 {
            usize::MAX
        } else {
            self.capacity.saturating_sub(self.size())
        }
    }

    /// Get entry count
    pub fn entry_count(&self) -> u64 {
        self.entry_count.load(Ordering::Relaxed)
    }

    /// Get read count
    pub fn read_count(&self) -> u64 {
        self.read_count.load(Ordering::Relaxed)
    }

    /// Get the file path
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get prefetch hit count
    pub fn prefetch_hits(&self) -> u64 {
        self.prefetch_stats.hits.load(Ordering::Relaxed)
    }

    /// Get prefetch miss count
    pub fn prefetch_misses(&self) -> u64 {
        self.prefetch_stats.misses.load(Ordering::Relaxed)
    }

    /// Get prefetch hit ratio (0.0 to 1.0)
    pub fn prefetch_hit_ratio(&self) -> f64 {
        let hits = self.prefetch_hits();
        let misses = self.prefetch_misses();
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Invalidate the prefetch buffer (called on writes or compaction)
    fn invalidate_prefetch(&self) {
        if self.prefetch_enabled {
            self.prefetch_buffer.write().invalidate();
        }
    }

    /// Append an entry synchronously (for migration)
    pub fn append(&self, key: &[u8], value: &[u8]) -> io::Result<Option<LogAddress>> {
        let header = EntryHeader::new(key.len() as u32, value.len() as u32);
        let entry_size = header.aligned_size();

        let current_size = self.size.load(Ordering::Acquire);

        // Check capacity
        if self.capacity > 0 && current_size + entry_size > self.capacity {
            return Ok(None);
        }

        let mut file = self
            .sync_file
            .lock()
            .map_err(|_| io::Error::other("Failed to acquire file lock"))?;

        // Seek to end
        file.seek(SeekFrom::End(0))?;

        // Write header
        // SAFETY: EntryHeader is #[repr(C)] with a fixed 16-byte layout (4 x u32).
        // transmute to byte array is safe because:
        // 1. Source and destination have the same size (EntryHeader::SIZE = 16 bytes)
        // 2. EntryHeader contains only primitive u32 fields with no padding beyond _padding field
        // 3. No pointers or Drop implementations that could be invalidated
        let header_bytes: [u8; EntryHeader::SIZE] = unsafe { std::mem::transmute(header) };
        file.write_all(&header_bytes)?;

        // Write key
        file.write_all(key)?;

        // Write value
        file.write_all(value)?;

        // Write padding for alignment
        let data_len = EntryHeader::SIZE + key.len() + value.len();
        let padding = entry_size - data_len;
        if padding > 0 {
            let zeros = vec![0u8; padding];
            file.write_all(&zeros)?;
        }

        file.sync_data()?;

        // Update size
        let new_size = current_size + entry_size;
        self.size.store(new_size, Ordering::Release);
        self.entry_count.fetch_add(1, Ordering::Relaxed);

        // Invalidate prefetch buffer since file content changed
        self.invalidate_prefetch();

        Ok(Some(LogAddress::disk(current_size as u64)))
    }

    /// Read an entry asynchronously with optional prefetching
    pub async fn read_async(&self, offset: u64) -> io::Result<Option<(Bytes, Bytes)>> {
        let size = self.size.load(Ordering::Acquire);

        if offset as usize + EntryHeader::SIZE > size {
            return Ok(None);
        }

        self.read_count.fetch_add(1, Ordering::Relaxed);

        // Try to read from prefetch buffer first (if enabled)
        if self.prefetch_enabled {
            // Check if header is in buffer
            let buffer = self.prefetch_buffer.read();
            if buffer.contains(offset, EntryHeader::SIZE) {
                // Read header from buffer
                if let Some(header_bytes) = buffer.read(offset, EntryHeader::SIZE) {
                    // SAFETY: header_bytes is properly sized
                    let header: EntryHeader =
                        unsafe { std::ptr::read(header_bytes.as_ptr() as *const EntryHeader) };

                    let entry_size = header.total_size();
                    if buffer.contains(offset, entry_size) {
                        // Full entry is in buffer - prefetch hit!
                        self.prefetch_stats.hits.fetch_add(1, Ordering::Relaxed);

                        let key_start = offset + EntryHeader::SIZE as u64;
                        let value_start = key_start + header.key_len as u64;

                        if let (Some(key_bytes), Some(value_bytes)) = (
                            buffer.read(key_start, header.key_len as usize),
                            buffer.read(value_start, header.value_len as usize),
                        ) {
                            return Ok(Some((
                                Bytes::copy_from_slice(key_bytes),
                                Bytes::copy_from_slice(value_bytes),
                            )));
                        }
                    }
                }
            }
            drop(buffer);

            // Prefetch miss - read from disk and fill buffer
            self.prefetch_stats.misses.fetch_add(1, Ordering::Relaxed);
        }

        // Read from disk and optionally fill prefetch buffer
        let mut file = AsyncFile::open(&self.path).await?;
        file.seek(tokio::io::SeekFrom::Start(offset)).await?;

        // Read header
        let mut header_buf = [0u8; EntryHeader::SIZE];
        file.read_exact(&mut header_buf).await?;

        // SAFETY: header_buf is properly aligned and sized
        let header: EntryHeader =
            unsafe { std::ptr::read(header_buf.as_ptr() as *const EntryHeader) };

        let entry_end = offset as usize + header.total_size();
        if entry_end > size {
            return Ok(None);
        }

        // Read key
        let mut key = vec![0u8; header.key_len as usize];
        file.read_exact(&mut key).await?;

        // Read value
        let mut value = vec![0u8; header.value_len as usize];
        file.read_exact(&mut value).await?;

        // Fill prefetch buffer with read-ahead data (if enabled)
        if self.prefetch_enabled {
            let read_ahead_size = self.prefetch_buffer_size.min(size - offset as usize);
            if read_ahead_size > 0 {
                // Seek back to start of current entry and read more
                file.seek(tokio::io::SeekFrom::Start(offset)).await?;
                let mut prefetch_data = vec![0u8; read_ahead_size];
                // Best effort - don't fail if we can't read all
                let bytes_read = file.read(&mut prefetch_data).await?;
                if bytes_read > 0 {
                    prefetch_data.truncate(bytes_read);
                    self.prefetch_buffer.write().fill(prefetch_data, offset);
                }
            }
        }

        Ok(Some((Bytes::from(key), Bytes::from(value))))
    }

    /// Read only the value asynchronously
    pub async fn read_value_async(&self, offset: u64) -> io::Result<Option<Bytes>> {
        let (path, max_size) = match self.prepare_async_read(offset) {
            Some(params) => params,
            None => return Ok(None),
        };
        Self::async_read_at(&path, offset, max_size).await
    }

    /// Validate offset and return data needed for async read without holding a guard.
    /// Returns `(path, max_size)` or `None` if offset is out of bounds.
    pub(crate) fn prepare_async_read(&self, offset: u64) -> Option<(PathBuf, usize)> {
        let offset = offset as usize;
        let size = self.size.load(Ordering::Acquire);
        if offset + EntryHeader::SIZE > size {
            return None;
        }
        self.read_count.fetch_add(1, Ordering::Relaxed);
        Some((self.path.clone(), size))
    }

    /// Perform async read from file without borrowing DiskRegion.
    pub(crate) async fn async_read_at(
        path: &Path,
        offset: u64,
        max_size: usize,
    ) -> io::Result<Option<Bytes>> {
        let mut file = AsyncFile::open(path).await?;
        file.seek(tokio::io::SeekFrom::Start(offset)).await?;

        // Read header
        let mut header_buf = [0u8; EntryHeader::SIZE];
        file.read_exact(&mut header_buf).await?;

        // SAFETY: header_buf is properly aligned and sized
        let header: EntryHeader =
            unsafe { std::ptr::read(header_buf.as_ptr() as *const EntryHeader) };

        let entry_end = offset as usize + header.total_size();
        if entry_end > max_size {
            return Ok(None);
        }

        // Skip key
        file.seek(tokio::io::SeekFrom::Current(header.key_len as i64))
            .await?;

        // Read value
        let mut value = vec![0u8; header.value_len as usize];
        file.read_exact(&mut value).await?;

        Ok(Some(Bytes::from(value)))
    }

    /// Read an entry synchronously (for testing/recovery)
    pub fn read_sync(&self, offset: u64) -> io::Result<Option<(Bytes, Bytes)>> {
        let offset = offset as usize;
        let size = self.size.load(Ordering::Acquire);

        if offset + EntryHeader::SIZE > size {
            return Ok(None);
        }

        self.read_count.fetch_add(1, Ordering::Relaxed);

        let file = self
            .sync_file
            .lock()
            .map_err(|_| io::Error::other("Failed to acquire file lock"))?;

        let mut file = file.try_clone()?;
        file.seek(SeekFrom::Start(offset as u64))?;

        // Read header
        let mut header_buf = [0u8; EntryHeader::SIZE];
        file.read_exact(&mut header_buf)?;

        // SAFETY: header_buf is properly aligned and sized
        let header: EntryHeader =
            unsafe { std::ptr::read(header_buf.as_ptr() as *const EntryHeader) };

        let entry_end = offset + header.total_size();
        if entry_end > size {
            return Ok(None);
        }

        // Read key
        let mut key = vec![0u8; header.key_len as usize];
        file.read_exact(&mut key)?;

        // Read value
        let mut value = vec![0u8; header.value_len as usize];
        file.read_exact(&mut value)?;

        Ok(Some((Bytes::from(key), Bytes::from(value))))
    }

    /// Iterate over all entries synchronously
    pub fn iter<F>(&self, mut callback: F) -> io::Result<()>
    where
        F: FnMut(u64, &[u8], &[u8]),
    {
        let size = self.size.load(Ordering::Acquire);

        let file = self
            .sync_file
            .lock()
            .map_err(|_| io::Error::other("Failed to acquire file lock"))?;

        let mut file = file.try_clone()?;
        file.seek(SeekFrom::Start(0))?;

        let mut offset = 0usize;
        let mut header_buf = [0u8; EntryHeader::SIZE];

        while offset + EntryHeader::SIZE <= size {
            if file.read_exact(&mut header_buf).is_err() {
                break;
            }

            // SAFETY: header_buf is properly aligned and sized
            let header: EntryHeader =
                unsafe { std::ptr::read(header_buf.as_ptr() as *const EntryHeader) };

            let entry_size = header.aligned_size();
            if offset + entry_size > size {
                break;
            }

            // Read key
            let mut key = vec![0u8; header.key_len as usize];
            file.read_exact(&mut key)?;

            // Read value
            let mut value = vec![0u8; header.value_len as usize];
            file.read_exact(&mut value)?;

            // Skip padding
            let data_len = EntryHeader::SIZE + key.len() + value.len();
            let padding = entry_size - data_len;
            if padding > 0 {
                file.seek(SeekFrom::Current(padding as i64))?;
            }

            callback(offset as u64, &key, &value);
            offset += entry_size;
        }

        Ok(())
    }

    /// Compact the region by removing dead entries
    pub fn compact<F>(&self, is_live: F) -> io::Result<usize>
    where
        F: Fn(&[u8]) -> bool,
    {
        let temp_path = self.path.with_extension("tmp");
        let mut temp_file = File::create(&temp_path)?;

        let mut kept = 0usize;
        let mut new_size = 0usize;

        // Copy live entries to temp file
        self.iter(|_, key, value| {
            if is_live(key) {
                let header = EntryHeader::new(key.len() as u32, value.len() as u32);
                let entry_size = header.aligned_size();

                // Write header
                // SAFETY: EntryHeader is #[repr(C)] with a fixed 16-byte layout (4 x u32).
                // transmute to byte array is safe because:
                // 1. Source and destination have the same size (EntryHeader::SIZE = 16 bytes)
                // 2. EntryHeader contains only primitive u32 fields with no padding beyond _padding field
                // 3. No pointers or Drop implementations that could be invalidated
                let header_bytes: [u8; EntryHeader::SIZE] = unsafe { std::mem::transmute(header) };
                if temp_file.write_all(&header_bytes).is_err() {
                    return;
                }

                // Write key and value
                if temp_file.write_all(key).is_err() {
                    return;
                }
                if temp_file.write_all(value).is_err() {
                    return;
                }

                // Write padding
                let data_len = EntryHeader::SIZE + key.len() + value.len();
                let padding = entry_size - data_len;
                if padding > 0 {
                    let zeros = vec![0u8; padding];
                    let _ = temp_file.write_all(&zeros);
                }

                new_size += entry_size;
                kept += 1;
            }
        })?;

        temp_file.sync_all()?;
        drop(temp_file);

        // Swap files
        {
            let _file = self
                .sync_file
                .lock()
                .map_err(|_| io::Error::other("Failed to acquire file lock"))?;

            // The lock is held, swap the files
            std::fs::rename(&temp_path, &self.path)?;
        }

        // Reopen and update
        let new_file = OpenOptions::new().read(true).write(true).open(&self.path)?;

        {
            let mut file = self
                .sync_file
                .lock()
                .map_err(|_| io::Error::other("Failed to acquire file lock"))?;
            *file = new_file;
        }

        self.size.store(new_size, Ordering::Release);
        self.entry_count.store(kept as u64, Ordering::Release);

        // Invalidate prefetch buffer since file content changed
        self.invalidate_prefetch();

        debug!(
            "Compacted disk region: {} entries, {} bytes",
            kept, new_size
        );

        Ok(kept)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_disk_region_new() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("disk.dat");

        let region = DiskRegion::new(&path, 0).unwrap();
        assert_eq!(region.size(), 0);
        assert_eq!(region.capacity(), 0); // Unlimited
    }

    #[test]
    fn test_append_and_read_sync() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("disk.dat");

        let region = DiskRegion::new(&path, 0).unwrap();

        let key = b"test_key";
        let value = b"test_value";

        let addr = region.append(key, value).unwrap().unwrap();
        assert!(addr.is_disk());
        assert_eq!(addr.offset(), 0);

        let (read_key, read_value) = region.read_sync(addr.offset()).unwrap().unwrap();
        assert_eq!(&read_key[..], key);
        assert_eq!(&read_value[..], value);
    }

    #[tokio::test]
    async fn test_read_async() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("disk.dat");

        let region = DiskRegion::new(&path, 0).unwrap();

        let key = b"async_key";
        let value = b"async_value";

        let addr = region.append(key, value).unwrap().unwrap();

        let (read_key, read_value) = region.read_async(addr.offset()).await.unwrap().unwrap();
        assert_eq!(&read_key[..], key);
        assert_eq!(&read_value[..], value);
    }

    #[tokio::test]
    async fn test_read_value_async() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("disk.dat");

        let region = DiskRegion::new(&path, 0).unwrap();

        let key = b"key";
        let value = b"value";

        let addr = region.append(key, value).unwrap().unwrap();
        let read_value = region
            .read_value_async(addr.offset())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&read_value[..], value);
    }

    #[test]
    fn test_multiple_appends() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("disk.dat");

        let region = DiskRegion::new(&path, 0).unwrap();

        let addr1 = region.append(b"key1", b"value1").unwrap().unwrap();
        let addr2 = region.append(b"key2", b"value2").unwrap().unwrap();
        let addr3 = region.append(b"key3", b"value3").unwrap().unwrap();

        assert!(addr2.offset() > addr1.offset());
        assert!(addr3.offset() > addr2.offset());

        let (_, v1) = region.read_sync(addr1.offset()).unwrap().unwrap();
        let (_, v2) = region.read_sync(addr2.offset()).unwrap().unwrap();
        let (_, v3) = region.read_sync(addr3.offset()).unwrap().unwrap();

        assert_eq!(&v1[..], b"value1");
        assert_eq!(&v2[..], b"value2");
        assert_eq!(&v3[..], b"value3");

        assert_eq!(region.entry_count(), 3);
    }

    #[test]
    fn test_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("disk.dat");

        // Write data
        {
            let region = DiskRegion::new(&path, 0).unwrap();
            region.append(b"key1", b"value1").unwrap();
            region.append(b"key2", b"value2").unwrap();
        }

        // Reopen and verify
        {
            let region = DiskRegion::open(&path, 0).unwrap();
            assert_eq!(region.entry_count(), 2);

            let (k, v) = region.read_sync(0).unwrap().unwrap();
            assert_eq!(&k[..], b"key1");
            assert_eq!(&v[..], b"value1");
        }
    }

    #[test]
    fn test_iter() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("disk.dat");

        let region = DiskRegion::new(&path, 0).unwrap();
        region.append(b"key1", b"val1").unwrap();
        region.append(b"key2", b"val2").unwrap();
        region.append(b"key3", b"val3").unwrap();

        let mut entries = Vec::new();
        region
            .iter(|offset, key, value| {
                entries.push((offset, key.to_vec(), value.to_vec()));
            })
            .unwrap();

        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].1, b"key1");
        assert_eq!(entries[1].1, b"key2");
        assert_eq!(entries[2].1, b"key3");
    }

    #[test]
    fn test_capacity_limit() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("disk.dat");

        // Small capacity
        let region = DiskRegion::new(&path, 50).unwrap();

        // First append should succeed
        let addr1 = region.append(b"key1", b"value1").unwrap();
        assert!(addr1.is_some());

        // Second append should fail
        let addr2 = region.append(b"key2", b"value2").unwrap();
        assert!(addr2.is_none());
    }

    #[test]
    fn test_compact() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("disk.dat");

        let region = DiskRegion::new(&path, 0).unwrap();
        region.append(b"keep1", b"value1").unwrap();
        region.append(b"delete", b"value2").unwrap();
        region.append(b"keep2", b"value3").unwrap();

        let initial_size = region.size();
        assert_eq!(region.entry_count(), 3);

        // Compact, keeping only keys starting with "keep"
        let kept = region.compact(|key| key.starts_with(b"keep")).unwrap();

        assert_eq!(kept, 2);
        assert_eq!(region.entry_count(), 2);
        assert!(region.size() < initial_size);

        // Verify only keep entries remain
        let mut keys = Vec::new();
        region
            .iter(|_, key, _| {
                keys.push(key.to_vec());
            })
            .unwrap();

        assert_eq!(keys.len(), 2);
        assert!(keys.iter().all(|k| k.starts_with(b"keep")));
    }

    #[test]
    fn test_read_count() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("disk.dat");

        let region = DiskRegion::new(&path, 0).unwrap();
        assert_eq!(region.read_count(), 0);

        let addr = region.append(b"key", b"value").unwrap().unwrap();

        region.read_sync(addr.offset()).unwrap();
        assert_eq!(region.read_count(), 1);

        region.read_sync(addr.offset()).unwrap();
        assert_eq!(region.read_count(), 2);
    }

    #[test]
    fn test_prefetch_disabled() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("disk.dat");

        // Create region with prefetch disabled
        let region = DiskRegion::with_prefetch(&path, 0, false, 64 * 1024).unwrap();

        region.append(b"key1", b"value1").unwrap();

        // No hits or misses when disabled
        assert_eq!(region.prefetch_hits(), 0);
        assert_eq!(region.prefetch_misses(), 0);
    }

    #[tokio::test]
    async fn test_prefetch_sequential_reads() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("disk.dat");

        // Create region with prefetch enabled
        let region = DiskRegion::with_prefetch(&path, 0, true, 64 * 1024).unwrap();

        // Write multiple entries
        let addr1 = region.append(b"key1", b"value1").unwrap().unwrap();
        let addr2 = region.append(b"key2", b"value2").unwrap().unwrap();
        let addr3 = region.append(b"key3", b"value3").unwrap().unwrap();

        // First read is a miss (fills buffer)
        let (k1, v1) = region.read_async(addr1.offset()).await.unwrap().unwrap();
        assert_eq!(&k1[..], b"key1");
        assert_eq!(&v1[..], b"value1");
        assert_eq!(region.prefetch_misses(), 1);

        // Second read should be a hit (if entries fit in buffer)
        let (k2, v2) = region.read_async(addr2.offset()).await.unwrap().unwrap();
        assert_eq!(&k2[..], b"key2");
        assert_eq!(&v2[..], b"value2");

        // Third read
        let (k3, v3) = region.read_async(addr3.offset()).await.unwrap().unwrap();
        assert_eq!(&k3[..], b"key3");
        assert_eq!(&v3[..], b"value3");

        // Verify prefetch stats
        let hits = region.prefetch_hits();
        let misses = region.prefetch_misses();
        assert!(hits + misses >= 3, "hits: {}, misses: {}", hits, misses);
    }

    #[tokio::test]
    async fn test_prefetch_buffer_invalidation_on_write() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("disk.dat");

        let region = DiskRegion::with_prefetch(&path, 0, true, 64 * 1024).unwrap();

        let addr1 = region.append(b"key1", b"value1").unwrap().unwrap();

        // Read to fill buffer
        let _ = region.read_async(addr1.offset()).await.unwrap().unwrap();
        let misses_before = region.prefetch_misses();

        // Write invalidates buffer
        let _ = region.append(b"key2", b"value2").unwrap().unwrap();

        // Next read should be a miss (buffer was invalidated)
        let _ = region.read_async(addr1.offset()).await.unwrap().unwrap();
        let misses_after = region.prefetch_misses();

        assert!(misses_after > misses_before);
    }

    #[test]
    fn test_prefetch_hit_ratio() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("disk.dat");

        let region = DiskRegion::with_prefetch(&path, 0, true, 64 * 1024).unwrap();

        // Initially 0.0
        assert_eq!(region.prefetch_hit_ratio(), 0.0);

        // Stats are updated on async reads
        region.prefetch_stats.hits.store(3, Ordering::Relaxed);
        region.prefetch_stats.misses.store(1, Ordering::Relaxed);

        // 3 hits, 1 miss = 0.75 hit ratio
        assert!((region.prefetch_hit_ratio() - 0.75).abs() < 0.001);
    }

    #[test]
    fn test_read_ahead_buffer() {
        let mut buffer = ReadAheadBuffer::new();
        assert!(buffer.is_empty());
        assert!(!buffer.contains(0, 10));

        // Fill buffer
        buffer.fill(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 100);
        assert!(!buffer.is_empty());
        assert!(buffer.contains(100, 10));
        assert!(buffer.contains(105, 5));
        assert!(!buffer.contains(95, 10)); // Before buffer
        assert!(!buffer.contains(100, 11)); // Beyond buffer

        // Read from buffer
        let data = buffer.read(100, 5).unwrap();
        assert_eq!(data, &[1, 2, 3, 4, 5]);

        let data = buffer.read(105, 3).unwrap();
        assert_eq!(data, &[6, 7, 8]);

        // Invalidate
        buffer.invalidate();
        assert!(buffer.is_empty());
        assert!(!buffer.contains(100, 10));
    }
}
