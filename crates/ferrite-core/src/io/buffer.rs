//! Zero-Copy I/O Buffers
//!
//! Provides aligned, pooled buffers optimized for io_uring operations.
//! Supports buffer registration for reduced memory copy overhead.

use std::io::{self, Read, Write};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;

/// Default buffer size (64KB for optimal io_uring performance)
pub const BUFFER_SIZE: usize = 64 * 1024;

/// Alignment for direct I/O (4KB page alignment)
pub const BUFFER_ALIGNMENT: usize = 4096;

/// A buffer aligned for direct I/O operations
#[derive(Debug)]
pub struct IoBuffer {
    /// The underlying aligned memory
    data: Box<[u8]>,
    /// Current position for read/write operations
    pos: usize,
    /// Amount of valid data in the buffer
    len: usize,
    /// Buffer ID (for registered buffer tracking)
    buffer_id: Option<u16>,
}

impl IoBuffer {
    /// Create a new aligned buffer
    pub fn new(size: usize) -> Self {
        Self::with_alignment(size, BUFFER_ALIGNMENT)
    }

    /// Create a buffer with custom alignment
    pub fn with_alignment(size: usize, alignment: usize) -> Self {
        // Allocate aligned memory
        let layout =
            std::alloc::Layout::from_size_align(size, alignment).expect("Invalid buffer layout");

        // SAFETY: Layout is valid and we initialize with zeros
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }

        // SAFETY: ptr is valid, aligned, and size bytes
        let data = unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, size)) };

        Self {
            data,
            pos: 0,
            len: 0,
            buffer_id: None,
        }
    }

    /// Create a buffer with a specific ID (for registration)
    pub fn with_id(size: usize, id: u16) -> Self {
        let mut buf = Self::new(size);
        buf.buffer_id = Some(id);
        buf
    }

    /// Get the buffer ID
    pub fn buffer_id(&self) -> Option<u16> {
        self.buffer_id
    }

    /// Get the capacity of the buffer
    pub fn capacity(&self) -> usize {
        self.data.len()
    }

    /// Get the length of valid data
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Get remaining capacity for writes
    pub fn remaining(&self) -> usize {
        self.capacity() - self.len
    }

    /// Reset the buffer position
    pub fn reset(&mut self) {
        self.pos = 0;
        self.len = 0;
    }

    /// Set the valid data length
    pub fn set_len(&mut self, len: usize) {
        assert!(len <= self.capacity());
        self.len = len;
    }

    /// Get a slice of the valid data
    pub fn as_slice(&self) -> &[u8] {
        &self.data[..self.len]
    }

    /// Get a mutable slice of the buffer
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data[..]
    }

    /// Get the raw pointer (for io_uring)
    pub fn as_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    /// Get the mutable raw pointer (for io_uring)
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.data.as_mut_ptr()
    }

    /// Copy data from bytes
    pub fn copy_from(&mut self, data: &[u8]) -> usize {
        let n = data.len().min(self.remaining());
        self.data[self.len..self.len + n].copy_from_slice(&data[..n]);
        self.len += n;
        n
    }

    /// Convert to Bytes (zero-copy if possible)
    pub fn to_bytes(&self) -> Bytes {
        Bytes::copy_from_slice(self.as_slice())
    }

    /// Consume into BytesMut
    pub fn into_bytes_mut(self) -> BytesMut {
        BytesMut::from(self.as_slice())
    }
}

impl Read for IoBuffer {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let available = self.len - self.pos;
        let n = buf.len().min(available);
        buf[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

impl Write for IoBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.copy_from(buf);
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Deref for IoBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl DerefMut for IoBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let cap = self.data.len();
        let end = self.len.max(cap);
        &mut self.data[..end]
    }
}

impl Default for IoBuffer {
    fn default() -> Self {
        Self::new(BUFFER_SIZE)
    }
}

/// Registered buffer for io_uring fixed buffer operations
#[derive(Debug)]
pub struct RegisteredBuffer {
    /// The underlying buffer
    pub buffer: IoBuffer,
    /// Registration index in io_uring
    pub index: u16,
}

impl RegisteredBuffer {
    /// Create a new registered buffer
    pub fn new(size: usize, index: u16) -> Self {
        Self {
            buffer: IoBuffer::with_id(size, index),
            index,
        }
    }
}

impl Deref for RegisteredBuffer {
    type Target = IoBuffer;

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

impl DerefMut for RegisteredBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buffer
    }
}

/// Pool of reusable I/O buffers
pub struct IoBufferPool {
    /// Free buffers
    free: Mutex<Vec<IoBuffer>>,
    /// Buffer size
    buffer_size: usize,
    /// Maximum pool size
    max_buffers: usize,
    /// Current allocated count
    allocated: AtomicUsize,
    /// Statistics
    stats: Arc<BufferPoolStats>,
}

/// Buffer pool statistics
#[derive(Debug, Default)]
pub struct BufferPoolStats {
    /// Total allocations
    pub allocations: AtomicUsize,
    /// Total releases
    pub releases: AtomicUsize,
    /// Pool hits (reused buffer)
    pub hits: AtomicUsize,
    /// Pool misses (new allocation)
    pub misses: AtomicUsize,
}

impl IoBufferPool {
    /// Create a new buffer pool
    pub fn new(buffer_size: usize, max_buffers: usize) -> Self {
        Self {
            free: Mutex::new(Vec::with_capacity(max_buffers)),
            buffer_size,
            max_buffers,
            allocated: AtomicUsize::new(0),
            stats: Arc::new(BufferPoolStats::default()),
        }
    }

    /// Create a pool with pre-allocated buffers
    pub fn with_preallocated(buffer_size: usize, count: usize) -> Self {
        let pool = Self::new(buffer_size, count * 2);

        {
            let mut free = pool.free.lock();
            for _ in 0..count {
                free.push(IoBuffer::new(buffer_size));
            }
        }
        pool.allocated.store(count, Ordering::Release);

        pool
    }

    /// Acquire a buffer from the pool
    pub fn acquire(&self) -> IoBuffer {
        // Try to get from pool first
        if let Some(mut buf) = self.free.lock().pop() {
            buf.reset();
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            return buf;
        }

        // Allocate new buffer
        self.stats.misses.fetch_add(1, Ordering::Relaxed);
        self.stats.allocations.fetch_add(1, Ordering::Relaxed);
        self.allocated.fetch_add(1, Ordering::Release);
        IoBuffer::new(self.buffer_size)
    }

    /// Release a buffer back to the pool
    pub fn release(&self, buffer: IoBuffer) {
        let mut free = self.free.lock();

        // Only keep up to max_buffers
        if free.len() < self.max_buffers {
            free.push(buffer);
        } else {
            // Drop the buffer if pool is full
            self.allocated.fetch_sub(1, Ordering::Release);
        }

        self.stats.releases.fetch_add(1, Ordering::Relaxed);
    }

    /// Get current pool size
    pub fn size(&self) -> usize {
        self.free.lock().len()
    }

    /// Get total allocated buffers
    pub fn allocated(&self) -> usize {
        self.allocated.load(Ordering::Acquire)
    }

    /// Get buffer size
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    /// Get pool statistics
    pub fn stats(&self) -> &Arc<BufferPoolStats> {
        &self.stats
    }

    /// Clear the pool
    pub fn clear(&self) {
        let mut free = self.free.lock();
        let count = free.len();
        free.clear();
        self.allocated.fetch_sub(count, Ordering::Release);
    }
}

impl Default for IoBufferPool {
    fn default() -> Self {
        Self::new(BUFFER_SIZE, 32)
    }
}

/// RAII guard for pooled buffers
pub struct PooledBuffer {
    buffer: Option<IoBuffer>,
    pool: Arc<IoBufferPool>,
}

#[allow(dead_code)] // Planned for v0.2 — reserved for buffer pool API
impl PooledBuffer {
    /// Create a new pooled buffer
    pub fn new(pool: Arc<IoBufferPool>) -> Self {
        Self {
            buffer: Some(pool.acquire()),
            pool,
        }
    }

    /// Take ownership of the buffer (won't return to pool)
    pub fn take(mut self) -> IoBuffer {
        self.buffer.take().expect("buffer already consumed")
    }
}

impl Deref for PooledBuffer {
    type Target = IoBuffer;

    fn deref(&self) -> &Self::Target {
        self.buffer.as_ref().expect("buffer already consumed")
    }
}

impl DerefMut for PooledBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buffer.as_mut().expect("buffer already consumed")
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        if let Some(buf) = self.buffer.take() {
            self.pool.release(buf);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_creation() {
        let buf = IoBuffer::new(4096);
        assert_eq!(buf.capacity(), 4096);
        assert_eq!(buf.len(), 0);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_buffer_write() {
        let mut buf = IoBuffer::new(1024);
        let data = b"Hello, World!";
        let n = buf.copy_from(data);
        assert_eq!(n, data.len());
        assert_eq!(buf.len(), data.len());
        assert_eq!(buf.as_slice(), data);
    }

    #[test]
    fn test_buffer_read() {
        let mut buf = IoBuffer::new(1024);
        buf.copy_from(b"Test data");

        let mut read_buf = [0u8; 4];
        let n = buf.read(&mut read_buf).unwrap();
        assert_eq!(n, 4);
        assert_eq!(&read_buf, b"Test");
    }

    #[test]
    fn test_buffer_pool() {
        let pool = IoBufferPool::new(1024, 4);

        // Acquire buffers
        let buf1 = pool.acquire();
        let buf2 = pool.acquire();
        assert_eq!(pool.allocated(), 2);

        // Release one
        pool.release(buf1);
        assert_eq!(pool.size(), 1);

        // Acquire should reuse
        let buf3 = pool.acquire();
        assert_eq!(pool.allocated(), 2);

        pool.release(buf2);
        pool.release(buf3);
        assert_eq!(pool.size(), 2);
    }

    #[test]
    fn test_pooled_buffer() {
        let pool = Arc::new(IoBufferPool::new(1024, 4));

        {
            let mut buf = PooledBuffer::new(pool.clone());
            buf.copy_from(b"Test");
            assert_eq!(buf.len(), 4);
        }

        // Buffer should be back in pool
        assert_eq!(pool.size(), 1);
    }

    #[test]
    fn test_buffer_alignment() {
        let buf = IoBuffer::new(4096);
        let ptr = buf.as_ptr() as usize;
        assert_eq!(ptr % BUFFER_ALIGNMENT, 0);
    }

    #[test]
    fn test_preallocated_pool() {
        let pool = IoBufferPool::with_preallocated(1024, 8);
        assert_eq!(pool.size(), 8);
        assert_eq!(pool.allocated(), 8);
    }

    #[test]
    fn test_buffer_to_bytes() {
        let mut buf = IoBuffer::new(1024);
        buf.copy_from(b"Convert to bytes");

        let bytes = buf.to_bytes();
        assert_eq!(bytes.as_ref(), b"Convert to bytes");
    }
}
