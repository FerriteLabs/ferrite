//! Mutable Region for HybridLog
//!
//! The mutable region is the hot tier of the HybridLog. It stores recently
//! accessed data in memory with append-only writes and lock-free reads.
//!
//! Key features:
//! - Append-only writes with atomic tail pointer
//! - Lock-free reads (no synchronization needed for readers)
//! - Access tracking for tiering decisions
//! - Configurable capacity with overflow handling

use std::alloc::{alloc, dealloc, Layout};
use std::io;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use bytes::Bytes;

use super::address::LogAddress;

/// Entry header stored before each record in the mutable region
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct EntryHeader {
    /// Length of the key in bytes
    pub key_len: u32,
    /// Length of the value in bytes
    pub value_len: u32,
    /// Flags (reserved for future use)
    pub flags: u32,
    /// Padding for alignment
    _padding: u32,
}

impl EntryHeader {
    /// Size of the header in bytes
    pub const SIZE: usize = std::mem::size_of::<Self>();

    /// Create a new entry header
    pub fn new(key_len: u32, value_len: u32) -> Self {
        Self {
            key_len,
            value_len,
            flags: 0,
            _padding: 0,
        }
    }

    /// Total size of this entry (header + key + value)
    pub fn total_size(&self) -> usize {
        Self::SIZE + self.key_len as usize + self.value_len as usize
    }

    /// Calculate aligned total size (8-byte alignment)
    pub fn aligned_size(&self) -> usize {
        let size = self.total_size();
        (size + 7) & !7 // Round up to 8-byte boundary
    }
}

/// The mutable region - hot tier of the HybridLog
///
/// This region stores hot data in memory. Writes are append-only
/// and reads are lock-free.
pub struct MutableRegion {
    /// Pointer to the allocated memory buffer
    buffer: NonNull<u8>,
    /// Total capacity of the buffer in bytes
    capacity: usize,
    /// Current tail position (next write position)
    tail: AtomicUsize,
    /// Total bytes written (may exceed capacity if overflow occurred)
    bytes_written: AtomicU64,
    /// Number of entries written
    entry_count: AtomicU64,
    /// Access count for this region (for tiering decisions)
    access_count: AtomicU64,
}

// SAFETY: MutableRegion can be sent across threads
unsafe impl Send for MutableRegion {}
// SAFETY: MutableRegion can be shared across threads (with proper synchronization)
unsafe impl Sync for MutableRegion {}

impl MutableRegion {
    /// Create a new mutable region with the given capacity
    ///
    /// Returns an error if memory allocation fails.
    pub fn new(capacity: usize) -> io::Result<Self> {
        // Ensure minimum capacity
        let capacity = capacity.max(4096);

        // Allocate aligned memory
        let layout = Layout::from_size_align(capacity, 8).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid layout for mutable region: {e}"),
            )
        })?;

        // SAFETY: layout is valid and non-zero size
        let buffer = unsafe {
            let ptr = alloc(layout);
            if ptr.is_null() {
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "failed to allocate mutable region buffer",
                ));
            }
            // Zero-initialize the buffer
            std::ptr::write_bytes(ptr, 0, capacity);
            NonNull::new_unchecked(ptr)
        };

        Ok(Self {
            buffer,
            capacity,
            tail: AtomicUsize::new(0),
            bytes_written: AtomicU64::new(0),
            entry_count: AtomicU64::new(0),
            access_count: AtomicU64::new(0),
        })
    }

    /// Get the capacity of this region
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get the current used size
    pub fn used(&self) -> usize {
        self.tail.load(Ordering::Acquire)
    }

    /// Get remaining free space
    pub fn remaining(&self) -> usize {
        self.capacity.saturating_sub(self.used())
    }

    /// Check if the region is full
    pub fn is_full(&self) -> bool {
        self.remaining() == 0
    }

    /// Get total bytes written (including overflows)
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }

    /// Get entry count
    pub fn entry_count(&self) -> u64 {
        self.entry_count.load(Ordering::Relaxed)
    }

    /// Get access count
    pub fn access_count(&self) -> u64 {
        self.access_count.load(Ordering::Relaxed)
    }

    /// Increment access count
    pub fn record_access(&self) {
        self.access_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Try to append an entry to the region
    ///
    /// Returns the LogAddress of the entry if successful, or None if
    /// there isn't enough space.
    pub fn try_append(&self, key: &[u8], value: &[u8]) -> Option<LogAddress> {
        let header = EntryHeader::new(key.len() as u32, value.len() as u32);
        let entry_size = header.aligned_size();

        // Try to reserve space atomically
        let offset = self.reserve_space(entry_size)?;

        // Write the entry
        // SAFETY: We just reserved this space exclusively
        unsafe {
            let ptr = self.buffer.as_ptr().add(offset);

            // Write header
            std::ptr::write(ptr as *mut EntryHeader, header);

            // Write key
            let key_ptr = ptr.add(EntryHeader::SIZE);
            std::ptr::copy_nonoverlapping(key.as_ptr(), key_ptr, key.len());

            // Write value
            let value_ptr = key_ptr.add(key.len());
            std::ptr::copy_nonoverlapping(value.as_ptr(), value_ptr, value.len());
        }

        // Update statistics
        self.bytes_written
            .fetch_add(entry_size as u64, Ordering::Relaxed);
        self.entry_count.fetch_add(1, Ordering::Relaxed);

        Some(LogAddress::mutable(offset as u64))
    }

    /// Reserve space in the region atomically
    fn reserve_space(&self, size: usize) -> Option<usize> {
        loop {
            let current = self.tail.load(Ordering::Acquire);
            let new_tail = current.checked_add(size)?;

            if new_tail > self.capacity {
                return None;
            }

            if self
                .tail
                .compare_exchange_weak(current, new_tail, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Some(current);
            }
            // CAS failed, retry
        }
    }

    /// Read an entry at the given offset
    ///
    /// Returns the key and value as Bytes, or None if the offset is invalid.
    ///
    /// # Safety
    /// The caller must ensure the offset points to a valid entry.
    pub fn read(&self, offset: u64) -> Option<(Bytes, Bytes)> {
        let offset = offset as usize;

        // Bounds check
        if offset + EntryHeader::SIZE > self.used() {
            return None;
        }

        self.record_access();

        // SAFETY: We've verified the offset is within bounds
        unsafe {
            let ptr = self.buffer.as_ptr().add(offset);
            let header = std::ptr::read(ptr as *const EntryHeader);

            // Validate header
            let entry_end = offset + header.total_size();
            if entry_end > self.used() {
                return None;
            }

            // Read key
            let key_ptr = ptr.add(EntryHeader::SIZE);
            let key = std::slice::from_raw_parts(key_ptr, header.key_len as usize);

            // Read value
            let value_ptr = key_ptr.add(header.key_len as usize);
            let value = std::slice::from_raw_parts(value_ptr, header.value_len as usize);

            Some((Bytes::copy_from_slice(key), Bytes::copy_from_slice(value)))
        }
    }

    /// Read only the value at the given offset
    ///
    /// More efficient than read() when you don't need the key.
    pub fn read_value(&self, offset: u64) -> Option<Bytes> {
        let offset = offset as usize;

        if offset + EntryHeader::SIZE > self.used() {
            return None;
        }

        self.record_access();

        // SAFETY: We've verified the offset is within bounds
        unsafe {
            let ptr = self.buffer.as_ptr().add(offset);
            let header = std::ptr::read(ptr as *const EntryHeader);

            let entry_end = offset + header.total_size();
            if entry_end > self.used() {
                return None;
            }

            let value_ptr = ptr.add(EntryHeader::SIZE + header.key_len as usize);
            let value = std::slice::from_raw_parts(value_ptr, header.value_len as usize);

            Some(Bytes::copy_from_slice(value))
        }
    }

    /// Iterate over all entries in the region
    ///
    /// The callback receives (offset, key, value) for each entry.
    pub fn iter<F>(&self, mut callback: F)
    where
        F: FnMut(u64, &[u8], &[u8]),
    {
        let used = self.used();
        let mut offset = 0usize;

        while offset + EntryHeader::SIZE <= used {
            // SAFETY: We're within bounds
            let (header, key, value) = unsafe {
                let ptr = self.buffer.as_ptr().add(offset);
                let header = std::ptr::read(ptr as *const EntryHeader);

                let entry_end = offset + header.total_size();
                if entry_end > used {
                    break;
                }

                let key_ptr = ptr.add(EntryHeader::SIZE);
                let key = std::slice::from_raw_parts(key_ptr, header.key_len as usize);

                let value_ptr = key_ptr.add(header.key_len as usize);
                let value = std::slice::from_raw_parts(value_ptr, header.value_len as usize);

                (header, key, value)
            };

            callback(offset as u64, key, value);
            offset += header.aligned_size();
        }
    }

    /// Clear the region (reset to empty state)
    ///
    /// # Safety
    /// Caller must ensure no other threads are reading from the region.
    pub unsafe fn clear(&self) {
        self.tail.store(0, Ordering::Release);
        // Zero out the buffer for safety
        std::ptr::write_bytes(self.buffer.as_ptr(), 0, self.capacity);
    }

    /// Get raw pointer to the buffer (for advanced operations)
    ///
    /// # Safety
    /// Caller must ensure proper synchronization and bounds checking.
    pub unsafe fn as_ptr(&self) -> *const u8 {
        self.buffer.as_ptr()
    }
}

impl Drop for MutableRegion {
    fn drop(&mut self) {
        // SAFETY: capacity and alignment are the same as in new().
        // Layout::from_size_align cannot fail here because it succeeded in new().
        if let Ok(layout) = Layout::from_size_align(self.capacity, 8) {
            // SAFETY: We allocated this memory in new() with the same layout
            unsafe {
                dealloc(self.buffer.as_ptr(), layout);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry_header_size() {
        assert_eq!(EntryHeader::SIZE, 16);
    }

    #[test]
    fn test_entry_header_total_size() {
        let header = EntryHeader::new(10, 20);
        assert_eq!(header.total_size(), 16 + 10 + 20);
    }

    #[test]
    fn test_entry_header_aligned_size() {
        let header = EntryHeader::new(10, 20);
        assert_eq!(header.aligned_size(), 48); // 46 rounded to 48

        let header2 = EntryHeader::new(8, 8);
        assert_eq!(header2.aligned_size(), 32); // Already aligned
    }

    #[test]
    fn test_mutable_region_new() {
        let region = MutableRegion::new(4096).unwrap();
        assert_eq!(region.capacity(), 4096);
        assert_eq!(region.used(), 0);
        assert!(!region.is_full());
    }

    #[test]
    fn test_mutable_region_minimum_capacity() {
        let region = MutableRegion::new(100).unwrap(); // Less than 4096
        assert_eq!(region.capacity(), 4096);
    }

    #[test]
    fn test_append_and_read() {
        let region = MutableRegion::new(4096).unwrap();

        let key = b"test_key";
        let value = b"test_value";

        let addr = region
            .try_append(key, value)
            .expect("append should succeed");
        assert!(addr.is_mutable());
        assert_eq!(addr.offset(), 0);

        let (read_key, read_value) = region.read(addr.offset()).expect("read should succeed");
        assert_eq!(&read_key[..], key);
        assert_eq!(&read_value[..], value);
    }

    #[test]
    fn test_read_value() {
        let region = MutableRegion::new(4096).unwrap();

        let key = b"key";
        let value = b"value";

        let addr = region.try_append(key, value).unwrap();
        let read_value = region.read_value(addr.offset()).unwrap();
        assert_eq!(&read_value[..], value);
    }

    #[test]
    fn test_multiple_appends() {
        let region = MutableRegion::new(4096).unwrap();

        let addr1 = region.try_append(b"key1", b"value1").unwrap();
        let addr2 = region.try_append(b"key2", b"value2").unwrap();
        let addr3 = region.try_append(b"key3", b"value3").unwrap();

        assert!(addr2.offset() > addr1.offset());
        assert!(addr3.offset() > addr2.offset());

        let (_, v1) = region.read(addr1.offset()).unwrap();
        let (_, v2) = region.read(addr2.offset()).unwrap();
        let (_, v3) = region.read(addr3.offset()).unwrap();

        assert_eq!(&v1[..], b"value1");
        assert_eq!(&v2[..], b"value2");
        assert_eq!(&v3[..], b"value3");

        assert_eq!(region.entry_count(), 3);
    }

    #[test]
    fn test_overflow_protection() {
        let region = MutableRegion::new(4096).unwrap();

        // Fill the region with entries until it's full
        let mut count = 0;
        while region.try_append(b"key", b"value_data_here").is_some() {
            count += 1;
            if count > 1000 {
                panic!("Too many appends - should have run out of space");
            }
        }

        // Verify we wrote some entries
        assert!(count > 0);

        // Next append should fail - region is full
        let addr = region.try_append(b"another_key", b"another_value");
        assert!(addr.is_none());
    }

    #[test]
    fn test_iter() {
        let region = MutableRegion::new(4096).unwrap();

        region.try_append(b"key1", b"val1").unwrap();
        region.try_append(b"key2", b"val2").unwrap();
        region.try_append(b"key3", b"val3").unwrap();

        let mut entries = Vec::new();
        region.iter(|offset, key, value| {
            entries.push((offset, key.to_vec(), value.to_vec()));
        });

        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].1, b"key1");
        assert_eq!(entries[1].1, b"key2");
        assert_eq!(entries[2].1, b"key3");
    }

    #[test]
    fn test_access_tracking() {
        let region = MutableRegion::new(4096).unwrap();
        assert_eq!(region.access_count(), 0);

        let addr = region.try_append(b"key", b"value").unwrap();

        region.read(addr.offset()).unwrap();
        assert_eq!(region.access_count(), 1);

        region.read_value(addr.offset()).unwrap();
        assert_eq!(region.access_count(), 2);
    }

    #[test]
    fn test_concurrent_appends() {
        use std::sync::Arc;
        use std::thread;

        let region = Arc::new(MutableRegion::new(1024 * 1024).unwrap()); // 1MB
        let mut handles = Vec::new();

        for i in 0..4 {
            let region = Arc::clone(&region);
            handles.push(thread::spawn(move || {
                for j in 0..100 {
                    let key = format!("key_{i}_{j}");
                    let value = format!("value_{i}_{j}");
                    region.try_append(key.as_bytes(), value.as_bytes()).unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(region.entry_count(), 400);
    }
}
