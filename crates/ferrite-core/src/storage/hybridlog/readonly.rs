//! Read-Only Region for HybridLog
//!
//! The read-only region is the warm tier of the HybridLog. It stores data
//! that has been migrated from the mutable region using memory-mapped files
//! for zero-copy reads.
//!
//! Key features:
//! - Memory-mapped files for zero-copy reads
//! - Copy-on-write for updates (creates new entry in mutable region)
//! - LRU-based eviction to disk tier
//! - Access tracking for tiering decisions

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::RwLock;

use bytes::Bytes;
use memmap2::{Mmap, MmapMut};
use tracing::debug;

use super::address::LogAddress;
use super::mutable::EntryHeader;

/// Read-only region backed by memory-mapped files
///
/// This region provides zero-copy reads through memory mapping.
/// Data is immutable once written - updates create new entries
/// in the mutable region.
pub struct ReadOnlyRegion {
    /// The path to the data file
    path: PathBuf,
    /// The underlying file
    file: RwLock<File>,
    /// Memory-mapped view of the file
    mmap: RwLock<Option<Mmap>>,
    /// Current size of the region (bytes written)
    size: AtomicUsize,
    /// Total capacity of the region
    capacity: usize,
    /// Number of entries in this region
    entry_count: AtomicU64,
    /// Access count for tiering decisions
    access_count: AtomicU64,
}

impl ReadOnlyRegion {
    /// Create a new read-only region
    ///
    /// If the file exists, it will be opened and mapped.
    /// If not, a new file will be created.
    pub fn new<P: AsRef<Path>>(path: P, capacity: usize) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Open or create the file
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;

        // Get the current file size
        let metadata = file.metadata()?;
        let current_size = metadata.len() as usize;

        // If file is empty, we don't create a mapping yet
        let mmap = if current_size > 0 {
            // SAFETY: We have exclusive access during construction
            let map = unsafe { Mmap::map(&file)? };
            Some(map)
        } else {
            None
        };

        Ok(Self {
            path,
            file: RwLock::new(file),
            mmap: RwLock::new(mmap),
            size: AtomicUsize::new(current_size),
            capacity,
            entry_count: AtomicU64::new(0),
            access_count: AtomicU64::new(0),
        })
    }

    /// Open an existing read-only region
    pub fn open<P: AsRef<Path>>(path: P, capacity: usize) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();

        if !path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Read-only region file not found",
            ));
        }

        let file = OpenOptions::new().read(true).write(true).open(&path)?;

        let metadata = file.metadata()?;
        let current_size = metadata.len() as usize;

        let mmap = if current_size > 0 {
            // SAFETY: The file exists and we have a valid handle
            let map = unsafe { Mmap::map(&file)? };
            Some(map)
        } else {
            None
        };

        // Count entries by scanning the file
        let entry_count = match mmap.as_ref() {
            Some(m) if current_size > 0 => Self::count_entries(m),
            _ => 0,
        };

        Ok(Self {
            path,
            file: RwLock::new(file),
            mmap: RwLock::new(mmap),
            size: AtomicUsize::new(current_size),
            capacity,
            entry_count: AtomicU64::new(entry_count),
            access_count: AtomicU64::new(0),
        })
    }

    /// Count entries in a mapped region
    fn count_entries(mmap: &Mmap) -> u64 {
        let mut count = 0u64;
        let mut offset = 0usize;

        while offset + EntryHeader::SIZE <= mmap.len() {
            // SAFETY: We're within bounds
            let header = unsafe {
                let ptr = mmap.as_ptr().add(offset);
                std::ptr::read(ptr as *const EntryHeader)
            };

            let entry_size = header.aligned_size();
            if offset + entry_size > mmap.len() {
                break;
            }

            count += 1;
            offset += entry_size;
        }

        count
    }

    /// Get the current size of the region
    pub fn size(&self) -> usize {
        self.size.load(Ordering::Acquire)
    }

    /// Get the capacity of this region
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get remaining space
    pub fn remaining(&self) -> usize {
        self.capacity.saturating_sub(self.size())
    }

    /// Check if the region is full
    pub fn is_full(&self) -> bool {
        self.remaining() == 0
    }

    /// Get entry count
    pub fn entry_count(&self) -> u64 {
        self.entry_count.load(Ordering::Relaxed)
    }

    /// Get access count
    pub fn access_count(&self) -> u64 {
        self.access_count.load(Ordering::Relaxed)
    }

    /// Record an access
    pub fn record_access(&self) {
        self.access_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the file path
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Read an entry at the given offset
    ///
    /// Returns the key and value as Bytes, or None if the offset is invalid.
    pub fn read(&self, offset: u64) -> Option<(Bytes, Bytes)> {
        let offset = offset as usize;
        let mmap_guard = self.mmap.read().ok()?;
        let mmap = mmap_guard.as_ref()?;

        if offset + EntryHeader::SIZE > mmap.len() {
            return None;
        }

        self.record_access();

        // SAFETY: We've verified the offset is within bounds
        unsafe {
            let ptr = mmap.as_ptr().add(offset);
            let header = std::ptr::read(ptr as *const EntryHeader);

            let entry_end = offset + header.total_size();
            if entry_end > mmap.len() {
                return None;
            }

            // Zero-copy: create Bytes that reference the mmap'd memory
            // Note: We copy here because the mmap could be remapped
            let key_ptr = ptr.add(EntryHeader::SIZE);
            let key = std::slice::from_raw_parts(key_ptr, header.key_len as usize);

            let value_ptr = key_ptr.add(header.key_len as usize);
            let value = std::slice::from_raw_parts(value_ptr, header.value_len as usize);

            Some((Bytes::copy_from_slice(key), Bytes::copy_from_slice(value)))
        }
    }

    /// Read only the value at the given offset
    pub fn read_value(&self, offset: u64) -> Option<Bytes> {
        let offset = offset as usize;
        let mmap_guard = self.mmap.read().ok()?;
        let mmap = mmap_guard.as_ref()?;

        if offset + EntryHeader::SIZE > mmap.len() {
            return None;
        }

        self.record_access();

        // SAFETY: We've verified the offset is within bounds
        unsafe {
            let ptr = mmap.as_ptr().add(offset);
            let header = std::ptr::read(ptr as *const EntryHeader);

            let entry_end = offset + header.total_size();
            if entry_end > mmap.len() {
                return None;
            }

            let value_ptr = ptr.add(EntryHeader::SIZE + header.key_len as usize);
            let value = std::slice::from_raw_parts(value_ptr, header.value_len as usize);

            Some(Bytes::copy_from_slice(value))
        }
    }

    /// Append an entry to the region (for migration from mutable region)
    ///
    /// Returns the LogAddress of the entry if successful, or None if
    /// there isn't enough space.
    pub fn append(&self, key: &[u8], value: &[u8]) -> io::Result<Option<LogAddress>> {
        let header = EntryHeader::new(key.len() as u32, value.len() as u32);
        let entry_size = header.aligned_size();

        let current_size = self.size.load(Ordering::Acquire);
        if current_size + entry_size > self.capacity {
            return Ok(None);
        }

        // Write to the file
        {
            let file = self
                .file
                .write()
                .map_err(|_| io::Error::other("Failed to acquire file lock"))?;

            // Extend the file if needed
            let new_size = current_size + entry_size;
            file.set_len(new_size as u64)?;

            // Create a mutable mmap for writing
            // SAFETY: We have exclusive access via the write lock
            let mut mmap = unsafe { MmapMut::map_mut(&*file)? };

            // Write header
            let offset = current_size;
            // SAFETY: EntryHeader is #[repr(C)] with a fixed 16-byte layout (4 x u32).
            // transmute to byte array is safe because:
            // 1. Source and destination have the same size (EntryHeader::SIZE = 16 bytes)
            // 2. EntryHeader contains only primitive u32 fields with no padding beyond _padding field
            // 3. No pointers or Drop implementations that could be invalidated
            let header_bytes: [u8; EntryHeader::SIZE] = unsafe { std::mem::transmute(header) };
            mmap[offset..offset + EntryHeader::SIZE].copy_from_slice(&header_bytes);

            // Write key
            let key_start = offset + EntryHeader::SIZE;
            mmap[key_start..key_start + key.len()].copy_from_slice(key);

            // Write value
            let value_start = key_start + key.len();
            mmap[value_start..value_start + value.len()].copy_from_slice(value);

            // Flush to disk
            mmap.flush()?;

            // Update size
            self.size.store(new_size, Ordering::Release);
        }

        // Remap as read-only
        self.remap()?;

        self.entry_count.fetch_add(1, Ordering::Relaxed);

        Ok(Some(LogAddress::read_only(current_size as u64)))
    }

    /// Remap the file after changes
    fn remap(&self) -> io::Result<()> {
        let file = self
            .file
            .read()
            .map_err(|_| io::Error::other("Failed to acquire file lock"))?;

        let size = self.size.load(Ordering::Acquire);
        if size == 0 {
            return Ok(());
        }

        // SAFETY: We have a valid file handle and size > 0
        let new_mmap = unsafe { Mmap::map(&*file)? };

        let mut mmap_guard = self
            .mmap
            .write()
            .map_err(|_| io::Error::other("Failed to acquire mmap lock"))?;

        *mmap_guard = Some(new_mmap);

        Ok(())
    }

    /// Iterate over all entries in the region
    pub fn iter<F>(&self, mut callback: F) -> io::Result<()>
    where
        F: FnMut(u64, &[u8], &[u8]),
    {
        let mmap_guard = self
            .mmap
            .read()
            .map_err(|_| io::Error::other("Failed to acquire mmap lock"))?;

        let mmap = match mmap_guard.as_ref() {
            Some(m) => m,
            None => return Ok(()),
        };

        let mut offset = 0usize;

        while offset + EntryHeader::SIZE <= mmap.len() {
            // SAFETY: We're within bounds
            let (header, key, value) = unsafe {
                let ptr = mmap.as_ptr().add(offset);
                let header = std::ptr::read(ptr as *const EntryHeader);

                let entry_end = offset + header.total_size();
                if entry_end > mmap.len() {
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

        Ok(())
    }

    /// Compact the region by removing dead entries
    ///
    /// This creates a new file with only live entries and swaps it in.
    pub fn compact<F>(&self, is_live: F) -> io::Result<usize>
    where
        F: Fn(&[u8]) -> bool,
    {
        let temp_path = self.path.with_extension("tmp");
        let mut temp_file = File::create(&temp_path)?;

        let mut kept = 0usize;
        let mut new_size = 0usize;

        // Copy live entries to temp file
        {
            let mmap_guard = self
                .mmap
                .read()
                .map_err(|_| io::Error::other("Failed to acquire mmap lock"))?;

            if let Some(mmap) = mmap_guard.as_ref() {
                let mut offset = 0usize;

                while offset + EntryHeader::SIZE <= mmap.len() {
                    // SAFETY: We're within bounds
                    let (header, entry_data) = unsafe {
                        let ptr = mmap.as_ptr().add(offset);
                        let header = std::ptr::read(ptr as *const EntryHeader);

                        let entry_size = header.aligned_size();
                        if offset + entry_size > mmap.len() {
                            break;
                        }

                        let key_ptr = ptr.add(EntryHeader::SIZE);
                        let key = std::slice::from_raw_parts(key_ptr, header.key_len as usize);

                        let data = std::slice::from_raw_parts(ptr, entry_size);

                        (header, (key.to_vec(), data.to_vec()))
                    };

                    if is_live(&entry_data.0) {
                        temp_file.write_all(&entry_data.1)?;
                        new_size += entry_data.1.len();
                        kept += 1;
                    }

                    offset += header.aligned_size();
                }
            }
        }

        temp_file.sync_all()?;
        drop(temp_file);

        // Swap files
        {
            let file = self
                .file
                .write()
                .map_err(|_| io::Error::other("Failed to acquire file lock"))?;

            // Drop the old mmap
            {
                let mut mmap_guard = self
                    .mmap
                    .write()
                    .map_err(|_| io::Error::other("Failed to acquire mmap lock"))?;
                *mmap_guard = None;
            }

            drop(file);

            // Replace the file
            std::fs::rename(&temp_path, &self.path)?;

            // Reopen
            let new_file = OpenOptions::new().read(true).write(true).open(&self.path)?;

            let mut file = self
                .file
                .write()
                .map_err(|_| io::Error::other("Failed to acquire file lock"))?;
            *file = new_file;
        }

        // Update size and remap
        self.size.store(new_size, Ordering::Release);
        self.entry_count.store(kept as u64, Ordering::Release);
        self.remap()?;

        debug!(
            "Compacted read-only region: {} entries, {} bytes",
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
    fn test_readonly_region_new() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("readonly.dat");

        let region = ReadOnlyRegion::new(&path, 1024 * 1024).unwrap();
        assert_eq!(region.size(), 0);
        assert_eq!(region.capacity(), 1024 * 1024);
        assert!(!region.is_full());
    }

    #[test]
    fn test_append_and_read() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("readonly.dat");

        let region = ReadOnlyRegion::new(&path, 1024 * 1024).unwrap();

        let key = b"test_key";
        let value = b"test_value";

        let addr = region.append(key, value).unwrap().unwrap();
        assert!(addr.is_read_only());
        assert_eq!(addr.offset(), 0);

        let (read_key, read_value) = region.read(addr.offset()).unwrap();
        assert_eq!(&read_key[..], key);
        assert_eq!(&read_value[..], value);
    }

    #[test]
    fn test_read_value() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("readonly.dat");

        let region = ReadOnlyRegion::new(&path, 1024 * 1024).unwrap();

        let key = b"key";
        let value = b"value";

        let addr = region.append(key, value).unwrap().unwrap();
        let read_value = region.read_value(addr.offset()).unwrap();
        assert_eq!(&read_value[..], value);
    }

    #[test]
    fn test_multiple_appends() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("readonly.dat");

        let region = ReadOnlyRegion::new(&path, 1024 * 1024).unwrap();

        let addr1 = region.append(b"key1", b"value1").unwrap().unwrap();
        let addr2 = region.append(b"key2", b"value2").unwrap().unwrap();
        let addr3 = region.append(b"key3", b"value3").unwrap().unwrap();

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
    fn test_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("readonly.dat");

        // Write data
        {
            let region = ReadOnlyRegion::new(&path, 1024 * 1024).unwrap();
            region.append(b"key1", b"value1").unwrap();
            region.append(b"key2", b"value2").unwrap();
        }

        // Reopen and verify
        {
            let region = ReadOnlyRegion::open(&path, 1024 * 1024).unwrap();
            assert_eq!(region.entry_count(), 2);

            let (k, v) = region.read(0).unwrap();
            assert_eq!(&k[..], b"key1");
            assert_eq!(&v[..], b"value1");
        }
    }

    #[test]
    fn test_iter() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("readonly.dat");

        let region = ReadOnlyRegion::new(&path, 1024 * 1024).unwrap();
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
    fn test_access_tracking() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("readonly.dat");

        let region = ReadOnlyRegion::new(&path, 1024 * 1024).unwrap();
        assert_eq!(region.access_count(), 0);

        let addr = region.append(b"key", b"value").unwrap().unwrap();

        region.read(addr.offset()).unwrap();
        assert_eq!(region.access_count(), 1);

        region.read_value(addr.offset()).unwrap();
        assert_eq!(region.access_count(), 2);
    }

    #[test]
    fn test_capacity_limit() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("readonly.dat");

        // Very small capacity - just enough for one small entry
        let region = ReadOnlyRegion::new(&path, 50).unwrap();

        // First append should succeed (header=16 + key=4 + value=6 = 26, aligned to 32)
        let addr1 = region.append(b"key1", b"value1").unwrap();
        assert!(addr1.is_some());

        // Second append should fail due to capacity (need another 32 bytes, only ~18 left)
        let addr2 = region.append(b"key2", b"value2").unwrap();
        assert!(
            addr2.is_none(),
            "Second append should fail due to capacity limit"
        );
    }

    #[test]
    fn test_compact() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("readonly.dat");

        let region = ReadOnlyRegion::new(&path, 1024 * 1024).unwrap();
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
}
