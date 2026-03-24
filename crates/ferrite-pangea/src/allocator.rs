//! CXL allocator trait + in-memory test impl.
//!
//! The trait is intentionally minimal: page-granular alloc/free and a
//! "view" accessor that returns the page contents.  Production impls
//! mmap CXL-attached memory; the in-memory impl backs each page with a
//! slice of a `Vec<u8>` arena.

use parking_lot::RwLock;
use std::sync::Arc;

pub type PageId = u64;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AllocError {
    #[error("out of CXL memory: requested {requested} bytes, only {available} available")]
    OutOfMemory { requested: usize, available: usize },
    #[error("payload too large: {len} bytes exceeds page size {page_size}")]
    PayloadTooLarge { len: usize, page_size: usize },
    #[error("page {0} not allocated")]
    PageNotFound(PageId),
}

pub trait CxlAllocator: Send + Sync + 'static {
    fn page_size(&self) -> usize;
    fn allocate(&self, payload: &[u8]) -> Result<PageId, AllocError>;
    fn read(&self, id: PageId) -> Result<Vec<u8>, AllocError>;
    fn free(&self, id: PageId) -> Result<(), AllocError>;
    fn stats(&self) -> AllocStats;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AllocStats {
    pub pages_total: usize,
    pub pages_used: usize,
    pub bytes_total: usize,
    pub bytes_used: usize,
}

/// Reference impl backed by a `Vec<u8>` arena divided into fixed-size pages.
pub struct InMemoryCxlAllocator {
    page_size: usize,
    pages_total: usize,
    arena: RwLock<Vec<u8>>,
    /// `slots[i] = Some(payload_len)` if page i is allocated.
    slots: RwLock<Vec<Option<usize>>>,
}

impl std::fmt::Debug for InMemoryCxlAllocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stats = self.stats();
        f.debug_struct("InMemoryCxlAllocator")
            .field("page_size", &self.page_size)
            .field("stats", &stats)
            .finish()
    }
}

impl InMemoryCxlAllocator {
    pub fn new(arena_bytes: usize, page_size: usize) -> Self {
        assert!(page_size > 0);
        let pages_total = arena_bytes / page_size;
        Self {
            page_size,
            pages_total,
            arena: RwLock::new(vec![0u8; pages_total * page_size]),
            slots: RwLock::new(vec![None; pages_total]),
        }
    }

    /// Convenience constructor used in tests.
    pub fn shared(arena_bytes: usize, page_size: usize) -> Arc<Self> {
        Arc::new(Self::new(arena_bytes, page_size))
    }
}

impl CxlAllocator for InMemoryCxlAllocator {
    fn page_size(&self) -> usize {
        self.page_size
    }

    fn allocate(&self, payload: &[u8]) -> Result<PageId, AllocError> {
        if payload.len() > self.page_size {
            return Err(AllocError::PayloadTooLarge {
                len: payload.len(),
                page_size: self.page_size,
            });
        }
        let mut slots = self.slots.write();
        let free_index = slots.iter().position(Option::is_none).ok_or_else(|| {
            let used = slots.iter().filter(|s| s.is_some()).count();
            AllocError::OutOfMemory {
                requested: self.page_size,
                available: (self.pages_total - used) * self.page_size,
            }
        })?;
        let mut arena = self.arena.write();
        let start = free_index * self.page_size;
        arena[start..start + payload.len()].copy_from_slice(payload);
        slots[free_index] = Some(payload.len());
        Ok(free_index as PageId)
    }

    fn read(&self, id: PageId) -> Result<Vec<u8>, AllocError> {
        let slots = self.slots.read();
        let idx = id as usize;
        let len = slots
            .get(idx)
            .and_then(|s| *s)
            .ok_or(AllocError::PageNotFound(id))?;
        let arena = self.arena.read();
        let start = idx * self.page_size;
        Ok(arena[start..start + len].to_vec())
    }

    fn free(&self, id: PageId) -> Result<(), AllocError> {
        let mut slots = self.slots.write();
        let idx = id as usize;
        let slot = slots.get_mut(idx).ok_or(AllocError::PageNotFound(id))?;
        if slot.is_none() {
            return Err(AllocError::PageNotFound(id));
        }
        *slot = None;
        Ok(())
    }

    fn stats(&self) -> AllocStats {
        let slots = self.slots.read();
        let used = slots.iter().filter_map(|s| s.as_ref()).count();
        let bytes_used: usize = slots.iter().filter_map(|s| *s).sum();
        AllocStats {
            pages_total: self.pages_total,
            pages_used: used,
            bytes_total: self.pages_total * self.page_size,
            bytes_used,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_read_roundtrip() {
        let a = InMemoryCxlAllocator::new(4096, 128);
        let id = a.allocate(b"hello").unwrap();
        assert_eq!(a.read(id).unwrap(), b"hello");
    }

    #[test]
    fn free_reclaims_slot() {
        let a = InMemoryCxlAllocator::new(256, 128);
        let id1 = a.allocate(b"a").unwrap();
        let id2 = a.allocate(b"b").unwrap();
        assert!(a.allocate(b"c").is_err());
        a.free(id1).unwrap();
        let id3 = a.allocate(b"c").unwrap();
        assert_eq!(id3, id1); // first-fit reuses the freed slot
        assert_eq!(a.read(id2).unwrap(), b"b");
    }

    #[test]
    fn payload_larger_than_page_is_rejected() {
        let a = InMemoryCxlAllocator::new(256, 8);
        let err = a.allocate(b"way too big for a page").unwrap_err();
        assert!(matches!(err, AllocError::PayloadTooLarge { .. }));
    }

    #[test]
    fn out_of_memory_when_full() {
        let a = InMemoryCxlAllocator::new(128, 128); // 1 page
        a.allocate(b"x").unwrap();
        let err = a.allocate(b"y").unwrap_err();
        assert!(matches!(err, AllocError::OutOfMemory { .. }));
    }

    #[test]
    fn stats_reflect_usage() {
        let a = InMemoryCxlAllocator::new(512, 128); // 4 pages
        a.allocate(b"abcd").unwrap();
        a.allocate(b"xy").unwrap();
        let s = a.stats();
        assert_eq!(s.pages_total, 4);
        assert_eq!(s.pages_used, 2);
        assert_eq!(s.bytes_used, 6);
    }

    #[test]
    fn double_free_is_an_error() {
        let a = InMemoryCxlAllocator::new(256, 128);
        let id = a.allocate(b"x").unwrap();
        a.free(id).unwrap();
        let err = a.free(id).unwrap_err();
        assert!(matches!(err, AllocError::PageNotFound(_)));
    }
}
