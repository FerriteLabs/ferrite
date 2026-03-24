//! Tier-0 cache: per-keyspace LRU on top of a `CxlAllocator`.

use crate::allocator::{AllocError, CxlAllocator, PageId};
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPolicy {
    Lru,
    /// Promotion is disabled; entries stay until explicitly removed.
    Manual,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}

struct Inner {
    capacity: usize,
    policy: EvictionPolicy,
    index: HashMap<String, PageId>,
    lru: VecDeque<String>, // most-recent at the back
    stats: CacheStats,
}

pub struct Tier0Cache<A: CxlAllocator> {
    alloc: Arc<A>,
    inner: Mutex<Inner>,
}

impl<A: CxlAllocator> std::fmt::Debug for Tier0Cache<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let g = self.inner.lock();
        f.debug_struct("Tier0Cache")
            .field("size", &g.index.len())
            .field("capacity", &g.capacity)
            .field("policy", &g.policy)
            .field("stats", &g.stats)
            .finish()
    }
}

impl<A: CxlAllocator> Tier0Cache<A> {
    pub fn new(alloc: Arc<A>, capacity: usize, policy: EvictionPolicy) -> Self {
        Self {
            alloc,
            inner: Mutex::new(Inner {
                capacity,
                policy,
                index: HashMap::new(),
                lru: VecDeque::new(),
                stats: CacheStats::default(),
            }),
        }
    }

    pub fn stats(&self) -> CacheStats {
        self.inner.lock().stats
    }
    pub fn len(&self) -> usize {
        self.inner.lock().index.len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn insert(&self, key: String, value: Vec<u8>) -> Result<(), AllocError> {
        let page = self.alloc.allocate(&value)?;
        let mut g = self.inner.lock();
        // Evict-if-needed when adding a NEW key.
        if !g.index.contains_key(&key) && g.index.len() >= g.capacity {
            self.evict_one(&mut g);
        }
        if let Some(old) = g.index.insert(key.clone(), page) {
            // Replacing an existing key — free the old page.
            let _ = self.alloc.free(old);
            g.lru.retain(|k| k != &key);
        }
        g.lru.push_back(key);
        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let mut g = self.inner.lock();
        let Some(&page) = g.index.get(key) else {
            g.stats.misses += 1;
            return None;
        };
        g.stats.hits += 1;
        if g.policy == EvictionPolicy::Lru {
            g.lru.retain(|k| k != key);
            g.lru.push_back(key.to_string());
        }
        drop(g);
        self.alloc.read(page).ok()
    }

    pub fn remove(&self, key: &str) -> bool {
        let mut g = self.inner.lock();
        if let Some(page) = g.index.remove(key) {
            g.lru.retain(|k| k != key);
            drop(g);
            let _ = self.alloc.free(page);
            true
        } else {
            false
        }
    }

    fn evict_one(&self, g: &mut Inner) {
        if let Some(victim) = match g.policy {
            EvictionPolicy::Lru => g.lru.pop_front(),
            EvictionPolicy::Manual => None,
        } {
            if let Some(page) = g.index.remove(&victim) {
                let _ = self.alloc.free(page);
                g.stats.evictions += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::InMemoryCxlAllocator;

    fn fresh(cap: usize) -> Tier0Cache<InMemoryCxlAllocator> {
        let a = InMemoryCxlAllocator::shared(64 * 1024, 4096);
        Tier0Cache::new(a, cap, EvictionPolicy::Lru)
    }

    #[test]
    fn insert_then_get() {
        let c = fresh(4);
        c.insert("k".into(), b"v".to_vec()).unwrap();
        assert_eq!(c.get("k"), Some(b"v".to_vec()));
        assert_eq!(c.stats().hits, 1);
    }

    #[test]
    fn miss_increments_misses() {
        let c = fresh(4);
        assert!(c.get("absent").is_none());
        assert_eq!(c.stats().misses, 1);
    }

    #[test]
    fn lru_evicts_least_recently_used() {
        let c = fresh(2);
        c.insert("a".into(), b"1".to_vec()).unwrap();
        c.insert("b".into(), b"2".to_vec()).unwrap();
        // touch a => b becomes LRU
        c.get("a");
        c.insert("c".into(), b"3".to_vec()).unwrap();
        assert!(c.get("b").is_none(), "b should have been evicted");
        assert_eq!(c.get("a"), Some(b"1".to_vec()));
        assert_eq!(c.get("c"), Some(b"3".to_vec()));
        assert_eq!(c.stats().evictions, 1);
    }

    #[test]
    fn manual_policy_does_not_evict_and_returns_oom() {
        let alloc = InMemoryCxlAllocator::shared(8192, 4096); // 2 pages
        let c: Tier0Cache<InMemoryCxlAllocator> = Tier0Cache::new(alloc, 2, EvictionPolicy::Manual);
        c.insert("a".into(), b"1".to_vec()).unwrap();
        c.insert("b".into(), b"2".to_vec()).unwrap();
        // Cache cap = 2 and Manual policy => no eviction.  Allocator runs OOM.
        let err = c.insert("c".into(), b"3".to_vec()).unwrap_err();
        assert!(matches!(err, AllocError::OutOfMemory { .. }));
        assert_eq!(c.stats().evictions, 0);
    }

    #[test]
    fn replace_existing_key_frees_old_page() {
        let alloc = InMemoryCxlAllocator::shared(8192, 4096); // 2 pages
        let c: Tier0Cache<InMemoryCxlAllocator> =
            Tier0Cache::new(alloc.clone(), 4, EvictionPolicy::Lru);
        c.insert("k".into(), b"old".to_vec()).unwrap();
        c.insert("k".into(), b"new".to_vec()).unwrap();
        assert_eq!(c.get("k"), Some(b"new".to_vec()));
        // Only one slot consumed even though we inserted twice.
        assert_eq!(alloc.stats().pages_used, 1);
    }

    #[test]
    fn remove_frees_underlying_page() {
        let alloc = InMemoryCxlAllocator::shared(8192, 4096);
        let c: Tier0Cache<InMemoryCxlAllocator> =
            Tier0Cache::new(alloc.clone(), 4, EvictionPolicy::Lru);
        c.insert("k".into(), b"v".to_vec()).unwrap();
        assert_eq!(alloc.stats().pages_used, 1);
        assert!(c.remove("k"));
        assert_eq!(alloc.stats().pages_used, 0);
    }
}
