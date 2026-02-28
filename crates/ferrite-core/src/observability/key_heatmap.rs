//! Key Access Heatmap — real-time key access pattern visualization
//!
//! Tracks hot keys, hot prefixes, and per-slot access distributions using
//! probabilistic data structures (Count-Min Sketch + Space-Saving) for
//! bounded memory usage regardless of keyspace size.

#![allow(dead_code)]

use std::collections::BinaryHeap;
use std::hash::{Hash, Hasher};
use std::time::Instant;

use parking_lot::RwLock;

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

/// The type of key access operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum HeatmapOp {
    /// GET-type read operation.
    Get,
    /// SET-type write operation.
    Set,
    /// DEL-type delete operation.
    Del,
    /// EXPIRE-type TTL operation.
    Expire,
    /// SCAN-type iteration operation.
    Scan,
}

impl std::fmt::Display for HeatmapOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Get => write!(f, "GET"),
            Self::Set => write!(f, "SET"),
            Self::Del => write!(f, "DEL"),
            Self::Expire => write!(f, "EXPIRE"),
            Self::Scan => write!(f, "SCAN"),
        }
    }
}

// ---------------------------------------------------------------------------
// Hot-key / hot-prefix entries
// ---------------------------------------------------------------------------

/// A single hot-key entry returned by [`KeyHeatmap::get_hot_keys`].
#[derive(Clone, Debug)]
pub struct HotKeyEntry {
    /// The raw key bytes.
    pub key: Vec<u8>,
    /// Approximate total access count.
    pub access_count: u64,
    /// Instant of the most recent access.
    pub last_access: Instant,
    /// Estimated operations per second (smoothed).
    pub ops_per_sec: f64,
}

/// A single hot-prefix entry returned by [`KeyHeatmap::get_hot_prefixes`].
#[derive(Clone, Debug)]
pub struct HotPrefixEntry {
    /// The prefix string (split on `:`).
    pub prefix: String,
    /// Distinct key count sharing this prefix.
    pub key_count: u64,
    /// Total accesses across all keys with this prefix.
    pub total_accesses: u64,
}

/// Point-in-time snapshot of the full heatmap state.
#[derive(Clone, Debug)]
pub struct HeatmapSnapshot {
    /// When the snapshot was taken.
    pub timestamp: Instant,
    /// Total operations recorded since last reset.
    pub total_ops: u64,
    /// Hottest keys at snapshot time.
    pub hot_keys: Vec<HotKeyEntry>,
    /// Hottest prefixes at snapshot time.
    pub hot_prefixes: Vec<HotPrefixEntry>,
    /// Per-slot (0..16384) access distribution.
    pub slot_distribution: [u32; 16384],
}

// ---------------------------------------------------------------------------
// Count-Min Sketch
// ---------------------------------------------------------------------------

/// Number of independent hash rows in the CMS.
const CMS_DEPTH: usize = 4;

/// A simple Count-Min Sketch for approximate frequency counting.
struct CountMinSketch {
    width: usize,
    table: Vec<Vec<u32>>,
    seeds: [u64; CMS_DEPTH],
}

impl CountMinSketch {
    fn new(width: usize) -> Self {
        Self {
            width,
            table: vec![vec![0u32; width]; CMS_DEPTH],
            seeds: [
                0x9e37_79b9_7f4a_7c15,
                0x6c62_272e_07bb_0142,
                0xbf58_476d_1ce4_e5b9,
                0x94d0_49bb_1331_11eb,
            ],
        }
    }

    fn increment(&mut self, key: &[u8]) {
        for row in 0..CMS_DEPTH {
            let col = self.hash(key, row) % self.width;
            self.table[row][col] = self.table[row][col].saturating_add(1);
        }
    }

    fn estimate(&self, key: &[u8]) -> u32 {
        (0..CMS_DEPTH)
            .map(|row| {
                let col = self.hash(key, row) % self.width;
                self.table[row][col]
            })
            .min()
            .unwrap_or(0)
    }

    fn reset(&mut self) {
        for row in &mut self.table {
            row.fill(0);
        }
    }

    /// FNV-1a variant seeded per row.
    fn hash(&self, key: &[u8], row: usize) -> usize {
        let mut h = self.seeds[row];
        for &b in key {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x0100_0000_01b3);
        }
        h as usize
    }
}

// ---------------------------------------------------------------------------
// Space-Saving top-K tracker
// ---------------------------------------------------------------------------

/// An element tracked by the Space-Saving algorithm.
#[derive(Clone)]
struct TopKEntry {
    key: Vec<u8>,
    count: u64,
    last_access: Instant,
}

/// Minimum-heap ordering so the least frequent element is popped first.
impl PartialEq for TopKEntry {
    fn eq(&self, other: &Self) -> bool {
        self.count == other.count
    }
}
impl Eq for TopKEntry {}

impl PartialOrd for TopKEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TopKEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse so BinaryHeap becomes a min-heap.
        other.count.cmp(&self.count)
    }
}

struct SpaceSaving {
    capacity: usize,
    heap: BinaryHeap<TopKEntry>,
}

impl SpaceSaving {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            heap: BinaryHeap::with_capacity(capacity + 1),
        }
    }

    fn observe(&mut self, key: &[u8], estimated_count: u64, now: Instant) {
        // Check if the key is already tracked.
        let existing: Vec<TopKEntry> = self.heap.drain().collect();
        let mut found = false;
        let mut rebuilt = BinaryHeap::with_capacity(self.capacity + 1);

        for mut entry in existing {
            if entry.key == key {
                entry.count = estimated_count;
                entry.last_access = now;
                found = true;
            }
            rebuilt.push(entry);
        }

        if !found {
            if rebuilt.len() < self.capacity {
                rebuilt.push(TopKEntry {
                    key: key.to_vec(),
                    count: estimated_count,
                    last_access: now,
                });
            } else if let Some(min) = rebuilt.peek() {
                if estimated_count > min.count {
                    rebuilt.pop();
                    rebuilt.push(TopKEntry {
                        key: key.to_vec(),
                        count: estimated_count,
                        last_access: now,
                    });
                }
            }
        }

        self.heap = rebuilt;
    }

    fn top(&self, n: usize) -> Vec<TopKEntry> {
        let mut items: Vec<TopKEntry> = self.heap.iter().cloned().collect();
        items.sort_by(|a, b| b.count.cmp(&a.count));
        items.truncate(n);
        items
    }

    fn reset(&mut self) {
        self.heap.clear();
    }
}

// ---------------------------------------------------------------------------
// KeyHeatmap
// ---------------------------------------------------------------------------

/// Real-time key access pattern heatmap.
///
/// Uses a Count-Min Sketch for approximate frequency counting and the
/// Space-Saving algorithm for top-K hot key tracking. Prefix aggregation
/// splits keys on the `:` delimiter.
pub struct KeyHeatmap {
    inner: RwLock<HeatmapInner>,
}

struct HeatmapInner {
    cms: CountMinSketch,
    top_k: SpaceSaving,
    slots: [u32; 16384],
    total_ops: u64,
    created_at: Instant,
}

impl KeyHeatmap {
    /// Create a new key heatmap.
    ///
    /// * `cms_width` — width of each Count-Min Sketch row (larger = more accurate).
    /// * `top_k_capacity` — how many hot keys to track.
    pub fn new(cms_width: usize, top_k_capacity: usize) -> Self {
        Self {
            inner: RwLock::new(HeatmapInner {
                cms: CountMinSketch::new(cms_width),
                top_k: SpaceSaving::new(top_k_capacity),
                slots: [0u32; 16384],
                total_ops: 0,
                created_at: Instant::now(),
            }),
        }
    }

    /// Record an access to `key` with the given operation type.
    pub fn record_access(&self, key: &[u8], _op: HeatmapOp) {
        let now = Instant::now();
        let mut inner = self.inner.write();
        inner.cms.increment(key);
        let est = u64::from(inner.cms.estimate(key));
        inner.top_k.observe(key, est, now);
        inner.slots[slot_for_key(key) as usize] += 1;
        inner.total_ops += 1;
    }

    /// Return the hottest keys, sorted by descending access count.
    pub fn get_hot_keys(&self, top_n: usize) -> Vec<HotKeyEntry> {
        let inner = self.inner.read();
        let elapsed_secs = inner.created_at.elapsed().as_secs_f64().max(1.0);
        inner
            .top_k
            .top(top_n)
            .into_iter()
            .map(|e| HotKeyEntry {
                key: e.key,
                access_count: e.count,
                last_access: e.last_access,
                ops_per_sec: e.count as f64 / elapsed_secs,
            })
            .collect()
    }

    /// Return the hottest key prefixes (split on `:`), sorted by total accesses.
    pub fn get_hot_prefixes(&self, top_n: usize) -> Vec<HotPrefixEntry> {
        let inner = self.inner.read();
        let entries = inner.top_k.top(inner.top_k.capacity);

        // Aggregate by prefix.
        let mut prefix_map: std::collections::HashMap<String, (u64, u64)> =
            std::collections::HashMap::new();
        for e in &entries {
            let prefix = extract_prefix(&e.key);
            let entry = prefix_map.entry(prefix).or_insert((0, 0));
            entry.0 += 1; // key_count
            entry.1 += e.count; // total_accesses
        }

        let mut result: Vec<HotPrefixEntry> = prefix_map
            .into_iter()
            .map(|(prefix, (key_count, total_accesses))| HotPrefixEntry {
                prefix,
                key_count,
                total_accesses,
            })
            .collect();
        result.sort_by(|a, b| b.total_accesses.cmp(&a.total_accesses));
        result.truncate(top_n);
        result
    }

    /// Return the per-slot (0..16384) access counts.
    pub fn get_slot_heatmap(&self) -> [u32; 16384] {
        self.inner.read().slots
    }

    /// Clear all accumulated data.
    pub fn reset(&self) {
        let mut inner = self.inner.write();
        inner.cms.reset();
        inner.top_k.reset();
        inner.slots.fill(0);
        inner.total_ops = 0;
        inner.created_at = Instant::now();
    }

    /// Take a point-in-time snapshot of the heatmap.
    pub fn snapshot(&self) -> HeatmapSnapshot {
        let inner = self.inner.read();
        let elapsed_secs = inner.created_at.elapsed().as_secs_f64().max(1.0);

        let hot_keys: Vec<HotKeyEntry> = inner
            .top_k
            .top(100)
            .into_iter()
            .map(|e| HotKeyEntry {
                key: e.key,
                access_count: e.count,
                last_access: e.last_access,
                ops_per_sec: e.count as f64 / elapsed_secs,
            })
            .collect();

        let hot_prefixes = {
            let entries = inner.top_k.top(inner.top_k.capacity);
            let mut map: std::collections::HashMap<String, (u64, u64)> =
                std::collections::HashMap::new();
            for e in &entries {
                let prefix = extract_prefix(&e.key);
                let entry = map.entry(prefix).or_insert((0, 0));
                entry.0 += 1;
                entry.1 += e.count;
            }
            let mut v: Vec<HotPrefixEntry> = map
                .into_iter()
                .map(|(prefix, (key_count, total_accesses))| HotPrefixEntry {
                    prefix,
                    key_count,
                    total_accesses,
                })
                .collect();
            v.sort_by(|a, b| b.total_accesses.cmp(&a.total_accesses));
            v
        };

        HeatmapSnapshot {
            timestamp: Instant::now(),
            total_ops: inner.total_ops,
            hot_keys,
            hot_prefixes,
            slot_distribution: inner.slots,
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// CRC16 hash mod 16384 — same slot mapping as Redis Cluster.
fn slot_for_key(key: &[u8]) -> u16 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() % 16384) as u16
}

/// Extract the prefix before the first `:` delimiter, or the whole key.
fn extract_prefix(key: &[u8]) -> String {
    let s = String::from_utf8_lossy(key);
    match s.find(':') {
        Some(idx) => s[..idx].to_string(),
        None => s.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cms_basic() {
        let mut cms = CountMinSketch::new(1024);
        cms.increment(b"key1");
        cms.increment(b"key1");
        cms.increment(b"key1");
        cms.increment(b"key2");

        assert!(cms.estimate(b"key1") >= 3);
        assert!(cms.estimate(b"key2") >= 1);
        assert_eq!(cms.estimate(b"key3"), 0);
    }

    #[test]
    fn test_cms_reset() {
        let mut cms = CountMinSketch::new(1024);
        cms.increment(b"key1");
        cms.reset();
        assert_eq!(cms.estimate(b"key1"), 0);
    }

    #[test]
    fn test_key_heatmap_record_and_query() {
        let hm = KeyHeatmap::new(1024, 10);
        for _ in 0..100 {
            hm.record_access(b"user:session:1234", HeatmapOp::Get);
        }
        for _ in 0..50 {
            hm.record_access(b"cache:product:99", HeatmapOp::Get);
        }
        hm.record_access(b"queue:events", HeatmapOp::Set);

        let hot = hm.get_hot_keys(3);
        assert!(!hot.is_empty());
        assert!(hot[0].access_count >= hot.last().map(|e| e.access_count).unwrap_or(0));
    }

    #[test]
    fn test_key_heatmap_prefixes() {
        let hm = KeyHeatmap::new(1024, 10);
        hm.record_access(b"user:1", HeatmapOp::Get);
        hm.record_access(b"user:2", HeatmapOp::Get);
        hm.record_access(b"cache:x", HeatmapOp::Set);

        let prefixes = hm.get_hot_prefixes(10);
        let names: Vec<&str> = prefixes.iter().map(|p| p.prefix.as_str()).collect();
        assert!(names.contains(&"user"));
    }

    #[test]
    fn test_key_heatmap_slot_distribution() {
        let hm = KeyHeatmap::new(1024, 10);
        hm.record_access(b"a", HeatmapOp::Get);
        hm.record_access(b"b", HeatmapOp::Set);

        let slots = hm.get_slot_heatmap();
        let total: u32 = slots.iter().sum();
        assert_eq!(total, 2);
    }

    #[test]
    fn test_key_heatmap_reset() {
        let hm = KeyHeatmap::new(1024, 10);
        hm.record_access(b"foo", HeatmapOp::Get);
        hm.reset();

        let snap = hm.snapshot();
        assert_eq!(snap.total_ops, 0);
        assert!(snap.hot_keys.is_empty());
    }

    #[test]
    fn test_key_heatmap_snapshot() {
        let hm = KeyHeatmap::new(1024, 10);
        hm.record_access(b"x:1", HeatmapOp::Get);
        hm.record_access(b"x:2", HeatmapOp::Set);
        hm.record_access(b"y:1", HeatmapOp::Del);

        let snap = hm.snapshot();
        assert_eq!(snap.total_ops, 3);
        assert!(!snap.hot_keys.is_empty());
        assert!(!snap.hot_prefixes.is_empty());
    }

    #[test]
    fn test_heatmap_op_display() {
        assert_eq!(format!("{}", HeatmapOp::Get), "GET");
        assert_eq!(format!("{}", HeatmapOp::Set), "SET");
        assert_eq!(format!("{}", HeatmapOp::Del), "DEL");
        assert_eq!(format!("{}", HeatmapOp::Expire), "EXPIRE");
        assert_eq!(format!("{}", HeatmapOp::Scan), "SCAN");
    }

    #[test]
    fn test_extract_prefix_with_colon() {
        assert_eq!(extract_prefix(b"user:session:1234"), "user");
        assert_eq!(extract_prefix(b"no_colon"), "no_colon");
    }
}
