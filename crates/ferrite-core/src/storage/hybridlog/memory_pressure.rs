//! Memory Pressure Detection & Eviction for HybridLog
//!
//! Provides memory pressure detection, automatic tier promotion under pressure,
//! and configurable eviction policies (LRU, LFU, Random) when approaching
//! memory limits.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use tracing::{debug, warn};

/// Eviction policy selector
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPolicy {
    /// Least Recently Used — evict entries with oldest last-access time
    Lru,
    /// Least Frequently Used — evict entries with lowest access count
    Lfu,
    /// Random eviction
    Random,
}

impl Default for EvictionPolicy {
    fn default() -> Self {
        Self::Lru
    }
}

/// Configuration for memory pressure handling
#[derive(Debug, Clone)]
pub struct MemoryPressureConfig {
    /// Memory limit in bytes (total across mutable + readonly)
    pub memory_limit: usize,
    /// Fraction (0.0–1.0) at which to start evicting (soft pressure)
    pub soft_pressure_threshold: f64,
    /// Fraction (0.0–1.0) at which aggressive eviction kicks in (hard pressure)
    pub hard_pressure_threshold: f64,
    /// Eviction policy to use
    pub eviction_policy: EvictionPolicy,
    /// Number of candidate entries to sample when choosing eviction victims
    pub eviction_sample_size: usize,
}

impl Default for MemoryPressureConfig {
    fn default() -> Self {
        Self {
            memory_limit: 512 * 1024 * 1024, // 512 MB
            soft_pressure_threshold: 0.7,
            hard_pressure_threshold: 0.9,
            eviction_policy: EvictionPolicy::Lru,
            eviction_sample_size: 16,
        }
    }
}

/// Current memory pressure level
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PressureLevel {
    /// Below soft threshold — no action needed
    None,
    /// Between soft and hard thresholds — background eviction recommended
    Soft,
    /// Above hard threshold — aggressive eviction required
    Hard,
}

/// Memory pressure metrics
#[derive(Debug)]
pub struct MemoryPressureMetrics {
    /// Total bytes evicted from hot tier
    pub bytes_evicted: AtomicU64,
    /// Total entries evicted
    pub entries_evicted: AtomicU64,
    /// Number of soft pressure events
    pub soft_pressure_events: AtomicU64,
    /// Number of hard pressure events
    pub hard_pressure_events: AtomicU64,
    /// Current memory usage snapshot (bytes)
    pub current_usage: AtomicU64,
}

impl Default for MemoryPressureMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryPressureMetrics {
    /// Create zeroed metrics
    pub fn new() -> Self {
        Self {
            bytes_evicted: AtomicU64::new(0),
            entries_evicted: AtomicU64::new(0),
            soft_pressure_events: AtomicU64::new(0),
            hard_pressure_events: AtomicU64::new(0),
            current_usage: AtomicU64::new(0),
        }
    }

    /// Record an eviction
    pub fn record_eviction(&self, bytes: u64, entries: u64) {
        self.bytes_evicted.fetch_add(bytes, Ordering::Relaxed);
        self.entries_evicted.fetch_add(entries, Ordering::Relaxed);
    }

    /// Record a pressure event
    pub fn record_pressure(&self, level: PressureLevel) {
        match level {
            PressureLevel::None => {}
            PressureLevel::Soft => {
                self.soft_pressure_events.fetch_add(1, Ordering::Relaxed);
            }
            PressureLevel::Hard => {
                self.hard_pressure_events.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Get a snapshot of the metrics
    pub fn snapshot(&self) -> MemoryPressureSnapshot {
        MemoryPressureSnapshot {
            bytes_evicted: self.bytes_evicted.load(Ordering::Relaxed),
            entries_evicted: self.entries_evicted.load(Ordering::Relaxed),
            soft_pressure_events: self.soft_pressure_events.load(Ordering::Relaxed),
            hard_pressure_events: self.hard_pressure_events.load(Ordering::Relaxed),
            current_usage: self.current_usage.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time snapshot of memory pressure metrics
#[derive(Debug, Clone, Default)]
pub struct MemoryPressureSnapshot {
    /// Total bytes evicted
    pub bytes_evicted: u64,
    /// Total entries evicted
    pub entries_evicted: u64,
    /// Soft pressure events
    pub soft_pressure_events: u64,
    /// Hard pressure events
    pub hard_pressure_events: u64,
    /// Current memory usage
    pub current_usage: u64,
}

/// Detect the current memory pressure level
pub fn detect_pressure(
    used_bytes: usize,
    config: &MemoryPressureConfig,
) -> PressureLevel {
    if config.memory_limit == 0 {
        return PressureLevel::None;
    }
    let ratio = used_bytes as f64 / config.memory_limit as f64;

    if ratio >= config.hard_pressure_threshold {
        PressureLevel::Hard
    } else if ratio >= config.soft_pressure_threshold {
        PressureLevel::Soft
    } else {
        PressureLevel::None
    }
}

/// Entry metadata used for eviction candidate scoring
#[derive(Debug, Clone)]
pub struct EvictionCandidate {
    /// Key bytes
    pub key: Vec<u8>,
    /// Number of accesses
    pub access_count: u64,
    /// Last access time
    pub last_access: Instant,
    /// Approximate entry size in bytes
    pub size_bytes: u64,
}

/// Select eviction victims from a set of candidates
///
/// Returns candidates sorted by eviction priority (first = best victim).
pub fn select_victims(
    candidates: &mut [EvictionCandidate],
    policy: EvictionPolicy,
    count: usize,
) -> Vec<usize> {
    if candidates.is_empty() || count == 0 {
        return Vec::new();
    }

    let n = count.min(candidates.len());

    match policy {
        EvictionPolicy::Lru => {
            // Sort by last_access ascending (oldest first)
            let mut indices: Vec<usize> = (0..candidates.len()).collect();
            indices.sort_by(|&a, &b| candidates[a].last_access.cmp(&candidates[b].last_access));
            indices.truncate(n);
            indices
        }
        EvictionPolicy::Lfu => {
            // Sort by access_count ascending (least accessed first)
            let mut indices: Vec<usize> = (0..candidates.len()).collect();
            indices.sort_by_key(|&i| candidates[i].access_count);
            indices.truncate(n);
            indices
        }
        EvictionPolicy::Random => {
            use rand::seq::SliceRandom;
            let mut indices: Vec<usize> = (0..candidates.len()).collect();
            let mut rng = rand::thread_rng();
            indices.shuffle(&mut rng);
            indices.truncate(n);
            indices
        }
    }
}

/// Compute how many entries should be evicted to bring usage below the soft threshold
pub fn entries_to_evict(
    used_bytes: usize,
    avg_entry_size: usize,
    config: &MemoryPressureConfig,
) -> usize {
    if config.memory_limit == 0 || avg_entry_size == 0 {
        return 0;
    }

    let target = (config.memory_limit as f64 * config.soft_pressure_threshold) as usize;
    let excess = used_bytes.saturating_sub(target);

    if excess == 0 {
        return 0;
    }

    let count = excess.div_ceil(avg_entry_size);

    debug!(
        used_bytes = used_bytes,
        target_bytes = target,
        excess_bytes = excess,
        evict_count = count,
        "Calculated entries to evict"
    );

    count
}

/// Log a memory pressure warning
pub fn log_pressure_event(level: PressureLevel, used_bytes: usize, limit: usize) {
    match level {
        PressureLevel::None => {}
        PressureLevel::Soft => {
            warn!(
                used_bytes = used_bytes,
                limit = limit,
                "Soft memory pressure detected ({:.1}%)",
                used_bytes as f64 / limit as f64 * 100.0
            );
        }
        PressureLevel::Hard => {
            warn!(
                used_bytes = used_bytes,
                limit = limit,
                "HARD memory pressure detected ({:.1}%)",
                used_bytes as f64 / limit as f64 * 100.0
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config(limit: usize) -> MemoryPressureConfig {
        MemoryPressureConfig {
            memory_limit: limit,
            soft_pressure_threshold: 0.7,
            hard_pressure_threshold: 0.9,
            ..Default::default()
        }
    }

    #[test]
    fn test_detect_no_pressure() {
        let config = make_config(1000);
        assert_eq!(detect_pressure(500, &config), PressureLevel::None);
    }

    #[test]
    fn test_detect_soft_pressure() {
        let config = make_config(1000);
        assert_eq!(detect_pressure(750, &config), PressureLevel::Soft);
    }

    #[test]
    fn test_detect_hard_pressure() {
        let config = make_config(1000);
        assert_eq!(detect_pressure(950, &config), PressureLevel::Hard);
    }

    #[test]
    fn test_detect_zero_limit() {
        let config = make_config(0);
        assert_eq!(detect_pressure(999, &config), PressureLevel::None);
    }

    #[test]
    fn test_entries_to_evict_no_excess() {
        let config = make_config(1000);
        assert_eq!(entries_to_evict(500, 100, &config), 0);
    }

    #[test]
    fn test_entries_to_evict_with_excess() {
        let config = make_config(1000);
        // used=900, target=700, excess=200, avg=100 → 2 entries
        assert_eq!(entries_to_evict(900, 100, &config), 2);
    }

    #[test]
    fn test_entries_to_evict_rounds_up() {
        let config = make_config(1000);
        // used=850, target=700, excess=150, avg=100 → ceil(1.5) = 2
        assert_eq!(entries_to_evict(850, 100, &config), 2);
    }

    #[test]
    fn test_select_victims_lru() {
        let now = Instant::now();
        let old = now - std::time::Duration::from_secs(100);
        let older = now - std::time::Duration::from_secs(200);

        let mut candidates = vec![
            EvictionCandidate {
                key: b"recent".to_vec(),
                access_count: 10,
                last_access: now,
                size_bytes: 100,
            },
            EvictionCandidate {
                key: b"oldest".to_vec(),
                access_count: 5,
                last_access: older,
                size_bytes: 100,
            },
            EvictionCandidate {
                key: b"old".to_vec(),
                access_count: 8,
                last_access: old,
                size_bytes: 100,
            },
        ];

        let victims = select_victims(&mut candidates, EvictionPolicy::Lru, 2);
        assert_eq!(victims.len(), 2);
        // "oldest" (idx 1) should be first, then "old" (idx 2)
        assert_eq!(victims[0], 1);
        assert_eq!(victims[1], 2);
    }

    #[test]
    fn test_select_victims_lfu() {
        let now = Instant::now();
        let mut candidates = vec![
            EvictionCandidate {
                key: b"frequent".to_vec(),
                access_count: 100,
                last_access: now,
                size_bytes: 100,
            },
            EvictionCandidate {
                key: b"rare".to_vec(),
                access_count: 1,
                last_access: now,
                size_bytes: 100,
            },
            EvictionCandidate {
                key: b"medium".to_vec(),
                access_count: 10,
                last_access: now,
                size_bytes: 100,
            },
        ];

        let victims = select_victims(&mut candidates, EvictionPolicy::Lfu, 2);
        assert_eq!(victims.len(), 2);
        // "rare" (idx 1) then "medium" (idx 2)
        assert_eq!(victims[0], 1);
        assert_eq!(victims[1], 2);
    }

    #[test]
    fn test_select_victims_random() {
        let now = Instant::now();
        let mut candidates = vec![
            EvictionCandidate {
                key: b"a".to_vec(),
                access_count: 1,
                last_access: now,
                size_bytes: 100,
            },
            EvictionCandidate {
                key: b"b".to_vec(),
                access_count: 1,
                last_access: now,
                size_bytes: 100,
            },
        ];

        let victims = select_victims(&mut candidates, EvictionPolicy::Random, 1);
        assert_eq!(victims.len(), 1);
    }

    #[test]
    fn test_select_victims_empty() {
        let victims = select_victims(&mut [], EvictionPolicy::Lru, 5);
        assert!(victims.is_empty());
    }

    #[test]
    fn test_select_victims_count_exceeds_candidates() {
        let now = Instant::now();
        let mut candidates = vec![EvictionCandidate {
            key: b"only".to_vec(),
            access_count: 1,
            last_access: now,
            size_bytes: 100,
        }];

        let victims = select_victims(&mut candidates, EvictionPolicy::Lru, 10);
        assert_eq!(victims.len(), 1);
    }

    #[test]
    fn test_metrics_snapshot() {
        let metrics = MemoryPressureMetrics::new();
        metrics.record_eviction(500, 5);
        metrics.record_pressure(PressureLevel::Soft);
        metrics.record_pressure(PressureLevel::Hard);

        let snap = metrics.snapshot();
        assert_eq!(snap.bytes_evicted, 500);
        assert_eq!(snap.entries_evicted, 5);
        assert_eq!(snap.soft_pressure_events, 1);
        assert_eq!(snap.hard_pressure_events, 1);
    }
}
