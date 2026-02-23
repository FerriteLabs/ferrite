//! Hot Key Detection
//!
//! Tracks per-key access frequency over a sliding time window using
//! lock-free concurrent counters to identify "hot" keys.

#![forbid(unsafe_code)]

use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

/// A single hot-key entry with access statistics.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HotKeyEntry {
    /// The key.
    pub key: String,
    /// Total accesses in the current window.
    pub access_count: u64,
    /// Estimated operations per second.
    pub ops_per_sec: f64,
}

/// Concurrent hot key detector using `DashMap` for lock-free counting.
pub struct HotKeyDetector {
    access_counts: DashMap<String, u64>,
    window: RwLock<WindowState>,
    window_duration: Duration,
    top_k: usize,
}

struct WindowState {
    start: Instant,
}

impl HotKeyDetector {
    /// Create a new detector.
    ///
    /// * `window_duration` – measurement window length.
    /// * `top_k` – default number of hot keys returned.
    pub fn new(window_duration: Duration, top_k: usize) -> Self {
        Self {
            access_counts: DashMap::new(),
            window: RwLock::new(WindowState {
                start: Instant::now(),
            }),
            window_duration,
            top_k,
        }
    }

    /// Record an access to `key`.
    pub fn record_access(&self, key: &str) {
        self.maybe_reset_window();
        self.access_counts
            .entry(key.to_string())
            .and_modify(|c| *c += 1)
            .or_insert(1);
    }

    /// Get the top `n` hot keys. Returns up to `n` entries sorted by access count descending.
    pub fn get_hot_keys(&self, n: usize) -> Vec<HotKeyEntry> {
        let elapsed = self.window_elapsed_secs();

        let mut entries: Vec<(String, u64)> = self
            .access_counts
            .iter()
            .map(|r| (r.key().clone(), *r.value()))
            .collect();

        entries.sort_by(|a, b| b.1.cmp(&a.1));
        entries.truncate(n);

        entries
            .into_iter()
            .map(|(key, count)| HotKeyEntry {
                key,
                access_count: count,
                ops_per_sec: if elapsed > 0.0 {
                    count as f64 / elapsed
                } else {
                    count as f64
                },
            })
            .collect()
    }

    /// Convenience method using the configured `top_k`.
    pub fn top_keys(&self) -> Vec<HotKeyEntry> {
        self.get_hot_keys(self.top_k)
    }

    /// Reset the measurement window — clears all counters.
    pub fn reset_window(&self) {
        self.access_counts.clear();
        let mut window = self.window.write();
        window.start = Instant::now();
    }

    /// Total distinct keys currently tracked.
    pub fn tracked_keys(&self) -> usize {
        self.access_counts.len()
    }

    /// Total accesses in the current window.
    pub fn total_accesses(&self) -> u64 {
        self.access_counts.iter().map(|r| *r.value()).sum()
    }

    // ---- private helpers ----

    fn window_elapsed_secs(&self) -> f64 {
        let window = self.window.read();
        window.start.elapsed().as_secs_f64()
    }

    fn maybe_reset_window(&self) {
        let should_reset = {
            let window = self.window.read();
            window.start.elapsed() >= self.window_duration
        };
        if should_reset {
            self.reset_window();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_and_get() {
        let detector = HotKeyDetector::new(Duration::from_secs(60), 10);
        for _ in 0..100 {
            detector.record_access("user:1");
        }
        for _ in 0..50 {
            detector.record_access("user:2");
        }
        detector.record_access("user:3");

        let hot = detector.get_hot_keys(2);
        assert_eq!(hot.len(), 2);
        assert_eq!(hot[0].key, "user:1");
        assert_eq!(hot[0].access_count, 100);
        assert_eq!(hot[1].key, "user:2");
        assert_eq!(hot[1].access_count, 50);
    }

    #[test]
    fn test_reset_window() {
        let detector = HotKeyDetector::new(Duration::from_secs(60), 10);
        detector.record_access("key1");
        assert_eq!(detector.tracked_keys(), 1);

        detector.reset_window();
        assert_eq!(detector.tracked_keys(), 0);
    }

    #[test]
    fn test_total_accesses() {
        let detector = HotKeyDetector::new(Duration::from_secs(60), 10);
        detector.record_access("a");
        detector.record_access("a");
        detector.record_access("b");
        assert_eq!(detector.total_accesses(), 3);
    }

    #[test]
    fn test_top_keys_defaults() {
        let detector = HotKeyDetector::new(Duration::from_secs(60), 2);
        detector.record_access("x");
        detector.record_access("y");
        detector.record_access("z");
        let top = detector.top_keys();
        assert_eq!(top.len(), 2);
    }

    #[test]
    fn test_ops_per_sec() {
        let detector = HotKeyDetector::new(Duration::from_secs(3600), 10);
        detector.record_access("fast");
        let hot = detector.get_hot_keys(1);
        assert_eq!(hot.len(), 1);
        assert!(hot[0].ops_per_sec > 0.0);
    }
}
