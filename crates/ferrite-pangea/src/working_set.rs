//! Working-set tracker: counts hits per key over a sliding window so
//! Tier-0 can decide which keys to *keep hot* and which to demote.

use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WorkingSetStats {
    pub tracked_keys: usize,
    pub recorded_hits: usize,
}

/// Bounded sliding-window hit counter.
pub struct WorkingSet {
    window: usize,
    /// Recent (key) hits, ordered oldest-first.
    history: Mutex<VecDeque<String>>,
    /// Live counter: key → hits-still-in-window.
    counts: Mutex<HashMap<String, u32>>,
}

impl std::fmt::Debug for WorkingSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkingSet")
            .field("window", &self.window)
            .field("stats", &self.stats())
            .finish()
    }
}

impl WorkingSet {
    pub fn new(window: usize) -> Self {
        assert!(window > 0);
        Self {
            window,
            history: Mutex::new(VecDeque::with_capacity(window)),
            counts: Mutex::default(),
        }
    }

    /// Record one access to `key`.  If the window is full, the oldest
    /// entry is dropped and its counter decremented.
    pub fn record(&self, key: &str) {
        let mut history = self.history.lock();
        let mut counts = self.counts.lock();
        if history.len() == self.window {
            if let Some(old) = history.pop_front() {
                if let Some(c) = counts.get_mut(&old) {
                    *c -= 1;
                    if *c == 0 {
                        counts.remove(&old);
                    }
                }
            }
        }
        history.push_back(key.to_string());
        *counts.entry(key.to_string()).or_insert(0) += 1;
    }

    /// Top-N hottest keys (by hit count in current window), ties broken
    /// by lexicographic key order.
    pub fn top(&self, n: usize) -> Vec<(String, u32)> {
        let counts = self.counts.lock();
        let mut v: Vec<(String, u32)> = counts.iter().map(|(k, v)| (k.clone(), *v)).collect();
        v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        v.truncate(n);
        v
    }

    /// Keys whose hit count in the current window is at least `min_hits`.
    pub fn promotion_candidates(&self, min_hits: u32) -> Vec<String> {
        let counts = self.counts.lock();
        let mut v: Vec<(String, u32)> = counts
            .iter()
            .filter(|(_, c)| **c >= min_hits)
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        v.into_iter().map(|(k, _)| k).collect()
    }

    pub fn stats(&self) -> WorkingSetStats {
        let counts = self.counts.lock();
        let history = self.history.lock();
        WorkingSetStats {
            tracked_keys: counts.len(),
            recorded_hits: history.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_increments_count() {
        let w = WorkingSet::new(10);
        w.record("a");
        w.record("a");
        w.record("b");
        let top = w.top(5);
        assert_eq!(top, vec![("a".into(), 2), ("b".into(), 1)]);
    }

    #[test]
    fn window_evicts_oldest_hit() {
        let w = WorkingSet::new(2);
        w.record("a");
        w.record("b");
        w.record("c"); // evicts 'a'
        let top = w.top(5);
        assert_eq!(top.len(), 2);
        assert!(top.iter().all(|(k, _)| k == "b" || k == "c"));
    }

    #[test]
    fn promotion_candidates_filter_by_min_hits() {
        let w = WorkingSet::new(10);
        for _ in 0..3 {
            w.record("hot");
        }
        for _ in 0..2 {
            w.record("warm");
        }
        w.record("cold");
        let promo = w.promotion_candidates(2);
        assert_eq!(promo, vec!["hot".to_string(), "warm".to_string()]);
    }

    #[test]
    fn stats_match_recorded_state() {
        let w = WorkingSet::new(5);
        w.record("a");
        w.record("b");
        w.record("a");
        let s = w.stats();
        assert_eq!(s.tracked_keys, 2);
        assert_eq!(s.recorded_hits, 3);
    }

    #[test]
    fn full_eviction_when_all_hits_fall_off() {
        let w = WorkingSet::new(2);
        w.record("a");
        w.record("a");
        w.record("b");
        w.record("c"); // evicts the second 'a' → 'a' count == 0 → removed
        assert!(w.top(5).iter().all(|(k, _)| k != "a"));
    }
}
