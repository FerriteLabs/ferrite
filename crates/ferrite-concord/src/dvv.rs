//! Dotted Version Vectors (DVV) — causal context for CRDT operations.
//!
//! Based on Almeida et al. "Scalable and Accurate Causality Tracking for
//! Eventually Consistent Stores" (2014).

use std::collections::BTreeMap;

/// A version vector mapping replica IDs to their latest sequence number.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VersionVector {
    entries: BTreeMap<String, u64>,
}

impl VersionVector {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    /// Increment the counter for a replica.
    pub fn increment(&mut self, replica: &str) -> u64 {
        let entry = self.entries.entry(replica.to_string()).or_insert(0);
        *entry += 1;
        *entry
    }

    /// Get the counter for a replica.
    pub fn get(&self, replica: &str) -> u64 {
        self.entries.get(replica).copied().unwrap_or(0)
    }

    /// Merge with another version vector (point-wise max).
    pub fn merge(&mut self, other: &VersionVector) {
        for (k, &v) in &other.entries {
            let entry = self.entries.entry(k.clone()).or_insert(0);
            *entry = (*entry).max(v);
        }
    }

    /// Check if this vector dominates (happens-after) another.
    pub fn dominates(&self, other: &VersionVector) -> bool {
        other.entries.iter().all(|(k, &v)| self.get(k) >= v)
            && self.entries.iter().any(|(k, &v)| v > other.get(k))
    }

    /// Check if two vectors are concurrent (neither dominates).
    pub fn concurrent(&self, other: &VersionVector) -> bool {
        !self.dominates(other) && !other.dominates(self) && self != other
    }

    /// Total size (number of replicas tracked).
    pub fn size(&self) -> usize {
        self.entries.len()
    }

    /// Compact: remove entries with value 0.
    pub fn compact(&mut self) {
        self.entries.retain(|_, v| *v > 0);
    }

    /// Metadata overhead in bytes (approximate).
    pub fn metadata_bytes(&self) -> usize {
        self.entries.keys().map(|k| k.len() + 8).sum()
    }

    /// Iterate over all entries.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &u64)> {
        self.entries.iter()
    }
}

impl Default for VersionVector {
    fn default() -> Self {
        Self::new()
    }
}

/// A Dotted Version Vector — a version vector plus a "dot" (the specific event).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DottedVersionVector {
    /// The causal context (what has been seen).
    pub context: VersionVector,
    /// The specific event dot (replica, sequence).
    pub dot: Option<(String, u64)>,
}

impl DottedVersionVector {
    pub fn new() -> Self {
        Self {
            context: VersionVector::new(),
            dot: None,
        }
    }

    /// Create a new event on a replica.
    pub fn event(&mut self, replica: &str) -> (String, u64) {
        let seq = self.context.increment(replica);
        self.dot = Some((replica.to_string(), seq));
        (replica.to_string(), seq)
    }

    /// Sync with another DVV (merge contexts).
    pub fn sync(&mut self, other: &DottedVersionVector) {
        self.context.merge(&other.context);
        // Include the other's dot in our context
        if let Some((ref r, s)) = other.dot {
            let entry = self.context.entries.entry(r.clone()).or_insert(0);
            *entry = (*entry).max(s);
        }
    }

    /// Check if this DVV has seen the other's dot.
    pub fn has_seen(&self, other: &DottedVersionVector) -> bool {
        match &other.dot {
            Some((r, s)) => self.context.get(r) >= *s,
            None => true,
        }
    }

    /// Discard the dot (after it's been incorporated into context).
    pub fn discard_dot(&mut self) {
        if let Some((ref r, s)) = self.dot.take() {
            let entry = self.context.entries.entry(r.clone()).or_insert(0);
            *entry = (*entry).max(s);
        }
    }
}

impl Default for DottedVersionVector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── VersionVector tests ─────────────────────────────────────────

    #[test]
    fn increment_and_get() {
        let mut vv = VersionVector::new();
        assert_eq!(vv.get("a"), 0);
        assert_eq!(vv.increment("a"), 1);
        assert_eq!(vv.increment("a"), 2);
        assert_eq!(vv.get("a"), 2);
        assert_eq!(vv.get("b"), 0);
    }

    #[test]
    fn merge_takes_pointwise_max() {
        let mut a = VersionVector::new();
        a.increment("x"); // x=1
        a.increment("x"); // x=2
        a.increment("y"); // y=1

        let mut b = VersionVector::new();
        b.increment("x"); // x=1
        b.increment("y"); // y=1
        b.increment("y"); // y=2
        b.increment("z"); // z=1

        a.merge(&b);
        assert_eq!(a.get("x"), 2); // max(2, 1)
        assert_eq!(a.get("y"), 2); // max(1, 2)
        assert_eq!(a.get("z"), 1); // max(0, 1)
    }

    #[test]
    fn dominates_when_strictly_higher() {
        let mut a = VersionVector::new();
        a.increment("x");
        a.increment("x");

        let mut b = VersionVector::new();
        b.increment("x");

        assert!(a.dominates(&b));
        assert!(!b.dominates(&a));
    }

    #[test]
    fn dominates_returns_false_for_equal() {
        let mut a = VersionVector::new();
        a.increment("x");
        let b = a.clone();
        assert!(!a.dominates(&b));
    }

    #[test]
    fn concurrent_when_neither_dominates() {
        let mut a = VersionVector::new();
        a.increment("x"); // a has x=1

        let mut b = VersionVector::new();
        b.increment("y"); // b has y=1

        assert!(a.concurrent(&b));
        assert!(b.concurrent(&a));
    }

    #[test]
    fn not_concurrent_when_equal() {
        let mut a = VersionVector::new();
        a.increment("x");
        let b = a.clone();
        assert!(!a.concurrent(&b));
    }

    #[test]
    fn compact_removes_zeros() {
        let mut vv = VersionVector::new();
        vv.entries.insert("a".to_string(), 0);
        vv.entries.insert("b".to_string(), 3);
        vv.entries.insert("c".to_string(), 0);
        assert_eq!(vv.size(), 3);

        vv.compact();
        assert_eq!(vv.size(), 1);
        assert_eq!(vv.get("b"), 3);
        assert_eq!(vv.get("a"), 0);
    }

    #[test]
    fn metadata_bytes_reasonable_estimate() {
        let mut vv = VersionVector::new();
        vv.increment("node-1");
        vv.increment("node-2");
        let bytes = vv.metadata_bytes();
        // Each entry: key_len + 8 bytes for u64
        // "node-1" = 6 + 8 = 14, "node-2" = 6 + 8 = 14 => 28
        assert_eq!(bytes, 28);
        assert!(bytes > 0);
    }

    // ─── DottedVersionVector tests ───────────────────────────────────

    #[test]
    fn event_creates_a_dot() {
        let mut dvv = DottedVersionVector::new();
        let (replica, seq) = dvv.event("node-a");
        assert_eq!(replica, "node-a");
        assert_eq!(seq, 1);
        assert_eq!(dvv.dot, Some(("node-a".to_string(), 1)));
        assert_eq!(dvv.context.get("node-a"), 1);
    }

    #[test]
    fn sync_incorporates_context_and_dot() {
        let mut a = DottedVersionVector::new();
        a.event("node-a"); // a: context={node-a:1}, dot=(node-a, 1)

        let mut b = DottedVersionVector::new();
        b.event("node-b"); // b: context={node-b:1}, dot=(node-b, 1)

        a.sync(&b);
        assert_eq!(a.context.get("node-a"), 1);
        assert_eq!(a.context.get("node-b"), 1);
    }

    #[test]
    fn has_seen_returns_true_after_sync() {
        let mut a = DottedVersionVector::new();
        a.event("node-a");

        let mut b = DottedVersionVector::new();
        b.event("node-b");

        assert!(!a.has_seen(&b));
        a.sync(&b);
        assert!(a.has_seen(&b));
    }

    #[test]
    fn has_seen_returns_true_for_no_dot() {
        let a = DottedVersionVector::new();
        let b = DottedVersionVector::new();
        assert!(a.has_seen(&b));
    }

    #[test]
    fn discard_dot_moves_dot_into_context() {
        let mut dvv = DottedVersionVector::new();
        dvv.event("node-a");
        assert!(dvv.dot.is_some());

        dvv.discard_dot();
        assert!(dvv.dot.is_none());
        assert_eq!(dvv.context.get("node-a"), 1);
    }

    #[test]
    fn sequential_events_on_same_replica() {
        let mut dvv = DottedVersionVector::new();
        let (_, s1) = dvv.event("node-a");
        let (_, s2) = dvv.event("node-a");
        assert_eq!(s1, 1);
        assert_eq!(s2, 2);
        assert_eq!(dvv.context.get("node-a"), 2);
    }
}
