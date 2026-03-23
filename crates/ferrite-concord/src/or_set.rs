//! OR-Set (Observed-Remove Set): add wins on tie.
//!
//! Each add carries a unique tag (replica + monotonic counter).  Removes
//! tombstone the specific (member, tag) pairs they observed.  An element
//! is "in" the set iff at least one of its tags is non-tombstoned.

use crate::Crdt;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
struct Tag {
    replica: String,
    seq: u64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrSet<T: Ord + Clone + std::fmt::Debug> {
    /// member → set of live tags
    additions: BTreeMap<T, BTreeSet<Tag>>,
    /// (member, tag) tombstones
    removals: BTreeSet<(T, Tag)>,
    /// per-replica monotonic counter
    seqs: BTreeMap<String, u64>,
}

impl<T> OrSet<T>
where
    T: Ord + Clone + std::fmt::Debug,
{
    pub fn new() -> Self {
        Self {
            additions: BTreeMap::new(),
            removals: BTreeSet::new(),
            seqs: BTreeMap::new(),
        }
    }

    pub fn add(&mut self, replica: impl Into<String>, member: T) {
        let r = replica.into();
        let seq = self.seqs.entry(r.clone()).or_insert(0);
        *seq += 1;
        let tag = Tag {
            replica: r,
            seq: *seq,
        };
        self.additions.entry(member).or_default().insert(tag);
    }

    /// Remove all currently-observed tags for `member`.
    pub fn remove(&mut self, member: &T) {
        if let Some(tags) = self.additions.get(member) {
            for tag in tags {
                self.removals.insert((member.clone(), tag.clone()));
            }
        }
    }

    pub fn contains(&self, member: &T) -> bool {
        match self.additions.get(member) {
            None => false,
            Some(tags) => tags
                .iter()
                .any(|t| !self.removals.contains(&(member.clone(), t.clone()))),
        }
    }

    pub fn members(&self) -> Vec<T> {
        self.additions
            .keys()
            .filter(|m| self.contains(m))
            .cloned()
            .collect()
    }
}

impl<T> Crdt for OrSet<T>
where
    T: Ord + Clone + std::fmt::Debug,
{
    fn merge(&mut self, other: &Self) {
        for (member, tags) in &other.additions {
            self.additions
                .entry(member.clone())
                .or_default()
                .extend(tags.iter().cloned());
        }
        self.removals.extend(other.removals.iter().cloned());
        for (replica, &v) in &other.seqs {
            let entry = self.seqs.entry(replica.clone()).or_insert(0);
            if v > *entry {
                *entry = v;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_contains() {
        let mut s: OrSet<String> = OrSet::new();
        s.add("a", "x".into());
        assert!(s.contains(&"x".into()));
        assert!(!s.contains(&"y".into()));
    }

    #[test]
    fn add_wins_on_concurrent_remove_then_add() {
        let mut a: OrSet<String> = OrSet::new();
        let mut b: OrSet<String> = OrSet::new();
        a.add("a", "x".into());
        // Sync to b so the remove sees the tag.
        b.merge(&a);
        b.remove(&"x".into());
        // a concurrently re-adds (with a fresh tag).
        a.add("a", "x".into());
        a.merge(&b);
        b.merge(&a);
        assert_eq!(a, b);
        // The new "add" tag survives the older "remove" — add wins.
        assert!(a.contains(&"x".into()));
    }

    #[test]
    fn remove_after_add_drops_member() {
        let mut s: OrSet<String> = OrSet::new();
        s.add("a", "x".into());
        s.remove(&"x".into());
        assert!(!s.contains(&"x".into()));
    }

    #[test]
    fn merge_converges_under_partition() {
        let mut a: OrSet<String> = OrSet::new();
        let mut b: OrSet<String> = OrSet::new();
        a.add("a", "apple".into());
        a.add("a", "pear".into());
        b.add("b", "banana".into());
        b.add("b", "pear".into());
        a.merge(&b);
        b.merge(&a);
        assert_eq!(a, b);
        let mut m = a.members();
        m.sort();
        assert_eq!(m, vec!["apple".to_string(), "banana".into(), "pear".into()]);
    }

    #[test]
    fn merge_is_idempotent() {
        let mut s: OrSet<String> = OrSet::new();
        s.add("a", "x".into());
        let snap = s.clone();
        s.merge(&snap);
        assert_eq!(s, snap);
    }
}
