//! Multi-Value Register: keeps every concurrent write that wasn't
//! happens-before some other write.  Each entry carries a vector clock
//! over replica ids; on merge, dominated entries are dropped.

use crate::{Crdt, Delta};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type VectorClock = BTreeMap<String, u64>;

fn dominates(a: &VectorClock, b: &VectorClock) -> bool {
    let mut strictly_greater = false;
    for (k, v_b) in b {
        let v_a = a.get(k).copied().unwrap_or(0);
        if v_a < *v_b {
            return false;
        }
        if v_a > *v_b {
            strictly_greater = true;
        }
    }
    for (k, v_a) in a {
        if !b.contains_key(k) && *v_a > 0 {
            strictly_greater = true;
        }
    }
    strictly_greater
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Entry<T: Clone + std::fmt::Debug + PartialEq + Eq> {
    pub value: T,
    pub clock: VectorClock,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MvRegister<T: Clone + std::fmt::Debug + PartialEq + Eq> {
    entries: Vec<Entry<T>>,
}

impl<T: Clone + std::fmt::Debug + PartialEq + Eq> Default for MvRegister<T> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl<T: Clone + std::fmt::Debug + PartialEq + Eq> MvRegister<T> {
    pub fn new() -> Self {
        Self::default()
    }

    /// All current concurrent values.
    pub fn values(&self) -> Vec<&T> {
        self.entries.iter().map(|e| &e.value).collect()
    }

    fn add_entry(&mut self, e: Entry<T>) {
        // Drop entries dominated by `e`.
        self.entries
            .retain(|existing| !dominates(&e.clock, &existing.clock));
        // Skip if `e` is dominated by any existing entry.
        if self
            .entries
            .iter()
            .any(|existing| dominates(&existing.clock, &e.clock))
        {
            return;
        }
        // Skip exact duplicate.
        if self.entries.iter().any(|existing| existing == &e) {
            return;
        }
        self.entries.push(e);
    }
}

impl<T: Clone + std::fmt::Debug + PartialEq + Eq> Crdt for MvRegister<T> {
    fn merge(&mut self, other: &Self) {
        for e in &other.entries {
            self.add_entry(e.clone());
        }
    }
}

pub struct MvWrite<T> {
    pub value: T,
    pub replica: String,
    /// The vector clock observed by the writer at write time (excluding
    /// the writer's own freshly-incremented slot).
    pub observed: VectorClock,
}

impl<T: Clone + std::fmt::Debug + PartialEq + Eq> Delta for MvRegister<T> {
    type Mutation = MvWrite<T>;
    type DeltaState = Entry<T>;

    fn mutate(&mut self, m: Self::Mutation) -> Self::DeltaState {
        let mut clock = m.observed;
        let counter = clock.entry(m.replica).or_insert(0);
        *counter += 1;
        let entry = Entry {
            value: m.value,
            clock,
        };
        self.add_entry(entry.clone());
        entry
    }

    fn merge_delta(&mut self, delta: &Self::DeltaState) {
        self.add_entry(delta.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vc(pairs: &[(&str, u64)]) -> VectorClock {
        pairs.iter().map(|(k, v)| ((*k).to_string(), *v)).collect()
    }

    #[test]
    fn dominance_is_correct() {
        assert!(dominates(&vc(&[("A", 2)]), &vc(&[("A", 1)])));
        assert!(!dominates(&vc(&[("A", 1)]), &vc(&[("A", 2)])));
        assert!(!dominates(
            &vc(&[("A", 1), ("B", 1)]),
            &vc(&[("A", 2), ("B", 0)])
        ));
        assert!(dominates(&vc(&[("A", 1), ("B", 1)]), &vc(&[("A", 1)])));
    }

    #[test]
    fn sequential_writes_keep_only_latest() {
        let mut r = MvRegister::<u32>::new();
        r.mutate(MvWrite {
            value: 1,
            replica: "A".into(),
            observed: vc(&[]),
        });
        r.mutate(MvWrite {
            value: 2,
            replica: "A".into(),
            observed: vc(&[("A", 1)]),
        });
        assert_eq!(r.values(), vec![&2]);
    }

    #[test]
    fn concurrent_writes_keep_both() {
        let mut a = MvRegister::<u32>::new();
        let mut b = MvRegister::<u32>::new();
        a.mutate(MvWrite {
            value: 1,
            replica: "A".into(),
            observed: vc(&[]),
        });
        b.mutate(MvWrite {
            value: 2,
            replica: "B".into(),
            observed: vc(&[]),
        });
        a.merge(&b);
        let mut vals: Vec<&u32> = a.values();
        vals.sort();
        assert_eq!(vals, vec![&1, &2]);
    }

    #[test]
    fn observed_write_supersedes_concurrent_one() {
        let mut a = MvRegister::<u32>::new();
        a.mutate(MvWrite {
            value: 1,
            replica: "A".into(),
            observed: vc(&[]),
        });
        // B observes A=1 then writes — its clock dominates A's.
        let delta_b = {
            let mut b = a.clone();
            b.mutate(MvWrite {
                value: 9,
                replica: "B".into(),
                observed: vc(&[("A", 1)]),
            })
        };
        a.merge_delta(&delta_b);
        assert_eq!(a.values(), vec![&9]);
    }

    #[test]
    fn merge_is_idempotent() {
        let mut r = MvRegister::<u32>::new();
        let delta = r.mutate(MvWrite {
            value: 1,
            replica: "A".into(),
            observed: vc(&[]),
        });
        r.merge_delta(&delta);
        r.merge_delta(&delta);
        assert_eq!(r.values(), vec![&1]);
    }
}
