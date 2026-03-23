//! Last-Writer-Wins Register.  Concurrent writes are resolved by the
//! larger `(timestamp, replica_id)` pair (timestamps are caller-supplied
//! Lamport clocks; ties broken lexicographically by replica id).

use crate::{Crdt, Delta};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LwwRegister<T: Clone + std::fmt::Debug> {
    value: Option<T>,
    ts: u64,
    replica: String,
}

impl<T: Clone + std::fmt::Debug> Default for LwwRegister<T> {
    fn default() -> Self {
        Self {
            value: None,
            ts: 0,
            replica: String::new(),
        }
    }
}

impl<T: Clone + std::fmt::Debug> LwwRegister<T> {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn value(&self) -> Option<&T> {
        self.value.as_ref()
    }
    pub fn timestamp(&self) -> u64 {
        self.ts
    }
}

impl<T: Clone + std::fmt::Debug + PartialEq> Crdt for LwwRegister<T> {
    fn merge(&mut self, other: &Self) {
        if (other.ts, other.replica.as_str()) > (self.ts, self.replica.as_str()) {
            *self = other.clone();
        }
    }
}

pub struct LwwWrite<T> {
    pub value: T,
    pub ts: u64,
    pub replica: String,
}

impl<T: Clone + std::fmt::Debug + PartialEq> Delta for LwwRegister<T> {
    type Mutation = LwwWrite<T>;
    type DeltaState = Self;

    fn mutate(&mut self, m: Self::Mutation) -> Self::DeltaState {
        let candidate = Self {
            value: Some(m.value),
            ts: m.ts,
            replica: m.replica,
        };
        self.merge(&candidate);
        candidate
    }

    fn merge_delta(&mut self, delta: &Self::DeltaState) {
        self.merge(delta);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(value: u32, ts: u64, replica: &str) -> LwwRegister<u32> {
        let mut r = LwwRegister::new();
        r.mutate(LwwWrite {
            value,
            ts,
            replica: replica.into(),
        });
        r
    }

    #[test]
    fn newer_timestamp_wins() {
        let mut a = write(1, 1, "A");
        let b = write(2, 2, "A");
        a.merge(&b);
        assert_eq!(a.value(), Some(&2));
    }

    #[test]
    fn older_timestamp_loses() {
        let mut a = write(2, 5, "A");
        let b = write(1, 1, "A");
        a.merge(&b);
        assert_eq!(a.value(), Some(&2));
    }

    #[test]
    fn replica_id_breaks_timestamp_ties() {
        let mut a = write(1, 5, "A");
        let b = write(2, 5, "B");
        a.merge(&b);
        // Higher replica id wins on tie.
        assert_eq!(a.value(), Some(&2));
    }

    #[test]
    fn merge_delta_equivalent_to_merge() {
        let mut a = write(1, 1, "A");
        let mut b_full = write(2, 2, "B");
        let delta = b_full.mutate(LwwWrite {
            value: 3,
            ts: 5,
            replica: "B".into(),
        });
        a.merge_delta(&delta);
        assert_eq!(a.value(), Some(&3));
    }

    #[test]
    fn convergence_under_reordering() {
        let d1 = {
            let mut r = LwwRegister::<u32>::new();
            r.mutate(LwwWrite {
                value: 1,
                ts: 1,
                replica: "A".into(),
            })
        };
        let d2 = {
            let mut r = LwwRegister::<u32>::new();
            r.mutate(LwwWrite {
                value: 2,
                ts: 2,
                replica: "B".into(),
            })
        };
        let d3 = {
            let mut r = LwwRegister::<u32>::new();
            r.mutate(LwwWrite {
                value: 3,
                ts: 3,
                replica: "C".into(),
            })
        };
        let mut order_a = LwwRegister::<u32>::new();
        order_a.merge_delta(&d1);
        order_a.merge_delta(&d2);
        order_a.merge_delta(&d3);
        let mut order_b = LwwRegister::<u32>::new();
        order_b.merge_delta(&d3);
        order_b.merge_delta(&d1);
        order_b.merge_delta(&d2);
        assert_eq!(order_a.value(), order_b.value());
    }
}
