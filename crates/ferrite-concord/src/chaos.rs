//! Chaos testing utilities for CRDT convergence under adverse conditions.

use crate::Crdt;

/// Simulate partition + heal for a set of CRDT replicas.
///
/// Applies the given operations to individual replicas (simulating a network
/// partition), then merges all pairs (simulating partition heal). Returns
/// `true` if all replicas converged to the same state.
/// Type alias for partition operations: each entry is `(replica_index, operation)`.
pub type PartitionOps<C> = [(usize, Box<dyn Fn(&mut C)>)];

pub fn simulate_partition_heal<C: Crdt + Clone + PartialEq>(
    replicas: &mut [C],
    ops_during_partition: &PartitionOps<C>,
) -> bool {
    // Apply ops to individual replicas (simulating partition)
    for (replica_idx, op) in ops_during_partition {
        if *replica_idx < replicas.len() {
            op(&mut replicas[*replica_idx]);
        }
    }
    // Heal: merge all pairs
    let n = replicas.len();
    for i in 0..n {
        for j in 0..n {
            if i != j {
                let other = replicas[j].clone();
                replicas[i].merge(&other);
            }
        }
    }
    // Check convergence: all replicas equal
    replicas.windows(2).all(|w| w[0] == w[1])
}

/// Simulate message duplication (idempotence test).
///
/// Merges `delta` into `base` multiple times. The result should be
/// identical regardless of how many times the merge is applied.
pub fn simulate_duplicates<C: Crdt + Clone>(base: &C, delta: &C, duplications: usize) -> C {
    let mut result = base.clone();
    for _ in 0..duplications {
        result.merge(delta);
    }
    result
}

/// Simulate message reordering.
///
/// Returns `true` if applying deltas in forward order and reverse order
/// produces the same result (commutativity check).
pub fn simulate_reorder<C: Crdt + Clone + PartialEq>(base: &C, deltas: &[C]) -> bool {
    // Apply in order
    let mut forward = base.clone();
    for d in deltas {
        forward.merge(d);
    }
    // Apply in reverse
    let mut reverse = base.clone();
    for d in deltas.iter().rev() {
        reverse.merge(d);
    }
    // Must converge
    forward == reverse
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{GCounter, OrSet, PnCounter};

    #[test]
    fn gcounter_converges_after_partition_heal() {
        let mut replicas = vec![GCounter::new(), GCounter::new(), GCounter::new()];
        let ops: Vec<(usize, Box<dyn Fn(&mut GCounter)>)> = vec![
            (0, Box::new(|c: &mut GCounter| c.increment("r0", 5))),
            (1, Box::new(|c: &mut GCounter| c.increment("r1", 3))),
            (2, Box::new(|c: &mut GCounter| c.increment("r2", 7))),
        ];
        assert!(simulate_partition_heal(&mut replicas, &ops));
        assert_eq!(replicas[0].value(), 15);
    }

    #[test]
    fn pncounter_converges_after_partition_heal() {
        let mut replicas = vec![PnCounter::new(), PnCounter::new()];
        let ops: Vec<(usize, Box<dyn Fn(&mut PnCounter)>)> = vec![
            (0, Box::new(|c: &mut PnCounter| c.increment("r0", 10))),
            (1, Box::new(|c: &mut PnCounter| c.decrement("r1", 3))),
        ];
        assert!(simulate_partition_heal(&mut replicas, &ops));
        assert_eq!(replicas[0].value(), 7);
    }

    #[test]
    fn orset_converges_with_add_wins() {
        let mut replicas: Vec<OrSet<String>> = vec![OrSet::new(), OrSet::new()];
        // Replica 0 adds "x", replica 1 also adds "x" independently
        let ops: Vec<(usize, Box<dyn Fn(&mut OrSet<String>)>)> = vec![
            (
                0,
                Box::new(|s: &mut OrSet<String>| {
                    s.add("r0", "apple".to_string());
                    s.add("r0", "banana".to_string());
                }),
            ),
            (
                1,
                Box::new(|s: &mut OrSet<String>| {
                    s.add("r1", "banana".to_string());
                    s.add("r1", "cherry".to_string());
                }),
            ),
        ];
        assert!(simulate_partition_heal(&mut replicas, &ops));
        let mut members = replicas[0].members();
        members.sort();
        assert_eq!(
            members,
            vec![
                "apple".to_string(),
                "banana".to_string(),
                "cherry".to_string()
            ]
        );
    }

    #[test]
    fn duplicate_application_is_idempotent() {
        let mut base = GCounter::new();
        base.increment("a", 5);
        let mut delta = GCounter::new();
        delta.increment("b", 3);

        let once = simulate_duplicates(&base, &delta, 1);
        let many = simulate_duplicates(&base, &delta, 100);
        assert_eq!(once, many);
        assert_eq!(once.value(), 8);
    }

    #[test]
    fn reordered_messages_converge() {
        let base = GCounter::new();
        let mut d1 = GCounter::new();
        d1.increment("a", 5);
        let mut d2 = GCounter::new();
        d2.increment("b", 3);
        let mut d3 = GCounter::new();
        d3.increment("c", 7);

        assert!(simulate_reorder(&base, &[d1, d2, d3]));
    }
}
