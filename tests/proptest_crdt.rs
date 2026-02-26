//! Property-based tests for CRDT convergence and active-active conflict resolution
//!
//! Jepsen-style property tests verifying:
//! 1. Vector clock partial ordering is consistent
//! 2. CRDTs converge regardless of merge order (commutativity)
//! 3. Merging is idempotent (re-merging doesn't change state)
//! 4. Concurrent operations are resolved deterministically

use proptest::prelude::*;

// CRDT types from ferrite-plugins
use ferrite_plugins::crdt::{GCounter, HybridTimestamp, LwwRegister, OrSet, PNCounter, SiteId};

// ── Strategies ──────────────────────────────────────────────────────────

fn site_id_strategy() -> impl Strategy<Value = SiteId> {
    (1u64..=10).prop_map(SiteId::new)
}

fn delta_strategy() -> impl Strategy<Value = u64> {
    1u64..=1000
}

fn physical_ts_strategy() -> impl Strategy<Value = u64> {
    1u64..=1_000_000
}

// ── GCounter Properties ─────────────────────────────────────────────────

proptest! {
    /// GCounter merge is commutative: merge(a,b) == merge(b,a)
    #[test]
    fn prop_gcounter_merge_commutative(
        ops_a in prop::collection::vec((site_id_strategy(), delta_strategy()), 1..5),
        ops_b in prop::collection::vec((site_id_strategy(), delta_strategy()), 1..5),
    ) {
        let mut counter_a = GCounter::new();
        for (site, delta) in &ops_a {
            counter_a.increment(*site, *delta);
        }

        let mut counter_b = GCounter::new();
        for (site, delta) in &ops_b {
            counter_b.increment(*site, *delta);
        }

        let mut merged_ab = counter_a.clone();
        merged_ab.merge(&counter_b);

        let mut merged_ba = counter_b.clone();
        merged_ba.merge(&counter_a);

        prop_assert_eq!(merged_ab.value(), merged_ba.value(),
            "GCounter merge not commutative");
    }

    /// GCounter merge is idempotent: merge(a, a) == a
    #[test]
    fn prop_gcounter_merge_idempotent(
        ops in prop::collection::vec((site_id_strategy(), delta_strategy()), 1..10),
    ) {
        let mut counter = GCounter::new();
        for (site, delta) in &ops {
            counter.increment(*site, *delta);
        }

        let before_value = counter.value();
        let snapshot = counter.clone();
        counter.merge(&snapshot);

        prop_assert_eq!(counter.value(), before_value,
            "GCounter merge not idempotent");
    }

    /// GCounter value is monotonically increasing under increments
    #[test]
    fn prop_gcounter_monotonic(
        ops in prop::collection::vec((site_id_strategy(), delta_strategy()), 1..20),
    ) {
        let mut counter = GCounter::new();
        let mut prev_value = 0u64;

        for (site, delta) in &ops {
            counter.increment(*site, *delta);
            let current = counter.value();
            prop_assert!(current >= prev_value,
                "GCounter not monotonic: {} < {}", current, prev_value);
            prev_value = current;
        }
    }

    /// GCounter merge is associative: merge(merge(a,b), c) == merge(a, merge(b,c))
    #[test]
    fn prop_gcounter_merge_associative(
        ops_a in prop::collection::vec((site_id_strategy(), delta_strategy()), 1..4),
        ops_b in prop::collection::vec((site_id_strategy(), delta_strategy()), 1..4),
        ops_c in prop::collection::vec((site_id_strategy(), delta_strategy()), 1..4),
    ) {
        let mut a = GCounter::new();
        for (site, delta) in &ops_a { a.increment(*site, *delta); }

        let mut b = GCounter::new();
        for (site, delta) in &ops_b { b.increment(*site, *delta); }

        let mut c = GCounter::new();
        for (site, delta) in &ops_c { c.increment(*site, *delta); }

        // (a merge b) merge c
        let mut ab = a.clone();
        ab.merge(&b);
        let mut abc_left = ab;
        abc_left.merge(&c);

        // a merge (b merge c)
        let mut bc = b.clone();
        bc.merge(&c);
        let mut abc_right = a.clone();
        abc_right.merge(&bc);

        prop_assert_eq!(abc_left.value(), abc_right.value(),
            "GCounter merge not associative");
    }

    /// GCounter per-site count is preserved through merge
    #[test]
    fn prop_gcounter_per_site_preserved(
        site in site_id_strategy(),
        delta_a in delta_strategy(),
        delta_b in delta_strategy(),
    ) {
        let mut a = GCounter::new();
        a.increment(site, delta_a);

        let mut b = GCounter::new();
        b.increment(site, delta_b);

        a.merge(&b);

        // Merge takes the max for each site
        let expected_max = delta_a.max(delta_b);
        prop_assert_eq!(a.site_count(site), expected_max,
            "Site count after merge should be max of both");
    }
}

// ── PNCounter Properties ────────────────────────────────────────────────

proptest! {
    /// PNCounter merge is commutative
    #[test]
    fn prop_pncounter_merge_commutative(
        ops_a in prop::collection::vec((site_id_strategy(), delta_strategy(), prop::bool::ANY), 1..5),
        ops_b in prop::collection::vec((site_id_strategy(), delta_strategy(), prop::bool::ANY), 1..5),
    ) {
        let mut counter_a = PNCounter::new();
        for (site, delta, is_incr) in &ops_a {
            if *is_incr {
                counter_a.increment(*site, *delta);
            } else {
                counter_a.decrement(*site, *delta);
            }
        }

        let mut counter_b = PNCounter::new();
        for (site, delta, is_incr) in &ops_b {
            if *is_incr {
                counter_b.increment(*site, *delta);
            } else {
                counter_b.decrement(*site, *delta);
            }
        }

        let mut merged_ab = counter_a.clone();
        merged_ab.merge(&counter_b);

        let mut merged_ba = counter_b.clone();
        merged_ba.merge(&counter_a);

        prop_assert_eq!(merged_ab.value(), merged_ba.value(),
            "PNCounter merge not commutative");
    }

    /// PNCounter merge is idempotent
    #[test]
    fn prop_pncounter_merge_idempotent(
        ops in prop::collection::vec((site_id_strategy(), delta_strategy(), prop::bool::ANY), 1..10),
    ) {
        let mut counter = PNCounter::new();
        for (site, delta, is_incr) in &ops {
            if *is_incr {
                counter.increment(*site, *delta);
            } else {
                counter.decrement(*site, *delta);
            }
        }

        let before = counter.value();
        let snapshot = counter.clone();
        counter.merge(&snapshot);

        prop_assert_eq!(counter.value(), before,
            "PNCounter merge not idempotent");
    }

    /// PNCounter merge is associative
    #[test]
    fn prop_pncounter_merge_associative(
        ops_a in prop::collection::vec((site_id_strategy(), delta_strategy(), prop::bool::ANY), 1..4),
        ops_b in prop::collection::vec((site_id_strategy(), delta_strategy(), prop::bool::ANY), 1..4),
        ops_c in prop::collection::vec((site_id_strategy(), delta_strategy(), prop::bool::ANY), 1..4),
    ) {
        let mut a = PNCounter::new();
        for (s, d, i) in &ops_a { if *i { a.increment(*s, *d); } else { a.decrement(*s, *d); } }

        let mut b = PNCounter::new();
        for (s, d, i) in &ops_b { if *i { b.increment(*s, *d); } else { b.decrement(*s, *d); } }

        let mut c = PNCounter::new();
        for (s, d, i) in &ops_c { if *i { c.increment(*s, *d); } else { c.decrement(*s, *d); } }

        let mut ab = a.clone(); ab.merge(&b);
        let mut abc_left = ab; abc_left.merge(&c);

        let mut bc = b.clone(); bc.merge(&c);
        let mut abc_right = a.clone(); abc_right.merge(&bc);

        prop_assert_eq!(abc_left.value(), abc_right.value(),
            "PNCounter merge not associative");
    }

    /// PNCounter value equals positive minus negative
    #[test]
    fn prop_pncounter_value_equals_pos_minus_neg(
        ops in prop::collection::vec((site_id_strategy(), delta_strategy(), prop::bool::ANY), 1..10),
    ) {
        let mut counter = PNCounter::new();
        for (site, delta, is_incr) in &ops {
            if *is_incr {
                counter.increment(*site, *delta);
            } else {
                counter.decrement(*site, *delta);
            }
        }

        let expected = counter.positive_value() as i64 - counter.negative_value() as i64;
        prop_assert_eq!(counter.value(), expected,
            "PNCounter value should equal positive - negative");
    }
}

// ── LwwRegister Properties ──────────────────────────────────────────────

proptest! {
    /// LwwRegister merge is commutative — same result regardless of merge order
    #[test]
    fn prop_lww_register_merge_commutative(
        val_a in "[a-z]{1,10}",
        ts_a in physical_ts_strategy(),
        val_b in "[a-z]{1,10}",
        ts_b in physical_ts_strategy(),
    ) {
        let site_a = SiteId::new(1);
        let site_b = SiteId::new(2);

        let mut reg_a = LwwRegister::<String>::new();
        reg_a.set(val_a.clone(), HybridTimestamp::new(ts_a, 0, site_a));

        let mut reg_b = LwwRegister::<String>::new();
        reg_b.set(val_b.clone(), HybridTimestamp::new(ts_b, 0, site_b));

        let mut merged_ab = reg_a.clone();
        merged_ab.merge(&reg_b);

        let mut merged_ba = reg_b.clone();
        merged_ba.merge(&reg_a);

        prop_assert_eq!(merged_ab.value(), merged_ba.value(),
            "LwwRegister merge not commutative");
    }

    /// LwwRegister merge is idempotent
    #[test]
    fn prop_lww_register_merge_idempotent(
        val in "[a-z]{1,10}",
        ts in physical_ts_strategy(),
    ) {
        let site = SiteId::new(1);
        let mut reg = LwwRegister::<String>::new();
        reg.set(val, HybridTimestamp::new(ts, 0, site));

        let before = reg.value().cloned();
        let snapshot = reg.clone();
        reg.merge(&snapshot);

        prop_assert_eq!(reg.value().cloned(), before,
            "LwwRegister merge not idempotent");
    }

    /// LwwRegister always picks the later timestamp
    #[test]
    fn prop_lww_register_latest_wins(
        val_early in "[a-z]{1,10}",
        val_late in "[a-z]{1,10}",
        ts_early in 1u64..500_000,
        ts_offset in 1u64..500_000,
    ) {
        let site_a = SiteId::new(1);
        let site_b = SiteId::new(2);
        let ts_late = ts_early + ts_offset; // Guarantee ts_late > ts_early

        let mut reg_a = LwwRegister::<String>::new();
        reg_a.set(val_early, HybridTimestamp::new(ts_early, 0, site_a));

        let mut reg_b = LwwRegister::<String>::new();
        reg_b.set(val_late.clone(), HybridTimestamp::new(ts_late, 0, site_b));

        reg_a.merge(&reg_b);

        prop_assert_eq!(reg_a.value(), Some(&val_late),
            "LwwRegister should pick the later timestamp's value");
    }
}

// ── OrSet Properties ────────────────────────────────────────────────────

proptest! {
    /// OrSet merge is commutative
    #[test]
    fn prop_orset_merge_commutative(
        items_a in prop::collection::vec((1u64..100, site_id_strategy(), 1u64..100), 1..5),
        items_b in prop::collection::vec((1u64..100, site_id_strategy(), 1u64..100), 1..5),
    ) {
        let mut set_a = OrSet::<u64>::new();
        for (elem, site, counter) in &items_a {
            set_a.add(*elem, *site, *counter);
        }

        let mut set_b = OrSet::<u64>::new();
        for (elem, site, counter) in &items_b {
            set_b.add(*elem, *site, *counter);
        }

        let mut merged_ab = set_a.clone();
        merged_ab.merge(&set_b);

        let mut merged_ba = set_b.clone();
        merged_ba.merge(&set_a);

        // Both merges should contain the same elements
        let members_ab: std::collections::HashSet<u64> = merged_ab.members().copied().collect();
        let members_ba: std::collections::HashSet<u64> = merged_ba.members().copied().collect();

        prop_assert_eq!(members_ab, members_ba,
            "OrSet merge not commutative");
    }

    /// OrSet merge is idempotent
    #[test]
    fn prop_orset_merge_idempotent(
        items in prop::collection::vec((1u64..100, site_id_strategy(), 1u64..100), 1..10),
    ) {
        let mut set = OrSet::<u64>::new();
        for (elem, site, counter) in &items {
            set.add(*elem, *site, *counter);
        }

        let before: std::collections::HashSet<u64> = set.members().copied().collect();
        let snapshot = set.clone();
        set.merge(&snapshot);
        let after: std::collections::HashSet<u64> = set.members().copied().collect();

        prop_assert_eq!(after, before,
            "OrSet merge not idempotent");
    }

    /// OrSet add is preserved through merge
    #[test]
    fn prop_orset_add_preserved(
        elem in 1u64..100,
        site in site_id_strategy(),
        counter in 1u64..100,
    ) {
        let mut set_a = OrSet::<u64>::new();
        set_a.add(elem, site, counter);

        let set_b = OrSet::<u64>::new();
        set_a.merge(&set_b);

        prop_assert!(set_a.contains(&elem),
            "OrSet add should be preserved through merge with empty set");
    }
}

// ── Vector Clock Properties (enterprise) ────────────────────────────────

mod vector_clock_tests {
    use super::*;
    use ferrite_enterprise::active_active::{ClockOrdering, VectorClock};

    fn region_id_strategy() -> impl Strategy<Value = String> {
        prop::sample::select(vec![
            "us-east-1".to_string(),
            "us-west-2".to_string(),
            "eu-west-1".to_string(),
            "ap-southeast-1".to_string(),
            "sa-east-1".to_string(),
        ])
    }

    proptest! {
        /// Vector clock merge is commutative: merge(a,b) == merge(b,a)
        #[test]
        fn prop_vector_clock_merge_commutative(
            ops_a in prop::collection::vec(region_id_strategy(), 1..5),
            ops_b in prop::collection::vec(region_id_strategy(), 1..5),
        ) {
            let mut vc_a = VectorClock::new();
            for region in &ops_a {
                vc_a.increment(region);
            }

            let mut vc_b = VectorClock::new();
            for region in &ops_b {
                vc_b.increment(region);
            }

            let mut merged_ab = vc_a.clone();
            merged_ab.merge(&vc_b);

            let mut merged_ba = vc_b.clone();
            merged_ba.merge(&vc_a);

            let all_keys: std::collections::BTreeSet<&String> = merged_ab.to_map().keys()
                .chain(merged_ba.to_map().keys())
                .collect();

            for key in all_keys {
                prop_assert_eq!(merged_ab.get(key), merged_ba.get(key),
                    "Vector clock merge not commutative for key {}", key);
            }
        }

        /// Vector clock merge is idempotent: merge(a, a) == a
        #[test]
        fn prop_vector_clock_merge_idempotent(
            ops in prop::collection::vec(region_id_strategy(), 1..10),
        ) {
            let mut vc = VectorClock::new();
            for region in &ops {
                vc.increment(region);
            }

            let original = vc.clone();
            vc.merge(&original);

            for (key, &val) in vc.to_map() {
                prop_assert_eq!(val, original.get(key),
                    "Vector clock merge not idempotent for key {}", key);
            }
        }

        /// Vector clock compare is reflexive: compare(a, a) == Equal
        #[test]
        fn prop_vector_clock_partial_order_reflexive(
            ops in prop::collection::vec(region_id_strategy(), 1..5),
        ) {
            let mut vc = VectorClock::new();
            for region in &ops {
                vc.increment(region);
            }

            let order = vc.compare(&vc);
            prop_assert_eq!(order, ClockOrdering::Equal);
        }

        /// If vc_a dominates vc_b, then vc_b must NOT dominate vc_a (antisymmetry)
        #[test]
        fn prop_vector_clock_dominance_antisymmetric(
            shared_ops in prop::collection::vec(region_id_strategy(), 0..3),
            extra_region in region_id_strategy(),
            extra_count in 1u32..5,
        ) {
            let mut vc_base = VectorClock::new();
            for region in &shared_ops {
                vc_base.increment(region);
            }

            let mut vc_extended = vc_base.clone();
            for _ in 0..extra_count {
                vc_extended.increment(&extra_region);
            }

            let extended_dominates = vc_extended.dominates(&vc_base);
            let base_dominates = vc_base.dominates(&vc_extended);

            if extended_dominates {
                prop_assert!(!base_dominates,
                    "Antisymmetry violated: both clocks dominate each other");
            }
        }

        /// Merged clock dominates (or equals) both inputs
        #[test]
        fn prop_vector_clock_merge_dominates_inputs(
            ops_a in prop::collection::vec(region_id_strategy(), 1..5),
            ops_b in prop::collection::vec(region_id_strategy(), 1..5),
        ) {
            let mut vc_a = VectorClock::new();
            for region in &ops_a { vc_a.increment(region); }

            let mut vc_b = VectorClock::new();
            for region in &ops_b { vc_b.increment(region); }

            let mut merged = vc_a.clone();
            merged.merge(&vc_b);

            // Merged clock should dominate or equal each input
            let cmp_a = merged.compare(&vc_a);
            prop_assert!(cmp_a == ClockOrdering::After || cmp_a == ClockOrdering::Equal,
                "Merged clock should dominate or equal input A, got {:?}", cmp_a);

            let cmp_b = merged.compare(&vc_b);
            prop_assert!(cmp_b == ClockOrdering::After || cmp_b == ClockOrdering::Equal,
                "Merged clock should dominate or equal input B, got {:?}", cmp_b);
        }

        /// Concurrent clocks: if a has unique regions and b has unique regions, they're concurrent
        #[test]
        fn prop_vector_clock_disjoint_are_concurrent(
            count_a in 1u32..4,
            count_b in 1u32..4,
        ) {
            let mut vc_a = VectorClock::new();
            for _ in 0..count_a { vc_a.increment("region-a-only"); }

            let mut vc_b = VectorClock::new();
            for _ in 0..count_b { vc_b.increment("region-b-only"); }

            prop_assert!(vc_a.is_concurrent(&vc_b),
                "Clocks with disjoint regions should be concurrent");
        }

        /// Vector clock merge is associative
        #[test]
        fn prop_vector_clock_merge_associative(
            ops_a in prop::collection::vec(region_id_strategy(), 1..4),
            ops_b in prop::collection::vec(region_id_strategy(), 1..4),
            ops_c in prop::collection::vec(region_id_strategy(), 1..4),
        ) {
            let mut a = VectorClock::new();
            for r in &ops_a { a.increment(r); }

            let mut b = VectorClock::new();
            for r in &ops_b { b.increment(r); }

            let mut c = VectorClock::new();
            for r in &ops_c { c.increment(r); }

            // (a merge b) merge c
            let mut ab = a.clone(); ab.merge(&b);
            let mut abc_left = ab; abc_left.merge(&c);

            // a merge (b merge c)
            let mut bc = b.clone(); bc.merge(&c);
            let mut abc_right = a.clone(); abc_right.merge(&bc);

            let all_keys: std::collections::BTreeSet<&String> = abc_left.to_map().keys()
                .chain(abc_right.to_map().keys())
                .collect();

            for key in all_keys {
                prop_assert_eq!(abc_left.get(key), abc_right.get(key),
                    "Vector clock merge not associative for key {}", key);
            }
        }
    }
}

// ── Multi-Replica Convergence Simulation ────────────────────────────────

proptest! {
    /// Simulates N replicas applying operations and merging in random order.
    /// All replicas must converge to the same GCounter value.
    #[test]
    fn prop_gcounter_multi_replica_convergence(
        num_replicas in 2usize..=5,
        ops_per_replica in prop::collection::vec(
            prop::collection::vec((site_id_strategy(), delta_strategy()), 1..4),
            2..=5,
        ),
    ) {
        let num = num_replicas.min(ops_per_replica.len());

        // Each replica applies its own operations
        let mut replicas: Vec<GCounter> = Vec::new();
        for ops in ops_per_replica.iter().take(num) {
            let mut counter = GCounter::new();
            for (site, delta) in ops {
                counter.increment(*site, *delta);
            }
            replicas.push(counter);
        }

        // Merge all replicas into each other (full mesh sync)
        let snapshots: Vec<GCounter> = replicas.clone();
        for replica in &mut replicas {
            for snapshot in &snapshots {
                replica.merge(snapshot);
            }
        }

        // All replicas should have converged to the same value
        let first_value = replicas[0].value();
        for (i, replica) in replicas.iter().enumerate().skip(1) {
            prop_assert_eq!(replica.value(), first_value,
                "Replica {} diverged: {} != {}", i, replica.value(), first_value);
        }
    }

    /// PNCounter multi-replica convergence
    #[test]
    fn prop_pncounter_multi_replica_convergence(
        ops_per_replica in prop::collection::vec(
            prop::collection::vec((site_id_strategy(), delta_strategy(), prop::bool::ANY), 1..4),
            2..=4,
        ),
    ) {
        let mut replicas: Vec<PNCounter> = Vec::new();
        for ops in &ops_per_replica {
            let mut counter = PNCounter::new();
            for (site, delta, is_incr) in ops {
                if *is_incr {
                    counter.increment(*site, *delta);
                } else {
                    counter.decrement(*site, *delta);
                }
            }
            replicas.push(counter);
        }

        let snapshots: Vec<PNCounter> = replicas.clone();
        for replica in &mut replicas {
            for snapshot in &snapshots {
                replica.merge(snapshot);
            }
        }

        let first_value = replicas[0].value();
        for (i, replica) in replicas.iter().enumerate().skip(1) {
            prop_assert_eq!(replica.value(), first_value,
                "PNCounter replica {} diverged", i);
        }
    }
}
