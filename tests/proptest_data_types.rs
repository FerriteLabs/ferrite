//! Property-based tests for Ferrite's Tier 1 data types
//!
//! Uses proptest to verify invariants across randomly generated inputs
//! for Strings, Lists, Hashes, Sets, and Sorted Sets.
#![allow(clippy::unwrap_used)]

use proptest::prelude::*;

use bytes::Bytes;
use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use ferrite::storage::{Store, Value};

// ============================================================================
// Test Strategies
// ============================================================================

/// Strategy for generating valid Redis keys
fn key_strategy() -> impl Strategy<Value = String> {
    "[a-z]{1,20}"
}

/// Strategy for generating string values
fn value_strategy() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 1..100)
}

/// Strategy for generating string values as Strings (for numeric operations)
fn string_value_strategy() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9]{1,50}"
}

// ============================================================================
// String Command Properties
// ============================================================================

proptest! {
    /// SET k v → GET k returns v (for any valid key/value)
    #[test]
    fn prop_set_get_roundtrip(key in key_strategy(), value in value_strategy()) {
        let store = Store::new(16);
        let k = Bytes::from(key);
        let v = Bytes::from(value.clone());

        store.set(0, k.clone(), Value::String(v.clone()));

        let result = store.get(0, &k);
        match result {
            Some(Value::String(got)) => prop_assert_eq!(got, v),
            other => prop_assert!(false, "Expected String value, got {:?}", other),
        }
    }

    /// INCR x N times results in value N
    #[test]
    fn prop_incr_is_monotonic(key in key_strategy(), n in 1u32..500) {
        let store = Store::new(16);
        let k = Bytes::from(key);

        for i in 1..=n {
            let current = match store.get(0, &k) {
                Some(Value::String(s)) => {
                    String::from_utf8_lossy(&s).parse::<i64>().unwrap_or(0)
                }
                _ => 0,
            };
            let new_val = current + 1;
            store.set(0, k.clone(), Value::String(Bytes::from(new_val.to_string())));

            let result = store.get(0, &k);
            match result {
                Some(Value::String(s)) => {
                    let val: i64 = String::from_utf8_lossy(&s).parse().unwrap();
                    prop_assert_eq!(val, i as i64);
                }
                other => prop_assert!(false, "Expected String value, got {:?}", other),
            }
        }
    }

    /// APPEND increases STRLEN by appended length
    #[test]
    fn prop_append_length(
        key in key_strategy(),
        initial in value_strategy(),
        suffix in value_strategy()
    ) {
        let store = Store::new(16);
        let k = Bytes::from(key);

        // Set initial value
        store.set(0, k.clone(), Value::String(Bytes::from(initial.clone())));
        let initial_len = initial.len();

        // Append suffix
        let current = match store.get(0, &k) {
            Some(Value::String(s)) => s.to_vec(),
            _ => vec![],
        };
        let mut new_value = current;
        new_value.extend_from_slice(&suffix);
        let expected_len = new_value.len();
        store.set(0, k.clone(), Value::String(Bytes::from(new_value)));

        // Verify length
        match store.get(0, &k) {
            Some(Value::String(s)) => {
                prop_assert_eq!(s.len(), expected_len);
                prop_assert_eq!(s.len(), initial_len + suffix.len());
            }
            other => prop_assert!(false, "Expected String value, got {:?}", other),
        }
    }

    /// GETRANGE never panics on any start/end combination
    #[test]
    fn prop_getrange_bounds(
        key in key_strategy(),
        value in value_strategy(),
        start in any::<i64>(),
        end in any::<i64>()
    ) {
        let store = Store::new(16);
        let k = Bytes::from(key);
        let v = Bytes::from(value.clone());

        store.set(0, k.clone(), Value::String(v));

        // Simulate GETRANGE logic — should never panic
        if let Some(Value::String(s)) = store.get(0, &k) {
            let len = s.len() as i64;
            let start_idx = if start < 0 { (len + start).max(0) } else { start };
            let end_idx = if end < 0 { (len + end).max(-1) } else { end };

            if start_idx <= end_idx && start_idx < len {
                let start_u = start_idx as usize;
                let end_u = ((end_idx + 1) as usize).min(s.len());
                if start_u < s.len() {
                    let _slice = &s[start_u..end_u];
                }
            }
            // No panic = success
        }
    }
}

// ============================================================================
// List Command Properties
// ============================================================================

proptest! {
    /// LPUSH items → RPOP returns in FIFO order
    #[test]
    fn prop_lpush_rpop_fifo(
        key in key_strategy(),
        values in prop::collection::vec(value_strategy(), 1..30)
    ) {
        let store = Store::new(16);
        let k = Bytes::from(key);

        // LPUSH each value one at a time (push_front)
        let mut list = VecDeque::new();
        for v in &values {
            list.push_front(Bytes::from(v.clone()));
        }
        store.set(0, k.clone(), Value::List(list));

        // RPOP should return values in FIFO (original insertion) order
        for v in &values {
            if let Some(Value::List(mut l)) = store.get(0, &k) {
                let popped = l.pop_back();
                prop_assert_eq!(popped.as_ref().map(|b| b.to_vec()), Some(v.clone()));
                if l.is_empty() {
                    store.del(0, &[k.clone()]);
                } else {
                    store.set(0, k.clone(), Value::List(l));
                }
            }
        }
    }

    /// RPUSH items → LPOP returns in FIFO order
    #[test]
    fn prop_rpush_lpop_fifo(
        key in key_strategy(),
        values in prop::collection::vec(value_strategy(), 1..30)
    ) {
        let store = Store::new(16);
        let k = Bytes::from(key);

        // RPUSH each value (push_back)
        let mut list = VecDeque::new();
        for v in &values {
            list.push_back(Bytes::from(v.clone()));
        }
        store.set(0, k.clone(), Value::List(list));

        // LPOP should return values in FIFO order
        for v in &values {
            if let Some(Value::List(mut l)) = store.get(0, &k) {
                let popped = l.pop_front();
                prop_assert_eq!(popped.as_ref().map(|b| b.to_vec()), Some(v.clone()));
                if l.is_empty() {
                    store.del(0, &[k.clone()]);
                } else {
                    store.set(0, k.clone(), Value::List(l));
                }
            }
        }
    }

    /// LLEN equals number of pushes minus pops
    #[test]
    fn prop_llen_tracks_size(
        key in key_strategy(),
        push_count in 1usize..50,
        pop_count in 0usize..25
    ) {
        let store = Store::new(16);
        let k = Bytes::from(key);

        // Push N items
        let mut list = VecDeque::new();
        for i in 0..push_count {
            list.push_back(Bytes::from(format!("item{}", i)));
        }
        store.set(0, k.clone(), Value::List(list));

        // Pop some items
        let actual_pops = pop_count.min(push_count);
        for _ in 0..actual_pops {
            if let Some(Value::List(mut l)) = store.get(0, &k) {
                l.pop_front();
                if l.is_empty() {
                    store.del(0, &[k.clone()]);
                } else {
                    store.set(0, k.clone(), Value::List(l));
                }
            }
        }

        // Verify length
        let expected_len = push_count - actual_pops;
        match store.get(0, &k) {
            Some(Value::List(l)) => prop_assert_eq!(l.len(), expected_len),
            None => prop_assert_eq!(expected_len, 0),
            other => prop_assert!(false, "Expected List or None, got {:?}", other),
        }
    }

    /// LRANGE 0 -1 returns all elements
    #[test]
    fn prop_lrange_full_equals_all(
        key in key_strategy(),
        values in prop::collection::vec(value_strategy(), 1..50)
    ) {
        let store = Store::new(16);
        let k = Bytes::from(key);

        let list: VecDeque<Bytes> = values.iter().map(|v| Bytes::from(v.clone())).collect();
        store.set(0, k.clone(), Value::List(list.clone()));

        // Simulate LRANGE 0 -1
        if let Some(Value::List(l)) = store.get(0, &k) {
            let len = l.len() as i64;
            let start_idx = 0usize;
            let stop_idx = ((len - 1 + 1).max(0)) as usize; // -1 in Redis = last element

            let range: Vec<Bytes> = l.iter().skip(start_idx).take(stop_idx).cloned().collect();
            prop_assert_eq!(range.len(), values.len());
            for (i, v) in values.iter().enumerate() {
                prop_assert_eq!(range[i].to_vec(), v.clone());
            }
        }
    }
}

// ============================================================================
// Hash Command Properties
// ============================================================================

proptest! {
    /// HSET k f v → HGET k f returns v
    #[test]
    fn prop_hset_hget_roundtrip(
        key in key_strategy(),
        field in key_strategy(),
        value in value_strategy()
    ) {
        let store = Store::new(16);
        let k = Bytes::from(key);
        let f = Bytes::from(field);
        let v = Bytes::from(value.clone());

        let mut hash = HashMap::new();
        hash.insert(f.clone(), v.clone());
        store.set(0, k.clone(), Value::Hash(hash));

        match store.get(0, &k) {
            Some(Value::Hash(h)) => {
                prop_assert_eq!(h.get(&f), Some(&v));
            }
            other => prop_assert!(false, "Expected Hash, got {:?}", other),
        }
    }

    /// HLEN equals number of distinct fields set
    #[test]
    fn prop_hlen_counts_fields(
        key in key_strategy(),
        fields in prop::collection::hash_map(key_strategy(), value_strategy(), 1..30)
    ) {
        let store = Store::new(16);
        let k = Bytes::from(key);

        let hash: HashMap<Bytes, Bytes> = fields
            .iter()
            .map(|(f, v)| (Bytes::from(f.clone()), Bytes::from(v.clone())))
            .collect();
        let expected_len = hash.len();
        store.set(0, k.clone(), Value::Hash(hash));

        match store.get(0, &k) {
            Some(Value::Hash(h)) => prop_assert_eq!(h.len(), expected_len),
            other => prop_assert!(false, "Expected Hash, got {:?}", other),
        }
    }

    /// HDEL then HEXISTS returns false
    #[test]
    fn prop_hdel_removes(
        key in key_strategy(),
        field in key_strategy(),
        value in value_strategy(),
        extra_field in key_strategy(),
        extra_value in value_strategy()
    ) {
        prop_assume!(field != extra_field);
        let store = Store::new(16);
        let k = Bytes::from(key);

        let mut hash = HashMap::new();
        hash.insert(Bytes::from(field.clone()), Bytes::from(value));
        hash.insert(Bytes::from(extra_field.clone()), Bytes::from(extra_value));
        store.set(0, k.clone(), Value::Hash(hash));

        // Remove the field
        if let Some(Value::Hash(mut h)) = store.get(0, &k) {
            h.remove(&Bytes::from(field.clone()));
            store.set(0, k.clone(), Value::Hash(h));
        }

        // Verify field is gone
        match store.get(0, &k) {
            Some(Value::Hash(h)) => {
                prop_assert!(!h.contains_key(&Bytes::from(field)));
                prop_assert!(h.contains_key(&Bytes::from(extra_field)));
            }
            other => prop_assert!(false, "Expected Hash, got {:?}", other),
        }
    }

    /// HGETALL returns all set fields
    #[test]
    fn prop_hgetall_contains_all(
        key in key_strategy(),
        fields in prop::collection::hash_map(key_strategy(), value_strategy(), 1..20)
    ) {
        let store = Store::new(16);
        let k = Bytes::from(key);

        let hash: HashMap<Bytes, Bytes> = fields
            .iter()
            .map(|(f, v)| (Bytes::from(f.clone()), Bytes::from(v.clone())))
            .collect();
        store.set(0, k.clone(), Value::Hash(hash.clone()));

        match store.get(0, &k) {
            Some(Value::Hash(h)) => {
                // All fields should be present with correct values
                for (field, value) in &hash {
                    prop_assert!(h.contains_key(field), "Missing field: {:?}", field);
                    prop_assert_eq!(h.get(field), Some(value));
                }
                prop_assert_eq!(h.len(), hash.len());
            }
            other => prop_assert!(false, "Expected Hash, got {:?}", other),
        }
    }
}

// ============================================================================
// Set Command Properties
// ============================================================================

proptest! {
    /// SADD x → SISMEMBER x returns true
    #[test]
    fn prop_sadd_sismember(
        key in key_strategy(),
        member in value_strategy()
    ) {
        let store = Store::new(16);
        let k = Bytes::from(key);
        let m = Bytes::from(member);

        let mut set = HashSet::new();
        set.insert(m.clone());
        store.set(0, k.clone(), Value::Set(set));

        match store.get(0, &k) {
            Some(Value::Set(s)) => prop_assert!(s.contains(&m)),
            other => prop_assert!(false, "Expected Set, got {:?}", other),
        }
    }

    /// SCARD equals number of unique members added
    #[test]
    fn prop_scard_counts_unique(
        key in key_strategy(),
        members in prop::collection::vec(string_value_strategy(), 1..50)
    ) {
        let store = Store::new(16);
        let k = Bytes::from(key);

        let set: HashSet<Bytes> = members.iter().map(|m| Bytes::from(m.clone())).collect();
        let expected_card = set.len();
        store.set(0, k.clone(), Value::Set(set));

        match store.get(0, &k) {
            Some(Value::Set(s)) => prop_assert_eq!(s.len(), expected_card),
            other => prop_assert!(false, "Expected Set, got {:?}", other),
        }
    }

    /// SREM then SISMEMBER returns false
    #[test]
    fn prop_srem_removes(
        key in key_strategy(),
        member in key_strategy(),
        extra in key_strategy()
    ) {
        prop_assume!(member != extra);
        let store = Store::new(16);
        let k = Bytes::from(key);
        let m = Bytes::from(member.clone());

        let mut set = HashSet::new();
        set.insert(m.clone());
        set.insert(Bytes::from(extra.clone()));
        store.set(0, k.clone(), Value::Set(set));

        // Remove member
        if let Some(Value::Set(mut s)) = store.get(0, &k) {
            s.remove(&m);
            store.set(0, k.clone(), Value::Set(s));
        }

        match store.get(0, &k) {
            Some(Value::Set(s)) => {
                prop_assert!(!s.contains(&m));
                prop_assert!(s.contains(&Bytes::from(extra)));
            }
            other => prop_assert!(false, "Expected Set, got {:?}", other),
        }
    }

    /// SUNION A B contains all members of A and B
    #[test]
    fn prop_sunion_contains_both(
        key_a in key_strategy(),
        key_b in key_strategy(),
        members_a in prop::collection::hash_set(string_value_strategy(), 1..20),
        members_b in prop::collection::hash_set(string_value_strategy(), 1..20)
    ) {
        let store = Store::new(16);
        let ka = Bytes::from(key_a);
        let kb = Bytes::from(key_b);

        let set_a: HashSet<Bytes> = members_a.iter().map(|m| Bytes::from(m.clone())).collect();
        let set_b: HashSet<Bytes> = members_b.iter().map(|m| Bytes::from(m.clone())).collect();

        store.set(0, ka.clone(), Value::Set(set_a.clone()));
        store.set(0, kb.clone(), Value::Set(set_b.clone()));

        // Compute union manually
        let union_set: HashSet<Bytes> = set_a.union(&set_b).cloned().collect();

        // Verify it contains all members from both sets
        let a = store.get(0, &ka);
        let b = store.get(0, &kb);

        let retrieved_a = match a {
            Some(Value::Set(s)) => s,
            _ => HashSet::new(),
        };
        let retrieved_b = match b {
            Some(Value::Set(s)) => s,
            _ => HashSet::new(),
        };

        let computed_union: HashSet<Bytes> = retrieved_a.union(&retrieved_b).cloned().collect();
        prop_assert_eq!(computed_union.len(), union_set.len());
        for member in &union_set {
            prop_assert!(computed_union.contains(member));
        }
    }
}

// ============================================================================
// Sorted Set Command Properties
// ============================================================================

proptest! {
    /// ZADD k score member → ZSCORE returns score
    #[test]
    fn prop_zadd_zscore(
        key in key_strategy(),
        member in key_strategy(),
        score in -1000.0f64..1000.0
    ) {
        let store = Store::new(16);
        let k = Bytes::from(key);
        let m = Bytes::from(member);

        let mut by_score = BTreeMap::new();
        let mut by_member = HashMap::new();
        by_score.insert((OrderedFloat(score), m.clone()), ());
        by_member.insert(m.clone(), score);

        store.set(0, k.clone(), Value::SortedSet { by_score, by_member });

        match store.get(0, &k) {
            Some(Value::SortedSet { by_member: bm, .. }) => {
                let got = bm.get(&m);
                prop_assert_eq!(got, Some(&score));
            }
            other => prop_assert!(false, "Expected SortedSet, got {:?}", other),
        }
    }

    /// ZCARD equals unique members
    #[test]
    fn prop_zcard_counts(
        key in key_strategy(),
        members in prop::collection::hash_map(key_strategy(), -1000.0f64..1000.0, 1..30)
    ) {
        let store = Store::new(16);
        let k = Bytes::from(key);

        let mut by_score = BTreeMap::new();
        let mut by_member = HashMap::new();
        for (member, score) in &members {
            let m = Bytes::from(member.clone());
            by_score.insert((OrderedFloat(*score), m.clone()), ());
            by_member.insert(m, *score);
        }

        let expected_card = by_member.len();
        store.set(0, k.clone(), Value::SortedSet { by_score, by_member });

        match store.get(0, &k) {
            Some(Value::SortedSet { by_member: bm, .. }) => {
                prop_assert_eq!(bm.len(), expected_card);
            }
            other => prop_assert!(false, "Expected SortedSet, got {:?}", other),
        }
    }

    /// ZRANK ordering matches score ordering
    #[test]
    fn prop_zrank_ordering(
        key in key_strategy(),
        members in prop::collection::hash_map(key_strategy(), -1000.0f64..1000.0, 2..20)
    ) {
        let store = Store::new(16);
        let k = Bytes::from(key);

        let mut by_score = BTreeMap::new();
        let mut by_member = HashMap::new();
        for (member, score) in &members {
            let m = Bytes::from(member.clone());
            by_score.insert((OrderedFloat(*score), m.clone()), ());
            by_member.insert(m, *score);
        }

        store.set(0, k.clone(), Value::SortedSet {
            by_score: by_score.clone(),
            by_member: by_member.clone(),
        });

        match store.get(0, &k) {
            Some(Value::SortedSet { by_score: bs, by_member: bm }) => {
                // Collect sorted entries
                let sorted_entries: Vec<_> = bs.keys().collect();

                // For any two members, if score(a) < score(b), rank(a) < rank(b)
                let entries: Vec<_> = bm.iter().collect();
                for i in 0..entries.len() {
                    for j in (i + 1)..entries.len() {
                        let (m1, s1) = entries[i];
                        let (m2, s2) = entries[j];

                        let rank1 = sorted_entries
                            .iter()
                            .position(|(os, m)| os.0 == *s1 && m == m1);
                        let rank2 = sorted_entries
                            .iter()
                            .position(|(os, m)| os.0 == *s2 && m == m2);

                        if let (Some(r1), Some(r2)) = (rank1, rank2) {
                            if s1 < s2 {
                                prop_assert!(r1 < r2, "rank({:?})={} should be < rank({:?})={}", m1, r1, m2, r2);
                            } else if s1 > s2 {
                                prop_assert!(r1 > r2, "rank({:?})={} should be > rank({:?})={}", m1, r1, m2, r2);
                            }
                        }
                    }
                }
            }
            other => prop_assert!(false, "Expected SortedSet, got {:?}", other),
        }
    }

    /// ZRANGEBYSCORE returns only items in range
    #[test]
    fn prop_zrangebyscore_bounds(
        key in key_strategy(),
        members in prop::collection::hash_map(key_strategy(), -1000.0f64..1000.0, 1..30),
        min_score in -500.0f64..500.0,
        range_width in 0.0f64..500.0
    ) {
        let store = Store::new(16);
        let k = Bytes::from(key);
        let max_score = min_score + range_width;

        let mut by_score = BTreeMap::new();
        let mut by_member = HashMap::new();
        for (member, score) in &members {
            let m = Bytes::from(member.clone());
            by_score.insert((OrderedFloat(*score), m.clone()), ());
            by_member.insert(m, *score);
        }

        store.set(0, k.clone(), Value::SortedSet {
            by_score: by_score.clone(),
            by_member,
        });

        // Simulate ZRANGEBYSCORE
        let range_start = (OrderedFloat(min_score), Bytes::new());
        let range_end = (OrderedFloat(max_score), Bytes::from(vec![0xff; 20]));

        let in_range: Vec<_> = by_score
            .range(range_start..=range_end)
            .map(|((score, member), _)| (score.0, member.clone()))
            .collect();

        // Verify all returned items are within range
        for (score, _member) in &in_range {
            prop_assert!(*score >= min_score, "Score {} below min {}", score, min_score);
            prop_assert!(*score <= max_score, "Score {} above max {}", score, max_score);
        }

        // Verify no items outside range are missing
        for ((score, member), _) in &by_score {
            let s = score.0;
            if s >= min_score && s <= max_score {
                prop_assert!(
                    in_range.iter().any(|(_, m)| m == member),
                    "Member {:?} with score {} should be in range [{}, {}]",
                    member, s, min_score, max_score
                );
            }
        }
    }
}
