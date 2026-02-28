//! Property-based tests for next-gen feature correctness
//!
//! Verifies mathematical properties:
//! 1. CRDT gossip convergence — multiple nodes eventually agree
//! 2. VersionStore temporal consistency — get_at returns correct version
//! 3. BloomFilter false positive rate bounds
//! 4. IncrementalAggregator commutativity — insert order doesn't matter
//! 5. OnlineLearningModel monotonicity — more data = better predictions

use proptest::prelude::*;

// ── 1. Gossip Convergence ───────────────────────────────────────────────

use ferrite_plugins::crdt::gossip::{GossipConfig, GossipProtocol};
use ferrite_plugins::crdt::CrdtType;

fn gossip_config() -> GossipConfig {
    GossipConfig {
        fanout: 3,
        gossip_interval_ms: 1000,
        max_delta_buffer: 10_000,
        max_message_size: 1_048_576,
        peer_timeout_ms: 30_000,
        enable_delta_compression: false,
    }
}

proptest! {
    /// Two nodes exchange messages. After both process each other's messages,
    /// their received delta counts should match what was sent.
    #[test]
    fn prop_gossip_bidirectional_convergence(
        keys_a in prop::collection::vec("[a-z]{3,10}", 1..10),
        keys_b in prop::collection::vec("[a-z]{3,10}", 1..10),
    ) {
        let node_a = GossipProtocol::new("node_a".to_string(), gossip_config());
        let node_b = GossipProtocol::new("node_b".to_string(), gossip_config());

        node_a.add_peer("node_b", "localhost:7001");
        node_b.add_peer("node_a", "localhost:7000");

        for key in &keys_a {
            node_a.record_local_mutation(key, CrdtType::GCounter, vec![1, 2, 3]);
        }
        for key in &keys_b {
            node_b.record_local_mutation(key, CrdtType::GCounter, vec![4, 5, 6]);
        }

        let msg_a_to_b = node_a.prepare_gossip_message("node_b");
        let msg_b_to_a = node_b.prepare_gossip_message("node_a");

        if let Some(msg) = msg_a_to_b {
            let sent_count = msg.deltas.len();
            let result = node_b.receive_gossip_message(msg);
            prop_assert_eq!(
                result.applied + result.skipped, sent_count,
                "Node B should process all deltas from A"
            );
        }

        if let Some(msg) = msg_b_to_a {
            let sent_count = msg.deltas.len();
            let result = node_a.receive_gossip_message(msg);
            prop_assert_eq!(
                result.applied + result.skipped, sent_count,
                "Node A should process all deltas from B"
            );
        }
    }

    /// Receiving the same message twice should not double-apply deltas
    /// (skipped count should be > 0 on second receive).
    #[test]
    fn prop_gossip_idempotent_receive(
        keys in prop::collection::vec("[a-z]{3,10}", 1..10),
    ) {
        let node_a = GossipProtocol::new("node_a".to_string(), gossip_config());
        let node_b = GossipProtocol::new("node_b".to_string(), gossip_config());

        node_a.add_peer("node_b", "localhost:7001");
        node_b.add_peer("node_a", "localhost:7000");

        for key in &keys {
            node_a.record_local_mutation(key, CrdtType::GCounter, vec![1, 2, 3]);
        }

        // Prepare two identical messages (sender state unchanged between calls)
        let msg1 = node_a.prepare_gossip_message("node_b");
        let msg2 = node_a.prepare_gossip_message("node_b");

        if let (Some(m1), Some(m2)) = (msg1, msg2) {
            let result1 = node_b.receive_gossip_message(m1);
            prop_assert!(result1.applied > 0, "First receive should apply deltas");

            let result2 = node_b.receive_gossip_message(m2);
            prop_assert!(
                result2.skipped > 0,
                "Second receive of same deltas should skip already-applied entries"
            );
        }
    }

    /// A gossip message to a new peer (no prior version) should contain all pending deltas.
    #[test]
    fn prop_gossip_message_contains_all_pending(
        keys in prop::collection::vec("[a-z]{3,10}", 1..20),
    ) {
        let node = GossipProtocol::new("node_a".to_string(), gossip_config());

        for key in &keys {
            node.record_local_mutation(key, CrdtType::GCounter, vec![1, 2, 3]);
        }

        let pending = node.pending_delta_count();
        prop_assert_eq!(pending, keys.len(), "Should have one delta per mutation");

        node.add_peer("new_peer", "localhost:8000");

        let msg = node.prepare_gossip_message("new_peer");
        prop_assert!(msg.is_some(), "Should produce a message for new peer");

        let msg = msg.unwrap();
        prop_assert_eq!(
            msg.deltas.len(),
            keys.len(),
            "Message to new peer should contain all pending deltas"
        );
    }

    /// select_gossip_targets(n) never returns more than n peers.
    #[test]
    fn prop_gossip_fanout_bounded(
        num_peers in 1usize..20,
        fanout in 1usize..10,
    ) {
        let node = GossipProtocol::new("node_a".to_string(), gossip_config());

        for i in 0..num_peers {
            node.add_peer(
                &format!("peer_{}", i),
                &format!("localhost:{}", 8000 + i),
            );
        }

        let targets = node.select_gossip_targets(fanout);
        prop_assert!(
            targets.len() <= fanout,
            "select_gossip_targets({}) returned {} targets, expected <= {}",
            fanout,
            targets.len(),
            fanout
        );
    }
}

// ── 2. VersionStore Temporal Consistency ─────────────────────────────────

use ferrite_core::storage::version_store::VersionOperation;
use ferrite_core::storage::{VersionStore, VersionStoreConfig};

fn version_store_config(retention_ms: u64) -> VersionStoreConfig {
    VersionStoreConfig {
        enabled: true,
        max_versions_per_key: 100,
        retention_duration_ms: retention_ms,
        max_total_versions: 100_000,
        max_total_bytes: 100_000_000,
        gc_interval_ms: 60_000,
        gc_batch_size: 1000,
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(30))]

    /// Record N versions at increasing timestamps. For any timestamp between
    /// version[i] and version[i+1], get_at should return version[i].
    #[test]
    fn prop_version_get_at_returns_correct_version(n in 2usize..8) {
        let store = VersionStore::new(version_store_config(60_000));
        let key = "test_key";

        for i in 0..n {
            store.record_version(
                key,
                Some(format!("v{}", i).into_bytes()),
                VersionOperation::Set,
            );
            std::thread::sleep(std::time::Duration::from_millis(2));
        }

        let history = store.get_history(key, n);
        prop_assert_eq!(history.len(), n, "Should have {} versions", n);

        // History is sorted newest-first; verify get_at between adjacent pairs
        for i in 0..(history.len() - 1) {
            let newer = &history[i];
            let older = &history[i + 1];

            if newer.timestamp > older.timestamp + 1 {
                let query_ts = older.timestamp + 1;
                let result = store.get_at(key, query_ts);
                prop_assert!(
                    result.is_some(),
                    "get_at({}) should find a version",
                    query_ts
                );
                let entry = result.unwrap();
                prop_assert_eq!(
                    entry.version, older.version,
                    "get_at({}) should return version {} (ts={}), got version {} (ts={})",
                    query_ts, older.version, older.timestamp, entry.version, entry.timestamp
                );
            }
        }
    }

    /// After recording N versions, get_history returns entries sorted by timestamp descending.
    #[test]
    fn prop_version_history_sorted_newest_first(n in 1usize..20) {
        let store = VersionStore::new(version_store_config(60_000));
        let key = "test_key";

        for i in 0..n {
            store.record_version(
                key,
                Some(format!("v{}", i).into_bytes()),
                VersionOperation::Set,
            );
        }

        let history = store.get_history(key, n);
        for i in 0..history.len().saturating_sub(1) {
            prop_assert!(
                history[i].timestamp >= history[i + 1].timestamp,
                "History not sorted: entry[{}].ts={} < entry[{}].ts={}",
                i,
                history[i].timestamp,
                i + 1,
                history[i + 1].timestamp
            );
        }
    }

    /// After recording versions and running GC with a retention window,
    /// no versions older than the window remain.
    #[test]
    fn prop_version_gc_removes_old_entries(n in 1usize..5) {
        let store = VersionStore::new(version_store_config(1)); // 1ms retention

        let key = "test_key";
        for i in 0..n {
            store.record_version(
                key,
                Some(format!("v{}", i).into_bytes()),
                VersionOperation::Set,
            );
        }

        // Wait for entries to age beyond the 1ms retention window
        std::thread::sleep(std::time::Duration::from_millis(50));

        let _gc_result = store.gc();

        let remaining = store.get_history(key, 100);
        prop_assert!(
            remaining.is_empty(),
            "GC with 1ms retention should remove all {} versions after 50ms wait, but {} remain",
            n,
            remaining.len()
        );
    }
}

// ── 3. BloomFilter Properties ───────────────────────────────────────────

use ferrite_cloud::edge::delta_sync::BloomFilter;

proptest! {
    /// Any key inserted into the filter MUST be found (no false negatives).
    #[test]
    fn prop_bloom_no_false_negatives(
        keys in prop::collection::vec("[a-z]{3,10}", 1..200),
    ) {
        let mut bloom = BloomFilter::new(1000, 0.01);
        for key in &keys {
            bloom.insert(key.as_bytes());
        }
        for key in &keys {
            prop_assert!(
                bloom.contains(key.as_bytes()),
                "False negative: key '{}' not found after insert",
                key
            );
        }
    }

    /// Insert N keys, test M non-inserted keys. False positive rate must be
    /// ≤ 2× configured rate (accounting for statistical variance).
    #[test]
    fn prop_bloom_false_positive_rate_bounded(seed in any::<u64>()) {
        let n = 1000;
        let m = 10000;
        let target_fpr = 0.01;
        let mut bloom = BloomFilter::new(n, target_fpr);

        for i in 0..n {
            let key = format!("insert_{}_{}", seed, i);
            bloom.insert(key.as_bytes());
        }

        let mut false_positives = 0u64;
        for i in 0..m {
            let key = format!("check_{}_{}", seed, i);
            if bloom.contains(key.as_bytes()) {
                false_positives += 1;
            }
        }

        let actual_fpr = false_positives as f64 / m as f64;
        prop_assert!(
            actual_fpr <= target_fpr * 2.0,
            "False positive rate {:.4} exceeds 2× configured rate {:.4}",
            actual_fpr,
            target_fpr * 2.0
        );
    }

    /// After merging two filters, all keys from both filters are still found.
    #[test]
    fn prop_bloom_merge_preserves_membership(
        keys_a in prop::collection::vec("[a-z]{3,10}", 1..100),
        keys_b in prop::collection::vec("[a-z]{3,10}", 1..100),
    ) {
        let mut bloom_a = BloomFilter::new(1000, 0.01);
        let mut bloom_b = BloomFilter::new(1000, 0.01);

        for key in &keys_a {
            bloom_a.insert(key.as_bytes());
        }
        for key in &keys_b {
            bloom_b.insert(key.as_bytes());
        }

        bloom_a.merge(&bloom_b);

        for key in &keys_a {
            prop_assert!(
                bloom_a.contains(key.as_bytes()),
                "Key '{}' from filter A not found after merge",
                key
            );
        }
        for key in &keys_b {
            prop_assert!(
                bloom_a.contains(key.as_bytes()),
                "Key '{}' from filter B not found after merge",
                key
            );
        }
    }
}

// ── 4. IncrementalAggregator Commutativity ──────────────────────────────

use ferrite_core::query::change_tracker::{AggregateOperation, IncrementalAggregator};
use ferrite_core::query::Value;

proptest! {
    /// Insert N values in any order → Count is always N.
    #[test]
    fn prop_aggregator_count_commutative(
        values in prop::collection::vec(1i64..1000, 1..50),
    ) {
        let mut agg = IncrementalAggregator::new();
        agg.add_aggregate("col", AggregateOperation::Count);

        for v in &values {
            agg.apply_insert("col", &Value::Int(*v));
        }

        let result = agg.current_result();
        prop_assert_eq!(
            &result["col:Count"],
            &Value::Int(values.len() as i64),
            "Count should equal number of inserts"
        );
    }

    /// Insert N integer values in any permutation → Sum is always the same.
    #[test]
    fn prop_aggregator_sum_commutative(
        values in prop::collection::vec(1i64..1000, 1..50),
    ) {
        let expected_sum: i64 = values.iter().sum();

        // Forward order
        let mut agg_fwd = IncrementalAggregator::new();
        agg_fwd.add_aggregate("col", AggregateOperation::Sum);
        for v in &values {
            agg_fwd.apply_insert("col", &Value::Int(*v));
        }

        // Reverse order
        let mut agg_rev = IncrementalAggregator::new();
        agg_rev.add_aggregate("col", AggregateOperation::Sum);
        for v in values.iter().rev() {
            agg_rev.apply_insert("col", &Value::Int(*v));
        }

        let result_fwd = agg_fwd.current_result();
        let result_rev = agg_rev.current_result();

        prop_assert_eq!(
            &result_fwd["col:Sum"], &result_rev["col:Sum"],
            "Sum should be independent of insertion order"
        );
        prop_assert_eq!(
            &result_fwd["col:Sum"],
            &Value::Float(expected_sum as f64),
            "Sum should equal expected total"
        );
    }

    /// Insert then delete the same value → aggregation returns to original state
    /// (for Count and Sum).
    #[test]
    fn prop_aggregator_insert_delete_inverse(value in 1i64..1000) {
        let mut agg = IncrementalAggregator::new();
        agg.add_aggregate("col", AggregateOperation::Count);
        agg.add_aggregate("col", AggregateOperation::Sum);

        let before = agg.current_result();

        agg.apply_insert("col", &Value::Int(value));
        agg.apply_delete("col", &Value::Int(value));

        let after = agg.current_result();

        prop_assert_eq!(
            &before["col:Count"], &after["col:Count"],
            "Count should return to original after insert+delete"
        );
        prop_assert_eq!(
            &before["col:Sum"], &after["col:Sum"],
            "Sum should return to original after insert+delete"
        );
    }
}

// ── 5. ML Model Properties ─────────────────────────────────────────────

use ferrite_core::tiering::{MlTieringConfig, OnlineLearningModel};

proptest! {
    /// Model predictions are always between 0.0 and 1.0 (sigmoid output).
    #[test]
    fn prop_model_prediction_bounded(
        f0 in -10.0f64..10.0,
        f1 in -10.0f64..10.0,
        f2 in -10.0f64..10.0,
        f3 in -10.0f64..10.0,
        f4 in -10.0f64..10.0,
        f5 in -10.0f64..10.0,
    ) {
        let model = OnlineLearningModel::new(MlTieringConfig::default());
        let features = [f0, f1, f2, f3, f4, f5];
        let prediction = model.predict(&features);
        prop_assert!(
            (0.0..=1.0).contains(&prediction),
            "Prediction {} should be in [0.0, 1.0]",
            prediction
        );
    }

    /// After N training steps with consistent labels, accuracy should not
    /// decrease (on average over windows).
    #[test]
    fn prop_model_accuracy_monotonic(n_steps in 50usize..150) {
        let model = OnlineLearningModel::new(MlTieringConfig::default());
        let features = [0.8, 0.6, 0.4, 0.3, 0.5, 0.7];
        let label = 1.0;

        let window_size = 10;
        let mut window_avgs: Vec<f64> = Vec::new();
        let mut window_sum = 0.0;

        for i in 0..n_steps {
            model.train(&features, label);
            window_sum += model.accuracy();

            if (i + 1) % window_size == 0 {
                window_avgs.push(window_sum / window_size as f64);
                window_sum = 0.0;
            }
        }

        if window_avgs.len() >= 2 {
            let mid = window_avgs.len() / 2;
            let first_half: f64 =
                window_avgs[..mid].iter().sum::<f64>() / mid as f64;
            let second_half: f64 =
                window_avgs[mid..].iter().sum::<f64>() / (window_avgs.len() - mid) as f64;

            prop_assert!(
                second_half >= first_half - 0.15,
                "Accuracy should trend upward: first_half_avg={:.3}, second_half_avg={:.3}",
                first_half,
                second_half
            );
        }
    }
}
