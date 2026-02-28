#![allow(clippy::unwrap_used, unused_must_use)]
//! Benchmarks for Next-Gen Feature Modules
//!
//! Covers the core algorithms added in the next-gen feature set:
//! 1. BloomFilter operations (insert, contains, false positive rate)
//! 2. VersionStore record/get_at performance
//! 3. LatencyHistogram percentile calculation
//! 4. IncrementalAggregator throughput
//! 5. OnlineLearningModel predict
//! 6. GossipProtocol prepare_message
//! 7. DeltaSyncEngine delta operations

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

// ---------------------------------------------------------------------------
// 1. BloomFilter (cloud feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "cloud")]
fn bench_bloom_filter(c: &mut Criterion) {
    use ferrite::cloud::edge::delta_sync::BloomFilter;

    let mut group = c.benchmark_group("bloom_filter");
    group.throughput(Throughput::Elements(1));

    group.bench_function("insert", |b| {
        let mut bloom = BloomFilter::new(10_000, 0.01);
        let mut i = 0u64;
        b.iter(|| {
            bloom.insert(format!("key:{}", i).as_bytes());
            i += 1;
        });
    });

    group.bench_function("contains_positive", |b| {
        let mut bloom = BloomFilter::new(10_000, 0.01);
        for i in 0..10_000u64 {
            bloom.insert(format!("key:{}", i).as_bytes());
        }
        let mut i = 0u64;
        b.iter(|| {
            let result = bloom.contains(format!("key:{}", i % 10_000).as_bytes());
            black_box(result);
            i += 1;
        });
    });

    group.bench_function("contains_negative", |b| {
        let mut bloom = BloomFilter::new(10_000, 0.01);
        for i in 0..10_000u64 {
            bloom.insert(format!("key:{}", i).as_bytes());
        }
        let mut i = 0u64;
        b.iter(|| {
            let result = bloom.contains(format!("miss:{}", i).as_bytes());
            black_box(result);
            i += 1;
        });
    });

    group.bench_function("merge", |b| {
        b.iter_batched(
            || {
                let mut a = BloomFilter::new(10_000, 0.01);
                let mut b = BloomFilter::new(10_000, 0.01);
                for i in 0..5_000u64 {
                    a.insert(format!("a:{}", i).as_bytes());
                    b.insert(format!("b:{}", i).as_bytes());
                }
                (a, b)
            },
            |(mut a, b)| {
                a.merge(&b);
                black_box(a);
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 2. VersionStore
// ---------------------------------------------------------------------------

fn bench_version_store(c: &mut Criterion) {
    use ferrite::storage::version_store::{VersionOperation, VersionStoreConfig};
    use ferrite::storage::VersionStore;

    let mut group = c.benchmark_group("version_store");
    group.throughput(Throughput::Elements(1));

    group.bench_function("record", |b| {
        let store = VersionStore::new(VersionStoreConfig::default());
        let mut i = 0u64;
        b.iter(|| {
            store.record_version(
                &format!("key:{}", i % 1_000),
                Some(format!("val:{}", i).into_bytes()),
                VersionOperation::Set,
            );
            i += 1;
        });
    });

    group.bench_function("get_at", |b| {
        let store = VersionStore::new(VersionStoreConfig::default());
        for i in 0..1_000u64 {
            store.record_version(
                "bench_key",
                Some(format!("val:{}", i).into_bytes()),
                VersionOperation::Set,
            );
        }
        let mut i = 0u64;
        b.iter(|| {
            // Timestamps are millis since epoch; use a large value to hit the latest
            let ts = u64::MAX - (i % 1_000);
            let result = store.get_at(black_box("bench_key"), ts);
            black_box(result);
            i += 1;
        });
    });

    group.bench_function("get_history", |b| {
        let store = VersionStore::new(VersionStoreConfig::default());
        for i in 0..100u64 {
            store.record_version(
                "history_key",
                Some(format!("val:{}", i).into_bytes()),
                VersionOperation::Set,
            );
        }
        b.iter(|| {
            let result = store.get_history(black_box("history_key"), 100);
            black_box(result);
        });
    });

    group.bench_function("gc", |b| {
        b.iter_batched(
            || {
                let config = VersionStoreConfig {
                    retention_duration_ms: 0, // expire everything immediately
                    ..VersionStoreConfig::default()
                };
                let store = VersionStore::new(config);
                for i in 0..10_000u64 {
                    store.record_version(
                        &format!("key:{}", i),
                        Some(vec![0u8; 64]),
                        VersionOperation::Set,
                    );
                }
                store
            },
            |store| {
                let result = store.gc();
                black_box(result);
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 3. LatencyHistogram
// ---------------------------------------------------------------------------

fn bench_latency_histogram(c: &mut Criterion) {
    use ferrite::observability::query_profiler::LatencyHistogram;

    let mut group = c.benchmark_group("latency_histogram");
    group.throughput(Throughput::Elements(1));

    group.bench_function("record", |b| {
        let mut hist = LatencyHistogram::new();
        let mut i = 0u64;
        b.iter(|| {
            hist.record(black_box(i % 2_000_000));
            i += 1;
        });
    });

    group.bench_function("percentile", |b| {
        let mut hist = LatencyHistogram::new();
        for i in 0..10_000u64 {
            hist.record(i * 100);
        }
        b.iter(|| {
            black_box(hist.p50());
            black_box(hist.p95());
            black_box(hist.p99());
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 4. IncrementalAggregator
// ---------------------------------------------------------------------------

fn bench_incremental_aggregator(c: &mut Criterion) {
    use ferrite::query::change_tracker::{AggregateOperation, IncrementalAggregator};
    use ferrite::query::Value;

    let mut group = c.benchmark_group("incremental_aggregator");
    group.throughput(Throughput::Elements(1));

    group.bench_function("insert", |b| {
        let mut agg = IncrementalAggregator::new();
        agg.add_aggregate("amount", AggregateOperation::Count);
        agg.add_aggregate("amount", AggregateOperation::Sum);
        agg.add_aggregate("amount", AggregateOperation::Avg);
        let mut i = 0i64;
        b.iter(|| {
            agg.apply_insert("amount", black_box(&Value::Int(i)));
            i += 1;
        });
    });

    group.bench_function("mixed", |b| {
        let mut agg = IncrementalAggregator::new();
        agg.add_aggregate("score", AggregateOperation::Count);
        agg.add_aggregate("score", AggregateOperation::Sum);
        agg.add_aggregate("score", AggregateOperation::Avg);
        agg.add_aggregate("score", AggregateOperation::Min);
        agg.add_aggregate("score", AggregateOperation::Max);
        let mut i = 0i64;
        b.iter(|| {
            if i % 3 == 0 {
                agg.apply_delete("score", black_box(&Value::Int(i.saturating_sub(3))));
            } else {
                agg.apply_insert("score", black_box(&Value::Int(i)));
            }
            i += 1;
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 5. OnlineLearningModel
// ---------------------------------------------------------------------------

fn bench_ml_model(c: &mut Criterion) {
    use ferrite::tiering::MlTieringConfig;
    use ferrite::tiering::OnlineLearningModel;

    let mut group = c.benchmark_group("ml_model");
    group.throughput(Throughput::Elements(1));

    group.bench_function("predict", |b| {
        let model = OnlineLearningModel::new(MlTieringConfig::default());
        // Pre-train with some data
        for i in 0..500u64 {
            let features = [
                i as f64 / 500.0,
                (i % 10) as f64,
                0.5,
                (i % 3) as f64,
                1.0,
                0.0,
            ];
            model.train(&features, if i % 2 == 0 { 1.0 } else { 0.0 });
        }
        let mut i = 0u64;
        b.iter(|| {
            let features = [
                (i % 100) as f64 / 100.0,
                (i % 10) as f64,
                0.5,
                (i % 3) as f64,
                1.0,
                0.0,
            ];
            let result = model.predict(black_box(&features));
            black_box(result);
            i += 1;
        });
    });

    group.bench_function("train_step", |b| {
        let model = OnlineLearningModel::new(MlTieringConfig::default());
        let mut i = 0u64;
        b.iter(|| {
            let features = [
                (i % 100) as f64 / 100.0,
                (i % 10) as f64,
                0.5,
                (i % 3) as f64,
                1.0,
                0.0,
            ];
            let label = if i % 2 == 0 { 1.0 } else { 0.0 };
            model.train(black_box(&features), black_box(label));
            i += 1;
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 6. GossipProtocol
// ---------------------------------------------------------------------------

fn bench_gossip(c: &mut Criterion) {
    use ferrite::plugins::crdt::gossip::{GossipConfig, GossipProtocol};
    use ferrite::plugins::crdt::CrdtType;

    let mut group = c.benchmark_group("gossip");
    group.throughput(Throughput::Elements(1));

    group.bench_function("record_mutation", |b| {
        let gossip = GossipProtocol::new("node-1".to_string(), GossipConfig::default());
        let mut i = 0u64;
        b.iter(|| {
            gossip.record_local_mutation(
                &format!("key:{}", i),
                CrdtType::GCounter,
                vec![0u8; 32],
            );
            i += 1;
        });
    });

    group.bench_function("prepare_message", |b| {
        let gossip = GossipProtocol::new("node-1".to_string(), GossipConfig::default());
        gossip.add_peer("peer-1", "127.0.0.1:7001");
        for i in 0..1_000u64 {
            gossip.record_local_mutation(
                &format!("key:{}", i),
                CrdtType::GCounter,
                vec![0u8; 32],
            );
        }
        b.iter(|| {
            let result = gossip.prepare_gossip_message(black_box("peer-1"));
            black_box(result);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Criterion group & main
// ---------------------------------------------------------------------------

#[cfg(feature = "cloud")]
criterion_group!(
    benches,
    bench_bloom_filter,
    bench_version_store,
    bench_latency_histogram,
    bench_incremental_aggregator,
    bench_ml_model,
    bench_gossip,
);

#[cfg(not(feature = "cloud"))]
criterion_group!(
    benches,
    bench_version_store,
    bench_latency_histogram,
    bench_incremental_aggregator,
    bench_ml_model,
    bench_gossip,
);

criterion_main!(benches);
