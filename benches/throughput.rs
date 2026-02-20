#![allow(clippy::unwrap_used)]
//! Throughput benchmarks for Ferrite
//!
//! Measures operations per second for core Redis commands.

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use ferrite::storage::{Store, Value};
use std::sync::Arc;

/// Benchmark GET operations throughput
fn bench_get(c: &mut Criterion) {
    let store = Arc::new(Store::new(16));

    // Pre-populate store with data
    for i in 0..10000 {
        let key = Bytes::from(format!("key:{}", i));
        let value = Value::String(Bytes::from(format!("value:{}", i)));
        store.set(0, key, value);
    }

    let mut group = c.benchmark_group("get_throughput");
    group.throughput(Throughput::Elements(1));

    group.bench_function("get_existing", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = Bytes::from(format!("key:{}", i % 10000));
            let result = store.get(0, black_box(&key));
            black_box(result);
            i += 1;
        })
    });

    group.bench_function("get_missing", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = Bytes::from(format!("missing:{}", i));
            let result = store.get(0, black_box(&key));
            black_box(result);
            i += 1;
        })
    });

    group.finish();
}

/// Benchmark SET operations throughput
fn bench_set(c: &mut Criterion) {
    let store = Arc::new(Store::new(16));

    let mut group = c.benchmark_group("set_throughput");
    group.throughput(Throughput::Elements(1));

    group.bench_function("set_new", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = Bytes::from(format!("key:{}", i));
            let value = Value::String(Bytes::from(format!("value:{}", i)));
            store.set(0, black_box(key), black_box(value));
            i += 1;
        })
    });

    // Pre-populate for overwrite benchmark
    let store2 = Arc::new(Store::new(16));
    for i in 0..10000 {
        let key = Bytes::from(format!("key:{}", i));
        let value = Value::String(Bytes::from(format!("value:{}", i)));
        store2.set(0, key, value);
    }

    group.bench_function("set_overwrite", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = Bytes::from(format!("key:{}", i % 10000));
            let value = Value::String(Bytes::from(format!("newvalue:{}", i)));
            store2.set(0, black_box(key), black_box(value));
            i += 1;
        })
    });

    group.finish();
}

/// Benchmark DEL operations throughput
fn bench_del(c: &mut Criterion) {
    let mut group = c.benchmark_group("del_throughput");
    group.throughput(Throughput::Elements(1));

    group.bench_function("del_existing", |b| {
        b.iter_batched(
            || {
                // Setup: create a fresh store with data
                let store = Arc::new(Store::new(16));
                for i in 0..1000 {
                    let key = Bytes::from(format!("key:{}", i));
                    let value = Value::String(Bytes::from("value"));
                    store.set(0, key, value);
                }
                store
            },
            |store| {
                // Benchmark: delete all keys
                for i in 0..1000 {
                    let key = Bytes::from(format!("key:{}", i));
                    store.del(0, black_box(&[key]));
                }
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

/// Benchmark EXISTS operations throughput
fn bench_exists(c: &mut Criterion) {
    let store = Arc::new(Store::new(16));

    // Pre-populate store
    for i in 0..10000 {
        let key = Bytes::from(format!("key:{}", i));
        let value = Value::String(Bytes::from("value"));
        store.set(0, key, value);
    }

    let mut group = c.benchmark_group("exists_throughput");
    group.throughput(Throughput::Elements(1));

    group.bench_function("exists_true", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = Bytes::from(format!("key:{}", i % 10000));
            let result = store.exists(0, black_box(&[key]));
            black_box(result);
            i += 1;
        })
    });

    group.bench_function("exists_false", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = Bytes::from(format!("missing:{}", i));
            let result = store.exists(0, black_box(&[key]));
            black_box(result);
            i += 1;
        })
    });

    group.finish();
}

/// Benchmark LIST operations throughput
fn bench_list(c: &mut Criterion) {
    let mut group = c.benchmark_group("list_throughput");
    group.throughput(Throughput::Elements(1));

    // LPUSH/RPUSH benchmark
    group.bench_function("lpush", |b| {
        let store = Arc::new(Store::new(16));
        let key = Bytes::from("list");
        let mut i = 0u64;

        b.iter(|| {
            let value = Bytes::from(format!("value:{}", i));

            if let Some(Value::List(mut list)) = store.get(0, &key) {
                list.push_front(black_box(value));
                store.set(0, key.clone(), Value::List(list));
            } else {
                let mut list = std::collections::VecDeque::new();
                list.push_front(value);
                store.set(0, key.clone(), Value::List(list));
            }
            i += 1;
        })
    });

    group.finish();
}

/// Benchmark HASH operations throughput
fn bench_hash(c: &mut Criterion) {
    let store = Arc::new(Store::new(16));
    let key = Bytes::from("hash");

    // Initialize hash
    {
        let mut hash = std::collections::HashMap::new();
        for i in 0..1000 {
            hash.insert(
                Bytes::from(format!("field:{}", i)),
                Bytes::from(format!("value:{}", i)),
            );
        }
        store.set(0, key.clone(), Value::Hash(hash));
    }

    let mut group = c.benchmark_group("hash_throughput");
    group.throughput(Throughput::Elements(1));

    group.bench_function("hget", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let field = Bytes::from(format!("field:{}", i % 1000));
            if let Some(Value::Hash(hash)) = store.get(0, &key) {
                let result = hash.get(black_box(&field));
                black_box(result);
            }
            i += 1;
        })
    });

    group.bench_function("hset", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let field = Bytes::from(format!("newfield:{}", i));
            let value = Bytes::from(format!("newvalue:{}", i));
            if let Some(Value::Hash(mut hash)) = store.get(0, &key) {
                hash.insert(black_box(field), black_box(value));
                store.set(0, key.clone(), Value::Hash(hash));
            }
            i += 1;
        })
    });

    group.finish();
}

/// Benchmark SET operations throughput
fn bench_set_ops(c: &mut Criterion) {
    let store = Arc::new(Store::new(16));
    let key = Bytes::from("set");

    // Initialize set
    {
        let mut set = std::collections::HashSet::new();
        for i in 0..1000 {
            set.insert(Bytes::from(format!("member:{}", i)));
        }
        store.set(0, key.clone(), Value::Set(set));
    }

    let mut group = c.benchmark_group("set_ops_throughput");
    group.throughput(Throughput::Elements(1));

    group.bench_function("sismember", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let member = Bytes::from(format!("member:{}", i % 1000));
            if let Some(Value::Set(set)) = store.get(0, &key) {
                let result = set.contains(black_box(&member));
                black_box(result);
            }
            i += 1;
        })
    });

    group.bench_function("sadd", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let member = Bytes::from(format!("newmember:{}", i));
            if let Some(Value::Set(mut set)) = store.get(0, &key) {
                set.insert(black_box(member));
                store.set(0, key.clone(), Value::Set(set));
            }
            i += 1;
        })
    });

    group.finish();
}

/// Benchmark SORTED SET operations throughput
fn bench_sorted_set(c: &mut Criterion) {
    let store = Arc::new(Store::new(16));
    let key = Bytes::from("zset");

    // Initialize sorted set
    {
        let mut by_score = std::collections::BTreeMap::new();
        let mut by_member = std::collections::HashMap::new();
        for i in 0..1000 {
            let member = Bytes::from(format!("member:{}", i));
            let score = i as f64;
            by_score.insert((ordered_float::OrderedFloat(score), member.clone()), ());
            by_member.insert(member, score);
        }
        store.set(
            0,
            key.clone(),
            Value::SortedSet {
                by_score,
                by_member,
            },
        );
    }

    let mut group = c.benchmark_group("sorted_set_throughput");
    group.throughput(Throughput::Elements(1));

    group.bench_function("zscore", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let member = Bytes::from(format!("member:{}", i % 1000));
            if let Some(Value::SortedSet { by_member, .. }) = store.get(0, &key) {
                let result = by_member.get(black_box(&member));
                black_box(result);
            }
            i += 1;
        })
    });

    group.bench_function("zadd", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let member = Bytes::from(format!("newmember:{}", i));
            let score = i as f64;
            if let Some(Value::SortedSet {
                mut by_score,
                mut by_member,
            }) = store.get(0, &key)
            {
                by_score.insert((ordered_float::OrderedFloat(score), member.clone()), ());
                by_member.insert(black_box(member), black_box(score));
                store.set(
                    0,
                    key.clone(),
                    Value::SortedSet {
                        by_score,
                        by_member,
                    },
                );
            }
            i += 1;
        })
    });

    group.finish();
}

/// Benchmark varying value sizes
fn bench_value_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("value_sizes");

    for size in [64, 256, 1024, 4096, 16384].iter() {
        let value_data = "x".repeat(*size);
        let value = Value::String(Bytes::from(value_data));

        group.throughput(Throughput::Bytes(*size as u64));

        group.bench_with_input(BenchmarkId::new("set", size), size, |b, _| {
            let store = Arc::new(Store::new(16));
            let mut i = 0u64;

            b.iter(|| {
                let key = Bytes::from(format!("key:{}", i));
                store.set(0, black_box(key), black_box(value.clone()));
                i += 1;
            })
        });

        group.bench_with_input(BenchmarkId::new("get", size), size, |b, _| {
            let store = Arc::new(Store::new(16));

            // Pre-populate
            for i in 0..1000 {
                let key = Bytes::from(format!("key:{}", i));
                store.set(0, key, value.clone());
            }

            let mut i = 0u64;
            b.iter(|| {
                let key = Bytes::from(format!("key:{}", i % 1000));
                let result = store.get(0, black_box(&key));
                black_box(result);
                i += 1;
            })
        });
    }

    group.finish();
}

/// Benchmark concurrent access patterns
fn bench_concurrent(c: &mut Criterion) {
    use std::thread;

    let mut group = c.benchmark_group("concurrent");

    for num_threads in [1, 2, 4, 8].iter() {
        group.bench_with_input(
            BenchmarkId::new("parallel_get_set", num_threads),
            num_threads,
            |b, &num_threads| {
                let store = Arc::new(Store::new(16));

                // Pre-populate
                for i in 0..10000 {
                    let key = Bytes::from(format!("key:{}", i));
                    let value = Value::String(Bytes::from(format!("value:{}", i)));
                    store.set(0, key, value);
                }

                b.iter(|| {
                    let handles: Vec<_> = (0..num_threads)
                        .map(|t| {
                            let store = store.clone();
                            thread::spawn(move || {
                                for i in 0..1000 {
                                    let key_idx = (t * 1000 + i) % 10000;
                                    let key = Bytes::from(format!("key:{}", key_idx));

                                    // Mix of reads and writes
                                    if i % 4 == 0 {
                                        let value =
                                            Value::String(Bytes::from(format!("new:{}", i)));
                                        store.set(0, key, value);
                                    } else {
                                        let result = store.get(0, &key);
                                        black_box(result);
                                    }
                                }
                            })
                        })
                        .collect();

                    for handle in handles {
                        handle.join().unwrap();
                    }
                })
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_get,
    bench_set,
    bench_del,
    bench_exists,
    bench_list,
    bench_hash,
    bench_set_ops,
    bench_sorted_set,
    bench_value_sizes,
    bench_concurrent,
);

criterion_main!(benches);
