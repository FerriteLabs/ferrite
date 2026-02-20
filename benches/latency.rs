//! Latency benchmarks for Ferrite
//!
//! Measures latency distributions (P50, P99, P99.9) for core Redis commands.

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use ferrite::storage::{Store, Value};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Collect latency samples and calculate percentiles
struct LatencyStats {
    samples: Vec<Duration>,
}

impl LatencyStats {
    fn new() -> Self {
        Self {
            samples: Vec::with_capacity(100000),
        }
    }

    fn record(&mut self, duration: Duration) {
        self.samples.push(duration);
    }

    fn percentile(&mut self, p: f64) -> Duration {
        if self.samples.is_empty() {
            return Duration::ZERO;
        }

        self.samples.sort();
        let idx = ((self.samples.len() as f64) * (p / 100.0)) as usize;
        let idx = idx.min(self.samples.len() - 1);
        self.samples[idx]
    }

    fn mean(&self) -> Duration {
        if self.samples.is_empty() {
            return Duration::ZERO;
        }

        let total: Duration = self.samples.iter().sum();
        total / self.samples.len() as u32
    }

    fn print_stats(&mut self, name: &str) {
        println!("\n{} Latency Statistics:", name);
        println!("  Samples: {}", self.samples.len());
        println!("  Mean:    {:?}", self.mean());
        println!("  P50:     {:?}", self.percentile(50.0));
        println!("  P90:     {:?}", self.percentile(90.0));
        println!("  P99:     {:?}", self.percentile(99.0));
        println!("  P99.9:   {:?}", self.percentile(99.9));
    }
}

/// Benchmark GET latency
fn bench_get_latency(c: &mut Criterion) {
    let store = Arc::new(Store::new(16));

    // Pre-populate store with data
    for i in 0..10000 {
        let key = Bytes::from(format!("key:{}", i));
        let value = Value::String(Bytes::from(format!("value:{}", i)));
        store.set(0, key, value);
    }

    // Collect raw latency samples
    let mut stats = LatencyStats::new();
    for i in 0..100000 {
        let key = Bytes::from(format!("key:{}", i % 10000));
        let start = Instant::now();
        let result = store.get(0, black_box(&key));
        let elapsed = start.elapsed();
        black_box(result);
        stats.record(elapsed);
    }
    stats.print_stats("GET (existing)");

    // Criterion benchmark for comparisons
    let mut group = c.benchmark_group("get_latency");

    group.bench_function("existing_key", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = Bytes::from(format!("key:{}", i % 10000));
            let result = store.get(0, black_box(&key));
            black_box(result);
            i += 1;
        })
    });

    group.bench_function("missing_key", |b| {
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

/// Benchmark SET latency
fn bench_set_latency(c: &mut Criterion) {
    let store = Arc::new(Store::new(16));

    // Collect raw latency samples
    let mut stats = LatencyStats::new();
    for i in 0..100000 {
        let key = Bytes::from(format!("key:{}", i));
        let value = Value::String(Bytes::from(format!("value:{}", i)));
        let start = Instant::now();
        store.set(0, black_box(key), black_box(value));
        let elapsed = start.elapsed();
        stats.record(elapsed);
    }
    stats.print_stats("SET (new key)");

    // Criterion benchmark
    let mut group = c.benchmark_group("set_latency");

    group.bench_function("new_key", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = Bytes::from(format!("bench_key:{}", i));
            let value = Value::String(Bytes::from(format!("value:{}", i)));
            store.set(0, black_box(key), black_box(value));
            i += 1;
        })
    });

    // Pre-populate for overwrite
    let store2 = Arc::new(Store::new(16));
    for i in 0..10000 {
        let key = Bytes::from(format!("key:{}", i));
        let value = Value::String(Bytes::from("initial"));
        store2.set(0, key, value);
    }

    group.bench_function("overwrite", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = Bytes::from(format!("key:{}", i % 10000));
            let value = Value::String(Bytes::from(format!("new:{}", i)));
            store2.set(0, black_box(key), black_box(value));
            i += 1;
        })
    });

    group.finish();
}

/// Benchmark DEL latency
fn bench_del_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("del_latency");

    group.bench_function("existing_key", |b| {
        b.iter_batched(
            || {
                let store = Arc::new(Store::new(16));
                for i in 0..1000 {
                    let key = Bytes::from(format!("key:{}", i));
                    let value = Value::String(Bytes::from("value"));
                    store.set(0, key, value);
                }
                store
            },
            |store| {
                for i in 0..100 {
                    let key = Bytes::from(format!("key:{}", i));
                    store.del(0, black_box(&[key]));
                }
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

/// Benchmark HASH operations latency
fn bench_hash_latency(c: &mut Criterion) {
    let store = Arc::new(Store::new(16));
    let key = Bytes::from("hash");

    // Initialize hash
    let mut hash = std::collections::HashMap::new();
    for i in 0..1000 {
        hash.insert(
            Bytes::from(format!("field:{}", i)),
            Bytes::from(format!("value:{}", i)),
        );
    }
    store.set(0, key.clone(), Value::Hash(hash));

    // Collect latency samples
    let mut stats = LatencyStats::new();
    for i in 0..100000 {
        let field = Bytes::from(format!("field:{}", i % 1000));
        let start = Instant::now();
        if let Some(Value::Hash(hash)) = store.get(0, &key) {
            let result = hash.get(black_box(&field));
            black_box(result);
        }
        let elapsed = start.elapsed();
        stats.record(elapsed);
    }
    stats.print_stats("HGET");

    let mut group = c.benchmark_group("hash_latency");

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
            let value = Bytes::from(format!("value:{}", i));
            if let Some(Value::Hash(mut hash)) = store.get(0, &key) {
                hash.insert(black_box(field), black_box(value));
                store.set(0, key.clone(), Value::Hash(hash));
            }
            i += 1;
        })
    });

    group.finish();
}

/// Benchmark LIST operations latency
fn bench_list_latency(c: &mut Criterion) {
    let store = Arc::new(Store::new(16));
    let key = Bytes::from("list");

    // Initialize list
    let mut list = std::collections::VecDeque::new();
    for i in 0..1000 {
        list.push_back(Bytes::from(format!("value:{}", i)));
    }
    store.set(0, key.clone(), Value::List(list));

    let mut group = c.benchmark_group("list_latency");

    group.bench_function("lpush", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let value = Bytes::from(format!("new:{}", i));
            if let Some(Value::List(mut list)) = store.get(0, &key) {
                list.push_front(black_box(value));
                store.set(0, key.clone(), Value::List(list));
            }
            i += 1;
        })
    });

    group.bench_function("rpop", |b| {
        b.iter(|| {
            if let Some(Value::List(mut list)) = store.get(0, &key) {
                let result = list.pop_back();
                black_box(result);
                store.set(0, key.clone(), Value::List(list));
            }
        })
    });

    group.finish();
}

/// Benchmark SET operations latency
fn bench_set_latency_ops(c: &mut Criterion) {
    let store = Arc::new(Store::new(16));
    let key = Bytes::from("set");

    // Initialize set
    let mut set = std::collections::HashSet::new();
    for i in 0..1000 {
        set.insert(Bytes::from(format!("member:{}", i)));
    }
    store.set(0, key.clone(), Value::Set(set));

    // Collect latency samples
    let mut stats = LatencyStats::new();
    for i in 0..100000 {
        let member = Bytes::from(format!("member:{}", i % 1000));
        let start = Instant::now();
        if let Some(Value::Set(set)) = store.get(0, &key) {
            let result = set.contains(black_box(&member));
            black_box(result);
        }
        let elapsed = start.elapsed();
        stats.record(elapsed);
    }
    stats.print_stats("SISMEMBER");

    let mut group = c.benchmark_group("set_latency");

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

/// Benchmark SORTED SET operations latency
fn bench_sorted_set_latency(c: &mut Criterion) {
    let store = Arc::new(Store::new(16));
    let key = Bytes::from("zset");

    // Initialize sorted set
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

    // Collect latency samples
    let mut stats = LatencyStats::new();
    for i in 0..100000 {
        let member = Bytes::from(format!("member:{}", i % 1000));
        let start = Instant::now();
        if let Some(Value::SortedSet { by_member, .. }) = store.get(0, &key) {
            let result = by_member.get(black_box(&member));
            black_box(result);
        }
        let elapsed = start.elapsed();
        stats.record(elapsed);
    }
    stats.print_stats("ZSCORE");

    let mut group = c.benchmark_group("sorted_set_latency");

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

/// Benchmark varying value sizes for latency
fn bench_value_size_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("value_size_latency");

    for size in [64, 256, 1024, 4096, 16384].iter() {
        let value_data = "x".repeat(*size);
        let value = Value::String(Bytes::from(value_data));

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

/// Summary benchmark that prints overall latency statistics
fn bench_summary(_c: &mut Criterion) {
    println!("\n========================================");
    println!("Ferrite Latency Summary");
    println!("========================================");

    let store = Arc::new(Store::new(16));

    // GET latency
    for i in 0..10000 {
        let key = Bytes::from(format!("key:{}", i));
        let value = Value::String(Bytes::from(format!("value:{}", i)));
        store.set(0, key, value);
    }

    let mut get_stats = LatencyStats::new();
    for i in 0..100000 {
        let key = Bytes::from(format!("key:{}", i % 10000));
        let start = Instant::now();
        let result = store.get(0, &key);
        let elapsed = start.elapsed();
        black_box(result);
        get_stats.record(elapsed);
    }
    get_stats.print_stats("GET");

    // SET latency
    let store2 = Arc::new(Store::new(16));
    let mut set_stats = LatencyStats::new();
    for i in 0..100000 {
        let key = Bytes::from(format!("key:{}", i));
        let value = Value::String(Bytes::from(format!("value:{}", i)));
        let start = Instant::now();
        store2.set(0, key, value);
        let elapsed = start.elapsed();
        set_stats.record(elapsed);
    }
    set_stats.print_stats("SET");

    println!("\n========================================");
}

criterion_group!(
    benches,
    bench_get_latency,
    bench_set_latency,
    bench_del_latency,
    bench_hash_latency,
    bench_list_latency,
    bench_set_latency_ops,
    bench_sorted_set_latency,
    bench_value_size_latency,
    bench_summary,
);

criterion_main!(benches);
