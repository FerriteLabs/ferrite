#![allow(clippy::unwrap_used, unused_must_use)]
//! Tiered Storage Benchmarks for Ferrite
//!
//! Proves the core value proposition of Ferrite's three-tier HybridLog
//! architecture vs all-in-memory approaches (Redis) and disk-only alternatives.
//!
//! ## Benchmark Groups
//!
//! 1. **hot_tier** — Mutable region (in-memory, lock-free) reads/writes
//! 2. **warm_tier** — Read-only region (mmap, zero-copy) reads
//! 3. **cold_tier** — Disk region (sync I/O) reads
//! 4. **tier_promotion** — Cold → hot promotion on access
//! 5. **mixed_workload** — Realistic hot/cold access patterns (80/20, 50/50)
//! 6. **memory_efficiency** — Keys stored per MB at different tier configs
//! 7. **all_in_memory_vs_tiered** — Compare Store (all-in-memory) vs tiered regions

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use ferrite::storage::hybridlog::{DiskRegion, MutableRegion, ReadOnlyRegion};
use ferrite::storage::{Store, Value};
use ferrite::tiering::{AccessType, TieringConfig, TieringEngine};
use std::sync::Arc;
use tempfile::tempdir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Generate a key/value pair of a given value size.
fn kv(i: u64, value_size: usize) -> (Vec<u8>, Vec<u8>) {
    let key = format!("key:{:08}", i).into_bytes();
    let value = vec![b'x'; value_size];
    (key, value)
}

const VALUE_SIZE: usize = 128;

// ---------------------------------------------------------------------------
// 1. Hot-tier reads (MutableRegion – in-memory, lock-free)
// ---------------------------------------------------------------------------

fn bench_hot_tier(c: &mut Criterion) {
    let mut group = c.benchmark_group("hot_tier");
    group.throughput(Throughput::Elements(1));

    // --- Write ---
    group.bench_function("write", |b| {
        let region = MutableRegion::new(64 * 1024 * 1024).unwrap(); // 64 MB
        let mut i = 0u64;
        b.iter(|| {
            let (k, v) = kv(i, VALUE_SIZE);
            black_box(region.try_append(&k, &v));
            i += 1;
        });
    });

    // --- Sequential read ---
    {
        let region = MutableRegion::new(64 * 1024 * 1024).unwrap();
        let mut offsets = Vec::new();
        for i in 0..10_000u64 {
            let (k, v) = kv(i, VALUE_SIZE);
            if let Some(addr) = region.try_append(&k, &v) {
                offsets.push(addr.offset());
            }
        }

        group.bench_function("read_sequential", |b| {
            let mut idx = 0usize;
            b.iter(|| {
                let offset = offsets[idx % offsets.len()];
                black_box(region.read(offset));
                idx += 1;
            });
        });

        // --- Random read ---
        // Pre-shuffle offsets for pseudo-random access
        let mut rng_offsets = offsets.clone();
        // Deterministic shuffle via simple LCG-style permutation
        for i in 0..rng_offsets.len() {
            let j = (i.wrapping_mul(6364136223846793005) + 1) % rng_offsets.len();
            rng_offsets.swap(i, j);
        }

        group.bench_function("read_random", |b| {
            let mut idx = 0usize;
            b.iter(|| {
                let offset = rng_offsets[idx % rng_offsets.len()];
                black_box(region.read(offset));
                idx += 1;
            });
        });

        // --- read_value (skip key deserialization) ---
        group.bench_function("read_value_only", |b| {
            let mut idx = 0usize;
            b.iter(|| {
                let offset = offsets[idx % offsets.len()];
                black_box(region.read_value(offset));
                idx += 1;
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 2. Warm-tier reads (ReadOnlyRegion – mmap, zero-copy)
// ---------------------------------------------------------------------------

fn bench_warm_tier(c: &mut Criterion) {
    let mut group = c.benchmark_group("warm_tier");
    group.throughput(Throughput::Elements(1));

    let dir = tempdir().expect("failed to create tempdir");
    let path = dir.path().join("warm.dat");
    let region =
        ReadOnlyRegion::new(&path, 64 * 1024 * 1024).expect("failed to create ReadOnlyRegion");

    let mut offsets = Vec::new();
    for i in 0..10_000u64 {
        let (k, v) = kv(i, VALUE_SIZE);
        if let Ok(Some(addr)) = region.append(&k, &v) {
            offsets.push(addr.offset());
        }
    }

    group.bench_function("read_sequential", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let offset = offsets[idx % offsets.len()];
            black_box(region.read(offset));
            idx += 1;
        });
    });

    let mut rng_offsets = offsets.clone();
    for i in 0..rng_offsets.len() {
        let j = (i.wrapping_mul(6364136223846793005) + 1) % rng_offsets.len();
        rng_offsets.swap(i, j);
    }

    group.bench_function("read_random", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let offset = rng_offsets[idx % rng_offsets.len()];
            black_box(region.read(offset));
            idx += 1;
        });
    });

    group.bench_function("read_value_only", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let offset = offsets[idx % offsets.len()];
            black_box(region.read_value(offset));
            idx += 1;
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 3. Cold-tier reads (DiskRegion – file I/O)
// ---------------------------------------------------------------------------

fn bench_cold_tier(c: &mut Criterion) {
    let mut group = c.benchmark_group("cold_tier");
    group.throughput(Throughput::Elements(1));

    let dir = tempdir().expect("failed to create tempdir");
    let path = dir.path().join("cold.dat");
    let region = DiskRegion::new(&path, 0).expect("failed to create DiskRegion");

    let mut offsets = Vec::new();
    for i in 0..10_000u64 {
        let (k, v) = kv(i, VALUE_SIZE);
        if let Ok(Some(addr)) = region.append(&k, &v) {
            offsets.push(addr.offset());
        }
    }

    group.bench_function("read_sync_sequential", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let offset = offsets[idx % offsets.len()];
            black_box(region.read_sync(offset));
            idx += 1;
        });
    });

    let mut rng_offsets = offsets.clone();
    for i in 0..rng_offsets.len() {
        let j = (i.wrapping_mul(6364136223846793005) + 1) % rng_offsets.len();
        rng_offsets.swap(i, j);
    }

    group.bench_function("read_sync_random", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let offset = rng_offsets[idx % rng_offsets.len()];
            black_box(region.read_sync(offset));
            idx += 1;
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 4. Tier promotion (cold → hot on access)
// ---------------------------------------------------------------------------

fn bench_tier_promotion(c: &mut Criterion) {
    let mut group = c.benchmark_group("tier_promotion");
    group.throughput(Throughput::Elements(1));

    // Simulate: read from cold (disk), write to hot (mutable) — the promotion path.
    let dir = tempdir().expect("failed to create tempdir");
    let cold_path = dir.path().join("promo_cold.dat");
    let cold = DiskRegion::new(&cold_path, 0).expect("DiskRegion");

    let mut cold_offsets = Vec::new();
    for i in 0..5_000u64 {
        let (k, v) = kv(i, VALUE_SIZE);
        if let Ok(Some(addr)) = cold.append(&k, &v) {
            cold_offsets.push(addr.offset());
        }
    }

    let hot = MutableRegion::new(64 * 1024 * 1024).unwrap();

    group.bench_function("cold_to_hot", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let offset = cold_offsets[idx % cold_offsets.len()];
            if let Ok(Some((key, value))) = cold.read_sync(offset) {
                black_box(hot.try_append(&key, &value));
            }
            idx += 1;
        });
    });

    // Warm → hot promotion
    let warm_path = dir.path().join("promo_warm.dat");
    let warm = ReadOnlyRegion::new(&warm_path, 64 * 1024 * 1024).expect("ReadOnlyRegion");

    let mut warm_offsets = Vec::new();
    for i in 0..5_000u64 {
        let (k, v) = kv(i, VALUE_SIZE);
        if let Ok(Some(addr)) = warm.append(&k, &v) {
            warm_offsets.push(addr.offset());
        }
    }

    let hot2 = MutableRegion::new(64 * 1024 * 1024).unwrap();

    group.bench_function("warm_to_hot", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let offset = warm_offsets[idx % warm_offsets.len()];
            if let Some((key, value)) = warm.read(offset) {
                black_box(hot2.try_append(&key, &value));
            }
            idx += 1;
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 5. Mixed workloads (80/20 and 50/50 hot/cold ratios)
// ---------------------------------------------------------------------------

fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");
    group.throughput(Throughput::Elements(1));

    // Setup: hot data in MutableRegion, cold data in DiskRegion on disk.
    let dir = tempdir().expect("tempdir");
    let cold_path = dir.path().join("mixed_cold.dat");

    let hot = Arc::new(MutableRegion::new(64 * 1024 * 1024).unwrap());
    let cold = Arc::new(DiskRegion::new(&cold_path, 0).expect("DiskRegion"));

    let mut hot_offsets = Vec::new();
    let mut cold_offsets = Vec::new();

    for i in 0..10_000u64 {
        let (k, v) = kv(i, VALUE_SIZE);
        if i < 5_000 {
            if let Some(addr) = hot.try_append(&k, &v) {
                hot_offsets.push(addr.offset());
            }
        } else if let Ok(Some(addr)) = cold.append(&k, &v) {
            cold_offsets.push(addr.offset());
        }
    }

    // 80/20: 80% hot reads, 20% cold reads
    for (label, hot_pct) in [("80_20_hot_cold", 80u64), ("50_50_hot_cold", 50u64)] {
        let hot = hot.clone();
        let cold = cold.clone();
        let hot_offsets = hot_offsets.clone();
        let cold_offsets = cold_offsets.clone();

        group.bench_function(label, |b| {
            let mut i = 0u64;
            b.iter(|| {
                if i % 100 < hot_pct {
                    let offset = hot_offsets[(i as usize) % hot_offsets.len()];
                    black_box(hot.read(offset));
                } else {
                    let offset = cold_offsets[(i as usize) % cold_offsets.len()];
                    black_box(cold.read_sync(offset));
                }
                i += 1;
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 6. Memory efficiency — keys stored per MB at different tier configs
// ---------------------------------------------------------------------------

fn bench_memory_efficiency(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_efficiency");

    // Measure how many keys fit in 1 MB for each tier.
    for (label, region_size) in [
        ("hot_1mb", 1024 * 1024usize),
        ("hot_4mb", 4 * 1024 * 1024),
        ("hot_16mb", 16 * 1024 * 1024),
    ] {
        group.bench_function(BenchmarkId::new("fill_rate", label), |b| {
            b.iter_batched(
                || MutableRegion::new(region_size).unwrap(),
                |region| {
                    let mut count = 0u64;
                    loop {
                        let (k, v) = kv(count, VALUE_SIZE);
                        if region.try_append(&k, &v).is_none() {
                            break;
                        }
                        count += 1;
                    }
                    black_box(count);
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    // Warm tier fill rate
    let dir = tempdir().expect("tempdir");

    for (label, region_size) in [
        ("warm_1mb", 1024 * 1024usize),
        ("warm_4mb", 4 * 1024 * 1024),
    ] {
        let dir_ref = &dir;
        group.bench_function(BenchmarkId::new("fill_rate", label), |b| {
            b.iter_batched(
                || {
                    let path = dir_ref.path().join(format!(
                        "eff_{}.dat",
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_nanos()
                    ));
                    ReadOnlyRegion::new(&path, region_size).expect("ReadOnlyRegion")
                },
                |region| {
                    let mut count = 0u64;
                    loop {
                        let (k, v) = kv(count, VALUE_SIZE);
                        match region.append(&k, &v) {
                            Ok(Some(_)) => count += 1,
                            _ => break,
                        }
                    }
                    black_box(count);
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 7. All-in-memory (Store / DashMap) vs tiered for large datasets
// ---------------------------------------------------------------------------

fn bench_all_in_memory_vs_tiered(c: &mut Criterion) {
    let mut group = c.benchmark_group("all_in_memory_vs_tiered");
    group.throughput(Throughput::Elements(1));

    let num_keys = 50_000u64;

    // --- All-in-memory baseline (Store / DashMap) ---
    let store = Arc::new(Store::new(16));
    for i in 0..num_keys {
        let key = Bytes::from(format!("key:{:08}", i));
        let value = Value::String(Bytes::from(vec![b'x'; VALUE_SIZE]));
        store.set(0, key, value);
    }

    group.bench_function("in_memory_read", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = Bytes::from(format!("key:{:08}", i % num_keys));
            black_box(store.get(0, &key));
            i += 1;
        });
    });

    group.bench_function("in_memory_write", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = Bytes::from(format!("key:{:08}", i % num_keys));
            let value = Value::String(Bytes::from(vec![b'x'; VALUE_SIZE]));
            store.set(0, black_box(key), black_box(value));
            i += 1;
        });
    });

    // --- Tiered: hot in MutableRegion, warm in ReadOnlyRegion, cold on disk ---
    let dir = tempdir().expect("tempdir");

    let hot = Arc::new(MutableRegion::new(32 * 1024 * 1024).unwrap());
    let warm_path = dir.path().join("tiered_warm.dat");
    let warm = Arc::new(ReadOnlyRegion::new(&warm_path, 64 * 1024 * 1024).expect("ReadOnlyRegion"));
    let cold_path = dir.path().join("tiered_cold.dat");
    let cold = Arc::new(DiskRegion::new(&cold_path, 0).expect("DiskRegion"));

    let mut hot_offsets = Vec::new();
    let mut warm_offsets = Vec::new();
    let mut cold_offsets = Vec::new();

    // Distribute: 20% hot, 30% warm, 50% cold
    for i in 0..num_keys {
        let (k, v) = kv(i, VALUE_SIZE);
        let pct = i * 100 / num_keys;
        if pct < 20 {
            if let Some(addr) = hot.try_append(&k, &v) {
                hot_offsets.push(addr.offset());
            }
        } else if pct < 50 {
            if let Ok(Some(addr)) = warm.append(&k, &v) {
                warm_offsets.push(addr.offset());
            }
        } else if let Ok(Some(addr)) = cold.append(&k, &v) {
            cold_offsets.push(addr.offset());
        }
    }

    // Read workload that reflects a realistic Zipf-like distribution:
    // Most reads hit hot tier.
    group.bench_function("tiered_read_realistic", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let bucket = i % 100;
            if bucket < 70 && !hot_offsets.is_empty() {
                let offset = hot_offsets[(i as usize) % hot_offsets.len()];
                black_box(hot.read(offset));
            } else if bucket < 90 && !warm_offsets.is_empty() {
                let offset = warm_offsets[(i as usize) % warm_offsets.len()];
                black_box(warm.read(offset));
            } else if !cold_offsets.is_empty() {
                let offset = cold_offsets[(i as usize) % cold_offsets.len()];
                black_box(cold.read_sync(offset));
            }
            i += 1;
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Tiering engine overhead — measure cost of access tracking
// ---------------------------------------------------------------------------

fn bench_tiering_engine(c: &mut Criterion) {
    let mut group = c.benchmark_group("tiering_engine");
    group.throughput(Throughput::Elements(1));

    let engine = TieringEngine::new(TieringConfig::default());

    group.bench_function("record_access_read", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = Bytes::from(format!("key:{:08}", i % 10_000));
            engine.record_access(black_box(&key), AccessType::Read, VALUE_SIZE as u32);
            i += 1;
        });
    });

    group.bench_function("record_access_write", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = Bytes::from(format!("key:{:08}", i % 10_000));
            engine.record_access(black_box(&key), AccessType::Write, VALUE_SIZE as u32);
            i += 1;
        });
    });

    // Pre-populate, then measure get_stats
    let engine2 = TieringEngine::new(TieringConfig::default());
    for i in 0..10_000u64 {
        let key = Bytes::from(format!("key:{:08}", i));
        engine2.record_access(&key, AccessType::Read, VALUE_SIZE as u32);
    }

    group.bench_function("get_stats", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = Bytes::from(format!("key:{:08}", i % 10_000));
            black_box(engine2.get_stats(&key));
            i += 1;
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Value-size sensitivity across tiers
// ---------------------------------------------------------------------------

fn bench_value_size_across_tiers(c: &mut Criterion) {
    let mut group = c.benchmark_group("value_size_tiers");

    let dir = tempdir().expect("tempdir");

    for size in [64, 256, 1024, 4096] {
        // Hot tier
        {
            let region = MutableRegion::new(64 * 1024 * 1024).unwrap();
            let mut offsets = Vec::new();
            for i in 0..5_000u64 {
                let (k, v) = kv(i, size);
                if let Some(addr) = region.try_append(&k, &v) {
                    offsets.push(addr.offset());
                }
            }

            group.bench_with_input(BenchmarkId::new("hot_read", size), &size, |b, _| {
                let mut idx = 0usize;
                b.iter(|| {
                    let offset = offsets[idx % offsets.len()];
                    black_box(region.read(offset));
                    idx += 1;
                });
            });
        }

        // Cold tier
        {
            let path = dir.path().join(format!("vsize_cold_{}.dat", size));
            let disk = DiskRegion::new(&path, 0).expect("DiskRegion");
            let mut offsets = Vec::new();
            for i in 0..5_000u64 {
                let (k, v) = kv(i, size);
                if let Ok(Some(addr)) = disk.append(&k, &v) {
                    offsets.push(addr.offset());
                }
            }

            group.bench_with_input(BenchmarkId::new("cold_read", size), &size, |b, _| {
                let mut idx = 0usize;
                b.iter(|| {
                    let offset = offsets[idx % offsets.len()];
                    black_box(disk.read_sync(offset));
                    idx += 1;
                });
            });
        }
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Criterion harness
// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    bench_hot_tier,
    bench_warm_tier,
    bench_cold_tier,
    bench_tier_promotion,
    bench_mixed_workload,
    bench_memory_efficiency,
    bench_all_in_memory_vs_tiered,
    bench_tiering_engine,
    bench_value_size_across_tiers,
);

criterion_main!(benches);
