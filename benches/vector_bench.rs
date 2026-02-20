#![allow(clippy::unwrap_used, unused_must_use, dead_code, unused_imports)]
//! Vector search and semantic cache benchmarks
//!
//! Measures performance of vector operations across different index types,
//! dimensions, and k values. Also benchmarks semantic cache hit/miss paths.

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use ferrite::ai::semantic::{SemanticCache, SemanticCacheBuilder};
use ferrite::ai::vector::{
    DistanceMetric, FlatIndex, HnswIndex, IvfIndex, VectorId, VectorIndex, VectorIndexConfig,
    VectorStore,
};
use rand::Rng;

/// Generate a random f32 vector of the given dimension
fn random_vector(dim: usize) -> Vec<f32> {
    let mut rng = rand::thread_rng();
    (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect()
}

/// Generate a deterministic embedding for benchmarking (reproducible)
fn deterministic_embedding(dim: usize, seed: u64) -> Vec<f32> {
    (0..dim)
        .map(|i| {
            let x = ((i as u64).wrapping_mul(seed).wrapping_add(7919)) as f32;
            (x % 256.0) / 255.0 - 0.5
        })
        .collect()
}

/// Normalize a vector to unit length
fn normalize(vec: &mut [f32]) {
    let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in vec.iter_mut() {
            *x /= norm;
        }
    }
}

// ============================================================================
// Vector Insertion Benchmarks
// ============================================================================

fn bench_vector_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_insert");
    group.sample_size(50);

    for dim in [128, 256, 768, 1536] {
        // HNSW insert
        group.bench_with_input(BenchmarkId::new("hnsw", dim), &dim, |b, &dim| {
            let index = HnswIndex::with_params(dim, DistanceMetric::Cosine, 16, 200, 50);
            let mut counter = 0u64;
            b.iter(|| {
                let vec = deterministic_embedding(dim, counter);
                let id = VectorId::new(format!("v{}", counter));
                let _ = index.add(black_box(id), black_box(&vec));
                counter += 1;
            });
        });

        // IVF insert (untrained, accumulation mode)
        group.bench_with_input(BenchmarkId::new("ivf", dim), &dim, |b, &dim| {
            let index = IvfIndex::new(dim, DistanceMetric::Cosine, 64, 8);
            let mut counter = 0u64;
            b.iter(|| {
                let vec = deterministic_embedding(dim, counter);
                let id = VectorId::new(format!("v{}", counter));
                let _ = index.add(black_box(id), black_box(&vec));
                counter += 1;
            });
        });

        // Flat insert
        group.bench_with_input(BenchmarkId::new("flat", dim), &dim, |b, &dim| {
            let index = FlatIndex::new(dim, DistanceMetric::Cosine);
            let mut counter = 0u64;
            b.iter(|| {
                let vec = deterministic_embedding(dim, counter);
                let id = VectorId::new(format!("v{}", counter));
                let _ = index.add(black_box(id), black_box(&vec));
                counter += 1;
            });
        });
    }

    group.finish();
}

// ============================================================================
// KNN Search Benchmarks
// ============================================================================

fn bench_knn_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("knn_search");
    group.sample_size(30);

    let dim = 384; // Common embedding dimension (all-MiniLM-L6-v2)
    let dataset_size = 5000;

    // Pre-build HNSW index
    let hnsw_index = HnswIndex::with_params(dim, DistanceMetric::Cosine, 16, 200, 50);
    for i in 0..dataset_size {
        let vec = deterministic_embedding(dim, i as u64);
        let _ = hnsw_index.add(VectorId::new(format!("v{}", i)), &vec);
    }

    // Pre-build IVF index
    let ivf_index = IvfIndex::new(dim, DistanceMetric::Cosine, 64, 8);
    for i in 0..dataset_size {
        let vec = deterministic_embedding(dim, i as u64);
        let _ = ivf_index.add(VectorId::new(format!("v{}", i)), &vec);
    }
    ivf_index.train();

    // Pre-build Flat index
    let flat_index = FlatIndex::new(dim, DistanceMetric::Cosine);
    for i in 0..dataset_size {
        let vec = deterministic_embedding(dim, i as u64);
        let _ = flat_index.add(VectorId::new(format!("v{}", i)), &vec);
    }

    for k in [10, 50, 100] {
        // HNSW KNN search
        group.bench_with_input(
            BenchmarkId::new("hnsw", format!("k={}", k)),
            &k,
            |b, &k| {
                let mut query_idx = 0u64;
                b.iter(|| {
                    let query = deterministic_embedding(dim, 100_000 + query_idx);
                    let results = hnsw_index.search(black_box(&query), black_box(k));
                    black_box(results);
                    query_idx += 1;
                });
            },
        );

        // IVF KNN search
        group.bench_with_input(
            BenchmarkId::new("ivf", format!("k={}", k)),
            &k,
            |b, &k| {
                let mut query_idx = 0u64;
                b.iter(|| {
                    let query = deterministic_embedding(dim, 100_000 + query_idx);
                    let results = ivf_index.search(black_box(&query), black_box(k));
                    black_box(results);
                    query_idx += 1;
                });
            },
        );

        // Flat (brute force) KNN search
        group.bench_with_input(
            BenchmarkId::new("flat", format!("k={}", k)),
            &k,
            |b, &k| {
                let mut query_idx = 0u64;
                b.iter(|| {
                    let query = deterministic_embedding(dim, 100_000 + query_idx);
                    let results = flat_index.search(black_box(&query), black_box(k));
                    black_box(results);
                    query_idx += 1;
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// KNN Search with varying dimensions
// ============================================================================

fn bench_knn_dimensions(c: &mut Criterion) {
    let mut group = c.benchmark_group("knn_dimension_scaling");
    group.sample_size(20);

    let dataset_size = 2000;
    let k = 10;

    for dim in [128, 256, 768, 1536] {
        let index = HnswIndex::with_params(dim, DistanceMetric::Cosine, 16, 200, 50);
        for i in 0..dataset_size {
            let vec = deterministic_embedding(dim, i as u64);
            let _ = index.add(VectorId::new(format!("v{}", i)), &vec);
        }

        group.bench_with_input(BenchmarkId::new("hnsw_search", dim), &dim, |b, &dim| {
            let mut query_idx = 0u64;
            b.iter(|| {
                let query = deterministic_embedding(dim, 100_000 + query_idx);
                let results = index.search(black_box(&query), black_box(k));
                black_box(results);
                query_idx += 1;
            });
        });
    }

    group.finish();
}

// ============================================================================
// Distance Metric Benchmarks
// ============================================================================

fn bench_distance_metrics(c: &mut Criterion) {
    let mut group = c.benchmark_group("distance_metrics");
    group.sample_size(30);

    let dim = 384;
    let dataset_size = 3000;
    let k = 10;

    for metric in [
        DistanceMetric::Cosine,
        DistanceMetric::Euclidean,
        DistanceMetric::DotProduct,
        DistanceMetric::Manhattan,
    ] {
        let index = HnswIndex::with_params(dim, metric, 16, 200, 50);
        for i in 0..dataset_size {
            let vec = deterministic_embedding(dim, i as u64);
            let _ = index.add(VectorId::new(format!("v{}", i)), &vec);
        }

        group.bench_with_input(
            BenchmarkId::new("hnsw_search", format!("{}", metric)),
            &metric,
            |b, _| {
                let mut query_idx = 0u64;
                b.iter(|| {
                    let query = deterministic_embedding(dim, 100_000 + query_idx);
                    let results = index.search(black_box(&query), black_box(k));
                    black_box(results);
                    query_idx += 1;
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Semantic Cache Benchmarks
// ============================================================================

fn bench_semantic_cache(c: &mut Criterion) {
    let mut group = c.benchmark_group("semantic_cache");
    group.sample_size(50);

    let dim = 384;

    // Build a cache with entries
    let cache = SemanticCacheBuilder::new()
        .dimension(dim)
        .threshold(0.85)
        .max_entries(100_000)
        .build();

    // Pre-populate with 1000 entries
    let mut stored_embeddings: Vec<Vec<f32>> = Vec::new();
    for i in 0..1000 {
        let mut emb = deterministic_embedding(dim, i as u64);
        normalize(&mut emb);
        cache
            .set(
                &format!("query {}", i),
                Bytes::from(format!("response for query {}", i)),
                &emb,
                None,
            )
            .expect("cache set should succeed");
        stored_embeddings.push(emb);
    }

    // Benchmark: cache hit (query with a known embedding)
    group.bench_function("cache_hit", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let embedding = &stored_embeddings[idx % stored_embeddings.len()];
            let result = cache.get(black_box(embedding), Some(0.85));
            black_box(result);
            idx += 1;
        });
    });

    // Benchmark: cache miss (query with a random embedding)
    group.bench_function("cache_miss", |b| {
        let mut query_idx = 0u64;
        b.iter(|| {
            // Use a high seed to generate embeddings far from stored ones
            let mut emb = deterministic_embedding(dim, 500_000 + query_idx);
            normalize(&mut emb);
            let result = cache.get(black_box(&emb), Some(0.99));
            black_box(result);
            query_idx += 1;
        });
    });

    // Benchmark: cache set
    group.bench_function("cache_set", |b| {
        let set_cache = SemanticCacheBuilder::new()
            .dimension(dim)
            .threshold(0.85)
            .max_entries(1_000_000)
            .build();

        let mut counter = 0u64;
        b.iter(|| {
            let mut emb = deterministic_embedding(dim, 1_000_000 + counter);
            normalize(&mut emb);
            let result = set_cache.set(
                &format!("new query {}", counter),
                Bytes::from("new response"),
                black_box(&emb),
                None,
            );
            black_box(result);
            counter += 1;
        });
    });

    // Benchmark: cache get_many
    group.bench_function("cache_get_many_k5", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let embedding = &stored_embeddings[idx % stored_embeddings.len()];
            let result = cache.get_many(black_box(embedding), 5, Some(0.5));
            black_box(result);
            idx += 1;
        });
    });

    group.finish();
}

// ============================================================================
// Index Build Time Benchmarks
// ============================================================================

fn bench_index_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_build");
    group.sample_size(10);

    let dim = 128;

    // Benchmark building HNSW index with 10K vectors
    group.bench_function("hnsw_10k_128d", |b| {
        b.iter(|| {
            let index = HnswIndex::with_params(dim, DistanceMetric::Cosine, 16, 100, 50);
            for i in 0..10_000 {
                let vec = deterministic_embedding(dim, i as u64);
                let _ = index.add(VectorId::new(format!("v{}", i)), &vec);
            }
            black_box(index.len());
        });
    });

    // Benchmark building Flat index with 10K vectors
    group.bench_function("flat_10k_128d", |b| {
        b.iter(|| {
            let index = FlatIndex::new(dim, DistanceMetric::Cosine);
            for i in 0..10_000 {
                let vec = deterministic_embedding(dim, i as u64);
                let _ = index.add(VectorId::new(format!("v{}", i)), &vec);
            }
            black_box(index.len());
        });
    });

    // Benchmark building IVF index (insert + train) with 10K vectors
    group.bench_function("ivf_10k_128d", |b| {
        b.iter(|| {
            let index = IvfIndex::new(dim, DistanceMetric::Cosine, 100, 10);
            for i in 0..10_000 {
                let vec = deterministic_embedding(dim, i as u64);
                let _ = index.add(VectorId::new(format!("v{}", i)), &vec);
            }
            index.train();
            black_box(index.len());
        });
    });

    group.finish();
}

// ============================================================================
// Memory Usage Estimation
// ============================================================================

fn bench_memory_per_vector(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_estimation");
    group.sample_size(10);

    // This benchmark measures the overhead of storing vectors by timing
    // batch insertions (memory allocation dominates at scale)
    for dim in [128, 384, 768, 1536] {
        let batch_size = 1000;

        group.throughput(Throughput::Elements(batch_size));

        group.bench_with_input(
            BenchmarkId::new("flat_batch_insert", format!("{}d_x{}", dim, batch_size)),
            &dim,
            |b, &dim| {
                b.iter(|| {
                    let index = FlatIndex::new(dim, DistanceMetric::Cosine);
                    for i in 0..batch_size {
                        let vec = deterministic_embedding(dim, i);
                        let _ = index.add(VectorId::new(format!("v{}", i)), &vec);
                    }
                    black_box(index.len());
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// VectorStore (Multi-Index Manager) Benchmarks
// ============================================================================

fn bench_vector_store(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_store");
    group.sample_size(30);

    let dim = 384;
    let store = VectorStore::new();
    store
        .create_index(VectorIndexConfig::hnsw("embeddings", dim))
        .expect("create index should succeed");

    // Pre-populate
    for i in 0..5000 {
        let vec = deterministic_embedding(dim, i as u64);
        store
            .add("embeddings", VectorId::new(format!("v{}", i)), &vec)
            .expect("add should succeed");
    }

    group.bench_function("store_add", |b| {
        let mut counter = 10_000u64;
        b.iter(|| {
            let vec = deterministic_embedding(dim, counter);
            let _ = store.add(
                "embeddings",
                VectorId::new(format!("bench_{}", counter)),
                black_box(&vec),
            );
            counter += 1;
        });
    });

    group.bench_function("store_search_k10", |b| {
        let mut query_idx = 0u64;
        b.iter(|| {
            let query = deterministic_embedding(dim, 200_000 + query_idx);
            let results = store.search("embeddings", black_box(&query), 10);
            black_box(results);
            query_idx += 1;
        });
    });

    group.bench_function("store_search_k50", |b| {
        let mut query_idx = 0u64;
        b.iter(|| {
            let query = deterministic_embedding(dim, 300_000 + query_idx);
            let results = store.search("embeddings", black_box(&query), 50);
            black_box(results);
            query_idx += 1;
        });
    });

    group.finish();
}

// ============================================================================
// HNSW Parameter Tuning Benchmarks
// ============================================================================

fn bench_hnsw_parameters(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_parameters");
    group.sample_size(20);

    let dim = 384;
    let dataset_size = 3000;
    let k = 10;

    // Vary M parameter
    for m in [8, 16, 32] {
        let index = HnswIndex::with_params(dim, DistanceMetric::Cosine, m, 200, 50);
        for i in 0..dataset_size {
            let vec = deterministic_embedding(dim, i as u64);
            let _ = index.add(VectorId::new(format!("v{}", i)), &vec);
        }

        group.bench_with_input(
            BenchmarkId::new("m_param_search", format!("m={}", m)),
            &m,
            |b, _| {
                let mut query_idx = 0u64;
                b.iter(|| {
                    let query = deterministic_embedding(dim, 100_000 + query_idx);
                    let results = index.search(black_box(&query), black_box(k));
                    black_box(results);
                    query_idx += 1;
                });
            },
        );
    }

    // Vary ef_search parameter
    let index = HnswIndex::with_params(dim, DistanceMetric::Cosine, 16, 200, 50);
    for i in 0..dataset_size {
        let vec = deterministic_embedding(dim, i as u64);
        let _ = index.add(VectorId::new(format!("v{}", i)), &vec);
    }

    for ef in [20, 50, 100, 200] {
        index.set_ef_search(ef);

        group.bench_with_input(
            BenchmarkId::new("ef_search", format!("ef={}", ef)),
            &ef,
            |b, _| {
                let mut query_idx = 0u64;
                b.iter(|| {
                    let query = deterministic_embedding(dim, 100_000 + query_idx);
                    let results = index.search(black_box(&query), black_box(k));
                    black_box(results);
                    query_idx += 1;
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_vector_insert,
    bench_knn_search,
    bench_knn_dimensions,
    bench_distance_metrics,
    bench_semantic_cache,
    bench_index_build,
    bench_memory_per_vector,
    bench_vector_store,
    bench_hnsw_parameters,
);

criterion_main!(benches);
