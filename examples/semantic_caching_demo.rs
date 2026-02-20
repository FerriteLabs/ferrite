//! Semantic Caching Demo
//!
//! Demonstrates how Ferrite's semantic caching can reduce LLM API costs
//! by caching responses based on query similarity.
//!
//! This self-contained example simulates the full semantic caching pipeline:
//! - Cosine similarity on pre-computed vectors for semantic matching
//! - Cache-aside pattern for LLM queries (hit → cached, miss → API call)
//! - Benchmark of 1000 queries with realistic hit-rate distribution
//! - Cost savings analysis with P50/P99 latency breakdown
//!
//! No external services or the `onnx` feature are required.
//!
//! Run: cargo run --example semantic_caching_demo

use std::collections::HashMap;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Simulated embedding helpers
// ---------------------------------------------------------------------------
// In production, Ferrite generates embeddings via its configured provider
// (OpenAI, Cohere, or local ONNX). This demo uses deterministic mock
// embeddings so it runs without any external service or API key.
// ---------------------------------------------------------------------------

/// Generate a deterministic mock embedding from a seed.
/// Each topic gets a unique unit vector; queries for the same topic produce
/// the same embedding, simulating paraphrases that map to identical meaning.
fn mock_embedding(dim: usize, seed: u64) -> Vec<f32> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut emb = Vec::with_capacity(dim);
    for i in 0..dim {
        let mut h = DefaultHasher::new();
        (seed, i).hash(&mut h);
        let v = ((h.finish() as f64 / u64::MAX as f64) * 2.0 - 1.0) as f32;
        emb.push(v);
    }
    // L2-normalise so cosine similarity is just the dot product.
    let mag: f32 = emb.iter().map(|x| x * x).sum::<f32>().sqrt();
    if mag > 0.0 {
        for x in &mut emb {
            *x /= mag;
        }
    }
    emb
}

/// Cosine similarity between two L2-normalised vectors (== dot product).
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

// ---------------------------------------------------------------------------
// Semantic cache implementation
// ---------------------------------------------------------------------------
// This is a simplified in-memory version of Ferrite's semantic cache.
// The production implementation uses:
//   - HNSW index for O(log n) nearest-neighbor search (see crates/ferrite-ai/src/semantic/hnsw.rs)
//   - DashMap for lock-free concurrent access (see crates/ferrite-ai/src/semantic/cache.rs)
//   - LRU eviction + TTL expiration (see crates/ferrite-ai/src/semantic/semantic_cache.rs)
//   - Circuit breaker and retry patterns (see crates/ferrite-ai/src/semantic/resilience.rs)
// ---------------------------------------------------------------------------

struct CacheEntry {
    #[allow(dead_code)]
    query: String,
    response: String,
    embedding: Vec<f32>,
}

struct SemanticCache {
    entries: Vec<CacheEntry>,
    threshold: f32,
}

impl SemanticCache {
    fn new(threshold: f32) -> Self {
        Self {
            entries: Vec::new(),
            threshold,
        }
    }

    /// Look up the best match above `threshold`. Returns (response, similarity).
    fn get(&self, query_embedding: &[f32]) -> Option<(&str, f32)> {
        let mut best: Option<(usize, f32)> = None;
        for (i, entry) in self.entries.iter().enumerate() {
            let sim = cosine_similarity(query_embedding, &entry.embedding);
            if sim >= self.threshold && best.map_or(true, |(_, s)| sim > s) {
                best = Some((i, sim));
            }
        }
        best.map(|(i, sim)| (self.entries[i].response.as_str(), sim))
    }

    fn insert(&mut self, query: String, response: String, embedding: Vec<f32>) {
        self.entries.push(CacheEntry {
            query,
            response,
            embedding,
        });
    }
}

// ---------------------------------------------------------------------------
// Simulated LLM API
// ---------------------------------------------------------------------------
// In production, this would be a real HTTP call to OpenAI, Anthropic, etc.
// The simulated 200ms latency represents a typical LLM API round-trip.
// ---------------------------------------------------------------------------

/// Pretend to call an LLM. Adds ~200 ms latency to simulate a real API round-trip.
fn simulate_llm_call(query: &str) -> String {
    std::thread::sleep(Duration::from_millis(200));
    format!("[LLM response for '{}']", query)
}

// ---------------------------------------------------------------------------
// Benchmark harness
// ---------------------------------------------------------------------------
// Simulates a realistic workload: 1,000 queries with power-law topic
// distribution (popular topics get more queries). ~60% of queries reuse
// an existing topic embedding (simulating paraphrases), while ~40% are
// novel questions that miss the cache.
// ---------------------------------------------------------------------------

/// Percentile helper – expects a **sorted** slice.
fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn main() {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║          Ferrite – Semantic Caching Demo / Benchmark        ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    // -- Configuration -------------------------------------------------------
    const DIM: usize = 128;
    const SIMILARITY_THRESHOLD: f32 = 0.85;
    const TOTAL_QUERIES: usize = 1_000;
    const UNIQUE_TOPICS: u64 = 50;
    const COST_PER_1K_TOKENS: f64 = 0.002; // USD
    const AVG_TOKENS_PER_QUERY: f64 = 500.0;

    println!("Configuration:");
    println!("  Embedding dimensions : {}", DIM);
    println!("  Similarity threshold : {:.0}%", SIMILARITY_THRESHOLD * 100.0);
    println!("  Total queries        : {}", TOTAL_QUERIES);
    println!("  Unique topics        : {}", UNIQUE_TOPICS);
    println!(
        "  Cost model           : ${:.4} / 1K tokens, avg {} tokens/query\n",
        COST_PER_1K_TOKENS, AVG_TOKENS_PER_QUERY as u64
    );

    // -- Pre-compute topic embeddings ----------------------------------------
    // Each "topic" has a canonical embedding. We'll create query variants by
    // choosing the same topic seed, ensuring cosine similarity == 1.0 for an
    // exact re-ask and slightly less for a "rephrased" variant (achieved by
    // perturbing the seed).
    let topic_embeddings: HashMap<u64, Vec<f32>> = (0..UNIQUE_TOPICS)
        .map(|t| (t, mock_embedding(DIM, t * 1000)))
        .collect();

    let mut cache = SemanticCache::new(SIMILARITY_THRESHOLD);

    // -- Simulate workload ---------------------------------------------------
    // Realistic distribution: most queries cluster around popular topics.
    // We use a simple power-law-ish distribution via modulo bias.
    let mut hit_latencies_us: Vec<f64> = Vec::new();
    let mut miss_latencies_us: Vec<f64> = Vec::new();
    let mut hits: usize = 0;
    let mut misses: usize = 0;

    let bench_start = Instant::now();

    for i in 0..TOTAL_QUERIES {
        // Pick a topic – skewed towards lower IDs (popular topics).
        let topic_id = ((i as u64 * 7) % UNIQUE_TOPICS) % (UNIQUE_TOPICS / 2 + 1);
        let query_text = format!("query-{}-topic-{}", i, topic_id);

        // For ~60 % of queries, reuse the exact topic embedding (paraphrase).
        // For the rest, generate a unique embedding (novel question).
        let query_emb = if i % 5 < 3 {
            topic_embeddings
                .get(&topic_id)
                .expect("topic must exist")
                .clone()
        } else {
            mock_embedding(DIM, (i as u64 + 1) * 9999)
        };

        let start = Instant::now();

        if let Some((_response, _sim)) = cache.get(&query_emb) {
            // Cache hit – near-zero latency.
            let elapsed = start.elapsed();
            hit_latencies_us.push(elapsed.as_secs_f64() * 1_000_000.0);
            hits += 1;
        } else {
            // Cache miss – call the (simulated) LLM.
            let response = simulate_llm_call(&query_text);
            let elapsed = start.elapsed();
            miss_latencies_us.push(elapsed.as_secs_f64() * 1_000_000.0);
            misses += 1;

            cache.insert(query_text, response, query_emb);
        }

        // Progress indicator every 100 queries.
        if (i + 1) % 100 == 0 {
            print!("\r  Processed {}/{} queries …", i + 1, TOTAL_QUERIES);
        }
    }

    let total_wall = bench_start.elapsed();
    println!("\r  Processed {}/{} queries ✓   \n", TOTAL_QUERIES, TOTAL_QUERIES);

    // -- Sort latency arrays for percentile computation ----------------------
    hit_latencies_us.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    miss_latencies_us.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    // -- Cost analysis -------------------------------------------------------
    let cost_per_query = COST_PER_1K_TOKENS * (AVG_TOKENS_PER_QUERY / 1000.0);
    let cost_without_cache = TOTAL_QUERIES as f64 * cost_per_query;
    let cost_with_cache = misses as f64 * cost_per_query;
    let savings = cost_without_cache - cost_with_cache;
    let savings_pct = if cost_without_cache > 0.0 {
        (savings / cost_without_cache) * 100.0
    } else {
        0.0
    };

    // -- Print results -------------------------------------------------------
    println!("┌──────────────────────────────────────────────────────────────┐");
    println!("│                        Results                              │");
    println!("├──────────────────────────────────────────────────────────────┤");
    println!("│  Total queries        : {:>8}                             │", TOTAL_QUERIES);
    println!("│  Cache hits           : {:>8}  ({:.1}%)                    │", hits, hits as f64 / TOTAL_QUERIES as f64 * 100.0);
    println!("│  Cache misses         : {:>8}  ({:.1}%)                    │", misses, misses as f64 / TOTAL_QUERIES as f64 * 100.0);
    println!("│  Wall-clock time      : {:>8.2}s                           │", total_wall.as_secs_f64());
    println!("├──────────────────────────────────────────────────────────────┤");
    println!("│  Latency (cache hit)                                        │");
    println!("│    P50                : {:>10.1} µs                        │", percentile(&hit_latencies_us, 50.0));
    println!("│    P99                : {:>10.1} µs                        │", percentile(&hit_latencies_us, 99.0));
    println!("│  Latency (API call / miss)                                  │");
    println!("│    P50                : {:>10.1} ms                        │", percentile(&miss_latencies_us, 50.0) / 1000.0);
    println!("│    P99                : {:>10.1} ms                        │", percentile(&miss_latencies_us, 99.0) / 1000.0);
    println!("├──────────────────────────────────────────────────────────────┤");
    println!("│  Cost Analysis (@ ${:.4}/1K tokens, {} avg tokens)        │", COST_PER_1K_TOKENS, AVG_TOKENS_PER_QUERY as u64);
    println!("│    Without cache      : ${:>10.2}                          │", cost_without_cache);
    println!("│    With semantic cache: ${:>10.2}                          │", cost_with_cache);
    println!("│    Estimated savings  : ${:>10.2}  ({:.1}%)               │", savings, savings_pct);
    println!("└──────────────────────────────────────────────────────────────┘");
    println!();
    println!("💡 In production, Ferrite replaces the in-memory map with its");
    println!("   built-in HNSW vector index and ONNX embedding pipeline.");
    println!("   See: SEMANTIC.CACHE.SET / SEMANTIC.CACHE.GET commands.");
}
