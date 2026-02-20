//! AI Features Example
//!
//! Demonstrates Ferrite's AI/ML capabilities:
//! - Vector similarity search with HNSW indexes
//! - Semantic caching for LLM response caching
//! - Different distance metrics (cosine, euclidean, dot product)
//! - Index configuration and tuning
//!
//! Note: This example uses mock embeddings. In production, you would use
//! real embedding models (OpenAI, ONNX, etc.)
//!
//! Run with: cargo run --example ai_features

use bytes::Bytes;
use ferrite::ai::semantic::{
    DistanceMetric as SemanticDistanceMetric, EmbeddingModelConfig, EmbeddingModelType,
    IndexType as SemanticIndexType, SemanticCache, SemanticConfig,
};
use ferrite::ai::vector::{DistanceMetric, VectorId, VectorIndexConfig, VectorStore};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Ferrite AI Features Example ===\n");

    // ==================== Vector Store Setup ====================
    println!("--- Vector Store Setup ---");

    let store = VectorStore::new();

    // Create an HNSW index for document embeddings
    // HNSW is best for high-recall similarity search
    let hnsw_config = VectorIndexConfig::hnsw("documents", 384)
        .with_metric(DistanceMetric::Cosine)
        .with_hnsw_m(16) // Connections per node (higher = better recall, more memory)
        .with_ef_construction(200); // Build-time search width (higher = better quality)

    store.create_index(hnsw_config)?;

    println!("Created HNSW index 'documents' with:");
    println!("  Dimension: 384");
    println!("  Metric: Cosine similarity");
    println!("  M: 16, EF Construction: 200");

    // ==================== Adding Vectors ====================
    println!("\n--- Adding Vectors ---");

    // Generate mock embeddings (in production, use a real embedding model)
    let documents = vec![
        (
            "doc:1",
            "Introduction to machine learning",
            generate_mock_embedding(384, 1),
        ),
        (
            "doc:2",
            "Deep learning fundamentals",
            generate_mock_embedding(384, 2),
        ),
        (
            "doc:3",
            "Natural language processing basics",
            generate_mock_embedding(384, 3),
        ),
        (
            "doc:4",
            "Computer vision techniques",
            generate_mock_embedding(384, 4),
        ),
        (
            "doc:5",
            "Reinforcement learning overview",
            generate_mock_embedding(384, 5),
        ),
    ];

    for (id, title, embedding) in &documents {
        store.add("documents", VectorId::new(*id), embedding)?;
        println!("  Added: {} - {}", id, title);
    }

    println!("Added {} document vectors", documents.len());

    // Add more vectors
    for i in 6..=10 {
        let embedding = generate_mock_embedding(384, i as u64);
        store.add("documents", VectorId::new(format!("doc:{}", i)), &embedding)?;
    }
    println!("Added 5 more vectors");

    // ==================== Similarity Search ====================
    println!("\n--- Similarity Search ---");

    // Search for documents similar to a query
    let query_embedding = generate_mock_embedding(384, 1); // Similar to doc:1
    let results = store.search("documents", &query_embedding, 5)?;

    println!("Top 5 similar documents to query:");
    for result in &results {
        println!("  ID: {}, Score: {:.4}", result.id, result.score);
    }

    // ==================== Different Index Types ====================
    println!("\n--- Index Type Comparison ---");

    // Flat index - exact search, best for small datasets (<10K vectors)
    let flat_config =
        VectorIndexConfig::flat("small_dataset", 128).with_metric(DistanceMetric::Euclidean);
    store.create_index(flat_config)?;
    println!("Flat index: O(n) search, exact results, good for <10K vectors");

    // IVF index - approximate, best for large datasets (>1M vectors)
    let ivf_config = VectorIndexConfig::ivf("large_dataset", 512, 1024) // 1024 clusters
        .with_metric(DistanceMetric::DotProduct)
        .with_ivf_n_probe(32); // Clusters to search (higher = better recall)
    store.create_index(ivf_config)?;
    println!("IVF index: O(sqrt(n)) search, approximate, good for >1M vectors");

    // ==================== Distance Metrics ====================
    println!("\n--- Distance Metrics ---");

    println!("Cosine similarity:");
    println!("  - Best for text embeddings");
    println!("  - Measures angle between vectors");
    println!("  - Range: -1 to 1 (normalized: 0 to 1)");

    println!("\nEuclidean distance (L2):");
    println!("  - Best for image embeddings");
    println!("  - Measures straight-line distance");
    println!("  - Range: 0 to infinity");

    println!("\nDot product:");
    println!("  - Best for maximum inner product search");
    println!("  - Fast computation");
    println!("  - Range: -infinity to infinity");

    // ==================== Semantic Cache Setup ====================
    println!("\n--- Semantic Cache Setup ---");

    let cache_config = SemanticConfig {
        enabled: true,
        default_threshold: 0.85, // 85% similarity threshold
        max_entries: 10_000,
        embedding_dim: 384,
        default_ttl_secs: 3600, // 1 hour TTL
        index_type: SemanticIndexType::Hnsw,
        distance_metric: SemanticDistanceMetric::Cosine,
        auto_embed: false, // We'll provide embeddings manually
        embedding_model: EmbeddingModelConfig::default(),
    };

    let cache = SemanticCache::new(cache_config);
    println!("Created semantic cache with:");
    println!("  Threshold: 85%");
    println!("  Max entries: 10,000");
    println!("  TTL: 1 hour");

    // ==================== Semantic Caching Pattern ====================
    println!("\n--- Semantic Caching Pattern ---");

    // Store some Q&A pairs with their embeddings
    let qa_pairs = vec![
        (
            "What is the capital of France?",
            "Paris is the capital of France.",
            generate_mock_embedding(384, 100),
        ),
        (
            "How do I learn Rust?",
            "Start with The Rust Programming Language book (the Book).",
            generate_mock_embedding(384, 101),
        ),
        (
            "What is machine learning?",
            "Machine learning is a subset of AI that enables systems to learn from data.",
            generate_mock_embedding(384, 102),
        ),
    ];

    for (question, answer, embedding) in &qa_pairs {
        cache.set(question, Bytes::from(*answer), embedding, None)?;
    }
    println!("Cached {} Q&A pairs", qa_pairs.len());

    // Query with a similar question
    let query = "France's capital city?";
    let query_embedding = generate_mock_embedding(384, 100); // Similar to first Q&A

    if let Some(hit) = cache.get(&query_embedding, Some(0.85))? {
        println!("\nCache HIT for query: '{}'", query);
        println!(
            "  Cached answer: {}",
            String::from_utf8_lossy(&hit.entry.value)
        );
        println!("  Similarity: {:.1}%", hit.similarity * 100.0);
    } else {
        println!("\nCache MISS for query: '{}'", query);
        println!("  Would call LLM API...");
    }

    // ==================== Embedding Provider Configuration ====================
    println!("\n--- Embedding Provider Configuration ---");

    println!("Local ONNX model (recommended for latency-sensitive apps):");
    let _onnx_config = EmbeddingModelConfig {
        model_type: EmbeddingModelType::Onnx,
        model_path: Some("./models/all-MiniLM-L6-v2.onnx".to_string()),
        batch_size: 32,
        cache_embeddings: true,
        ..Default::default()
    };
    println!("  - No API calls, runs locally");
    println!("  - ~1ms per embedding");
    println!("  - 384 dimensions (all-MiniLM-L6-v2)");

    println!("\nOpenAI API:");
    let _openai_config = EmbeddingModelConfig {
        model_type: EmbeddingModelType::OpenAI,
        api_key: Some("sk-...".to_string()),
        batch_size: 100,
        ..Default::default()
    };
    println!("  - text-embedding-3-small: 1536 dimensions");
    println!("  - ~10-50ms per embedding");
    println!("  - Good quality, easy to use");

    // ==================== Vector Search Best Practices ====================
    println!("\n--- Best Practices ---");

    println!("1. Choose the right index:");
    println!("   - <10K vectors: Flat (exact)");
    println!("   - 10K-1M vectors: HNSW (approximate, high recall)");
    println!("   - >1M vectors: IVF (approximate, memory efficient)");

    println!("\n2. Normalize vectors for cosine similarity");
    let vector = vec![3.0, 4.0];
    let magnitude: f32 = vector.iter().map(|x| x * x).sum::<f32>().sqrt();
    let normalized: Vec<f32> = vector.iter().map(|x| x / magnitude).collect();
    println!("   Original: {:?}", vector);
    println!("   Normalized: {:?}", normalized);

    println!("\n3. Tune HNSW parameters:");
    println!("   - Higher M (16-64): Better recall, more memory");
    println!("   - Higher ef_construction (100-500): Better index quality");
    println!("   - Higher ef_search (50-200): Better query recall");

    println!("\n4. Semantic cache thresholds:");
    println!("   - 0.95+: Very high precision, low recall");
    println!("   - 0.90: Good balance for most apps");
    println!("   - 0.85: Higher recall, production default");
    println!("   - 0.80: High recall, watch for false positives");

    // ==================== Real-World Use Cases ====================
    println!("\n--- Real-World Use Cases ---");

    println!("1. Semantic Search:");
    println!("   - Document retrieval");
    println!("   - FAQ matching");
    println!("   - Code search");

    println!("\n2. Recommendation Systems:");
    println!("   - Product recommendations");
    println!("   - Content suggestions");
    println!("   - User similarity");

    println!("\n3. RAG (Retrieval-Augmented Generation):");
    println!("   - Store document chunks as vectors");
    println!("   - Search for relevant context");
    println!("   - Feed to LLM for generation");

    println!("\n4. LLM Cost Reduction:");
    println!("   - Cache LLM responses by semantic similarity");
    println!("   - 40-60% reduction in API calls");
    println!("   - Faster responses for similar queries");

    // ==================== Cache Statistics ====================
    println!("\n--- Cache Statistics ---");

    let stats = cache.stats();
    println!("Cache stats:");
    println!("  Total entries: {}", stats.entries);
    println!("  Hits: {}", stats.hits);
    println!("  Misses: {}", stats.misses);
    println!("  Hit rate: {:.1}%", stats.hit_rate * 100.0);

    println!("\n=== Example Complete ===");
    Ok(())
}

/// Generate a mock embedding for demonstration purposes.
/// In production, use a real embedding model (ONNX, OpenAI, etc.)
fn generate_mock_embedding(dimension: usize, seed: u64) -> Vec<f32> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut embedding = Vec::with_capacity(dimension);
    for i in 0..dimension {
        let mut hasher = DefaultHasher::new();
        (seed, i).hash(&mut hasher);
        let hash = hasher.finish();
        // Convert hash to f32 in range [-1, 1]
        let value = ((hash as f64 / u64::MAX as f64) * 2.0 - 1.0) as f32;
        embedding.push(value);
    }

    // Normalize the vector for cosine similarity
    let magnitude: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
    if magnitude > 0.0 {
        for x in &mut embedding {
            *x /= magnitude;
        }
    }

    embedding
}
