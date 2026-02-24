//! Integration tests for ferrite-ai vector search.
//!
//! Validates the vector store, HNSW index construction,
//! and similarity search operations.

use ferrite_ai::vector::{DistanceMetric, HnswIndex, VectorConfig, VectorIndex};

#[test]
fn test_vector_config_defaults() {
    let config = VectorConfig::default();
    assert!(config.enabled, "vectors should be enabled by default");
    assert!(config.max_dimension > 0, "max_dimension should be positive");
}

#[test]
fn test_hnsw_index_creation() {
    let index = HnswIndex::new(384, DistanceMetric::Cosine);
    assert_eq!(index.dimension(), 384, "HNSW index dimension should match");
}

#[test]
fn test_hnsw_add_and_search() {
    let index = HnswIndex::new(4, DistanceMetric::Cosine);

    // Add some vectors
    let v1 = vec![1.0, 0.0, 0.0, 0.0];
    let v2 = vec![0.0, 1.0, 0.0, 0.0];
    let v3 = vec![0.9, 0.1, 0.0, 0.0]; // similar to v1

    assert!(index.add("id1".into(), &v1).is_ok());
    assert!(index.add("id2".into(), &v2).is_ok());
    assert!(index.add("id3".into(), &v3).is_ok());

    // Search for vectors similar to v1
    let query = vec![1.0, 0.0, 0.0, 0.0];
    let results = index.search(&query, 2);
    assert!(results.is_ok(), "search should succeed");

    let hits = results.expect("should have results");
    assert_eq!(hits.len(), 2, "should return top-2 results");
    // id1 should be closest (exact match), id3 second (most similar)
    assert_eq!(hits[0].id.to_string(), "id1", "exact match should be first");
    assert_eq!(
        hits[1].id.to_string(),
        "id3",
        "similar vector should be second"
    );
}

#[test]
fn test_hnsw_dimension_mismatch() {
    let index = HnswIndex::new(4, DistanceMetric::Cosine);

    let wrong_dim = vec![1.0, 2.0]; // 2D instead of 4D
    let result = index.add("bad".into(), &wrong_dim);
    assert!(
        result.is_err(),
        "adding wrong-dimension vector should error"
    );
}
