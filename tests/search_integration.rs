#![allow(clippy::unwrap_used)]
//! Integration tests for ferrite-search.
//!
//! Validates that the search engine crate can be created, indexes built,
//! and queries executed independently of the server.

use ferrite_search::search::{Document, Query, SearchEngine, SearchOptions};

#[test]
fn test_search_engine_creation() {
    let engine = SearchEngine::new();
    assert!(engine.is_ok(), "SearchEngine::new() should succeed");
}

#[test]
fn test_search_engine_builder() {
    let engine = SearchEngine::builder()
        .max_indexes(10)
        .default_limit(25)
        .fuzzy_by_default(true)
        .build();
    assert!(
        engine.is_ok(),
        "SearchEngine::builder().build() should succeed"
    );
}

#[test]
fn test_create_index_and_search() {
    let engine = SearchEngine::new().expect("engine should create");

    // Create an index
    let index = engine.create_index("test_idx");
    assert!(index.is_ok(), "create_index should succeed");
    let index = index.unwrap();

    // Add a document
    let doc = Document::new("doc1")
        .field("title", "Hello World")
        .field("body", "This is a test document");
    let doc_result = index.index_document(doc);
    assert!(doc_result.is_ok(), "index_document should succeed");

    // Search for it
    let results = engine.search_with_options(
        Query::term("title", "hello"),
        SearchOptions::with_limit(10).index("test_idx"),
    );
    assert!(results.is_ok(), "search should succeed");

    let hits = results.expect("should have results");
    assert!(!hits.is_empty(), "should find at least one result");
}

#[test]
fn test_search_empty_index() {
    let engine = SearchEngine::new().expect("engine should create");
    let _ = engine.create_index("empty_idx");

    let results = engine.search_with_options(
        Query::term("title", "anything"),
        SearchOptions::with_limit(10).index("empty_idx"),
    );
    assert!(results.is_ok(), "search on empty index should not error");

    let hits = results.expect("should return results");
    assert!(hits.is_empty(), "empty index should return no hits");
}

#[test]
fn test_search_nonexistent_index() {
    let engine = SearchEngine::new().expect("engine should create");
    let results = engine.search_with_options(
        Query::term("title", "query"),
        SearchOptions::with_limit(10).index("nonexistent"),
    );
    assert!(results.is_err(), "search on missing index should error");
}
