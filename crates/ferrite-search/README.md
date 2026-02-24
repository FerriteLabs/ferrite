# Ferrite Search

Full-text search engine, auto-indexing, and query routing for Ferrite.

Part of the [Ferrite](https://github.com/ferritelabs/ferrite) workspace.

## Features

- **Full-text search** — BM25 and TF-IDF scoring, boolean/phrase/fuzzy/term queries, faceted search, and result highlighting
- **Hybrid search** — Combine keyword-based and semantic vector search for best-of-both-worlds retrieval
- **AI-powered auto-indexing** — ML-based access-pattern detection with automatic index recommendations, cost modeling, and A/B validation
- **Intelligent query routing** — Adaptive routing strategies (round-robin, least-latency, locality, consistent-hash) with query hints and caching
- **Schema management** — Builder-based schema definitions with automatic inference, versioning, and online/lazy/dual-write evolution strategies

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
ferrite-search = { git = "https://github.com/ferritelabs/ferrite" }
```

### Creating a search index, indexing documents, and querying

```rust,ignore
use ferrite_search::search::{SearchEngine, SearchConfig, Document, Field, FieldType, Query};

fn main() {
    // Initialize the search engine
    let config = SearchConfig::default();
    let engine = SearchEngine::new(config);

    // Define a document and index it
    let doc = Document::new("article:1")
        .add_field(Field::new("title", FieldType::Text, "Getting started with Ferrite"))
        .add_field(Field::new("body", FieldType::Text, "Ferrite is a high-performance key-value store..."))
        .add_field(Field::new("tags", FieldType::Text, "database rust performance"));

    engine.index(doc).expect("indexing failed");

    // Run a full-text query
    let query = Query::boolean()
        .must(Query::term("body", "high-performance"))
        .should(Query::fuzzy("title", "ferite", 1))
        .build();

    let results = engine.search(&query, 10).expect("search failed");

    for hit in &results.hits {
        println!("id={}, score={:.4}", hit.id, hit.score);
        if let Some(snippet) = &hit.highlight {
            println!("  ...{}...", snippet);
        }
    }
}
```

### Using auto-indexing

```rust,ignore
use ferrite_search::autoindex::{AutoIndexEngine, AutoIndexConfig};

fn main() {
    let config = AutoIndexConfig::default();
    let auto = AutoIndexEngine::new(config);

    // The engine observes access patterns and recommends indexes
    let recommendations = auto.recommend();
    for rec in &recommendations {
        println!(
            "Suggested index: {:?} (confidence: {:?})",
            rec.index_type, rec.confidence
        );
    }
}
```

## Module Overview

| Module | Description |
|--------|-------------|
| `search` | `SearchEngine`, `Query`, `BooleanQuery`, `PhraseQuery`, `FuzzyQuery`, `Document`, `Analyzer`, `BM25Scorer`, `Highlighter`, `HybridSearchEngine` — Full-text indexing, scoring, and hybrid search |
| `autoindex` | `AutoIndexEngine`, `AutoIndexConfig`, `PatternCollector`, `Recommender`, `AccessPredictor`, `CostModel`, `ABTest` — ML-driven automatic index creation and validation |
| `routing` | `QueryRouter`, `RouterConfig`, `RouteDecision`, `QueryAnalyzer`, `RoutingStrategy`, `QueryHint` — Adaptive query routing with metrics tracking |
| `schema` | `Schema`, `SchemaBuilder`, `SchemaRegistry`, `SchemaEvolution`, `SchemaMigrator`, `SchemaInferrer`, `SchemaValidator` — Schema definition, versioning, inference, and migration |

## License

Apache-2.0 — see [LICENSE](../../LICENSE) for details.
