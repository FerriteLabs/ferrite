//! Full-Text Search Engine for Ferrite
//!
//! A high-performance full-text search engine supporting:
//! - Inverted index with efficient updates
//! - Advanced text analysis (tokenization, stemming, stop words)
//! - Boolean queries (AND, OR, NOT)
//! - Phrase search and proximity queries
//! - Fuzzy search with Levenshtein distance
//! - TF-IDF and BM25 ranking
//! - Faceted search and aggregations
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────┐
//! │                    Full-Text Search Engine                    │
//! ├──────────────────────────────────────────────────────────────┤
//! │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐   │
//! │  │   Analyzer   │  │   Indexer    │  │  Query Parser    │   │
//! │  │  (Tokenize)  │──│  (Inverted)  │──│  (Boolean/Phrase)│   │
//! │  └──────────────┘  └──────────────┘  └──────────────────┘   │
//! │         │                 │                   │              │
//! │  ┌──────▼──────┐  ┌──────▼──────┐  ┌─────────▼─────────┐   │
//! │  │  Tokenizer  │  │   Scorer    │  │     Highlighter   │   │
//! │  │  Normalizer │  │  (BM25)     │  │    (Snippets)     │   │
//! │  └─────────────┘  └─────────────┘  └───────────────────┘   │
//! └──────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use ferrite::search::{SearchEngine, Document, Query};
//!
//! let engine = SearchEngine::new()?;
//!
//! // Index documents
//! engine.index(Document::new("doc1")
//!     .field("title", "Hello World")
//!     .field("body", "This is a test document"))?;
//!
//! // Search
//! let results = engine.search(Query::parse("hello AND world")?)?;
//!
//! for result in results {
//!     println!("{}: {}", result.doc_id, result.score);
//! }
//! ```

pub mod analyzer;
pub mod document;
pub mod facet;
pub mod highlight;
pub mod hybrid;
pub mod index;
pub mod query;
pub mod scorer;
pub mod suggest;

pub use analyzer::{
    Analyzer, AnalyzerBuilder, ConfigurableAnalyzer, EdgeNgramTokenizer, PatternTokenizer,
    StandardAnalyzer, TokenFilter, Tokenizer,
};
pub mod language;
pub use document::{Document, DocumentId, Field, FieldType};
pub use facet::{Facet, FacetResult, FacetValue};
pub use highlight::{HighlightConfig, Highlighter, Snippet};
pub use hybrid::{
    FusionStrategy, HybridSearchConfig, HybridSearchEngine, HybridSearchResult,
    HybridSearchResults, HybridSearchStats,
};
pub use index::{Index, IndexConfig, IndexReader, IndexWriter, Posting, Term};
pub use language::{Language, LanguageAnalyzer, LanguageDetector};
pub use query::{BooleanQuery, FuzzyQuery, PhraseQuery, Query, QueryParser, TermQuery};
pub use scorer::{BM25Scorer, Scorer, ScoringParams, TfIdfScorer};
pub use suggest::{SuggestConfig, Suggester, Suggestion};

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Search engine error types
#[derive(Debug, thiserror::Error)]
pub enum SearchError {
    /// Document not found
    #[error("document not found: {0}")]
    DocumentNotFound(String),

    /// Invalid query
    #[error("invalid query: {0}")]
    InvalidQuery(String),

    /// Index error
    #[error("index error: {0}")]
    IndexError(String),

    /// Analysis error
    #[error("analysis error: {0}")]
    AnalysisError(String),

    /// Field not found
    #[error("field not found: {0}")]
    FieldNotFound(String),

    /// Storage error
    #[error("storage error: {0}")]
    StorageError(String),
}

/// Result type for search operations
pub type Result<T> = std::result::Result<T, SearchError>;

/// Main search engine
pub struct SearchEngine {
    /// Indexes by name
    indexes: RwLock<HashMap<String, Arc<Index>>>,
    /// Default analyzer
    default_analyzer: Arc<dyn Analyzer>,
    /// Configuration
    config: SearchConfig,
    /// Metrics
    metrics: SearchMetrics,
}

/// Search engine configuration
#[derive(Debug, Clone)]
pub struct SearchConfig {
    /// Maximum number of indexes
    pub max_indexes: usize,
    /// Maximum documents per index
    pub max_documents_per_index: usize,
    /// Default result limit
    pub default_limit: usize,
    /// Enable fuzzy search by default
    pub fuzzy_by_default: bool,
    /// Default fuzzy distance
    pub default_fuzzy_distance: u8,
    /// Enable highlighting by default
    pub highlight_by_default: bool,
    /// Snippet length for highlighting
    pub snippet_length: usize,
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            max_indexes: 100,
            max_documents_per_index: 10_000_000,
            default_limit: 10,
            fuzzy_by_default: false,
            default_fuzzy_distance: 2,
            highlight_by_default: false,
            snippet_length: 150,
        }
    }
}

/// Search engine metrics
#[derive(Debug, Default)]
pub struct SearchMetrics {
    /// Total documents indexed
    pub documents_indexed: std::sync::atomic::AtomicU64,
    /// Total searches performed
    pub searches_performed: std::sync::atomic::AtomicU64,
    /// Total search time in milliseconds
    pub total_search_time_ms: std::sync::atomic::AtomicU64,
    /// Cache hits
    pub cache_hits: std::sync::atomic::AtomicU64,
    /// Cache misses
    pub cache_misses: std::sync::atomic::AtomicU64,
}

impl SearchEngine {
    /// Create a new search engine
    pub fn new() -> Result<Self> {
        Self::with_config(SearchConfig::default())
    }

    /// Create a new search engine with configuration
    pub fn with_config(config: SearchConfig) -> Result<Self> {
        Ok(Self {
            indexes: RwLock::new(HashMap::new()),
            default_analyzer: Arc::new(StandardAnalyzer::new()),
            config,
            metrics: SearchMetrics::default(),
        })
    }

    /// Create a builder
    pub fn builder() -> SearchEngineBuilder {
        SearchEngineBuilder::new()
    }

    /// Create a new index
    pub fn create_index(&self, name: &str) -> Result<Arc<Index>> {
        self.create_index_with_config(name, IndexConfig::default())
    }

    /// Create a new index with configuration
    pub fn create_index_with_config(&self, name: &str, config: IndexConfig) -> Result<Arc<Index>> {
        let mut indexes = self.indexes.write();

        if indexes.len() >= self.config.max_indexes {
            return Err(SearchError::IndexError(
                "maximum indexes reached".to_string(),
            ));
        }

        if indexes.contains_key(name) {
            return Err(SearchError::IndexError(format!(
                "index '{}' already exists",
                name
            )));
        }

        let index = Arc::new(Index::new(
            name.to_string(),
            config,
            Arc::clone(&self.default_analyzer),
        ));
        indexes.insert(name.to_string(), Arc::clone(&index));

        Ok(index)
    }

    /// Get an index by name
    pub fn get_index(&self, name: &str) -> Option<Arc<Index>> {
        self.indexes.read().get(name).cloned()
    }

    /// Delete an index
    pub fn delete_index(&self, name: &str) -> Result<()> {
        self.indexes
            .write()
            .remove(name)
            .ok_or_else(|| SearchError::IndexError(format!("index '{}' not found", name)))?;
        Ok(())
    }

    /// List all indexes
    pub fn list_indexes(&self) -> Vec<String> {
        self.indexes.read().keys().cloned().collect()
    }

    /// Index a document (shorthand for default index)
    pub fn index(&self, doc: Document) -> Result<()> {
        let index = self.get_or_create_default_index()?;
        index.index_document(doc)?;
        self.metrics
            .documents_indexed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Search across all indexes
    pub fn search(&self, query: Query) -> Result<SearchResults> {
        self.search_with_options(query, SearchOptions::default())
    }

    /// Search with options
    pub fn search_with_options(
        &self,
        query: Query,
        options: SearchOptions,
    ) -> Result<SearchResults> {
        let start = std::time::Instant::now();
        let mut all_results = Vec::new();

        let indexes = self.indexes.read();
        let target_indexes: Vec<_> = if let Some(ref index_name) = options.index {
            indexes.get(index_name).into_iter().cloned().collect()
        } else {
            indexes.values().cloned().collect()
        };

        for index in target_indexes {
            let results = index.search(&query, &options)?;
            all_results.extend(results);
        }

        // Sort by score
        all_results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Apply limit
        let limit = options.limit.unwrap_or(self.config.default_limit);
        all_results.truncate(limit);

        let elapsed = start.elapsed();
        self.metrics
            .searches_performed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.metrics.total_search_time_ms.fetch_add(
            elapsed.as_millis() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        let total_hits = all_results.len();
        Ok(SearchResults {
            hits: all_results,
            total_hits,
            took_ms: elapsed.as_millis() as u64,
        })
    }

    /// Execute a query string
    pub fn query(&self, query_str: &str) -> Result<SearchResults> {
        let query = QueryParser::parse(query_str)?;
        self.search(query)
    }

    /// Get or create the default index
    fn get_or_create_default_index(&self) -> Result<Arc<Index>> {
        const DEFAULT_INDEX: &str = "_default";

        if let Some(index) = self.get_index(DEFAULT_INDEX) {
            return Ok(index);
        }

        self.create_index(DEFAULT_INDEX)
    }

    /// Get metrics
    pub fn metrics(&self) -> &SearchMetrics {
        &self.metrics
    }

    /// Get total documents across all indexes
    pub fn total_documents(&self) -> u64 {
        self.indexes
            .read()
            .values()
            .map(|idx| idx.document_count() as u64)
            .sum()
    }
}

impl Default for SearchEngine {
    fn default() -> Self {
        Self::new().expect("failed to create search engine")
    }
}

/// Builder for SearchEngine
pub struct SearchEngineBuilder {
    config: SearchConfig,
    analyzer: Option<Arc<dyn Analyzer>>,
}

impl SearchEngineBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            config: SearchConfig::default(),
            analyzer: None,
        }
    }

    /// Set configuration
    pub fn config(mut self, config: SearchConfig) -> Self {
        self.config = config;
        self
    }

    /// Set maximum indexes
    pub fn max_indexes(mut self, max: usize) -> Self {
        self.config.max_indexes = max;
        self
    }

    /// Set default limit
    pub fn default_limit(mut self, limit: usize) -> Self {
        self.config.default_limit = limit;
        self
    }

    /// Enable fuzzy search by default
    pub fn fuzzy_by_default(mut self, enabled: bool) -> Self {
        self.config.fuzzy_by_default = enabled;
        self
    }

    /// Set analyzer
    pub fn analyzer(mut self, analyzer: Arc<dyn Analyzer>) -> Self {
        self.analyzer = Some(analyzer);
        self
    }

    /// Build the search engine
    pub fn build(self) -> Result<SearchEngine> {
        let mut engine = SearchEngine::with_config(self.config)?;
        if let Some(analyzer) = self.analyzer {
            engine.default_analyzer = analyzer;
        }
        Ok(engine)
    }
}

impl Default for SearchEngineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Search options
#[derive(Debug, Clone, Default)]
pub struct SearchOptions {
    /// Specific index to search
    pub index: Option<String>,
    /// Maximum results
    pub limit: Option<usize>,
    /// Offset for pagination
    pub offset: Option<usize>,
    /// Fields to search
    pub fields: Option<Vec<String>>,
    /// Enable highlighting
    pub highlight: bool,
    /// Fields to highlight
    pub highlight_fields: Option<Vec<String>>,
    /// Enable fuzzy matching
    pub fuzzy: bool,
    /// Fuzzy distance
    pub fuzzy_distance: Option<u8>,
    /// Minimum score threshold
    pub min_score: Option<f32>,
    /// Sort order
    pub sort: Option<SortOrder>,
    /// Facets to compute
    pub facets: Option<Vec<String>>,
}

impl SearchOptions {
    /// Create with limit
    pub fn with_limit(limit: usize) -> Self {
        Self {
            limit: Some(limit),
            ..Default::default()
        }
    }

    /// Set index
    pub fn index(mut self, index: impl Into<String>) -> Self {
        self.index = Some(index.into());
        self
    }

    /// Enable highlighting
    pub fn highlight(mut self, enabled: bool) -> Self {
        self.highlight = enabled;
        self
    }

    /// Enable fuzzy search
    pub fn fuzzy(mut self, enabled: bool) -> Self {
        self.fuzzy = enabled;
        self
    }
}

/// Sort order
#[derive(Debug, Clone, Default)]
pub enum SortOrder {
    /// Sort by score (default)
    #[default]
    Score,
    /// Sort by field ascending
    FieldAsc(String),
    /// Sort by field descending
    FieldDesc(String),
}

/// Search results
#[derive(Debug, Clone)]
pub struct SearchResults {
    /// Matching documents
    pub hits: Vec<SearchHit>,
    /// Total number of hits
    pub total_hits: usize,
    /// Time taken in milliseconds
    pub took_ms: u64,
}

impl SearchResults {
    /// Get number of results
    pub fn len(&self) -> usize {
        self.hits.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.hits.is_empty()
    }

    /// Iterate over hits
    pub fn iter(&self) -> impl Iterator<Item = &SearchHit> {
        self.hits.iter()
    }
}

/// A single search result
#[derive(Debug, Clone)]
pub struct SearchHit {
    /// Document ID
    pub doc_id: DocumentId,
    /// Relevance score
    pub score: f32,
    /// Index name
    pub index: String,
    /// Highlighted snippets
    pub highlights: Option<HashMap<String, Vec<String>>>,
    /// Document source (if requested)
    pub source: Option<Document>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_engine_creation() {
        let engine = SearchEngine::new().unwrap();
        assert_eq!(engine.list_indexes().len(), 0);
    }

    #[test]
    fn test_create_index() {
        let engine = SearchEngine::new().unwrap();
        engine.create_index("test").unwrap();

        assert!(engine.get_index("test").is_some());
        assert_eq!(engine.list_indexes().len(), 1);
    }

    #[test]
    fn test_delete_index() {
        let engine = SearchEngine::new().unwrap();
        engine.create_index("test").unwrap();
        engine.delete_index("test").unwrap();

        assert!(engine.get_index("test").is_none());
    }

    #[test]
    fn test_index_document() {
        let engine = SearchEngine::new().unwrap();

        let doc = Document::new("doc1")
            .field("title", "Hello World")
            .field("body", "This is a test");

        engine.index(doc).unwrap();
        assert_eq!(engine.total_documents(), 1);
    }

    #[test]
    fn test_search_options() {
        let options = SearchOptions::with_limit(20)
            .index("myindex")
            .highlight(true)
            .fuzzy(true);

        assert_eq!(options.limit, Some(20));
        assert_eq!(options.index, Some("myindex".to_string()));
        assert!(options.highlight);
        assert!(options.fuzzy);
    }

    #[test]
    fn test_builder() {
        let engine = SearchEngine::builder()
            .max_indexes(50)
            .default_limit(25)
            .fuzzy_by_default(true)
            .build()
            .unwrap();

        assert!(engine.config.fuzzy_by_default);
    }
}
