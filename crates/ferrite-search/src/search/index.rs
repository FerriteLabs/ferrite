//! Inverted Index for Full-Text Search
//!
//! Provides the core indexing data structure.

use super::analyzer::{Analyzer, Token};
use super::document::{Document, DocumentId, FieldType, FieldValue};
use super::query::Query;
use super::scorer::{BM25Scorer, Scorer};
use super::*;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Index configuration
#[derive(Debug, Clone)]
pub struct IndexConfig {
    /// Maximum documents
    pub max_documents: usize,
    /// Enable term positions (for phrase queries)
    pub store_positions: bool,
    /// Enable term frequencies
    pub store_frequencies: bool,
    /// Default field for queries
    pub default_field: String,
    /// Field configurations
    pub field_configs: HashMap<String, FieldConfig>,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            max_documents: 10_000_000,
            store_positions: true,
            store_frequencies: true,
            default_field: "_all".to_string(),
            field_configs: HashMap::new(),
        }
    }
}

/// Field configuration
#[derive(Debug, Clone)]
pub struct FieldConfig {
    /// Field type
    pub field_type: FieldType,
    /// Custom analyzer name
    pub analyzer: Option<String>,
    /// Boost factor
    pub boost: f32,
    /// Store field value
    pub stored: bool,
    /// Index field for searching
    pub indexed: bool,
}

impl Default for FieldConfig {
    fn default() -> Self {
        Self {
            field_type: FieldType::Text,
            analyzer: None,
            boost: 1.0,
            stored: true,
            indexed: true,
        }
    }
}

/// A term in the index
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Term {
    /// Field name
    pub field: String,
    /// Term text
    pub text: String,
}

impl Term {
    /// Create a new term
    pub fn new(field: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            text: text.into(),
        }
    }
}

impl std::fmt::Display for Term {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.field, self.text)
    }
}

/// A posting (document reference) in the inverted index
#[derive(Debug, Clone)]
pub struct Posting {
    /// Document ID
    pub doc_id: DocumentId,
    /// Internal document number
    pub doc_num: u32,
    /// Term frequency in document
    pub frequency: u32,
    /// Positions of term in document
    pub positions: Vec<u32>,
    /// Field boost
    pub field_boost: f32,
}

impl Posting {
    /// Create a new posting
    pub fn new(doc_id: DocumentId, doc_num: u32) -> Self {
        Self {
            doc_id,
            doc_num,
            frequency: 1,
            positions: Vec::new(),
            field_boost: 1.0,
        }
    }

    /// Add a position
    pub fn add_position(&mut self, position: u32) {
        self.positions.push(position);
        self.frequency = self.positions.len() as u32;
    }
}

/// The main index structure
pub struct Index {
    /// Index name
    name: String,
    /// Configuration
    #[allow(dead_code)] // Planned for v0.2 — stored for index config-driven behavior
    config: IndexConfig,
    /// Analyzer
    analyzer: Arc<dyn Analyzer>,
    /// Inverted index: term -> postings
    inverted_index: RwLock<HashMap<Term, Vec<Posting>>>,
    /// Document store
    documents: RwLock<HashMap<DocumentId, Document>>,
    /// Document ID to internal number mapping
    doc_id_map: RwLock<HashMap<DocumentId, u32>>,
    /// Next document number
    next_doc_num: RwLock<u32>,
    /// Field document counts
    field_doc_counts: RwLock<HashMap<String, usize>>,
    /// Total field lengths
    field_lengths: RwLock<HashMap<String, u64>>,
    /// Scorer
    scorer: Arc<dyn Scorer>,
    /// Metrics
    metrics: IndexMetrics,
    /// Per-document field lengths for BM25
    doc_field_lengths: RwLock<HashMap<DocumentId, HashMap<String, u32>>>,
}

/// Index metrics
#[derive(Debug, Default)]
pub struct IndexMetrics {
    /// Documents indexed
    pub documents_indexed: std::sync::atomic::AtomicU64,
    /// Documents deleted
    pub documents_deleted: std::sync::atomic::AtomicU64,
    /// Terms indexed
    pub terms_indexed: std::sync::atomic::AtomicU64,
    /// Searches performed
    pub searches: std::sync::atomic::AtomicU64,
}

/// Per-document field lengths for accurate BM25 scoring
#[derive(Debug, Default)]
#[allow(dead_code)] // Planned for v0.2 — will replace inline HashMap for BM25 field lengths
struct DocFieldLengths {
    /// Map of doc_id -> field_name -> length
    lengths: HashMap<DocumentId, HashMap<String, u32>>,
}

impl Index {
    /// Create a new index
    pub fn new(name: String, config: IndexConfig, analyzer: Arc<dyn Analyzer>) -> Self {
        Self {
            name,
            config,
            analyzer,
            inverted_index: RwLock::new(HashMap::new()),
            documents: RwLock::new(HashMap::new()),
            doc_id_map: RwLock::new(HashMap::new()),
            next_doc_num: RwLock::new(0),
            field_doc_counts: RwLock::new(HashMap::new()),
            field_lengths: RwLock::new(HashMap::new()),
            scorer: Arc::new(BM25Scorer::default()),
            metrics: IndexMetrics::default(),
            doc_field_lengths: RwLock::new(HashMap::new()),
        }
    }

    /// Get the index name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Index a document
    pub fn index_document(&self, doc: Document) -> Result<()> {
        let doc_id = doc.id.clone();

        // Assign document number
        let doc_num = {
            let mut next = self.next_doc_num.write();
            let num = *next;
            *next += 1;
            num
        };

        // Track per-document field lengths for BM25
        let mut doc_lengths: HashMap<String, u32> = HashMap::new();

        // Index each field
        for (field_name, field) in &doc.fields {
            if !field.indexed {
                continue;
            }

            // Analyze field value
            let tokens = if field.field_type == FieldType::Keyword {
                vec![Token::new(field.value.to_string_value(), 0, 0, 0)]
            } else if let Some(text) = field.as_text() {
                self.analyzer.analyze(text)
            } else {
                continue;
            };

            // Track document field length
            doc_lengths.insert(field_name.clone(), tokens.len() as u32);

            // Update field statistics
            {
                let mut counts = self.field_doc_counts.write();
                *counts.entry(field_name.clone()).or_insert(0) += 1;
            }
            {
                let mut lengths = self.field_lengths.write();
                *lengths.entry(field_name.clone()).or_insert(0) += tokens.len() as u64;
            }

            // Add tokens to inverted index
            let mut inverted = self.inverted_index.write();
            for token in tokens {
                let term = Term::new(field_name.clone(), token.text.clone());

                let postings = inverted.entry(term).or_default();

                // Find or create posting for this document
                if let Some(posting) = postings.iter_mut().find(|p| p.doc_id == doc_id) {
                    posting.add_position(token.position as u32);
                } else {
                    let mut posting = Posting::new(doc_id.clone(), doc_num);
                    posting.add_position(token.position as u32);
                    posting.field_boost = field.boost;
                    postings.push(posting);
                }

                self.metrics
                    .terms_indexed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }

        // Store document and field lengths
        {
            let mut docs = self.documents.write();
            let mut id_map = self.doc_id_map.write();
            let mut field_lengths_map = self.doc_field_lengths.write();
            docs.insert(doc_id.clone(), doc);
            id_map.insert(doc_id.clone(), doc_num);
            field_lengths_map.insert(doc_id, doc_lengths);
        }

        self.metrics
            .documents_indexed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        Ok(())
    }

    /// Delete a document
    pub fn delete_document(&self, doc_id: &DocumentId) -> Result<bool> {
        let mut docs = self.documents.write();
        let mut id_map = self.doc_id_map.write();
        let mut inverted = self.inverted_index.write();
        let mut field_lengths_map = self.doc_field_lengths.write();

        if docs.remove(doc_id).is_some() {
            id_map.remove(doc_id);
            field_lengths_map.remove(doc_id);

            // Remove from inverted index
            for postings in inverted.values_mut() {
                postings.retain(|p| &p.doc_id != doc_id);
            }

            self.metrics
                .documents_deleted
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Get a document by ID
    pub fn get_document(&self, doc_id: &DocumentId) -> Option<Document> {
        self.documents.read().get(doc_id).cloned()
    }

    /// Search the index
    pub fn search(&self, query: &Query, options: &SearchOptions) -> Result<Vec<SearchHit>> {
        self.metrics
            .searches
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let matching_docs = self.find_matching_documents(query)?;
        let mut hits = Vec::new();

        let doc_count = self.document_count();
        let inverted = self.inverted_index.read();
        let docs = self.documents.read();
        let doc_field_lengths = self.doc_field_lengths.read();

        for (doc_id, term_matches) in matching_docs {
            let mut score = 0.0f32;

            for (term, posting) in &term_matches {
                // Get document frequency
                let df = inverted.get(term).map(|p| p.len()).unwrap_or(0);

                // Get actual document length for this field
                let doc_length = doc_field_lengths
                    .get(&doc_id)
                    .and_then(|fields| fields.get(&term.field))
                    .copied()
                    .unwrap_or(100);

                // Get average field length
                let avg_doc_length = self.avg_field_length(&term.field);
                let avg_len = if avg_doc_length > 0.0 {
                    avg_doc_length
                } else {
                    100.0
                };

                // Calculate score using BM25 parameters
                let term_score = self.scorer.score(
                    posting.frequency,
                    df as u32,
                    doc_length,
                    avg_len,
                    doc_count as u32,
                );

                score += term_score * posting.field_boost;
            }

            // Apply document boost
            if let Some(doc) = docs.get(&doc_id) {
                score *= doc.boost;
            }

            // Check minimum score
            if let Some(min_score) = options.min_score {
                if score < min_score {
                    continue;
                }
            }

            hits.push(SearchHit {
                doc_id: doc_id.clone(),
                score,
                index: self.name.clone(),
                highlights: None,
                source: if options.highlight {
                    docs.get(&doc_id).cloned()
                } else {
                    None
                },
            });
        }

        // Sort by score
        hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        Ok(hits)
    }

    /// Find documents matching query
    fn find_matching_documents(
        &self,
        query: &Query,
    ) -> Result<HashMap<DocumentId, Vec<(Term, Posting)>>> {
        let inverted = self.inverted_index.read();
        let mut results: HashMap<DocumentId, Vec<(Term, Posting)>> = HashMap::new();

        match query {
            Query::Term(term_query) => {
                let term = Term::new(&term_query.field, &term_query.term);
                if let Some(postings) = inverted.get(&term) {
                    for posting in postings {
                        results
                            .entry(posting.doc_id.clone())
                            .or_default()
                            .push((term.clone(), posting.clone()));
                    }
                }
            }
            Query::Boolean(bool_query) => {
                // Handle must clauses (AND)
                let mut must_docs: Option<HashSet<DocumentId>> = None;

                for clause in &bool_query.must {
                    let clause_docs = self.find_matching_documents(clause)?;
                    let doc_set: HashSet<_> = clause_docs.keys().cloned().collect();

                    match must_docs {
                        Some(ref mut existing) => {
                            existing.retain(|d| doc_set.contains(d));
                        }
                        None => {
                            must_docs = Some(doc_set);
                        }
                    }

                    // Merge term matches
                    for (doc_id, terms) in clause_docs {
                        if must_docs.as_ref().map_or(true, |m| m.contains(&doc_id)) {
                            results.entry(doc_id).or_default().extend(terms);
                        }
                    }
                }

                // Filter out must_not clauses (NOT)
                for clause in &bool_query.must_not {
                    let not_docs = self.find_matching_documents(clause)?;
                    for doc_id in not_docs.keys() {
                        results.remove(doc_id);
                    }
                }

                // Handle should clauses (OR)
                if !bool_query.should.is_empty() && bool_query.must.is_empty() {
                    for clause in &bool_query.should {
                        let clause_docs = self.find_matching_documents(clause)?;
                        for (doc_id, terms) in clause_docs {
                            results.entry(doc_id).or_default().extend(terms);
                        }
                    }
                }

                // Apply minimum_should_match if there are must clauses
                if !bool_query.must.is_empty() && !bool_query.should.is_empty() {
                    let _ = bool_query.minimum_should_match;
                    for clause in &bool_query.should {
                        let clause_docs = self.find_matching_documents(clause)?;
                        for (doc_id, terms) in clause_docs {
                            if let Some(existing) = results.get_mut(&doc_id) {
                                existing.extend(terms);
                            }
                        }
                    }
                }
            }
            Query::Phrase(phrase_query) => {
                // Find documents containing all terms in sequence
                let tokens = self.analyzer.analyze(&phrase_query.phrase);
                if tokens.is_empty() {
                    return Ok(results);
                }

                let slop = phrase_query.slop as i32;
                let first_term = Term::new(&phrase_query.field, &tokens[0].text);
                let first_postings = match inverted.get(&first_term) {
                    Some(p) => p.clone(),
                    None => return Ok(results),
                };

                'doc_loop: for posting in first_postings {
                    // Collect all term positions for this document
                    let mut term_positions: Vec<Vec<u32>> = vec![posting.positions.clone()];

                    for token in tokens.iter().skip(1) {
                        let term = Term::new(&phrase_query.field, &token.text);
                        if let Some(term_postings) = inverted.get(&term) {
                            if let Some(doc_posting) =
                                term_postings.iter().find(|p| p.doc_id == posting.doc_id)
                            {
                                term_positions.push(doc_posting.positions.clone());
                            } else {
                                continue 'doc_loop;
                            }
                        } else {
                            continue 'doc_loop;
                        }
                    }

                    // Check if positions form a valid phrase (with slop tolerance)
                    let mut found_phrase = false;
                    'position_loop: for &start_pos in &term_positions[0] {
                        let mut expected_pos = start_pos as i32;
                        for (_i, positions) in term_positions.iter().enumerate().skip(1) {
                            expected_pos += 1;
                            // Check if any position is within slop distance
                            let mut found_next = false;
                            for &pos in positions {
                                let diff = (pos as i32 - expected_pos).abs();
                                if diff <= slop {
                                    expected_pos = pos as i32;
                                    found_next = true;
                                    break;
                                }
                            }
                            if !found_next {
                                continue 'position_loop;
                            }
                        }
                        found_phrase = true;
                        break;
                    }

                    if found_phrase {
                        results
                            .entry(posting.doc_id.clone())
                            .or_default()
                            .push((first_term.clone(), posting.clone()));
                    }
                }
            }
            Query::Fuzzy(fuzzy_query) => {
                // Find terms within edit distance
                let target = &fuzzy_query.term.to_lowercase();
                let max_distance = fuzzy_query.max_distance as usize;

                for (term, postings) in inverted.iter() {
                    if term.field == fuzzy_query.field {
                        let distance = levenshtein(&term.text, target);
                        if distance <= max_distance {
                            for posting in postings {
                                results
                                    .entry(posting.doc_id.clone())
                                    .or_default()
                                    .push((term.clone(), posting.clone()));
                            }
                        }
                    }
                }
            }
            Query::Prefix(prefix_query) => {
                for (term, postings) in inverted.iter() {
                    if term.field == prefix_query.field
                        && term.text.starts_with(&prefix_query.prefix)
                    {
                        for posting in postings {
                            results
                                .entry(posting.doc_id.clone())
                                .or_default()
                                .push((term.clone(), posting.clone()));
                        }
                    }
                }
            }
            Query::Wildcard(wildcard_query) => {
                let pattern = &wildcard_query.pattern;
                for (term, postings) in inverted.iter() {
                    if term.field == wildcard_query.field && matches_wildcard(&term.text, pattern) {
                        for posting in postings {
                            results
                                .entry(posting.doc_id.clone())
                                .or_default()
                                .push((term.clone(), posting.clone()));
                        }
                    }
                }
            }
            Query::MatchAll => {
                let docs = self.documents.read();
                for doc_id in docs.keys() {
                    results.entry(doc_id.clone()).or_default();
                }
            }
            Query::Range(range_query) => {
                // Range queries iterate through documents and compare field values
                let docs = self.documents.read();
                for (doc_id, doc) in docs.iter() {
                    if let Some(field) = doc.fields.get(&range_query.field) {
                        let in_range = match &field.value {
                            FieldValue::Number(num) => {
                                let min_ok = match &range_query.min {
                                    Some(min_str) => {
                                        if let Ok(min_val) = min_str.parse::<f64>() {
                                            if range_query.include_min {
                                                *num >= min_val
                                            } else {
                                                *num > min_val
                                            }
                                        } else {
                                            false
                                        }
                                    }
                                    None => true,
                                };
                                let max_ok = match &range_query.max {
                                    Some(max_str) => {
                                        if let Ok(max_val) = max_str.parse::<f64>() {
                                            if range_query.include_max {
                                                *num <= max_val
                                            } else {
                                                *num < max_val
                                            }
                                        } else {
                                            false
                                        }
                                    }
                                    None => true,
                                };
                                min_ok && max_ok
                            }
                            FieldValue::Date(timestamp) => {
                                let min_ok = match &range_query.min {
                                    Some(min_str) => {
                                        if let Ok(min_val) = min_str.parse::<i64>() {
                                            if range_query.include_min {
                                                *timestamp >= min_val
                                            } else {
                                                *timestamp > min_val
                                            }
                                        } else {
                                            false
                                        }
                                    }
                                    None => true,
                                };
                                let max_ok = match &range_query.max {
                                    Some(max_str) => {
                                        if let Ok(max_val) = max_str.parse::<i64>() {
                                            if range_query.include_max {
                                                *timestamp <= max_val
                                            } else {
                                                *timestamp < max_val
                                            }
                                        } else {
                                            false
                                        }
                                    }
                                    None => true,
                                };
                                min_ok && max_ok
                            }
                            FieldValue::Text(text) => {
                                // String comparison for text/keyword fields
                                let min_ok = match &range_query.min {
                                    Some(min_str) => {
                                        if range_query.include_min {
                                            text.as_str() >= min_str.as_str()
                                        } else {
                                            text.as_str() > min_str.as_str()
                                        }
                                    }
                                    None => true,
                                };
                                let max_ok = match &range_query.max {
                                    Some(max_str) => {
                                        if range_query.include_max {
                                            text.as_str() <= max_str.as_str()
                                        } else {
                                            text.as_str() < max_str.as_str()
                                        }
                                    }
                                    None => true,
                                };
                                min_ok && max_ok
                            }
                            _ => false, // Boolean, Binary, Array, Null don't support range
                        };

                        if in_range {
                            // Add to results with empty postings (range matches don't have term positions)
                            results.entry(doc_id.clone()).or_default();
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    /// Get document count
    pub fn document_count(&self) -> usize {
        self.documents.read().len()
    }

    /// Get term count
    pub fn term_count(&self) -> usize {
        self.inverted_index.read().len()
    }

    /// Get field document count
    pub fn field_doc_count(&self, field: &str) -> usize {
        self.field_doc_counts
            .read()
            .get(field)
            .cloned()
            .unwrap_or(0)
    }

    /// Get average field length
    pub fn avg_field_length(&self, field: &str) -> f32 {
        let doc_count = self.field_doc_count(field);
        if doc_count == 0 {
            return 0.0;
        }
        let total_length = self.field_lengths.read().get(field).cloned().unwrap_or(0);
        total_length as f32 / doc_count as f32
    }

    /// Get metrics
    pub fn metrics(&self) -> &IndexMetrics {
        &self.metrics
    }
}

/// Index writer for batch operations
pub struct IndexWriter {
    index: Arc<Index>,
    batch: Vec<Document>,
    batch_size: usize,
}

impl IndexWriter {
    /// Create a new index writer
    pub fn new(index: Arc<Index>) -> Self {
        Self {
            index,
            batch: Vec::new(),
            batch_size: 1000,
        }
    }

    /// Set batch size
    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Add a document
    pub fn add(&mut self, doc: Document) -> Result<()> {
        self.batch.push(doc);
        if self.batch.len() >= self.batch_size {
            self.flush()?;
        }
        Ok(())
    }

    /// Flush pending documents
    pub fn flush(&mut self) -> Result<()> {
        for doc in self.batch.drain(..) {
            self.index.index_document(doc)?;
        }
        Ok(())
    }

    /// Commit and close writer
    pub fn commit(mut self) -> Result<()> {
        self.flush()
    }
}

/// Index reader for search operations
pub struct IndexReader {
    index: Arc<Index>,
}

impl IndexReader {
    /// Create a new index reader
    pub fn new(index: Arc<Index>) -> Self {
        Self { index }
    }

    /// Search
    pub fn search(&self, query: &Query, options: &SearchOptions) -> Result<Vec<SearchHit>> {
        self.index.search(query, options)
    }

    /// Get document
    pub fn get(&self, doc_id: &DocumentId) -> Option<Document> {
        self.index.get_document(doc_id)
    }

    /// Get document count
    pub fn num_docs(&self) -> usize {
        self.index.document_count()
    }
}

/// Levenshtein distance
fn levenshtein(a: &str, b: &str) -> usize {
    let a_chars: Vec<char> = a.chars().collect();
    let b_chars: Vec<char> = b.chars().collect();
    let m = a_chars.len();
    let n = b_chars.len();

    if m == 0 {
        return n;
    }
    if n == 0 {
        return m;
    }

    let mut dp = vec![vec![0; n + 1]; m + 1];

    for (i, row) in dp.iter_mut().enumerate().take(m + 1) {
        row[0] = i;
    }
    for j in 0..=n {
        dp[0][j] = j;
    }

    for i in 1..=m {
        for j in 1..=n {
            let cost = if a_chars[i - 1] == b_chars[j - 1] {
                0
            } else {
                1
            };
            dp[i][j] = (dp[i - 1][j] + 1)
                .min(dp[i][j - 1] + 1)
                .min(dp[i - 1][j - 1] + cost);
        }
    }

    dp[m][n]
}

/// Simple wildcard matching
fn matches_wildcard(text: &str, pattern: &str) -> bool {
    let text_chars: Vec<char> = text.chars().collect();
    let pattern_chars: Vec<char> = pattern.chars().collect();

    fn helper(text: &[char], pattern: &[char]) -> bool {
        match (text.is_empty(), pattern.is_empty()) {
            (true, true) => true,
            (_, true) => false,
            (true, false) => pattern.iter().all(|&c| c == '*'),
            _ => {
                let t = text[0];
                let p = pattern[0];
                match p {
                    '*' => helper(text, &pattern[1..]) || helper(&text[1..], pattern),
                    '?' => helper(&text[1..], &pattern[1..]),
                    _ => t == p && helper(&text[1..], &pattern[1..]),
                }
            }
        }
    }

    helper(&text_chars, &pattern_chars)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::analyzer::StandardAnalyzer;

    fn create_test_index() -> Index {
        Index::new(
            "test".to_string(),
            IndexConfig::default(),
            Arc::new(StandardAnalyzer::new()),
        )
    }

    #[test]
    fn test_index_document() {
        let index = create_test_index();

        let doc = Document::new("doc1")
            .field("title", "Hello World")
            .field("body", "This is a test document");

        index.index_document(doc).unwrap();

        assert_eq!(index.document_count(), 1);
        assert!(index.term_count() > 0);
    }

    #[test]
    fn test_get_document() {
        let index = create_test_index();

        let doc = Document::new("doc1").field("title", "Test");
        index.index_document(doc).unwrap();

        let retrieved = index.get_document(&DocumentId::new("doc1"));
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().get_text("title"), Some("Test"));
    }

    #[test]
    fn test_delete_document() {
        let index = create_test_index();

        let doc = Document::new("doc1").field("title", "Test");
        index.index_document(doc).unwrap();

        assert!(index.delete_document(&DocumentId::new("doc1")).unwrap());
        assert_eq!(index.document_count(), 0);
    }

    #[test]
    fn test_term() {
        let term = Term::new("title", "hello");
        assert_eq!(term.field, "title");
        assert_eq!(term.text, "hello");
    }

    #[test]
    fn test_levenshtein() {
        assert_eq!(levenshtein("hello", "hello"), 0);
        assert_eq!(levenshtein("hello", "hallo"), 1);
        assert_eq!(levenshtein("hello", "world"), 4);
        assert_eq!(levenshtein("", "test"), 4);
    }

    #[test]
    fn test_wildcard_matching() {
        assert!(matches_wildcard("hello", "hello"));
        assert!(matches_wildcard("hello", "h*"));
        assert!(matches_wildcard("hello", "*llo"));
        assert!(matches_wildcard("hello", "h?llo"));
        assert!(matches_wildcard("hello", "*"));
        assert!(!matches_wildcard("hello", "world"));
    }

    #[test]
    fn test_index_writer() {
        let index = Arc::new(create_test_index());
        let mut writer = IndexWriter::new(Arc::clone(&index)).batch_size(10);

        for i in 0..5 {
            let doc = Document::new(format!("doc{}", i)).field("title", format!("Document {}", i));
            writer.add(doc).unwrap();
        }

        writer.commit().unwrap();
        assert_eq!(index.document_count(), 5);
    }
}
