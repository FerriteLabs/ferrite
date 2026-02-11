//! Query Types for Full-Text Search
//!
//! Provides query parsing and execution.

use super::*;

/// Query types
#[derive(Debug, Clone)]
pub enum Query {
    /// Single term query
    Term(TermQuery),
    /// Boolean query with must/should/must_not
    Boolean(BooleanQuery),
    /// Phrase query
    Phrase(PhraseQuery),
    /// Fuzzy query (edit distance)
    Fuzzy(FuzzyQuery),
    /// Prefix query
    Prefix(PrefixQuery),
    /// Wildcard query
    Wildcard(WildcardQuery),
    /// Range query
    Range(RangeQuery),
    /// Match all documents
    MatchAll,
}

impl Query {
    /// Create a term query
    pub fn term(field: impl Into<String>, term: impl Into<String>) -> Self {
        Query::Term(TermQuery {
            field: field.into(),
            term: term.into(),
            boost: 1.0,
        })
    }

    /// Create a phrase query
    pub fn phrase(field: impl Into<String>, phrase: impl Into<String>) -> Self {
        Query::Phrase(PhraseQuery {
            field: field.into(),
            phrase: phrase.into(),
            slop: 0,
            boost: 1.0,
        })
    }

    /// Create a boolean AND query
    pub fn and(queries: Vec<Query>) -> Self {
        Query::Boolean(BooleanQuery {
            must: queries,
            should: Vec::new(),
            must_not: Vec::new(),
            minimum_should_match: 1,
            boost: 1.0,
        })
    }

    /// Create a boolean OR query
    pub fn or(queries: Vec<Query>) -> Self {
        Query::Boolean(BooleanQuery {
            must: Vec::new(),
            should: queries,
            must_not: Vec::new(),
            minimum_should_match: 1,
            boost: 1.0,
        })
    }

    /// Create a match all query
    pub fn match_all() -> Self {
        Query::MatchAll
    }

    /// Apply boost
    pub fn boost(self, boost: f32) -> Self {
        match self {
            Query::Term(mut q) => {
                q.boost = boost;
                Query::Term(q)
            }
            Query::Boolean(mut q) => {
                q.boost = boost;
                Query::Boolean(q)
            }
            Query::Phrase(mut q) => {
                q.boost = boost;
                Query::Phrase(q)
            }
            Query::Fuzzy(mut q) => {
                q.boost = boost;
                Query::Fuzzy(q)
            }
            Query::Prefix(mut q) => {
                q.boost = boost;
                Query::Prefix(q)
            }
            Query::Wildcard(mut q) => {
                q.boost = boost;
                Query::Wildcard(q)
            }
            Query::Range(mut q) => {
                q.boost = boost;
                Query::Range(q)
            }
            Query::MatchAll => Query::MatchAll,
        }
    }
}

/// Single term query
#[derive(Debug, Clone)]
pub struct TermQuery {
    /// Field to search
    pub field: String,
    /// Term to match
    pub term: String,
    /// Query boost
    pub boost: f32,
}

impl TermQuery {
    /// Create a new term query
    pub fn new(field: impl Into<String>, term: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            term: term.into(),
            boost: 1.0,
        }
    }
}

/// Boolean query with must/should/must_not
#[derive(Debug, Clone)]
pub struct BooleanQuery {
    /// Must match (AND)
    pub must: Vec<Query>,
    /// Should match (OR)
    pub should: Vec<Query>,
    /// Must not match (NOT)
    pub must_not: Vec<Query>,
    /// Minimum should clauses to match
    pub minimum_should_match: usize,
    /// Query boost
    pub boost: f32,
}

impl BooleanQuery {
    /// Create a new boolean query
    pub fn new() -> Self {
        Self {
            must: Vec::new(),
            should: Vec::new(),
            must_not: Vec::new(),
            minimum_should_match: 1,
            boost: 1.0,
        }
    }

    /// Add must clause
    pub fn must(mut self, query: Query) -> Self {
        self.must.push(query);
        self
    }

    /// Add should clause
    pub fn should(mut self, query: Query) -> Self {
        self.should.push(query);
        self
    }

    /// Add must_not clause
    pub fn must_not(mut self, query: Query) -> Self {
        self.must_not.push(query);
        self
    }

    /// Set minimum should match
    pub fn minimum_should_match(mut self, min: usize) -> Self {
        self.minimum_should_match = min;
        self
    }
}

impl Default for BooleanQuery {
    fn default() -> Self {
        Self::new()
    }
}

/// Phrase query for exact phrase matching
#[derive(Debug, Clone)]
pub struct PhraseQuery {
    /// Field to search
    pub field: String,
    /// Phrase to match
    pub phrase: String,
    /// Allowed slop (distance between terms)
    pub slop: u32,
    /// Query boost
    pub boost: f32,
}

impl PhraseQuery {
    /// Create a new phrase query
    pub fn new(field: impl Into<String>, phrase: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            phrase: phrase.into(),
            slop: 0,
            boost: 1.0,
        }
    }

    /// Set slop
    pub fn slop(mut self, slop: u32) -> Self {
        self.slop = slop;
        self
    }
}

/// Fuzzy query for approximate matching
#[derive(Debug, Clone)]
pub struct FuzzyQuery {
    /// Field to search
    pub field: String,
    /// Term to match approximately
    pub term: String,
    /// Maximum edit distance
    pub max_distance: u8,
    /// Query boost
    pub boost: f32,
}

impl FuzzyQuery {
    /// Create a new fuzzy query
    pub fn new(field: impl Into<String>, term: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            term: term.into(),
            max_distance: 2,
            boost: 1.0,
        }
    }

    /// Set maximum distance
    pub fn max_distance(mut self, distance: u8) -> Self {
        self.max_distance = distance;
        self
    }
}

/// Prefix query
#[derive(Debug, Clone)]
pub struct PrefixQuery {
    /// Field to search
    pub field: String,
    /// Prefix to match
    pub prefix: String,
    /// Query boost
    pub boost: f32,
}

impl PrefixQuery {
    /// Create a new prefix query
    pub fn new(field: impl Into<String>, prefix: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            prefix: prefix.into(),
            boost: 1.0,
        }
    }
}

/// Wildcard query
#[derive(Debug, Clone)]
pub struct WildcardQuery {
    /// Field to search
    pub field: String,
    /// Pattern with * and ? wildcards
    pub pattern: String,
    /// Query boost
    pub boost: f32,
}

impl WildcardQuery {
    /// Create a new wildcard query
    pub fn new(field: impl Into<String>, pattern: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            pattern: pattern.into(),
            boost: 1.0,
        }
    }
}

/// Range query for numeric/date fields
#[derive(Debug, Clone)]
pub struct RangeQuery {
    /// Field to search
    pub field: String,
    /// Minimum value (inclusive)
    pub min: Option<String>,
    /// Maximum value (inclusive)
    pub max: Option<String>,
    /// Include minimum
    pub include_min: bool,
    /// Include maximum
    pub include_max: bool,
    /// Query boost
    pub boost: f32,
}

impl RangeQuery {
    /// Create a new range query
    pub fn new(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            min: None,
            max: None,
            include_min: true,
            include_max: true,
            boost: 1.0,
        }
    }

    /// Set minimum value
    pub fn gte(mut self, value: impl Into<String>) -> Self {
        self.min = Some(value.into());
        self.include_min = true;
        self
    }

    /// Set minimum value (exclusive)
    pub fn gt(mut self, value: impl Into<String>) -> Self {
        self.min = Some(value.into());
        self.include_min = false;
        self
    }

    /// Set maximum value
    pub fn lte(mut self, value: impl Into<String>) -> Self {
        self.max = Some(value.into());
        self.include_max = true;
        self
    }

    /// Set maximum value (exclusive)
    pub fn lt(mut self, value: impl Into<String>) -> Self {
        self.max = Some(value.into());
        self.include_max = false;
        self
    }
}

/// Query parser
pub struct QueryParser {
    default_field: String,
}

impl QueryParser {
    /// Create a new query parser
    pub fn new() -> Self {
        Self {
            default_field: "_all".to_string(),
        }
    }

    /// Set default field
    pub fn default_field(mut self, field: impl Into<String>) -> Self {
        self.default_field = field.into();
        self
    }

    /// Parse a query string
    pub fn parse(query: &str) -> Result<Query> {
        let parser = QueryParser::new();
        parser.parse_query(query)
    }

    /// Parse query string
    pub fn parse_query(&self, query: &str) -> Result<Query> {
        let query = query.trim();

        if query.is_empty() {
            return Ok(Query::MatchAll);
        }

        // Handle quoted phrases
        if query.starts_with('"') && query.ends_with('"') {
            let phrase = &query[1..query.len() - 1];
            return Ok(Query::phrase(&self.default_field, phrase));
        }

        // Handle field:value syntax
        if let Some(colon_pos) = query.find(':') {
            let field = &query[..colon_pos];
            let value = &query[colon_pos + 1..];

            // Check for phrase in field
            if value.starts_with('"') && value.ends_with('"') {
                let phrase = &value[1..value.len() - 1];
                return Ok(Query::phrase(field, phrase));
            }

            // Check for wildcard
            if value.contains('*') || value.contains('?') {
                return Ok(Query::Wildcard(WildcardQuery::new(field, value)));
            }

            // Check for fuzzy ~
            if let Some(term) = value.strip_suffix('~') {
                return Ok(Query::Fuzzy(FuzzyQuery::new(field, term)));
            }

            return Ok(Query::term(field, value));
        }

        // Handle AND/OR/NOT
        let parts: Vec<&str> = query.split_whitespace().collect();

        if parts.len() == 1 {
            // Single term
            let term = parts[0];

            // Check for wildcard
            if term.contains('*') || term.contains('?') {
                return Ok(Query::Wildcard(WildcardQuery::new(
                    &self.default_field,
                    term,
                )));
            }

            // Check for fuzzy
            if let Some(t) = term.strip_suffix('~') {
                return Ok(Query::Fuzzy(FuzzyQuery::new(&self.default_field, t)));
            }

            return Ok(Query::term(&self.default_field, term));
        }

        // Parse complex query
        let mut must = Vec::new();
        let mut should = Vec::new();
        let mut must_not = Vec::new();
        let mut current_op = "AND";

        let mut i = 0;
        while i < parts.len() {
            let part = parts[i];

            match part.to_uppercase().as_str() {
                "AND" => {
                    current_op = "AND";
                    i += 1;
                    continue;
                }
                "OR" => {
                    current_op = "OR";
                    i += 1;
                    continue;
                }
                "NOT" => {
                    i += 1;
                    if i < parts.len() {
                        let term_query = Query::term(&self.default_field, parts[i]);
                        must_not.push(term_query);
                    }
                    i += 1;
                    continue;
                }
                _ => {}
            }

            // Handle +/- prefixes
            let (prefix, term) = if let Some(rest) = part.strip_prefix('+') {
                ("+", rest)
            } else if let Some(rest) = part.strip_prefix('-') {
                ("-", rest)
            } else {
                ("", part)
            };

            let term_query = Query::term(&self.default_field, term);

            match prefix {
                "+" => must.push(term_query),
                "-" => must_not.push(term_query),
                _ => match current_op {
                    "AND" => must.push(term_query),
                    "OR" => should.push(term_query),
                    _ => must.push(term_query),
                },
            }

            i += 1;
        }

        // Build final query
        if must.is_empty() && should.is_empty() && must_not.is_empty() {
            Ok(Query::MatchAll)
        } else {
            Ok(Query::Boolean(BooleanQuery {
                must,
                should,
                must_not,
                minimum_should_match: 1,
                boost: 1.0,
            }))
        }
    }
}

impl Default for QueryParser {
    fn default() -> Self {
        Self::new()
    }
}

/// Query builder for fluent query construction
pub struct QueryBuilder {
    query: BooleanQuery,
    default_field: String,
}

impl QueryBuilder {
    /// Create a new query builder
    pub fn new() -> Self {
        Self {
            query: BooleanQuery::new(),
            default_field: "_all".to_string(),
        }
    }

    /// Set default field
    pub fn default_field(mut self, field: impl Into<String>) -> Self {
        self.default_field = field.into();
        self
    }

    /// Add a term that must match
    pub fn must_match(mut self, term: impl Into<String>) -> Self {
        self.query.must.push(Query::term(&self.default_field, term));
        self
    }

    /// Add a term that should match
    pub fn should_match(mut self, term: impl Into<String>) -> Self {
        self.query
            .should
            .push(Query::term(&self.default_field, term));
        self
    }

    /// Add a term that must not match
    pub fn must_not_match(mut self, term: impl Into<String>) -> Self {
        self.query
            .must_not
            .push(Query::term(&self.default_field, term));
        self
    }

    /// Add a phrase that must match
    pub fn must_phrase(mut self, phrase: impl Into<String>) -> Self {
        self.query
            .must
            .push(Query::phrase(&self.default_field, phrase));
        self
    }

    /// Add a fuzzy term
    pub fn fuzzy(mut self, term: impl Into<String>, distance: u8) -> Self {
        self.query.must.push(Query::Fuzzy(FuzzyQuery {
            field: self.default_field.clone(),
            term: term.into(),
            max_distance: distance,
            boost: 1.0,
        }));
        self
    }

    /// Set minimum should match
    pub fn minimum_should_match(mut self, min: usize) -> Self {
        self.query.minimum_should_match = min;
        self
    }

    /// Build the query
    pub fn build(self) -> Query {
        Query::Boolean(self.query)
    }
}

impl Default for QueryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_term_query() {
        let query = Query::term("title", "hello");
        if let Query::Term(q) = query {
            assert_eq!(q.field, "title");
            assert_eq!(q.term, "hello");
        } else {
            panic!("Expected TermQuery");
        }
    }

    #[test]
    fn test_phrase_query() {
        let query = Query::phrase("body", "hello world");
        if let Query::Phrase(q) = query {
            assert_eq!(q.field, "body");
            assert_eq!(q.phrase, "hello world");
        } else {
            panic!("Expected PhraseQuery");
        }
    }

    #[test]
    fn test_boolean_query() {
        let query = BooleanQuery::new()
            .must(Query::term("title", "hello"))
            .should(Query::term("body", "world"))
            .must_not(Query::term("status", "draft"));

        assert_eq!(query.must.len(), 1);
        assert_eq!(query.should.len(), 1);
        assert_eq!(query.must_not.len(), 1);
    }

    #[test]
    fn test_fuzzy_query() {
        let query = FuzzyQuery::new("title", "helo").max_distance(2);
        assert_eq!(query.field, "title");
        assert_eq!(query.term, "helo");
        assert_eq!(query.max_distance, 2);
    }

    #[test]
    fn test_query_parser_simple() {
        let query = QueryParser::parse("hello").unwrap();
        if let Query::Term(q) = query {
            assert_eq!(q.term, "hello");
        } else {
            panic!("Expected TermQuery");
        }
    }

    #[test]
    fn test_query_parser_phrase() {
        let query = QueryParser::parse("\"hello world\"").unwrap();
        if let Query::Phrase(q) = query {
            assert_eq!(q.phrase, "hello world");
        } else {
            panic!("Expected PhraseQuery");
        }
    }

    #[test]
    fn test_query_parser_field() {
        let query = QueryParser::parse("title:hello").unwrap();
        if let Query::Term(q) = query {
            assert_eq!(q.field, "title");
            assert_eq!(q.term, "hello");
        } else {
            panic!("Expected TermQuery");
        }
    }

    #[test]
    fn test_query_parser_and() {
        let query = QueryParser::parse("hello AND world").unwrap();
        if let Query::Boolean(q) = query {
            assert_eq!(q.must.len(), 2);
        } else {
            panic!("Expected BooleanQuery");
        }
    }

    #[test]
    fn test_query_parser_not() {
        let query = QueryParser::parse("hello NOT world").unwrap();
        if let Query::Boolean(q) = query {
            assert_eq!(q.must.len(), 1);
            assert_eq!(q.must_not.len(), 1);
        } else {
            panic!("Expected BooleanQuery");
        }
    }

    #[test]
    fn test_query_builder() {
        let query = QueryBuilder::new()
            .default_field("body")
            .must_match("hello")
            .should_match("world")
            .must_not_match("goodbye")
            .build();

        if let Query::Boolean(q) = query {
            assert_eq!(q.must.len(), 1);
            assert_eq!(q.should.len(), 1);
            assert_eq!(q.must_not.len(), 1);
        } else {
            panic!("Expected BooleanQuery");
        }
    }

    #[test]
    fn test_query_boost() {
        let query = Query::term("title", "hello").boost(2.0);
        if let Query::Term(q) = query {
            assert_eq!(q.boost, 2.0);
        } else {
            panic!("Expected TermQuery");
        }
    }

    #[test]
    fn test_range_query() {
        let query = RangeQuery::new("price").gte("100").lte("500");
        assert_eq!(query.min, Some("100".to_string()));
        assert_eq!(query.max, Some("500".to_string()));
        assert!(query.include_min);
        assert!(query.include_max);
    }
}
