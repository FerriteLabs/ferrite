//! Text Analysis for Full-Text Search
//!
//! Provides tokenization, normalization, and filtering for text.

use super::*;
use std::collections::HashSet;

/// A token produced by analysis
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Token {
    /// Token text
    pub text: String,
    /// Start offset in original text
    pub start: usize,
    /// End offset in original text
    pub end: usize,
    /// Position in token stream
    pub position: usize,
    /// Token type
    pub token_type: TokenType,
}

impl Token {
    /// Create a new token
    pub fn new(text: impl Into<String>, start: usize, end: usize, position: usize) -> Self {
        Self {
            text: text.into(),
            start,
            end,
            position,
            token_type: TokenType::Word,
        }
    }

    /// Set token type
    pub fn with_type(mut self, token_type: TokenType) -> Self {
        self.token_type = token_type;
        self
    }
}

/// Token types
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum TokenType {
    /// Regular word
    #[default]
    Word,
    /// Number
    Number,
    /// Alphanumeric
    AlphaNum,
    /// Email address
    Email,
    /// URL
    Url,
    /// Punctuation
    Punctuation,
    /// Emoji
    Emoji,
    /// Unknown
    Unknown,
}

/// Analyzer trait for text processing
pub trait Analyzer: Send + Sync {
    /// Analyze text into tokens
    fn analyze(&self, text: &str) -> Vec<Token>;

    /// Get the tokenizer
    fn tokenizer(&self) -> &dyn Tokenizer;

    /// Get token filters
    fn token_filters(&self) -> &[Box<dyn TokenFilter>];
}

/// Tokenizer trait
pub trait Tokenizer: Send + Sync {
    /// Tokenize text
    fn tokenize(&self, text: &str) -> Vec<Token>;
}

/// Token filter trait
pub trait TokenFilter: Send + Sync {
    /// Filter/transform tokens
    fn filter(&self, tokens: Vec<Token>) -> Vec<Token>;
}

/// Standard analyzer with common settings
pub struct StandardAnalyzer {
    tokenizer: StandardTokenizer,
    filters: Vec<Box<dyn TokenFilter>>,
}

impl StandardAnalyzer {
    /// Create a new standard analyzer
    pub fn new() -> Self {
        Self {
            tokenizer: StandardTokenizer::new(),
            filters: vec![
                Box::new(LowercaseFilter),
                Box::new(StopWordFilter::english()),
                Box::new(PorterStemmer::new()),
            ],
        }
    }

    /// Create without stemming
    pub fn without_stemming() -> Self {
        Self {
            tokenizer: StandardTokenizer::new(),
            filters: vec![
                Box::new(LowercaseFilter),
                Box::new(StopWordFilter::english()),
            ],
        }
    }

    /// Create minimal (tokenize + lowercase only)
    pub fn minimal() -> Self {
        Self {
            tokenizer: StandardTokenizer::new(),
            filters: vec![Box::new(LowercaseFilter)],
        }
    }
}

impl Default for StandardAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

impl Analyzer for StandardAnalyzer {
    fn analyze(&self, text: &str) -> Vec<Token> {
        let mut tokens = self.tokenizer.tokenize(text);
        for filter in &self.filters {
            tokens = filter.filter(tokens);
        }
        tokens
    }

    fn tokenizer(&self) -> &dyn Tokenizer {
        &self.tokenizer
    }

    fn token_filters(&self) -> &[Box<dyn TokenFilter>] {
        &self.filters
    }
}

/// Standard tokenizer that splits on whitespace and punctuation
pub struct StandardTokenizer {
    min_length: usize,
    max_length: usize,
}

impl StandardTokenizer {
    /// Create a new standard tokenizer
    pub fn new() -> Self {
        Self {
            min_length: 1,
            max_length: 255,
        }
    }

    /// Set minimum token length
    pub fn min_length(mut self, len: usize) -> Self {
        self.min_length = len;
        self
    }

    /// Set maximum token length
    pub fn max_length(mut self, len: usize) -> Self {
        self.max_length = len;
        self
    }
}

impl Default for StandardTokenizer {
    fn default() -> Self {
        Self::new()
    }
}

impl Tokenizer for StandardTokenizer {
    fn tokenize(&self, text: &str) -> Vec<Token> {
        let mut tokens = Vec::new();
        let mut position = 0;
        let mut start = 0;
        let mut in_word = false;

        for (i, c) in text.char_indices() {
            let is_word_char = c.is_alphanumeric() || c == '_';

            if is_word_char {
                if !in_word {
                    start = i;
                    in_word = true;
                }
            } else if in_word {
                let word = &text[start..i];
                if word.len() >= self.min_length && word.len() <= self.max_length {
                    tokens.push(Token::new(word, start, i, position));
                    position += 1;
                }
                in_word = false;
            }
        }

        // Handle last word
        if in_word {
            let word = &text[start..];
            if word.len() >= self.min_length && word.len() <= self.max_length {
                tokens.push(Token::new(word, start, text.len(), position));
            }
        }

        tokens
    }
}

/// Whitespace tokenizer
pub struct WhitespaceTokenizer;

impl Tokenizer for WhitespaceTokenizer {
    fn tokenize(&self, text: &str) -> Vec<Token> {
        text.split_whitespace()
            .enumerate()
            .map(|(pos, word)| {
                let start = text.find(word).unwrap_or(0);
                Token::new(word, start, start + word.len(), pos)
            })
            .collect()
    }
}

/// Keyword tokenizer (no tokenization)
pub struct KeywordTokenizer;

impl Tokenizer for KeywordTokenizer {
    fn tokenize(&self, text: &str) -> Vec<Token> {
        if text.is_empty() {
            Vec::new()
        } else {
            vec![Token::new(text, 0, text.len(), 0)]
        }
    }
}

/// N-gram tokenizer
pub struct NgramTokenizer {
    min_gram: usize,
    max_gram: usize,
}

impl NgramTokenizer {
    /// Create a new n-gram tokenizer
    pub fn new(min_gram: usize, max_gram: usize) -> Self {
        Self { min_gram, max_gram }
    }
}

impl Tokenizer for NgramTokenizer {
    fn tokenize(&self, text: &str) -> Vec<Token> {
        let mut tokens = Vec::new();
        let chars: Vec<char> = text.chars().collect();
        let mut position = 0;

        for n in self.min_gram..=self.max_gram {
            for i in 0..=chars.len().saturating_sub(n) {
                let gram: String = chars[i..i + n].iter().collect();
                tokens.push(Token::new(gram, i, i + n, position));
                position += 1;
            }
        }

        tokens
    }
}

/// Edge n-gram tokenizer (for autocomplete)
///
/// Generates n-grams from the beginning of each word.
/// For example, "quick" with min=1, max=4 produces: "q", "qu", "qui", "quic"
pub struct EdgeNgramTokenizer {
    min_gram: usize,
    max_gram: usize,
}

impl EdgeNgramTokenizer {
    /// Create a new edge n-gram tokenizer
    pub fn new(min_gram: usize, max_gram: usize) -> Self {
        Self { min_gram, max_gram }
    }
}

impl Tokenizer for EdgeNgramTokenizer {
    fn tokenize(&self, text: &str) -> Vec<Token> {
        let mut tokens = Vec::new();
        let mut position = 0;

        for word in text.split_whitespace() {
            let chars: Vec<char> = word.chars().collect();
            let max = self.max_gram.min(chars.len());
            for n in self.min_gram..=max {
                let gram: String = chars[..n].iter().collect();
                let start = text.find(word).unwrap_or(0);
                tokens.push(Token::new(gram, start, start + n, position));
                position += 1;
            }
        }

        tokens
    }
}

/// Pattern tokenizer (regex-based splitting)
pub struct PatternTokenizer {
    pattern: regex::Regex,
}

impl PatternTokenizer {
    /// Create a new pattern tokenizer with a regex pattern
    pub fn new(pattern: &str) -> std::result::Result<Self, regex::Error> {
        Ok(Self {
            pattern: regex::Regex::new(pattern)?,
        })
    }

    /// Split on whitespace and punctuation (default)
    pub fn default_pattern() -> Self {
        Self {
            pattern: regex::Regex::new(r"[\s\p{Punct}]+").expect("valid regex"),
        }
    }
}

impl Tokenizer for PatternTokenizer {
    fn tokenize(&self, text: &str) -> Vec<Token> {
        let mut tokens = Vec::new();
        let mut position = 0;

        for part in self.pattern.split(text) {
            if !part.is_empty() {
                let start = text.find(part).unwrap_or(0);
                tokens.push(Token::new(part, start, start + part.len(), position));
                position += 1;
            }
        }

        tokens
    }
}

/// Configurable analyzer with pluggable tokenizer and filter pipeline
pub struct ConfigurableAnalyzer {
    tokenizer: Box<dyn Tokenizer>,
    filters: Vec<Box<dyn TokenFilter>>,
}

impl ConfigurableAnalyzer {
    /// Create with a tokenizer and no filters
    pub fn new(tokenizer: Box<dyn Tokenizer>) -> Self {
        Self {
            tokenizer,
            filters: Vec::new(),
        }
    }

    /// Create via the builder
    pub fn builder() -> AnalyzerBuilder {
        AnalyzerBuilder::new()
    }
}

impl Analyzer for ConfigurableAnalyzer {
    fn analyze(&self, text: &str) -> Vec<Token> {
        let mut tokens = self.tokenizer.tokenize(text);
        for filter in &self.filters {
            tokens = filter.filter(tokens);
        }
        tokens
    }

    fn tokenizer(&self) -> &dyn Tokenizer {
        &*self.tokenizer
    }

    fn token_filters(&self) -> &[Box<dyn TokenFilter>] {
        &self.filters
    }
}

/// Builder for constructing a configurable analysis pipeline:
/// tokenize → filter → normalize
pub struct AnalyzerBuilder {
    tokenizer: Option<Box<dyn Tokenizer>>,
    filters: Vec<Box<dyn TokenFilter>>,
}

impl AnalyzerBuilder {
    /// Create a new builder (defaults to StandardTokenizer)
    pub fn new() -> Self {
        Self {
            tokenizer: None,
            filters: Vec::new(),
        }
    }

    /// Set the tokenizer
    pub fn tokenizer(mut self, tokenizer: Box<dyn Tokenizer>) -> Self {
        self.tokenizer = Some(tokenizer);
        self
    }

    /// Add a token filter to the pipeline
    pub fn filter(mut self, filter: Box<dyn TokenFilter>) -> Self {
        self.filters.push(filter);
        self
    }

    /// Build the analyzer
    pub fn build(self) -> ConfigurableAnalyzer {
        ConfigurableAnalyzer {
            tokenizer: self
                .tokenizer
                .unwrap_or_else(|| Box::new(StandardTokenizer::new())),
            filters: self.filters,
        }
    }
}

impl Default for AnalyzerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Lowercase filter
pub struct LowercaseFilter;

impl TokenFilter for LowercaseFilter {
    fn filter(&self, tokens: Vec<Token>) -> Vec<Token> {
        tokens
            .into_iter()
            .map(|mut t| {
                t.text = t.text.to_lowercase();
                t
            })
            .collect()
    }
}

/// Stop word filter
pub struct StopWordFilter {
    stop_words: HashSet<String>,
}

impl StopWordFilter {
    /// Create a new stop word filter
    pub fn new(words: &[&str]) -> Self {
        Self {
            stop_words: words.iter().map(|s| s.to_lowercase()).collect(),
        }
    }

    /// English stop words
    pub fn english() -> Self {
        Self::new(&[
            "a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "has", "he", "in",
            "is", "it", "its", "of", "on", "that", "the", "to", "was", "were", "will", "with",
            "the", "this", "but", "they", "have", "had", "what", "when", "where", "who", "which",
            "why", "how", "all", "each", "every", "both", "few", "more", "most", "other", "some",
            "such", "no", "not", "only", "own", "same", "so", "than", "too", "very",
        ])
    }

    /// Add custom stop words
    pub fn add(&mut self, words: &[&str]) {
        for word in words {
            self.stop_words.insert(word.to_lowercase());
        }
    }
}

impl TokenFilter for StopWordFilter {
    fn filter(&self, tokens: Vec<Token>) -> Vec<Token> {
        tokens
            .into_iter()
            .filter(|t| !self.stop_words.contains(&t.text.to_lowercase()))
            .collect()
    }
}

/// Simple Porter Stemmer
pub struct PorterStemmer;

impl PorterStemmer {
    /// Create a new Porter stemmer
    pub fn new() -> Self {
        Self
    }

    /// Stem a word
    fn stem(&self, word: &str) -> String {
        let mut result = word.to_lowercase();

        // Simple suffix stripping (not full Porter algorithm)
        let suffixes = [
            ("ational", "ate"),
            ("tional", "tion"),
            ("enci", "ence"),
            ("anci", "ance"),
            ("izer", "ize"),
            ("isation", "ize"),
            ("ization", "ize"),
            ("ational", "ate"),
            ("ation", "ate"),
            ("ator", "ate"),
            ("alism", "al"),
            ("iveness", "ive"),
            ("fulness", "ful"),
            ("ousness", "ous"),
            ("aliti", "al"),
            ("iviti", "ive"),
            ("biliti", "ble"),
            ("alli", "al"),
            ("entli", "ent"),
            ("eli", "e"),
            ("ousli", "ous"),
            ("ization", "ize"),
            ("ation", "ate"),
            ("ator", "ate"),
            ("alism", "al"),
            ("iveness", "ive"),
            ("fulness", "ful"),
            ("ousness", "ous"),
            ("aliti", "al"),
            ("iviti", "ive"),
            ("biliti", "ble"),
            ("ing", ""),
            ("ed", ""),
            ("ly", ""),
            ("es", ""),
            ("s", ""),
        ];

        for (suffix, replacement) in suffixes.iter() {
            if result.ends_with(suffix) && result.len() > suffix.len() + 2 {
                result = format!("{}{}", &result[..result.len() - suffix.len()], replacement);
                break;
            }
        }

        result
    }
}

impl Default for PorterStemmer {
    fn default() -> Self {
        Self::new()
    }
}

impl TokenFilter for PorterStemmer {
    fn filter(&self, tokens: Vec<Token>) -> Vec<Token> {
        tokens
            .into_iter()
            .map(|mut t| {
                t.text = self.stem(&t.text);
                t
            })
            .collect()
    }
}

/// Length filter
pub struct LengthFilter {
    min: usize,
    max: usize,
}

impl LengthFilter {
    /// Create a new length filter
    pub fn new(min: usize, max: usize) -> Self {
        Self { min, max }
    }
}

impl TokenFilter for LengthFilter {
    fn filter(&self, tokens: Vec<Token>) -> Vec<Token> {
        tokens
            .into_iter()
            .filter(|t| t.text.len() >= self.min && t.text.len() <= self.max)
            .collect()
    }
}

/// Trim filter
pub struct TrimFilter;

impl TokenFilter for TrimFilter {
    fn filter(&self, tokens: Vec<Token>) -> Vec<Token> {
        tokens
            .into_iter()
            .map(|mut t| {
                t.text = t.text.trim().to_string();
                t
            })
            .filter(|t| !t.text.is_empty())
            .collect()
    }
}

/// ASCII folding filter (removes accents)
pub struct AsciiFoldingFilter;

impl TokenFilter for AsciiFoldingFilter {
    fn filter(&self, tokens: Vec<Token>) -> Vec<Token> {
        tokens
            .into_iter()
            .map(|mut t| {
                t.text = fold_to_ascii(&t.text);
                t
            })
            .collect()
    }
}

/// Simple ASCII folding
fn fold_to_ascii(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'à' | 'á' | 'â' | 'ã' | 'ä' | 'å' => 'a',
            'è' | 'é' | 'ê' | 'ë' => 'e',
            'ì' | 'í' | 'î' | 'ï' => 'i',
            'ò' | 'ó' | 'ô' | 'õ' | 'ö' => 'o',
            'ù' | 'ú' | 'û' | 'ü' => 'u',
            'ç' => 'c',
            'ñ' => 'n',
            'ß' => 's',
            _ => c,
        })
        .collect()
}

/// Synonym filter
pub struct SynonymFilter {
    synonyms: HashMap<String, Vec<String>>,
}

impl SynonymFilter {
    /// Create a new synonym filter
    pub fn new() -> Self {
        Self {
            synonyms: HashMap::new(),
        }
    }

    /// Add synonyms
    pub fn add(&mut self, word: &str, synonyms: &[&str]) {
        self.synonyms.insert(
            word.to_lowercase(),
            synonyms.iter().map(|s| s.to_lowercase()).collect(),
        );
    }

    /// Add bidirectional synonyms
    pub fn add_bidirectional(&mut self, words: &[&str]) {
        for word in words {
            let others: Vec<String> = words
                .iter()
                .filter(|w| *w != word)
                .map(|s| s.to_lowercase())
                .collect();
            self.synonyms.insert(word.to_lowercase(), others);
        }
    }
}

impl Default for SynonymFilter {
    fn default() -> Self {
        Self::new()
    }
}

impl TokenFilter for SynonymFilter {
    fn filter(&self, tokens: Vec<Token>) -> Vec<Token> {
        let mut result = Vec::new();

        for token in tokens {
            result.push(token.clone());

            if let Some(syns) = self.synonyms.get(&token.text.to_lowercase()) {
                for syn in syns {
                    result.push(Token::new(
                        syn.clone(),
                        token.start,
                        token.end,
                        token.position,
                    ));
                }
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_standard_tokenizer() {
        let tokenizer = StandardTokenizer::new();
        let tokens = tokenizer.tokenize("Hello, World! How are you?");

        assert_eq!(tokens.len(), 5);
        assert_eq!(tokens[0].text, "Hello");
        assert_eq!(tokens[1].text, "World");
    }

    #[test]
    fn test_whitespace_tokenizer() {
        let tokenizer = WhitespaceTokenizer;
        let tokens = tokenizer.tokenize("Hello World");

        assert_eq!(tokens.len(), 2);
    }

    #[test]
    fn test_ngram_tokenizer() {
        let tokenizer = NgramTokenizer::new(2, 3);
        let tokens = tokenizer.tokenize("hello");

        // 2-grams: he, el, ll, lo
        // 3-grams: hel, ell, llo
        assert_eq!(tokens.len(), 7);
    }

    #[test]
    fn test_lowercase_filter() {
        let filter = LowercaseFilter;
        let tokens = vec![Token::new("HELLO", 0, 5, 0)];
        let filtered = filter.filter(tokens);

        assert_eq!(filtered[0].text, "hello");
    }

    #[test]
    fn test_stop_word_filter() {
        let filter = StopWordFilter::english();
        let tokens = vec![
            Token::new("the", 0, 3, 0),
            Token::new("quick", 4, 9, 1),
            Token::new("brown", 10, 15, 2),
        ];

        let filtered = filter.filter(tokens);
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].text, "quick");
    }

    #[test]
    fn test_porter_stemmer() {
        let stemmer = PorterStemmer::new();
        let tokens = vec![
            Token::new("running", 0, 7, 0),
            Token::new("jumps", 8, 13, 1),
        ];

        let stemmed = stemmer.filter(tokens);
        assert_eq!(stemmed[0].text, "runn");
        assert_eq!(stemmed[1].text, "jump");
    }

    #[test]
    fn test_standard_analyzer() {
        let analyzer = StandardAnalyzer::new();
        let tokens = analyzer.analyze("The Quick Brown Fox Jumps");

        // "the" should be removed as stop word
        // Words should be lowercased and stemmed
        assert!(tokens.iter().all(|t| t.text == t.text.to_lowercase()));
        assert!(!tokens.iter().any(|t| t.text == "the"));
    }

    #[test]
    fn test_synonym_filter() {
        let mut filter = SynonymFilter::new();
        filter.add_bidirectional(&["quick", "fast", "rapid"]);

        let tokens = vec![Token::new("quick", 0, 5, 0)];
        let filtered = filter.filter(tokens);

        assert_eq!(filtered.len(), 3);
    }

    #[test]
    fn test_ascii_folding() {
        let filter = AsciiFoldingFilter;
        let tokens = vec![Token::new("café", 0, 4, 0)];
        let filtered = filter.filter(tokens);

        assert_eq!(filtered[0].text, "cafe");
    }

    #[test]
    fn test_length_filter() {
        let filter = LengthFilter::new(3, 10);
        let tokens = vec![
            Token::new("a", 0, 1, 0),
            Token::new("hello", 2, 7, 1),
            Token::new("superlongword", 8, 21, 2),
        ];

        let filtered = filter.filter(tokens);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].text, "hello");
    }

    // --- Edge N-gram tokenizer tests ---

    #[test]
    fn test_edge_ngram_tokenizer() {
        let tokenizer = EdgeNgramTokenizer::new(1, 4);
        let tokens = tokenizer.tokenize("quick");

        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens[0].text, "q");
        assert_eq!(tokens[1].text, "qu");
        assert_eq!(tokens[2].text, "qui");
        assert_eq!(tokens[3].text, "quic");
    }

    #[test]
    fn test_edge_ngram_tokenizer_short_word() {
        let tokenizer = EdgeNgramTokenizer::new(1, 10);
        let tokens = tokenizer.tokenize("hi");

        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].text, "h");
        assert_eq!(tokens[1].text, "hi");
    }

    #[test]
    fn test_edge_ngram_tokenizer_multi_word() {
        let tokenizer = EdgeNgramTokenizer::new(1, 3);
        let tokens = tokenizer.tokenize("ab cd");

        // "ab" -> "a", "ab"; "cd" -> "c", "cd"
        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens[0].text, "a");
        assert_eq!(tokens[1].text, "ab");
        assert_eq!(tokens[2].text, "c");
        assert_eq!(tokens[3].text, "cd");
    }

    #[test]
    fn test_edge_ngram_empty_input() {
        let tokenizer = EdgeNgramTokenizer::new(1, 3);
        let tokens = tokenizer.tokenize("");
        assert!(tokens.is_empty());
    }

    // --- Pattern tokenizer tests ---

    #[test]
    fn test_pattern_tokenizer_default() {
        let tokenizer = PatternTokenizer::default_pattern();
        let tokens = tokenizer.tokenize("hello, world! foo-bar");

        assert!(tokens.len() >= 3);
        assert!(tokens.iter().any(|t| t.text == "hello"));
        assert!(tokens.iter().any(|t| t.text == "world"));
    }

    #[test]
    fn test_pattern_tokenizer_custom() {
        let tokenizer = PatternTokenizer::new(r"[-_]+").unwrap();
        let tokens = tokenizer.tokenize("foo-bar_baz");

        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0].text, "foo");
        assert_eq!(tokens[1].text, "bar");
        assert_eq!(tokens[2].text, "baz");
    }

    #[test]
    fn test_pattern_tokenizer_empty() {
        let tokenizer = PatternTokenizer::default_pattern();
        let tokens = tokenizer.tokenize("");
        assert!(tokens.is_empty());
    }

    // --- ConfigurableAnalyzer and AnalyzerBuilder tests ---

    #[test]
    fn test_configurable_analyzer_basic() {
        let analyzer = ConfigurableAnalyzer::new(Box::new(StandardTokenizer::new()));
        let tokens = analyzer.analyze("Hello World");

        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].text, "Hello");
    }

    #[test]
    fn test_analyzer_builder_pipeline() {
        let analyzer = AnalyzerBuilder::new()
            .tokenizer(Box::new(StandardTokenizer::new()))
            .filter(Box::new(LowercaseFilter))
            .filter(Box::new(StopWordFilter::english()))
            .build();

        let tokens = analyzer.analyze("The Quick Brown Fox");
        // "the" removed by stopwords, all lowercase
        assert!(tokens.iter().all(|t| t.text == t.text.to_lowercase()));
        assert!(!tokens.iter().any(|t| t.text == "the"));
    }

    #[test]
    fn test_analyzer_builder_with_edge_ngram() {
        let analyzer = AnalyzerBuilder::new()
            .tokenizer(Box::new(EdgeNgramTokenizer::new(1, 3)))
            .filter(Box::new(LowercaseFilter))
            .build();

        let tokens = analyzer.analyze("Quick");
        assert!(tokens.iter().any(|t| t.text == "q"));
        assert!(tokens.iter().any(|t| t.text == "qu"));
        assert!(tokens.iter().any(|t| t.text == "qui"));
    }

    #[test]
    fn test_analyzer_builder_full_pipeline() {
        let analyzer = AnalyzerBuilder::new()
            .tokenizer(Box::new(StandardTokenizer::new()))
            .filter(Box::new(LowercaseFilter))
            .filter(Box::new(StopWordFilter::english()))
            .filter(Box::new(LengthFilter::new(3, 50)))
            .filter(Box::new(AsciiFoldingFilter))
            .filter(Box::new(PorterStemmer::new()))
            .build();

        let tokens = analyzer.analyze("The café serves running foxes");
        // "the" is a stopword; all tokens are lowercase, ascii-folded, stemmed
        assert!(tokens.iter().all(|t| t.text == t.text.to_lowercase()));
        assert!(!tokens.iter().any(|t| t.text == "the"));
    }

    #[test]
    fn test_analyzer_builder_default_tokenizer() {
        // Without explicit tokenizer, defaults to StandardTokenizer
        let analyzer = AnalyzerBuilder::new()
            .filter(Box::new(LowercaseFilter))
            .build();

        let tokens = analyzer.analyze("Hello World");
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].text, "hello");
    }

    #[test]
    fn test_keyword_tokenizer_preserves_entire_input() {
        let tokenizer = KeywordTokenizer;
        let tokens = tokenizer.tokenize("Hello World");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].text, "Hello World");
    }

    #[test]
    fn test_keyword_tokenizer_empty() {
        let tokenizer = KeywordTokenizer;
        let tokens = tokenizer.tokenize("");
        assert!(tokens.is_empty());
    }

    #[test]
    fn test_ngram_tokenizer_bounds() {
        let tokenizer = NgramTokenizer::new(2, 2);
        let tokens = tokenizer.tokenize("ab");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].text, "ab");
    }
}
