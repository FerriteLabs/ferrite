//! Search suggestions and autocomplete
//!
//! Provides autocomplete and "did you mean" functionality.

#[allow(unused_imports)]
use super::*;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

/// Suggestion configuration
#[derive(Debug, Clone)]
pub struct SuggestConfig {
    /// Maximum number of suggestions
    pub max_suggestions: usize,
    /// Minimum prefix length to trigger suggestions
    pub min_prefix_length: usize,
    /// Enable fuzzy matching
    pub fuzzy: bool,
    /// Maximum edit distance for fuzzy matching
    pub max_edit_distance: usize,
    /// Highlight suggestions
    pub highlight: bool,
    /// Include scores
    pub include_scores: bool,
    /// Context fields for contextual suggestions
    pub contexts: Option<Vec<String>>,
}

impl Default for SuggestConfig {
    fn default() -> Self {
        Self {
            max_suggestions: 10,
            min_prefix_length: 2,
            fuzzy: true,
            max_edit_distance: 2,
            highlight: false,
            include_scores: false,
            contexts: None,
        }
    }
}

impl SuggestConfig {
    /// Create with max suggestions
    pub fn new(max_suggestions: usize) -> Self {
        Self {
            max_suggestions,
            ..Default::default()
        }
    }

    /// Set minimum prefix length
    pub fn min_prefix_length(mut self, len: usize) -> Self {
        self.min_prefix_length = len;
        self
    }

    /// Enable/disable fuzzy matching
    pub fn fuzzy(mut self, enabled: bool) -> Self {
        self.fuzzy = enabled;
        self
    }

    /// Set max edit distance
    pub fn max_edit_distance(mut self, distance: usize) -> Self {
        self.max_edit_distance = distance;
        self
    }

    /// Enable highlighting
    pub fn highlight(mut self, enabled: bool) -> Self {
        self.highlight = enabled;
        self
    }
}

/// A single suggestion
#[derive(Debug, Clone)]
pub struct Suggestion {
    /// The suggested text
    pub text: String,
    /// Relevance score
    pub score: f32,
    /// Highlighted text (if enabled)
    pub highlighted: Option<String>,
    /// Payload (additional data)
    pub payload: Option<String>,
    /// Contexts this suggestion applies to
    pub contexts: Vec<String>,
}

impl Suggestion {
    /// Create a new suggestion
    pub fn new(text: impl Into<String>, score: f32) -> Self {
        Self {
            text: text.into(),
            score,
            highlighted: None,
            payload: None,
            contexts: Vec::new(),
        }
    }

    /// Set highlighted text
    pub fn with_highlight(mut self, highlighted: String) -> Self {
        self.highlighted = Some(highlighted);
        self
    }

    /// Set payload
    pub fn with_payload(mut self, payload: String) -> Self {
        self.payload = Some(payload);
        self
    }

    /// Add context
    pub fn with_context(mut self, context: String) -> Self {
        self.contexts.push(context);
        self
    }
}

impl PartialEq for Suggestion {
    fn eq(&self, other: &Self) -> bool {
        self.text == other.text
    }
}

impl Eq for Suggestion {}

impl PartialOrd for Suggestion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Suggestion {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher score = better
        self.score
            .partial_cmp(&other.score)
            .unwrap_or(Ordering::Equal)
    }
}

/// Suggester for autocomplete functionality
pub struct Suggester {
    /// Configuration
    config: SuggestConfig,
    /// Trie for prefix matching
    trie: Trie,
    /// Term frequencies
    frequencies: HashMap<String, u32>,
    /// Total terms
    total_terms: u64,
}

impl Suggester {
    /// Create a new suggester
    pub fn new(config: SuggestConfig) -> Self {
        Self {
            config,
            trie: Trie::new(),
            frequencies: HashMap::new(),
            total_terms: 0,
        }
    }

    /// Create with default config
    pub fn default_config() -> Self {
        Self::new(SuggestConfig::default())
    }

    /// Add a term to the suggester
    pub fn add(&mut self, term: &str, weight: Option<u32>) {
        let normalized = term.to_lowercase();
        let weight = weight.unwrap_or(1);

        self.trie.insert(&normalized, weight);
        *self.frequencies.entry(normalized).or_insert(0) += weight;
        self.total_terms += weight as u64;
    }

    /// Add multiple terms
    pub fn add_all<I, S>(&mut self, terms: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        for term in terms {
            self.add(term.as_ref(), None);
        }
    }

    /// Get suggestions for a prefix
    pub fn suggest(&self, prefix: &str) -> Vec<Suggestion> {
        if prefix.len() < self.config.min_prefix_length {
            return Vec::new();
        }

        let prefix_lower = prefix.to_lowercase();
        let mut suggestions: BinaryHeap<Suggestion> = BinaryHeap::new();

        // Prefix matches
        let prefix_matches = self.trie.prefix_search(&prefix_lower);
        for (term, weight) in prefix_matches {
            let score = self.calculate_score(&term, weight);
            let mut suggestion = Suggestion::new(&term, score);

            if self.config.highlight {
                suggestion.highlighted = Some(self.highlight_match(&term, &prefix_lower));
            }

            suggestions.push(suggestion);
        }

        // Fuzzy matches (if enabled)
        if self.config.fuzzy && suggestions.len() < self.config.max_suggestions {
            let fuzzy_matches = self.fuzzy_search(&prefix_lower);
            for (term, weight, distance) in fuzzy_matches {
                // Penalize by edit distance
                let base_score = self.calculate_score(&term, weight);
                let score = base_score / (1.0 + distance as f32);

                if !suggestions.iter().any(|s| s.text == term) {
                    let mut suggestion = Suggestion::new(&term, score);
                    if self.config.highlight {
                        suggestion.highlighted = Some(term.clone());
                    }
                    suggestions.push(suggestion);
                }
            }
        }

        // Collect top suggestions
        let mut result: Vec<Suggestion> = Vec::new();
        while let Some(suggestion) = suggestions.pop() {
            if result.len() >= self.config.max_suggestions {
                break;
            }
            result.push(suggestion);
        }

        // Reverse to get highest scores first
        result.reverse();
        result
    }

    /// Calculate suggestion score
    fn calculate_score(&self, _term: &str, weight: u32) -> f32 {
        // Simple TF-based scoring
        weight as f32
    }

    /// Highlight the matching prefix
    fn highlight_match(&self, term: &str, prefix: &str) -> String {
        if let Some(rest) = term.strip_prefix(prefix) {
            format!(
                "<em>{}</em>{}",
                prefix,
                rest
            )
        } else {
            term.to_string()
        }
    }

    /// Perform fuzzy search
    fn fuzzy_search(&self, query: &str) -> Vec<(String, u32, usize)> {
        let mut results = Vec::new();

        for (term, &weight) in &self.frequencies {
            let distance = levenshtein_distance(query, term);
            if distance <= self.config.max_edit_distance {
                results.push((term.clone(), weight, distance));
            }
        }

        results.sort_by(|a, b| a.2.cmp(&b.2).then(b.1.cmp(&a.1)));
        results
    }

    /// Get "did you mean" suggestions
    pub fn did_you_mean(&self, query: &str) -> Option<String> {
        let query_lower = query.to_lowercase();

        // Check if it's a known term
        if self.frequencies.contains_key(&query_lower) {
            return None;
        }

        // Find closest match
        let mut best: Option<(String, usize, u32)> = None;

        for (term, &weight) in &self.frequencies {
            let distance = levenshtein_distance(&query_lower, term);
            if distance <= self.config.max_edit_distance {
                match &best {
                    None => best = Some((term.clone(), distance, weight)),
                    Some((_, d, w)) => {
                        if distance < *d || (distance == *d && weight > *w) {
                            best = Some((term.clone(), distance, weight));
                        }
                    }
                }
            }
        }

        best.map(|(term, _, _)| term)
    }

    /// Get completions (like suggest but returns strings only)
    pub fn completions(&self, prefix: &str) -> Vec<String> {
        self.suggest(prefix).into_iter().map(|s| s.text).collect()
    }

    /// Get suggestion count
    pub fn len(&self) -> usize {
        self.frequencies.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.frequencies.is_empty()
    }
}

impl Default for Suggester {
    fn default() -> Self {
        Self::default_config()
    }
}

/// Trie data structure for prefix matching
struct Trie {
    root: TrieNode,
}

struct TrieNode {
    children: HashMap<char, TrieNode>,
    is_end: bool,
    weight: u32,
}

impl TrieNode {
    fn new() -> Self {
        Self {
            children: HashMap::new(),
            is_end: false,
            weight: 0,
        }
    }
}

impl Trie {
    fn new() -> Self {
        Self {
            root: TrieNode::new(),
        }
    }

    fn insert(&mut self, word: &str, weight: u32) {
        let mut node = &mut self.root;

        for c in word.chars() {
            node = node.children.entry(c).or_insert_with(TrieNode::new);
        }

        node.is_end = true;
        node.weight += weight;
    }

    fn prefix_search(&self, prefix: &str) -> Vec<(String, u32)> {
        let mut node = &self.root;

        // Navigate to prefix node
        for c in prefix.chars() {
            match node.children.get(&c) {
                Some(n) => node = n,
                None => return Vec::new(),
            }
        }

        // Collect all words under this node
        let mut results = Vec::new();
        self.collect_words(node, prefix.to_string(), &mut results);
        results
    }

    #[allow(clippy::only_used_in_recursion)]
    fn collect_words(&self, node: &TrieNode, current: String, results: &mut Vec<(String, u32)>) {
        if node.is_end {
            results.push((current.clone(), node.weight));
        }

        for (&c, child) in &node.children {
            let mut next = current.clone();
            next.push(c);
            self.collect_words(child, next, results);
        }
    }
}

/// Calculate Levenshtein distance between two strings
fn levenshtein_distance(s1: &str, s2: &str) -> usize {
    let s1_chars: Vec<char> = s1.chars().collect();
    let s2_chars: Vec<char> = s2.chars().collect();

    let m = s1_chars.len();
    let n = s2_chars.len();

    if m == 0 {
        return n;
    }
    if n == 0 {
        return m;
    }

    let mut dp = vec![vec![0usize; n + 1]; m + 1];

    for (i, row) in dp.iter_mut().enumerate().take(m + 1) {
        row[0] = i;
    }
    for j in 0..=n {
        dp[0][j] = j;
    }

    for i in 1..=m {
        for j in 1..=n {
            let cost = if s1_chars[i - 1] == s2_chars[j - 1] {
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

/// Phrase suggester for multi-word suggestions
pub struct PhraseSuggester {
    config: SuggestConfig,
    bigrams: HashMap<String, HashMap<String, u32>>,
    unigrams: HashMap<String, u32>,
}

impl PhraseSuggester {
    /// Create a new phrase suggester
    pub fn new(config: SuggestConfig) -> Self {
        Self {
            config,
            bigrams: HashMap::new(),
            unigrams: HashMap::new(),
        }
    }

    /// Add a phrase
    pub fn add_phrase(&mut self, phrase: &str) {
        let words: Vec<&str> = phrase.split_whitespace().collect();

        for word in &words {
            *self.unigrams.entry(word.to_lowercase()).or_insert(0) += 1;
        }

        for window in words.windows(2) {
            let first = window[0].to_lowercase();
            let second = window[1].to_lowercase();

            *self
                .bigrams
                .entry(first)
                .or_default()
                .entry(second)
                .or_insert(0) += 1;
        }
    }

    /// Suggest next words
    pub fn suggest_next(&self, previous: &str) -> Vec<Suggestion> {
        let prev_lower = previous.to_lowercase();

        match self.bigrams.get(&prev_lower) {
            Some(next_words) => {
                let mut suggestions: Vec<_> = next_words
                    .iter()
                    .map(|(word, &count)| Suggestion::new(word, count as f32))
                    .collect();

                suggestions.sort_by(|a, b| {
                    b.score
                        .partial_cmp(&a.score)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                suggestions.truncate(self.config.max_suggestions);
                suggestions
            }
            None => Vec::new(),
        }
    }

    /// Complete a phrase
    pub fn complete_phrase(&self, partial: &str) -> Vec<String> {
        let words: Vec<&str> = partial.split_whitespace().collect();

        if words.is_empty() {
            return Vec::new();
        }

        // Get suggestions for the last incomplete word
        let last_word = words.last().copied().unwrap_or("");
        let prefix = last_word.to_lowercase();

        // Find completions
        let mut completions: Vec<(String, u32)> = self
            .unigrams
            .iter()
            .filter(|(word, _)| word.starts_with(&prefix))
            .map(|(word, &count)| (word.clone(), count))
            .collect();

        completions.sort_by(|a, b| b.1.cmp(&a.1));

        // If we have previous words, boost by bigram probability
        if words.len() > 1 {
            let prev = words[words.len() - 2].to_lowercase();
            if let Some(bigrams) = self.bigrams.get(&prev) {
                for (word, count) in &mut completions {
                    if let Some(&bigram_count) = bigrams.get(word) {
                        *count += bigram_count * 2;
                    }
                }
                completions.sort_by(|a, b| b.1.cmp(&a.1));
            }
        }

        completions
            .into_iter()
            .take(self.config.max_suggestions)
            .map(|(word, _)| {
                let base: String = words[..words.len() - 1].join(" ");
                if base.is_empty() {
                    word
                } else {
                    format!("{} {}", base, word)
                }
            })
            .collect()
    }
}

/// Context-aware suggester
pub struct ContextualSuggester {
    config: SuggestConfig,
    contexts: HashMap<String, Suggester>,
    global: Suggester,
}

impl ContextualSuggester {
    /// Create a new contextual suggester
    pub fn new(config: SuggestConfig) -> Self {
        Self {
            config: config.clone(),
            contexts: HashMap::new(),
            global: Suggester::new(config),
        }
    }

    /// Add a term with contexts
    pub fn add(&mut self, term: &str, contexts: Vec<String>, weight: Option<u32>) {
        self.global.add(term, weight);

        for context in contexts {
            self.contexts
                .entry(context)
                .or_insert_with(|| Suggester::new(self.config.clone()))
                .add(term, weight);
        }
    }

    /// Suggest with context
    pub fn suggest(&self, prefix: &str, context: Option<&str>) -> Vec<Suggestion> {
        match context {
            Some(ctx) => match self.contexts.get(ctx) {
                Some(suggester) => suggester.suggest(prefix),
                None => self.global.suggest(prefix),
            },
            None => self.global.suggest(prefix),
        }
    }
}

/// Popular searches tracker
pub struct PopularSearches {
    /// Maximum tracked searches
    max_size: usize,
    /// Search counts
    counts: HashMap<String, u64>,
    /// Time-decayed counts
    decayed_counts: HashMap<String, f64>,
    /// Decay factor
    decay_factor: f64,
}

impl PopularSearches {
    /// Create a new tracker
    pub fn new(max_size: usize) -> Self {
        Self {
            max_size,
            counts: HashMap::new(),
            decayed_counts: HashMap::new(),
            decay_factor: 0.99,
        }
    }

    /// Record a search
    pub fn record(&mut self, query: &str) {
        let normalized = query.to_lowercase().trim().to_string();
        *self.counts.entry(normalized.clone()).or_insert(0) += 1;

        // Update decayed count
        let decayed = self.decayed_counts.entry(normalized).or_insert(0.0);
        *decayed = *decayed * self.decay_factor + 1.0;

        // Prune if necessary
        if self.counts.len() > self.max_size * 2 {
            self.prune();
        }
    }

    /// Get top searches
    pub fn top(&self, n: usize) -> Vec<(String, u64)> {
        let mut sorted: Vec<_> = self.counts.iter().collect();
        sorted.sort_by(|a, b| b.1.cmp(a.1));
        sorted
            .into_iter()
            .take(n)
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }

    /// Get trending searches (time-decayed)
    pub fn trending(&self, n: usize) -> Vec<(String, f64)> {
        let mut sorted: Vec<_> = self.decayed_counts.iter().collect();
        sorted.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap_or(std::cmp::Ordering::Equal));
        sorted
            .into_iter()
            .take(n)
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }

    /// Prune low-count entries
    fn prune(&mut self) {
        let mut sorted: Vec<_> = self.counts.iter().collect();
        sorted.sort_by(|a, b| b.1.cmp(a.1));

        let to_keep: HashSet<_> = sorted
            .into_iter()
            .take(self.max_size)
            .map(|(k, _)| k.clone())
            .collect();

        self.counts.retain(|k, _| to_keep.contains(k));
        self.decayed_counts.retain(|k, _| to_keep.contains(k));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_suggester_basic() {
        let mut suggester = Suggester::default_config();
        suggester.add("hello", None);
        suggester.add("help", None);
        suggester.add("helicopter", None);
        suggester.add("world", None);

        let suggestions = suggester.suggest("hel");
        assert!(suggestions.len() >= 3);
        assert!(suggestions.iter().any(|s| s.text == "hello"));
        assert!(suggestions.iter().any(|s| s.text == "help"));
    }

    #[test]
    fn test_suggester_weights() {
        let mut suggester = Suggester::default_config();
        suggester.add("common", Some(100));
        suggester.add("rare", Some(1));
        suggester.add("common", Some(50)); // Add more weight

        let suggestions = suggester.suggest("com");
        assert!(suggestions[0].text == "common");
        assert!(suggestions[0].score > 100.0);
    }

    #[test]
    fn test_suggester_min_prefix() {
        let config = SuggestConfig::new(10).min_prefix_length(3);
        let mut suggester = Suggester::new(config);
        suggester.add("test", None);

        // Too short prefix
        assert!(suggester.suggest("te").is_empty());

        // Long enough prefix
        assert!(!suggester.suggest("tes").is_empty());
    }

    #[test]
    fn test_fuzzy_suggestions() {
        let config = SuggestConfig::new(10).fuzzy(true).max_edit_distance(2);
        let mut suggester = Suggester::new(config);
        suggester.add("hello", None);

        // Typo in query
        let suggestions = suggester.suggest("helo");
        assert!(!suggestions.is_empty());
    }

    #[test]
    fn test_did_you_mean() {
        let mut suggester = Suggester::default_config();
        suggester.add("search", Some(100));
        suggester.add("searching", Some(50));

        // Known term
        assert!(suggester.did_you_mean("search").is_none());

        // Typo
        let suggestion = suggester.did_you_mean("serach");
        assert_eq!(suggestion, Some("search".to_string()));
    }

    #[test]
    fn test_suggestion_highlighting() {
        let config = SuggestConfig::new(10).highlight(true);
        let mut suggester = Suggester::new(config);
        suggester.add("testing", None);

        let suggestions = suggester.suggest("test");
        assert!(suggestions[0].highlighted.is_some());
        assert!(suggestions[0]
            .highlighted
            .as_ref()
            .unwrap()
            .contains("<em>"));
    }

    #[test]
    fn test_completions() {
        let mut suggester = Suggester::default_config();
        suggester.add("apple", None);
        suggester.add("application", None);
        suggester.add("banana", None);

        let completions = suggester.completions("app");
        assert!(completions.contains(&"apple".to_string()));
        assert!(completions.contains(&"application".to_string()));
        assert!(!completions.contains(&"banana".to_string()));
    }

    #[test]
    fn test_phrase_suggester() {
        let mut suggester = PhraseSuggester::new(SuggestConfig::default());
        suggester.add_phrase("hello world");
        suggester.add_phrase("hello there");
        suggester.add_phrase("hello world again");

        let next = suggester.suggest_next("hello");
        assert!(!next.is_empty());
        assert!(next.iter().any(|s| s.text == "world"));
    }

    #[test]
    fn test_phrase_completion() {
        let mut suggester = PhraseSuggester::new(SuggestConfig::default());
        suggester.add_phrase("hello world");
        suggester.add_phrase("hello there");
        suggester.add_phrase("hi world");

        let completions = suggester.complete_phrase("hello wor");
        assert!(completions.iter().any(|s| s.contains("world")));
    }

    #[test]
    fn test_contextual_suggester() {
        let mut suggester = ContextualSuggester::new(SuggestConfig::default());
        suggester.add("laptop", vec!["electronics".to_string()], None);
        suggester.add("lamp", vec!["furniture".to_string()], None);
        suggester.add("laser", vec!["electronics".to_string()], None);

        let electronics = suggester.suggest("la", Some("electronics"));
        let furniture = suggester.suggest("la", Some("furniture"));

        assert!(electronics.iter().any(|s| s.text == "laptop"));
        assert!(furniture.iter().any(|s| s.text == "lamp"));
    }

    #[test]
    fn test_popular_searches() {
        let mut tracker = PopularSearches::new(100);
        tracker.record("hello");
        tracker.record("hello");
        tracker.record("world");

        let top = tracker.top(10);
        assert_eq!(top[0].0, "hello");
        assert_eq!(top[0].1, 2);
    }

    #[test]
    fn test_trending_searches() {
        let mut tracker = PopularSearches::new(100);

        // Old searches
        for _ in 0..10 {
            tracker.record("old");
        }

        // Recent searches
        for _ in 0..5 {
            tracker.record("new");
        }

        // Note: in a real implementation, trending would factor in recency
        let trending = tracker.trending(10);
        assert!(!trending.is_empty());
    }

    #[test]
    fn test_levenshtein() {
        assert_eq!(levenshtein_distance("", "abc"), 3);
        assert_eq!(levenshtein_distance("abc", ""), 3);
        assert_eq!(levenshtein_distance("abc", "abc"), 0);
        assert_eq!(levenshtein_distance("abc", "abd"), 1);
        assert_eq!(levenshtein_distance("abc", "abcd"), 1);
        assert_eq!(levenshtein_distance("kitten", "sitting"), 3);
    }

    #[test]
    fn test_trie() {
        let mut trie = Trie::new();
        trie.insert("hello", 5);
        trie.insert("help", 3);
        trie.insert("world", 1);

        let results = trie.prefix_search("hel");
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_suggestion_builder() {
        let suggestion = Suggestion::new("test", 1.0)
            .with_highlight("<em>test</em>".to_string())
            .with_payload("data".to_string())
            .with_context("category".to_string());

        assert!(suggestion.highlighted.is_some());
        assert!(suggestion.payload.is_some());
        assert!(!suggestion.contexts.is_empty());
    }

    #[test]
    fn test_config_builder() {
        let config = SuggestConfig::new(5)
            .min_prefix_length(1)
            .fuzzy(false)
            .max_edit_distance(1)
            .highlight(true);

        assert_eq!(config.max_suggestions, 5);
        assert_eq!(config.min_prefix_length, 1);
        assert!(!config.fuzzy);
        assert!(config.highlight);
    }

    #[test]
    fn test_suggester_empty() {
        let suggester = Suggester::default_config();
        assert!(suggester.is_empty());
        assert_eq!(suggester.len(), 0);
    }

    #[test]
    fn test_add_all() {
        let mut suggester = Suggester::default_config();
        suggester.add_all(vec!["apple", "banana", "cherry"]);

        assert_eq!(suggester.len(), 3);
    }
}
