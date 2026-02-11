//! Highlighting and snippet extraction
//!
//! Provides hit highlighting and snippet generation.

#[allow(unused_imports)]
use super::*;
use std::collections::HashSet;

/// Highlight configuration
#[derive(Debug, Clone)]
pub struct HighlightConfig {
    /// Pre tag for highlighting
    pub pre_tag: String,
    /// Post tag for highlighting
    pub post_tag: String,
    /// Maximum snippet length
    pub snippet_length: usize,
    /// Number of snippets to return
    pub num_snippets: usize,
    /// Fragment separator
    pub separator: String,
    /// Merge overlapping highlights
    pub merge_overlapping: bool,
    /// Highlight whole words only
    pub whole_words: bool,
    /// Fields to highlight
    pub fields: Option<Vec<String>>,
}

impl Default for HighlightConfig {
    fn default() -> Self {
        Self {
            pre_tag: "<em>".to_string(),
            post_tag: "</em>".to_string(),
            snippet_length: 150,
            num_snippets: 3,
            separator: "...".to_string(),
            merge_overlapping: true,
            whole_words: true,
            fields: None,
        }
    }
}

impl HighlightConfig {
    /// Create with custom tags
    pub fn new(pre_tag: &str, post_tag: &str) -> Self {
        Self {
            pre_tag: pre_tag.to_string(),
            post_tag: post_tag.to_string(),
            ..Default::default()
        }
    }

    /// HTML bold highlighting
    pub fn html_bold() -> Self {
        Self::new("<b>", "</b>")
    }

    /// HTML mark highlighting
    pub fn html_mark() -> Self {
        Self::new("<mark>", "</mark>")
    }

    /// ANSI terminal highlighting
    pub fn ansi() -> Self {
        Self::new("\x1b[1;33m", "\x1b[0m")
    }

    /// Set snippet length
    pub fn snippet_length(mut self, length: usize) -> Self {
        self.snippet_length = length;
        self
    }

    /// Set number of snippets
    pub fn num_snippets(mut self, num: usize) -> Self {
        self.num_snippets = num;
        self
    }

    /// Set separator
    pub fn separator(mut self, sep: &str) -> Self {
        self.separator = sep.to_string();
        self
    }

    /// Set fields to highlight
    pub fn fields(mut self, fields: Vec<String>) -> Self {
        self.fields = Some(fields);
        self
    }
}

/// A highlighted snippet
#[derive(Debug, Clone)]
pub struct Snippet {
    /// The text with highlights
    pub text: String,
    /// Start offset in original text
    pub start: usize,
    /// End offset in original text
    pub end: usize,
    /// Score (number of highlighted terms)
    pub score: usize,
    /// Field name
    pub field: String,
}

impl Snippet {
    /// Create a new snippet
    pub fn new(text: String, start: usize, end: usize, field: &str) -> Self {
        Self {
            text,
            start,
            end,
            score: 0,
            field: field.to_string(),
        }
    }

    /// Set score
    pub fn with_score(mut self, score: usize) -> Self {
        self.score = score;
        self
    }
}

/// Highlighter for search results
pub struct Highlighter {
    config: HighlightConfig,
    terms: HashSet<String>,
}

impl Highlighter {
    /// Create a new highlighter
    pub fn new(config: HighlightConfig) -> Self {
        Self {
            config,
            terms: HashSet::new(),
        }
    }

    /// Create with default config
    pub fn default_config() -> Self {
        Self::new(HighlightConfig::default())
    }

    /// Add terms to highlight
    pub fn add_term(&mut self, term: &str) {
        self.terms.insert(term.to_lowercase());
    }

    /// Add multiple terms
    pub fn add_terms<I, S>(&mut self, terms: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        for term in terms {
            self.add_term(term.as_ref());
        }
    }

    /// Set terms from query
    pub fn with_terms<I, S>(mut self, terms: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.add_terms(terms);
        self
    }

    /// Highlight text
    pub fn highlight(&self, text: &str, _field: &str) -> String {
        if self.terms.is_empty() {
            return text.to_string();
        }

        // Find all match positions
        let mut positions: Vec<(usize, usize)> = Vec::new();
        let text_lower = text.to_lowercase();

        for term in &self.terms {
            let mut start = 0;
            while let Some(pos) = text_lower[start..].find(term) {
                let abs_pos = start + pos;
                let end_pos = abs_pos + term.len();

                // Check word boundaries if required
                if self.config.whole_words {
                    let valid_start = abs_pos == 0
                        || !text
                            .chars()
                            .nth(abs_pos - 1)
                            .map(|c| c.is_alphanumeric())
                            .unwrap_or(false);
                    let valid_end = end_pos == text.len()
                        || !text
                            .chars()
                            .nth(end_pos)
                            .map(|c| c.is_alphanumeric())
                            .unwrap_or(false);

                    if valid_start && valid_end {
                        positions.push((abs_pos, end_pos));
                    }
                } else {
                    positions.push((abs_pos, end_pos));
                }

                start = abs_pos + 1;
            }
        }

        if positions.is_empty() {
            return text.to_string();
        }

        // Sort by start position
        positions.sort_by_key(|p| p.0);

        // Merge overlapping if configured
        if self.config.merge_overlapping {
            positions = merge_positions(positions);
        }

        // Build highlighted text
        let mut result = String::with_capacity(text.len() + positions.len() * 10);
        let mut last_end = 0;

        for (start, end) in positions {
            // Add text before highlight
            result.push_str(&text[last_end..start]);
            // Add highlighted text
            result.push_str(&self.config.pre_tag);
            result.push_str(&text[start..end]);
            result.push_str(&self.config.post_tag);
            last_end = end;
        }

        // Add remaining text
        result.push_str(&text[last_end..]);

        result
    }

    /// Extract best snippets from text
    pub fn snippets(&self, text: &str, field: &str) -> Vec<Snippet> {
        if text.is_empty() {
            return Vec::new();
        }

        let text_lower = text.to_lowercase();
        let mut scored_positions: Vec<(usize, usize)> = Vec::new();

        // Find all term positions
        for term in &self.terms {
            let mut start = 0;
            while let Some(pos) = text_lower[start..].find(term) {
                let abs_pos = start + pos;
                scored_positions.push((abs_pos, term.len()));
                start = abs_pos + 1;
            }
        }

        if scored_positions.is_empty() {
            // Return a snippet from the beginning if no matches
            let end = text.len().min(self.config.snippet_length);
            return vec![Snippet::new(text[..end].to_string(), 0, end, field)];
        }

        // Group positions into snippet windows
        let snippet_windows = self.find_best_windows(&scored_positions, text.len());

        // Extract and highlight snippets
        let mut snippets: Vec<Snippet> = Vec::new();

        for (start, end, score) in snippet_windows.into_iter().take(self.config.num_snippets) {
            let snippet_text = &text[start..end];
            let highlighted = self.highlight(snippet_text, field);

            let mut snippet = Snippet::new(highlighted, start, end, field);
            snippet.score = score;
            snippets.push(snippet);
        }

        snippets
    }

    /// Find the best snippet windows
    fn find_best_windows(
        &self,
        positions: &[(usize, usize)],
        text_len: usize,
    ) -> Vec<(usize, usize, usize)> {
        let snippet_len = self.config.snippet_length;
        let mut windows: Vec<(usize, usize, usize)> = Vec::new();

        for &(pos, _) in positions {
            // Center window around match
            let start = pos.saturating_sub(snippet_len / 2);
            let end = (start + snippet_len).min(text_len);
            let actual_start = if end == text_len {
                text_len.saturating_sub(snippet_len)
            } else {
                start
            };

            // Count matches in this window
            let score = positions
                .iter()
                .filter(|&&(p, len)| p >= actual_start && p + len <= end)
                .count();

            windows.push((actual_start, end, score));
        }

        // Sort by score descending
        windows.sort_by(|a, b| b.2.cmp(&a.2));

        // Remove overlapping windows
        let mut result: Vec<(usize, usize, usize)> = Vec::new();
        for window in windows {
            let overlaps = result.iter().any(|w| {
                (window.0 >= w.0 && window.0 < w.1)
                    || (window.1 > w.0 && window.1 <= w.1)
                    || (window.0 <= w.0 && window.1 >= w.1)
            });

            if !overlaps {
                result.push(window);
            }
        }

        result
    }

    /// Highlight and join snippets
    pub fn highlight_snippets(&self, text: &str, field: &str) -> String {
        let snippets = self.snippets(text, field);

        snippets
            .iter()
            .map(|s| s.text.clone())
            .collect::<Vec<_>>()
            .join(&self.config.separator)
    }
}

/// Merge overlapping positions
fn merge_positions(mut positions: Vec<(usize, usize)>) -> Vec<(usize, usize)> {
    if positions.is_empty() {
        return positions;
    }

    positions.sort_by_key(|p| p.0);

    let mut merged: Vec<(usize, usize)> = Vec::new();
    let mut current = positions[0];

    for pos in positions.into_iter().skip(1) {
        if pos.0 <= current.1 {
            // Overlapping, extend current
            current.1 = current.1.max(pos.1);
        } else {
            // Non-overlapping, add current and start new
            merged.push(current);
            current = pos;
        }
    }
    merged.push(current);

    merged
}

/// Fast unified highlighter for multiple terms
pub struct FastHighlighter {
    terms: Vec<String>,
    pre_tag: String,
    post_tag: String,
}

impl FastHighlighter {
    /// Create a new fast highlighter
    pub fn new(terms: Vec<String>, pre_tag: &str, post_tag: &str) -> Self {
        Self {
            terms,
            pre_tag: pre_tag.to_string(),
            post_tag: post_tag.to_string(),
        }
    }

    /// Highlight using Aho-Corasick algorithm (simplified)
    pub fn highlight(&self, text: &str) -> String {
        let mut result = text.to_string();

        for term in &self.terms {
            let highlighted = format!("{}{}{}", self.pre_tag, term, self.post_tag);
            result = result.replace(term, &highlighted);
        }

        result
    }
}

/// Passage ranker for snippet extraction
pub struct PassageRanker {
    /// Window size
    window_size: usize,
    /// Overlap between windows
    overlap: usize,
}

impl PassageRanker {
    /// Create a new passage ranker
    pub fn new(window_size: usize, overlap: usize) -> Self {
        Self {
            window_size,
            overlap,
        }
    }

    /// Rank passages from text
    pub fn rank_passages(&self, text: &str, terms: &HashSet<String>) -> Vec<(usize, usize, f32)> {
        let words: Vec<&str> = text.split_whitespace().collect();
        let step = self.window_size.saturating_sub(self.overlap).max(1);

        let mut passages: Vec<(usize, usize, f32)> = Vec::new();

        let mut i = 0;
        while i < words.len() {
            let end = (i + self.window_size).min(words.len());
            let passage: Vec<&str> = words[i..end].to_vec();

            // Score passage
            let score = passage
                .iter()
                .filter(|w| terms.contains(&w.to_lowercase()))
                .count() as f32;

            // Calculate byte offsets (approximate)
            let start_offset = words[..i].iter().map(|w| w.len() + 1).sum::<usize>();
            let end_offset = start_offset + passage.iter().map(|w| w.len() + 1).sum::<usize>();

            passages.push((start_offset, end_offset, score));

            i += step;
        }

        // Sort by score
        passages.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));

        passages
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_highlighter_basic() {
        let mut highlighter = Highlighter::new(HighlightConfig::default());
        highlighter.add_term("world");

        let result = highlighter.highlight("Hello world, welcome to the world!", "text");
        assert!(result.contains("<em>world</em>"));
    }

    #[test]
    fn test_highlighter_multiple_terms() {
        let mut highlighter = Highlighter::new(HighlightConfig::default());
        highlighter.add_term("hello");
        highlighter.add_term("world");

        let result = highlighter.highlight("Hello World", "text");
        assert!(result.contains("<em>Hello</em>"));
        assert!(result.contains("<em>World</em>"));
    }

    #[test]
    fn test_highlighter_custom_tags() {
        let config = HighlightConfig::new("<b>", "</b>");
        let mut highlighter = Highlighter::new(config);
        highlighter.add_term("test");

        let result = highlighter.highlight("This is a test", "text");
        assert!(result.contains("<b>test</b>"));
    }

    #[test]
    fn test_highlighter_no_matches() {
        let mut highlighter = Highlighter::new(HighlightConfig::default());
        highlighter.add_term("missing");

        let result = highlighter.highlight("Hello world", "text");
        assert_eq!(result, "Hello world");
    }

    #[test]
    fn test_highlighter_empty_terms() {
        let highlighter = Highlighter::new(HighlightConfig::default());

        let result = highlighter.highlight("Hello world", "text");
        assert_eq!(result, "Hello world");
    }

    #[test]
    fn test_snippets_extraction() {
        let mut highlighter = Highlighter::new(
            HighlightConfig::default()
                .snippet_length(50)
                .num_snippets(2),
        );
        highlighter.add_term("quick");

        let text = "The quick brown fox jumps over the lazy dog. The quick rabbit runs fast.";
        let snippets = highlighter.snippets(text, "body");

        assert!(!snippets.is_empty());
        assert!(snippets.len() <= 2);
    }

    #[test]
    fn test_snippets_no_match() {
        let mut highlighter = Highlighter::new(HighlightConfig::default().snippet_length(20));
        highlighter.add_term("missing");

        let text = "Hello world";
        let snippets = highlighter.snippets(text, "body");

        assert_eq!(snippets.len(), 1);
        assert_eq!(snippets[0].text, "Hello world");
    }

    #[test]
    fn test_highlight_snippets() {
        let mut highlighter = Highlighter::new(
            HighlightConfig::default()
                .snippet_length(30)
                .num_snippets(2)
                .separator(" ... "),
        );
        highlighter.add_term("fox");

        let text = "The quick brown fox jumps over the lazy dog";
        let result = highlighter.highlight_snippets(text, "body");

        assert!(result.contains("fox"));
    }

    #[test]
    fn test_merge_positions() {
        let positions = vec![(0, 5), (3, 8), (10, 15)];
        let merged = merge_positions(positions);

        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0], (0, 8));
        assert_eq!(merged[1], (10, 15));
    }

    #[test]
    fn test_merge_positions_no_overlap() {
        let positions = vec![(0, 5), (10, 15), (20, 25)];
        let merged = merge_positions(positions);

        assert_eq!(merged.len(), 3);
    }

    #[test]
    fn test_config_presets() {
        let html = HighlightConfig::html_bold();
        assert_eq!(html.pre_tag, "<b>");
        assert_eq!(html.post_tag, "</b>");

        let mark = HighlightConfig::html_mark();
        assert_eq!(mark.pre_tag, "<mark>");
        assert_eq!(mark.post_tag, "</mark>");

        let ansi = HighlightConfig::ansi();
        assert!(ansi.pre_tag.contains("\x1b"));
    }

    #[test]
    fn test_config_builder() {
        let config = HighlightConfig::default()
            .snippet_length(200)
            .num_snippets(5)
            .separator("~~~")
            .fields(vec!["title".to_string(), "body".to_string()]);

        assert_eq!(config.snippet_length, 200);
        assert_eq!(config.num_snippets, 5);
        assert_eq!(config.separator, "~~~");
        assert!(config.fields.is_some());
    }

    #[test]
    fn test_snippet_score() {
        let snippet = Snippet::new("test".to_string(), 0, 4, "body").with_score(5);

        assert_eq!(snippet.score, 5);
        assert_eq!(snippet.field, "body");
    }

    #[test]
    fn test_fast_highlighter() {
        let highlighter =
            FastHighlighter::new(vec!["quick".to_string(), "fox".to_string()], "<b>", "</b>");

        let result = highlighter.highlight("The quick brown fox");
        assert!(result.contains("<b>quick</b>"));
        assert!(result.contains("<b>fox</b>"));
    }

    #[test]
    fn test_passage_ranker() {
        let ranker = PassageRanker::new(10, 2);
        let mut terms = HashSet::new();
        terms.insert("fox".to_string());

        let text = "The quick brown fox jumps over the lazy dog";
        let passages = ranker.rank_passages(text, &terms);

        assert!(!passages.is_empty());
        // Best passage should have highest score
        assert!(passages[0].2 >= passages.last().map(|p| p.2).unwrap_or(0.0));
    }

    #[test]
    fn test_with_terms() {
        let highlighter = Highlighter::default_config().with_terms(vec!["hello", "world"]);

        let result = highlighter.highlight("Hello World!", "text");
        assert!(result.contains("<em>"));
    }

    #[test]
    fn test_whole_words_disabled() {
        let config = HighlightConfig {
            whole_words: false,
            ..Default::default()
        };
        let mut highlighter = Highlighter::new(config);
        highlighter.add_term("test");

        let result = highlighter.highlight("testing tests test", "text");
        // Should match all occurrences including partial
        assert!(result.contains("<em>test</em>"));
    }
}
