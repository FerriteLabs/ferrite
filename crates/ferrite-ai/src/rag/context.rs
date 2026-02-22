//! Context Assembly
//!
//! Assembles retrieved chunks into coherent context for LLM prompts.
//! Handles token limits, deduplication, and context formatting.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use super::chunk::ChunkId;
use super::document::DocumentId;
use super::retrieve::RetrievalResult;

/// Context configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextConfig {
    /// Maximum tokens in context
    #[serde(default = "default_max_tokens")]
    pub max_tokens: usize,
    /// Approximate characters per token
    #[serde(default = "default_chars_per_token")]
    pub chars_per_token: f32,
    /// Include source citations
    #[serde(default = "default_true")]
    pub include_citations: bool,
    /// Include chunk metadata
    #[serde(default)]
    pub include_metadata: bool,
    /// Deduplication threshold (0.0 to 1.0)
    #[serde(default = "default_dedup_threshold")]
    pub dedup_threshold: f32,
    /// Context format template
    #[serde(default)]
    pub format: ContextFormat,
    /// Separator between chunks
    #[serde(default = "default_separator")]
    pub separator: String,
    /// Maximum chunks to include
    #[serde(default = "default_max_chunks")]
    pub max_chunks: usize,
}

fn default_max_tokens() -> usize {
    4096
}

fn default_chars_per_token() -> f32 {
    4.0
}

fn default_true() -> bool {
    true
}

fn default_dedup_threshold() -> f32 {
    0.9
}

fn default_separator() -> String {
    "\n\n---\n\n".to_string()
}

fn default_max_chunks() -> usize {
    10
}

impl Default for ContextConfig {
    fn default() -> Self {
        Self {
            max_tokens: default_max_tokens(),
            chars_per_token: default_chars_per_token(),
            include_citations: default_true(),
            include_metadata: false,
            dedup_threshold: default_dedup_threshold(),
            format: ContextFormat::default(),
            separator: default_separator(),
            max_chunks: default_max_chunks(),
        }
    }
}

/// Context format template
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum ContextFormat {
    /// Simple text concatenation
    #[default]
    Plain,
    /// Markdown formatted
    Markdown,
    /// XML-style tags
    Xml,
    /// JSON formatted
    Json,
    /// Custom template (use {content}, {source}, {score} placeholders)
    Custom(String),
}

/// Citation for a source document/chunk
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Citation {
    /// Citation index (1-indexed)
    pub index: usize,
    /// Document ID
    pub document_id: DocumentId,
    /// Chunk ID
    pub chunk_id: ChunkId,
    /// Source name/title
    pub source: Option<String>,
    /// Relevance score
    pub score: f32,
    /// Start offset in original document
    pub start_offset: usize,
    /// End offset in original document
    pub end_offset: usize,
}

/// Assembled context ready for LLM
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetrievalContext {
    /// Formatted context string
    pub content: String,
    /// Citations for included sources
    pub citations: Vec<Citation>,
    /// Total tokens (estimated)
    pub estimated_tokens: usize,
    /// Number of chunks included
    pub chunk_count: usize,
    /// Number of unique documents
    pub document_count: usize,
    /// Was context truncated?
    pub truncated: bool,
}

impl RetrievalContext {
    /// Create an empty context
    pub fn empty() -> Self {
        Self {
            content: String::new(),
            citations: Vec::new(),
            estimated_tokens: 0,
            chunk_count: 0,
            document_count: 0,
            truncated: false,
        }
    }

    /// Check if context is empty
    pub fn is_empty(&self) -> bool {
        self.content.is_empty()
    }

    /// Get formatted citations as string
    pub fn citations_string(&self) -> String {
        if self.citations.is_empty() {
            return String::new();
        }

        let mut result = String::from("\n\nSources:\n");
        for citation in &self.citations {
            let source = citation
                .source
                .as_deref()
                .unwrap_or(citation.document_id.as_str());
            result.push_str(&format!(
                "[{}] {} (score: {:.2})\n",
                citation.index, source, citation.score
            ));
        }
        result
    }
}

/// Context assembler builds context from retrieval results
pub struct ContextAssembler {
    /// Configuration
    config: ContextConfig,
}

impl ContextAssembler {
    /// Create a new context assembler
    pub fn new(config: ContextConfig) -> Self {
        Self { config }
    }

    /// Assemble context from retrieval results
    pub fn assemble(&self, results: Vec<RetrievalResult>) -> RetrievalContext {
        if results.is_empty() {
            return RetrievalContext::empty();
        }

        // Deduplicate similar chunks
        let deduped = self.deduplicate(results);

        // Calculate token budget
        let max_chars = (self.config.max_tokens as f32 * self.config.chars_per_token) as usize;

        let mut content = String::new();
        let mut citations = Vec::new();
        let mut seen_docs: HashSet<String> = HashSet::new();
        let mut total_chars = 0;
        let mut truncated = false;

        for (i, result) in deduped.iter().enumerate() {
            if i >= self.config.max_chunks {
                truncated = true;
                break;
            }

            let formatted = self.format_chunk(&result.chunk.content, result, i + 1);
            let chunk_chars = formatted.len();

            // Check if adding this chunk would exceed budget
            if total_chars + chunk_chars > max_chars {
                // Try to fit partial content
                let remaining = max_chars.saturating_sub(total_chars);
                if remaining > 100 {
                    // Only add if we can fit meaningful content
                    let partial = &formatted[..remaining.min(formatted.len())];
                    if !content.is_empty() {
                        content.push_str(&self.config.separator);
                    }
                    content.push_str(partial);
                    content.push_str("...");
                }
                truncated = true;
                break;
            }

            // Add separator between chunks
            if !content.is_empty() {
                content.push_str(&self.config.separator);
            }

            content.push_str(&formatted);
            total_chars += chunk_chars + self.config.separator.len();

            // Track documents
            seen_docs.insert(result.chunk.document_id.as_str().to_string());

            // Create citation
            if self.config.include_citations {
                citations.push(Citation {
                    index: i + 1,
                    document_id: result.chunk.document_id.clone(),
                    chunk_id: result.chunk.id.clone(),
                    source: result.chunk.metadata.source.clone(),
                    score: result.score,
                    start_offset: result.chunk.start_offset,
                    end_offset: result.chunk.end_offset,
                });
            }
        }

        let estimated_tokens = (content.len() as f32 / self.config.chars_per_token) as usize;

        let chunk_count = citations.len();
        let document_count = seen_docs.len();

        RetrievalContext {
            content,
            citations,
            estimated_tokens,
            chunk_count,
            document_count,
            truncated,
        }
    }

    /// Deduplicate similar chunks
    fn deduplicate(&self, results: Vec<RetrievalResult>) -> Vec<RetrievalResult> {
        if self.config.dedup_threshold >= 1.0 {
            return results;
        }

        let mut deduped: Vec<RetrievalResult> = Vec::new();

        for result in results {
            let is_duplicate = deduped.iter().any(|existing| {
                self.jaccard_similarity(&existing.chunk.content, &result.chunk.content)
                    >= self.config.dedup_threshold
            });

            if !is_duplicate {
                deduped.push(result);
            }
        }

        deduped
    }

    /// Calculate Jaccard similarity between two texts
    fn jaccard_similarity(&self, a: &str, b: &str) -> f32 {
        let words_a: HashSet<_> = a.split_whitespace().collect();
        let words_b: HashSet<_> = b.split_whitespace().collect();

        if words_a.is_empty() && words_b.is_empty() {
            return 1.0;
        }

        let intersection = words_a.intersection(&words_b).count();
        let union = words_a.union(&words_b).count();

        if union == 0 {
            0.0
        } else {
            intersection as f32 / union as f32
        }
    }

    /// Format a chunk according to the configured format
    #[allow(clippy::literal_string_with_formatting_args)]
    fn format_chunk(&self, content: &str, result: &RetrievalResult, index: usize) -> String {
        match &self.config.format {
            ContextFormat::Plain => {
                if self.config.include_citations {
                    format!("[{}] {}", index, content)
                } else {
                    content.to_string()
                }
            }

            ContextFormat::Markdown => {
                let mut formatted = String::new();
                if self.config.include_citations {
                    formatted.push_str(&format!("### Source [{}]\n\n", index));
                }
                formatted.push_str(content);
                if self.config.include_metadata {
                    formatted.push_str(&format!(
                        "\n\n*Score: {:.2}, Document: {}*",
                        result.score,
                        result.chunk.document_id.as_str()
                    ));
                }
                formatted
            }

            ContextFormat::Xml => {
                let mut formatted = format!(
                    "<chunk index=\"{}\" score=\"{:.2}\">\n",
                    index, result.score
                );
                formatted.push_str(content);
                formatted.push_str("\n</chunk>");
                formatted
            }

            ContextFormat::Json => {
                // Simple JSON-like format (not proper JSON to avoid escaping complexity)
                format!(
                    "{{\"index\": {}, \"score\": {:.2}, \"content\": \"{}\"}}",
                    index,
                    result.score,
                    content.replace('\"', "\\\"").replace('\n', "\\n")
                )
            }

            ContextFormat::Custom(template) => template
                .replace("{content}", content)
                .replace("{source}", result.chunk.document_id.as_str())
                .replace("{score}", &format!("{:.2}", result.score))
                .replace("{index}", &index.to_string()),
        }
    }

    /// Get the configuration
    pub fn config(&self) -> &ContextConfig {
        &self.config
    }

    /// Update configuration
    pub fn set_config(&mut self, config: ContextConfig) {
        self.config = config;
    }
}

impl Default for ContextAssembler {
    fn default() -> Self {
        Self::new(ContextConfig::default())
    }
}

/// Builder for creating context with fluent API
#[allow(dead_code)] // Planned for v0.2 — builder pattern for RAG context assembly
pub struct ContextBuilder {
    results: Vec<RetrievalResult>,
    config: ContextConfig,
    query: Option<String>,
    system_prompt: Option<String>,
}

impl ContextBuilder {
    /// Create a new context builder
    #[allow(dead_code)] // Planned for v0.2 — builder entry point
    pub fn new() -> Self {
        Self {
            results: Vec::new(),
            config: ContextConfig::default(),
            query: None,
            system_prompt: None,
        }
    }

    /// Add retrieval results
    #[allow(dead_code)] // Planned for v0.2 — builder method for retrieval results
    pub fn with_results(mut self, results: Vec<RetrievalResult>) -> Self {
        self.results = results;
        self
    }

    /// Set configuration
    #[allow(dead_code)] // Planned for v0.2 — builder method for context config
    pub fn with_config(mut self, config: ContextConfig) -> Self {
        self.config = config;
        self
    }

    /// Set the user query
    #[allow(dead_code)] // Planned for v0.2 — builder method for user query
    pub fn with_query(mut self, query: impl Into<String>) -> Self {
        self.query = Some(query.into());
        self
    }

    /// Set system prompt
    #[allow(dead_code)] // Planned for v0.2 — builder method for system prompt
    pub fn with_system_prompt(mut self, prompt: impl Into<String>) -> Self {
        self.system_prompt = Some(prompt.into());
        self
    }

    /// Set max tokens
    #[allow(dead_code)] // Planned for v0.2 — builder method for token limits
    pub fn max_tokens(mut self, tokens: usize) -> Self {
        self.config.max_tokens = tokens;
        self
    }

    /// Set context format
    #[allow(dead_code)] // Planned for v0.2 — builder method for output format
    pub fn format(mut self, format: ContextFormat) -> Self {
        self.config.format = format;
        self
    }

    /// Build the context
    #[allow(dead_code)] // Planned for v0.2 — builder finalizer
    pub fn build(self) -> RetrievalContext {
        let assembler = ContextAssembler::new(self.config);
        assembler.assemble(self.results)
    }

    /// Build a full prompt with context
    #[allow(dead_code)] // Planned for v0.2 — builder method for full prompt generation
    pub fn build_prompt(self) -> String {
        // Extract values before consuming self in build()
        let system_prompt = self.system_prompt.clone();
        let query = self.query.clone();
        let context = self.build();

        let mut prompt = String::new();

        if let Some(system) = &system_prompt {
            prompt.push_str(system);
            prompt.push_str("\n\n");
        }

        if !context.is_empty() {
            prompt.push_str("Context:\n");
            prompt.push_str(&context.content);
            prompt.push_str("\n\n");
        }

        if let Some(q) = &query {
            prompt.push_str("Question: ");
            prompt.push_str(q);
        }

        prompt
    }
}

impl Default for ContextBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rag::chunk::{Chunk, ChunkMetadata};
    use crate::rag::retrieve::MatchSource;

    fn create_result(id: &str, content: &str, score: f32) -> RetrievalResult {
        RetrievalResult {
            chunk: Chunk {
                id: ChunkId::from_string(id),
                document_id: DocumentId::from_string("doc:1"),
                content: content.to_string(),
                start_offset: 0,
                end_offset: content.len(),
                index: 0,
                total_chunks: 1,
                metadata: ChunkMetadata::default(),
                embedding: None,
            },
            score,
            rank: 1,
            source: MatchSource::Vector,
        }
    }

    #[test]
    fn test_empty_context() {
        let assembler = ContextAssembler::default();
        let context = assembler.assemble(vec![]);

        assert!(context.is_empty());
        assert_eq!(context.chunk_count, 0);
    }

    #[test]
    fn test_basic_assembly() {
        let assembler = ContextAssembler::default();
        let results = vec![
            create_result("chunk:1", "First chunk content", 0.9),
            create_result("chunk:2", "Second chunk content", 0.8),
        ];

        let context = assembler.assemble(results);

        assert!(!context.is_empty());
        assert_eq!(context.chunk_count, 2);
        assert!(context.content.contains("First chunk content"));
        assert!(context.content.contains("Second chunk content"));
    }

    #[test]
    fn test_citations() {
        let config = ContextConfig {
            include_citations: true,
            ..Default::default()
        };
        let assembler = ContextAssembler::new(config);

        let results = vec![create_result("chunk:1", "Content", 0.9)];
        let context = assembler.assemble(results);

        assert_eq!(context.citations.len(), 1);
        assert_eq!(context.citations[0].index, 1);
        assert!((context.citations[0].score - 0.9).abs() < f32::EPSILON);
    }

    #[test]
    fn test_deduplication() {
        let config = ContextConfig {
            dedup_threshold: 0.8,
            ..Default::default()
        };
        let assembler = ContextAssembler::new(config);

        let results = vec![
            create_result("chunk:1", "This is some test content", 0.9),
            create_result("chunk:2", "This is some test content here", 0.85), // Similar
            create_result("chunk:3", "Completely different text", 0.8),
        ];

        let context = assembler.assemble(results);

        // Should deduplicate similar chunks
        assert!(context.chunk_count <= 2);
    }

    #[test]
    fn test_token_limit() {
        let config = ContextConfig {
            max_tokens: 10,
            chars_per_token: 1.0,
            ..Default::default()
        };
        let assembler = ContextAssembler::new(config);

        let results = vec![create_result(
            "chunk:1",
            "This is a very long chunk that should be truncated",
            0.9,
        )];

        let context = assembler.assemble(results);

        assert!(context.truncated);
        assert!(context.content.len() <= 20); // Some leeway for formatting
    }

    #[test]
    fn test_markdown_format() {
        let config = ContextConfig {
            format: ContextFormat::Markdown,
            include_citations: true,
            ..Default::default()
        };
        let assembler = ContextAssembler::new(config);

        let results = vec![create_result("chunk:1", "Test content", 0.9)];
        let context = assembler.assemble(results);

        assert!(context.content.contains("### Source"));
    }

    #[test]
    fn test_xml_format() {
        let config = ContextConfig {
            format: ContextFormat::Xml,
            ..Default::default()
        };
        let assembler = ContextAssembler::new(config);

        let results = vec![create_result("chunk:1", "Test content", 0.9)];
        let context = assembler.assemble(results);

        assert!(context.content.contains("<chunk"));
        assert!(context.content.contains("</chunk>"));
    }

    #[test]
    fn test_custom_format() {
        let config = ContextConfig {
            format: ContextFormat::Custom("[{index}] {content} (score: {score})".to_string()),
            ..Default::default()
        };
        let assembler = ContextAssembler::new(config);

        let results = vec![create_result("chunk:1", "Test", 0.9)];
        let context = assembler.assemble(results);

        assert!(context.content.contains("[1] Test"));
        assert!(context.content.contains("(score: 0.90)"));
    }

    #[test]
    fn test_jaccard_similarity() {
        let assembler = ContextAssembler::default();

        // Identical
        let sim = assembler.jaccard_similarity("hello world", "hello world");
        assert!((sim - 1.0).abs() < f32::EPSILON);

        // Completely different
        let sim = assembler.jaccard_similarity("hello world", "foo bar");
        assert!(sim < 0.1);

        // Partial overlap
        let sim = assembler.jaccard_similarity("hello world test", "hello world other");
        assert!(sim > 0.3 && sim < 0.8);
    }

    #[test]
    fn test_context_builder() {
        let results = vec![create_result("chunk:1", "Context info", 0.9)];

        let prompt = ContextBuilder::new()
            .with_results(results)
            .with_system_prompt("You are a helpful assistant.")
            .with_query("What is the meaning?")
            .format(ContextFormat::Plain)
            .build_prompt();

        assert!(prompt.contains("You are a helpful assistant."));
        assert!(prompt.contains("Context:"));
        assert!(prompt.contains("Context info"));
        assert!(prompt.contains("Question: What is the meaning?"));
    }

    #[test]
    fn test_max_chunks() {
        let config = ContextConfig {
            max_chunks: 2,
            ..Default::default()
        };
        let assembler = ContextAssembler::new(config);

        let results = vec![
            create_result("chunk:1", "First", 0.9),
            create_result("chunk:2", "Second", 0.8),
            create_result("chunk:3", "Third", 0.7),
        ];

        let context = assembler.assemble(results);

        assert_eq!(context.chunk_count, 2);
        assert!(context.truncated);
    }

    #[test]
    fn test_citations_string() {
        let context = RetrievalContext {
            content: "test".to_string(),
            citations: vec![Citation {
                index: 1,
                document_id: DocumentId::from_string("doc:1"),
                chunk_id: ChunkId::from_string("chunk:1"),
                source: Some("Test Source".to_string()),
                score: 0.95,
                start_offset: 0,
                end_offset: 100,
            }],
            estimated_tokens: 10,
            chunk_count: 1,
            document_count: 1,
            truncated: false,
        };

        let citations = context.citations_string();
        assert!(citations.contains("[1] Test Source"));
        assert!(citations.contains("0.95"));
    }
}
