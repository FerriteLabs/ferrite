//! Document Chunking
//!
//! Splits documents into smaller chunks for embedding and retrieval.
//! Supports multiple chunking strategies for different use cases.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use super::document::{AttributeValue, Document, DocumentId};

/// Unique chunk identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChunkId(pub String);

impl ChunkId {
    /// Create a new chunk ID
    pub fn new(doc_id: &DocumentId, index: usize) -> Self {
        Self(format!("{}:chunk:{}", doc_id.as_str(), index))
    }

    /// Create from a string
    pub fn from_string(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Get the string representation
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ChunkId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Chunk metadata for filtering and context
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChunkMetadata {
    /// Source name/title
    pub source: Option<String>,
    /// Tags for categorization
    pub tags: Vec<String>,
    /// Custom attributes
    pub attributes: HashMap<String, AttributeValue>,
}

impl ChunkMetadata {
    /// Create new empty metadata
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a tag
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Set source
    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }

    /// Add an attribute
    pub fn with_attribute(mut self, key: impl Into<String>, value: AttributeValue) -> Self {
        self.attributes.insert(key.into(), value);
        self
    }
}

/// A chunk of text from a document
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Chunk {
    /// Chunk ID
    pub id: ChunkId,
    /// Parent document ID
    pub document_id: DocumentId,
    /// Chunk content
    pub content: String,
    /// Character offset in original document
    pub start_offset: usize,
    /// Character end offset
    pub end_offset: usize,
    /// Chunk index within document
    pub index: usize,
    /// Total chunks in document
    pub total_chunks: usize,
    /// Chunk metadata
    pub metadata: ChunkMetadata,
    /// Embedding vector (set after embedding)
    pub embedding: Option<Vec<f32>>,
}

impl Chunk {
    /// Create a new chunk
    pub fn new(
        document_id: DocumentId,
        content: String,
        start_offset: usize,
        end_offset: usize,
        index: usize,
        total_chunks: usize,
    ) -> Self {
        let id = ChunkId::new(&document_id, index);
        Self {
            id,
            document_id,
            content,
            start_offset,
            end_offset,
            index,
            total_chunks,
            metadata: ChunkMetadata::default(),
            embedding: None,
        }
    }

    /// Set metadata
    pub fn with_metadata(mut self, metadata: ChunkMetadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// Get content length
    pub fn content_length(&self) -> usize {
        self.content.len()
    }

    /// Set embedding
    pub fn set_embedding(&mut self, embedding: Vec<f32>) {
        self.embedding = Some(embedding);
    }

    /// Check if chunk has embedding
    pub fn has_embedding(&self) -> bool {
        self.embedding.is_some()
    }
}

/// Chunking strategy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ChunkingStrategy {
    /// Fixed-size chunks with optional overlap
    FixedSize {
        /// Chunk size in characters
        size: usize,
        /// Overlap between chunks
        overlap: usize,
    },
    /// Sentence-based chunking
    Sentence {
        /// Maximum sentences per chunk
        max_sentences: usize,
        /// Minimum chunk size
        min_size: usize,
    },
    /// Paragraph-based chunking
    Paragraph {
        /// Maximum paragraphs per chunk
        max_paragraphs: usize,
        /// Minimum chunk size
        min_size: usize,
    },
    /// Semantic chunking (based on content similarity)
    Semantic {
        /// Target chunk size
        target_size: usize,
        /// Similarity threshold for splits
        threshold: f32,
    },
    /// Recursive chunking (split by hierarchy)
    Recursive {
        /// Target chunk size
        target_size: usize,
        /// Separators in order of priority
        separators: Vec<String>,
    },
}

impl Default for ChunkingStrategy {
    fn default() -> Self {
        ChunkingStrategy::FixedSize {
            size: 512,
            overlap: 50,
        }
    }
}

/// Chunking configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkingConfig {
    /// Chunking strategy
    pub strategy: ChunkingStrategy,
    /// Minimum chunk size (skip smaller chunks)
    pub min_chunk_size: usize,
    /// Maximum chunk size (hard limit)
    pub max_chunk_size: usize,
    /// Preserve sentence boundaries when possible
    pub preserve_sentences: bool,
    /// Trim whitespace from chunks
    pub trim_whitespace: bool,
}

impl Default for ChunkingConfig {
    fn default() -> Self {
        Self {
            strategy: ChunkingStrategy::default(),
            min_chunk_size: 1,
            max_chunk_size: 2048,
            preserve_sentences: true,
            trim_whitespace: true,
        }
    }
}

/// Document chunker
pub struct Chunker {
    /// Configuration
    config: ChunkingConfig,
    /// Statistics
    stats: ChunkerStats,
}

/// Chunker statistics
#[derive(Debug, Default)]
pub struct ChunkerStats {
    /// Total documents chunked
    pub documents_chunked: AtomicU64,
    /// Total chunks created
    pub chunks_created: AtomicU64,
    /// Total characters processed
    pub chars_processed: AtomicU64,
}

impl Chunker {
    /// Create a new chunker with default config
    pub fn new() -> Self {
        Self::with_config(ChunkingConfig::default())
    }

    /// Create a chunker with custom config
    pub fn with_config(config: ChunkingConfig) -> Self {
        Self {
            config,
            stats: ChunkerStats::default(),
        }
    }

    /// Chunk a document
    pub fn chunk(&self, document: &Document) -> Vec<Chunk> {
        let content = &document.content;
        if content.is_empty() {
            return Vec::new();
        }

        self.stats
            .chars_processed
            .fetch_add(content.len() as u64, Ordering::Relaxed);

        let raw_chunks = match &self.config.strategy {
            ChunkingStrategy::FixedSize { size, overlap } => {
                self.chunk_fixed_size(content, *size, *overlap)
            }
            ChunkingStrategy::Sentence {
                max_sentences,
                min_size,
            } => self.chunk_by_sentence(content, *max_sentences, *min_size),
            ChunkingStrategy::Paragraph {
                max_paragraphs,
                min_size,
            } => self.chunk_by_paragraph(content, *max_paragraphs, *min_size),
            ChunkingStrategy::Recursive {
                target_size,
                separators,
            } => self.chunk_recursive(content, *target_size, separators),
            ChunkingStrategy::Semantic { target_size, .. } => {
                // Fall back to fixed size for now
                self.chunk_fixed_size(content, *target_size, 50)
            }
        };

        // Convert to Chunk structs
        let total = raw_chunks.len();
        let chunks: Vec<Chunk> = raw_chunks
            .into_iter()
            .enumerate()
            .filter(|(_, (text, _, _))| {
                let len = text.len();
                len >= self.config.min_chunk_size && len <= self.config.max_chunk_size
            })
            .map(|(i, (text, start, end))| {
                let content = if self.config.trim_whitespace {
                    text.trim().to_string()
                } else {
                    text
                };
                Chunk::new(document.id.clone(), content, start, end, i, total)
            })
            .collect();

        self.stats.documents_chunked.fetch_add(1, Ordering::Relaxed);
        self.stats
            .chunks_created
            .fetch_add(chunks.len() as u64, Ordering::Relaxed);

        chunks
    }

    /// Fixed-size chunking with overlap
    fn chunk_fixed_size(
        &self,
        content: &str,
        size: usize,
        overlap: usize,
    ) -> Vec<(String, usize, usize)> {
        let mut chunks = Vec::new();
        let chars: Vec<char> = content.chars().collect();
        let total_chars = chars.len();

        if total_chars == 0 {
            return chunks;
        }

        let step = if size > overlap { size - overlap } else { size };
        let mut start = 0;

        while start < total_chars {
            let end = (start + size).min(total_chars);
            let chunk_text: String = chars[start..end].iter().collect();

            // Find byte offsets
            let byte_start = content
                .char_indices()
                .nth(start)
                .map(|(i, _)| i)
                .unwrap_or(0);
            let byte_end = if end >= total_chars {
                content.len()
            } else {
                content
                    .char_indices()
                    .nth(end)
                    .map(|(i, _)| i)
                    .unwrap_or(content.len())
            };

            // Adjust to sentence boundary if configured
            let final_text = if self.config.preserve_sentences && end < total_chars {
                self.extend_to_sentence_end(&chunk_text)
            } else {
                chunk_text
            };

            chunks.push((final_text, byte_start, byte_end));

            if end >= total_chars {
                break;
            }
            start += step;
        }

        chunks
    }

    /// Sentence-based chunking
    fn chunk_by_sentence(
        &self,
        content: &str,
        max_sentences: usize,
        min_size: usize,
    ) -> Vec<(String, usize, usize)> {
        let sentences = self.split_sentences(content);
        let mut chunks = Vec::new();
        let mut current_chunk = String::new();
        let mut sentence_count = 0;
        let mut start_offset = 0;
        let mut current_start = 0;

        for sentence in sentences {
            if sentence_count >= max_sentences && current_chunk.len() >= min_size {
                // Emit current chunk
                let end_offset = start_offset + current_chunk.len();
                chunks.push((current_chunk.clone(), current_start, end_offset));
                current_chunk.clear();
                current_start = end_offset;
                sentence_count = 0;
            }

            if !current_chunk.is_empty() {
                current_chunk.push(' ');
            }
            current_chunk.push_str(&sentence);
            sentence_count += 1;
            start_offset += sentence.len() + 1;
        }

        // Emit remaining
        if !current_chunk.is_empty() {
            chunks.push((
                current_chunk.clone(),
                current_start,
                current_start + current_chunk.len(),
            ));
        }

        chunks
    }

    /// Paragraph-based chunking
    fn chunk_by_paragraph(
        &self,
        content: &str,
        max_paragraphs: usize,
        min_size: usize,
    ) -> Vec<(String, usize, usize)> {
        let paragraphs: Vec<&str> = content.split("\n\n").collect();
        let mut chunks = Vec::new();
        let mut current_chunk = String::new();
        let mut para_count = 0;
        let mut start_offset = 0;
        let mut current_start = 0;

        for para in paragraphs {
            let para = para.trim();
            if para.is_empty() {
                continue;
            }

            if para_count >= max_paragraphs && current_chunk.len() >= min_size {
                let end_offset = start_offset + current_chunk.len();
                chunks.push((current_chunk.clone(), current_start, end_offset));
                current_chunk.clear();
                current_start = end_offset;
                para_count = 0;
            }

            if !current_chunk.is_empty() {
                current_chunk.push_str("\n\n");
            }
            current_chunk.push_str(para);
            para_count += 1;
            start_offset += para.len() + 2;
        }

        if !current_chunk.is_empty() {
            chunks.push((
                current_chunk.clone(),
                current_start,
                current_start + current_chunk.len(),
            ));
        }

        chunks
    }

    /// Recursive chunking with multiple separators
    #[allow(clippy::only_used_in_recursion)]
    fn chunk_recursive(
        &self,
        content: &str,
        target_size: usize,
        separators: &[String],
    ) -> Vec<(String, usize, usize)> {
        if content.len() <= target_size || separators.is_empty() {
            return vec![(content.to_string(), 0, content.len())];
        }

        let separator = &separators[0];
        let parts: Vec<&str> = content.split(separator).collect();

        if parts.len() <= 1 {
            // Try next separator
            return self.chunk_recursive(content, target_size, &separators[1..]);
        }

        let mut chunks = Vec::new();
        let mut current = String::new();
        let mut offset = 0;
        let mut current_start = 0;

        for part in parts {
            if !current.is_empty() && current.len() + part.len() + separator.len() > target_size {
                chunks.push((
                    current.clone(),
                    current_start,
                    current_start + current.len(),
                ));
                current_start = offset;
                current.clear();
            }

            if !current.is_empty() {
                current.push_str(separator);
            }
            current.push_str(part);
            offset += part.len() + separator.len();
        }

        if !current.is_empty() {
            chunks.push((
                current.clone(),
                current_start,
                current_start + current.len(),
            ));
        }

        chunks
    }

    /// Split text into sentences
    fn split_sentences(&self, text: &str) -> Vec<String> {
        // Simple sentence splitting (can be enhanced with NLP)
        let mut sentences = Vec::new();
        let mut current = String::new();

        for c in text.chars() {
            current.push(c);
            if c == '.' || c == '!' || c == '?' {
                let trimmed = current.trim().to_string();
                if !trimmed.is_empty() {
                    sentences.push(trimmed);
                }
                current.clear();
            }
        }

        // Add remaining text
        let trimmed = current.trim().to_string();
        if !trimmed.is_empty() {
            sentences.push(trimmed);
        }

        sentences
    }

    /// Extend chunk to sentence end
    fn extend_to_sentence_end(&self, text: &str) -> String {
        // Find last sentence ending
        if let Some(pos) = text.rfind(['.', '!', '?']) {
            text[..=pos].to_string()
        } else {
            text.to_string()
        }
    }

    /// Get statistics
    pub fn stats(&self) -> &ChunkerStats {
        &self.stats
    }

    /// Get configuration
    pub fn config(&self) -> &ChunkingConfig {
        &self.config
    }
}

impl Default for Chunker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_id_creation() {
        let doc_id = DocumentId::from_string("doc:123");
        let chunk_id = ChunkId::new(&doc_id, 0);
        assert_eq!(chunk_id.as_str(), "doc:123:chunk:0");
    }

    #[test]
    fn test_fixed_size_chunking() {
        let chunker = Chunker::with_config(ChunkingConfig {
            strategy: ChunkingStrategy::FixedSize {
                size: 100,
                overlap: 20,
            },
            min_chunk_size: 10,
            max_chunk_size: 1000,
            preserve_sentences: false,
            trim_whitespace: true,
        });

        let doc = Document::from_text("A".repeat(250));
        let chunks = chunker.chunk(&doc);

        assert!(chunks.len() >= 2);
        for chunk in &chunks {
            assert!(!chunk.content.is_empty());
        }
    }

    #[test]
    fn test_sentence_chunking() {
        let chunker = Chunker::with_config(ChunkingConfig {
            strategy: ChunkingStrategy::Sentence {
                max_sentences: 2,
                min_size: 10,
            },
            min_chunk_size: 5,
            max_chunk_size: 1000,
            preserve_sentences: true,
            trim_whitespace: true,
        });

        let doc = Document::from_text(
            "First sentence. Second sentence. Third sentence. Fourth sentence.",
        );
        let chunks = chunker.chunk(&doc);

        assert!(chunks.len() >= 2);
    }

    #[test]
    fn test_paragraph_chunking() {
        let chunker = Chunker::with_config(ChunkingConfig {
            strategy: ChunkingStrategy::Paragraph {
                max_paragraphs: 1,
                min_size: 5,
            },
            min_chunk_size: 5,
            max_chunk_size: 1000,
            preserve_sentences: false,
            trim_whitespace: true,
        });

        let doc = Document::from_text(
            "Paragraph one content.\n\nParagraph two content.\n\nParagraph three.",
        );
        let chunks = chunker.chunk(&doc);

        assert_eq!(chunks.len(), 3);
    }

    #[test]
    fn test_recursive_chunking() {
        let chunker = Chunker::with_config(ChunkingConfig {
            strategy: ChunkingStrategy::Recursive {
                target_size: 50,
                separators: vec!["\n\n".to_string(), "\n".to_string(), ". ".to_string()],
            },
            min_chunk_size: 5,
            max_chunk_size: 100,
            preserve_sentences: false,
            trim_whitespace: true,
        });

        let doc = Document::from_text(
            "Short paragraph.\n\nAnother paragraph with more content that is longer.",
        );
        let chunks = chunker.chunk(&doc);

        assert!(!chunks.is_empty());
    }

    #[test]
    fn test_empty_document() {
        let chunker = Chunker::new();
        let doc = Document::from_text("");
        let chunks = chunker.chunk(&doc);
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_chunk_with_embedding() {
        let mut chunk = Chunk::new(
            DocumentId::from_string("doc:1"),
            "Test content".to_string(),
            0,
            12,
            0,
            1,
        );

        assert!(!chunk.has_embedding());

        chunk.set_embedding(vec![0.1, 0.2, 0.3]);
        assert!(chunk.has_embedding());
    }

    #[test]
    fn test_chunker_stats() {
        let chunker = Chunker::new();

        let doc1 = Document::from_text("A".repeat(1000));
        let doc2 = Document::from_text("B".repeat(500));

        chunker.chunk(&doc1);
        chunker.chunk(&doc2);

        let stats = chunker.stats();
        assert_eq!(stats.documents_chunked.load(Ordering::Relaxed), 2);
        assert!(stats.chunks_created.load(Ordering::Relaxed) > 0);
        assert_eq!(stats.chars_processed.load(Ordering::Relaxed), 1500);
    }
}
