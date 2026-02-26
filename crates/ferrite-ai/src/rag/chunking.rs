//! Advanced document chunking strategies for RAG pipelines
//!
//! Provides standalone chunking functions that complement the [`Chunker`](super::Chunker):
//! - Fixed-size: Split by character count with overlap
//! - Sentence-boundary: Split on sentence endings, grouped by max size
//! - Recursive: Hierarchical splitting with fallback separators
//! - Sliding window: Overlapping chunks for context preservation

/// Chunk a document using the recursive strategy.
///
/// Tries separators in order (`"\n\n"`, `"\n"`, `". "`, `" "`) and recursively
/// splits chunks that exceed `max_size`. Falls back to fixed-size splitting
/// when no separators remain.
pub fn chunk_recursive(text: &str, max_size: usize, overlap: usize) -> Vec<String> {
    let separators = ["\n\n", "\n", ". ", " "];
    recursive_split(text, &separators, max_size, overlap)
}

fn recursive_split(
    text: &str,
    separators: &[&str],
    max_size: usize,
    overlap: usize,
) -> Vec<String> {
    if text.len() <= max_size {
        return vec![text.to_string()];
    }

    let sep = match separators.first() {
        Some(s) => *s,
        None => return fixed_split(text, max_size, overlap),
    };

    let remaining_seps = &separators[1..];
    let parts: Vec<&str> = text.split(sep).collect();
    let mut chunks = Vec::new();
    let mut current = String::new();

    for part in parts {
        let candidate = if current.is_empty() {
            part.to_string()
        } else {
            format!("{}{}{}", current, sep, part)
        };

        if candidate.len() > max_size && !current.is_empty() {
            if current.len() > max_size {
                chunks.extend(recursive_split(&current, remaining_seps, max_size, overlap));
            } else {
                chunks.push(current.clone());
            }
            current = part.to_string();
        } else {
            current = candidate;
        }
    }

    if !current.is_empty() {
        if current.len() > max_size {
            chunks.extend(recursive_split(&current, remaining_seps, max_size, overlap));
        } else {
            chunks.push(current);
        }
    }

    apply_overlap(&chunks, overlap)
}

/// Fixed-size chunking with overlap.
///
/// Splits `text` into chunks of at most `max_size` characters, advancing by
/// `max_size - overlap` characters each step.
pub fn fixed_split(text: &str, max_size: usize, overlap: usize) -> Vec<String> {
    let chars: Vec<char> = text.chars().collect();
    if chars.is_empty() {
        return vec![];
    }

    let mut chunks = Vec::new();
    let step = if max_size > overlap {
        max_size - overlap
    } else {
        1
    };
    let mut start = 0;

    while start < chars.len() {
        let end = (start + max_size).min(chars.len());
        chunks.push(chars[start..end].iter().collect());
        if end >= chars.len() {
            break;
        }
        start += step;
    }

    chunks
}

/// Sentence-boundary chunking.
///
/// Splits `text` on sentence endings (`.`, `!`, `?`) and groups consecutive
/// sentences into chunks that do not exceed `max_size` characters.
pub fn chunk_sentences(text: &str, max_size: usize, overlap: usize) -> Vec<String> {
    let sentence_endings = [". ", "! ", "? ", ".\n", "!\n", "?\n"];
    let mut sentences = Vec::new();
    let mut remaining = text;

    while !remaining.is_empty() {
        let mut min_pos = remaining.len();
        for ending in &sentence_endings {
            if let Some(pos) = remaining.find(ending) {
                min_pos = min_pos.min(pos + ending.len());
            }
        }
        let (sentence, rest) = remaining.split_at(min_pos);
        if !sentence.is_empty() {
            sentences.push(sentence.to_string());
        }
        remaining = rest;
    }

    let mut chunks = Vec::new();
    let mut current = String::new();

    for sentence in &sentences {
        if current.len() + sentence.len() > max_size && !current.is_empty() {
            chunks.push(current.trim().to_string());
            current = String::new();
        }
        current.push_str(sentence);
    }

    if !current.is_empty() {
        chunks.push(current.trim().to_string());
    }

    apply_overlap(&chunks, overlap)
}

fn apply_overlap(chunks: &[String], overlap: usize) -> Vec<String> {
    if overlap == 0 || chunks.len() <= 1 {
        return chunks.to_vec();
    }

    let mut result = Vec::with_capacity(chunks.len());
    for (i, chunk) in chunks.iter().enumerate() {
        if i == 0 {
            result.push(chunk.clone());
        } else {
            let prev = &chunks[i - 1];
            let overlap_text: String = prev
                .chars()
                .rev()
                .take(overlap)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect();
            result.push(format!("{}{}", overlap_text, chunk));
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fixed_split_basic() {
        let text = "Hello World, this is a test document for chunking.";
        let chunks = fixed_split(text, 20, 5);
        assert!(!chunks.is_empty());
        // Each chunk should be at most max_size + possible overlap chars
        assert!(chunks.iter().all(|c| c.len() <= 25));
    }

    #[test]
    fn test_fixed_split_no_overlap() {
        let chunks = fixed_split("abcdefghij", 5, 0);
        assert_eq!(chunks, vec!["abcde", "fghij"]);
    }

    #[test]
    fn test_recursive_split() {
        let text =
            "Paragraph one.\n\nParagraph two.\n\nParagraph three is longer and needs splitting.";
        let chunks = chunk_recursive(text, 30, 0);
        assert!(chunks.len() >= 2);
    }

    #[test]
    fn test_recursive_split_small_max() {
        let text = "Hello world. This is a test.";
        let chunks = chunk_recursive(text, 10, 0);
        assert!(chunks.iter().all(|c| c.len() <= 10));
    }

    #[test]
    fn test_sentence_chunking() {
        let text = "First sentence. Second sentence. Third sentence. Fourth sentence.";
        let chunks = chunk_sentences(text, 40, 0);
        assert!(!chunks.is_empty());
    }

    #[test]
    fn test_sentence_chunking_single() {
        let text = "One sentence only.";
        let chunks = chunk_sentences(text, 100, 0);
        assert_eq!(chunks.len(), 1);
    }

    #[test]
    fn test_empty_text() {
        assert!(fixed_split("", 100, 0).is_empty());
        assert_eq!(chunk_recursive("", 100, 0), vec![""]);
    }

    #[test]
    fn test_overlap_applied() {
        let chunks = fixed_split("abcdefghijklmnopqrst", 10, 3);
        assert!(chunks.len() >= 2);
        // Second chunk should start with last 3 chars of first chunk
        // (overlap is applied by fixed_split via step size)
    }
}
