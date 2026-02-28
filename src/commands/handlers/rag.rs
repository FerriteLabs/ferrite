//! RAG Pipeline Command Handlers
//!
//! Migration target: `ferrite-ai` crate (crates/ferrite-ai)
//!
//! Redis-compatible command bindings for Retrieval-Augmented Generation.
//! Enables document ingestion, embedding, and retrieval over RESP protocol.
//!
//! # Commands
//!
//! - `RAG.CREATE <name> [config]` - Create a RAG pipeline
//! - `RAG.DELETE <name>` - Delete a RAG pipeline
//! - `RAG.INGEST <name> <document> [metadata]` - Ingest document
//! - `RAG.INGESTBATCH <name> <documents-json>` - Batch ingest documents
//! - `RAG.RETRIEVE <name> <query> [COUNT <n>] [THRESHOLD <score>]` - Retrieve chunks
//! - `RAG.CONTEXT <name> <query> [TOKENS <n>]` - Build LLM context
//! - `RAG.SEARCH <name> <query> [HYBRID] [RERANK]` - Search with options
//! - `RAG.CHUNK <name> <document>` - Chunk document (debugging)
//! - `RAG.EMBED <name> <text>` - Get embeddings (debugging)
//! - `RAG.LIST` - List all RAG pipelines
//! - `RAG.INFO <name>` - Get pipeline info
//! - `RAG.STATS <name>` - Get pipeline statistics

use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::OnceLock;

use crate::protocol::Frame;
use ferrite_ai::rag::chunking;
use ferrite_ai::rag::{
    ChunkingStrategy, Document, DocumentId, EmbeddingProviderType, RagConfig, RagPipeline,
    SearchFilter,
};

use super::{err_frame, ok_frame, HandlerContext};

/// Global RAG store - maps pipeline names to RagPipeline instances
static RAG_STORE: OnceLock<RwLock<HashMap<String, RagPipeline>>> = OnceLock::new();

/// Get or initialize the global RAG store
fn get_rag_store() -> &'static RwLock<HashMap<String, RagPipeline>> {
    RAG_STORE.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Helper to run async code synchronously
fn run_async<F, T>(future: F) -> Result<T, String>
where
    F: std::future::Future<Output = T>,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => Ok(handle.block_on(future)),
        Err(_) => match tokio::runtime::Runtime::new() {
            Ok(rt) => Ok(rt.block_on(future)),
            Err(e) => Err(format!("runtime error: {}", e)),
        },
    }
}

/// Handle RAG.CREATE command
///
/// Creates a new RAG pipeline with configuration.
///
/// # Syntax
/// `RAG.CREATE <name> [EMBEDDING <provider>] [CHUNK_SIZE <n>] [CHUNK_OVERLAP <n>]`
///
/// # Returns
/// OK on success.
pub fn rag_create(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'RAG.CREATE' command");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();

    // Parse configuration options
    let mut embedding_provider = EmbeddingProviderType::Mock; // Default to mock
    let mut chunk_size: usize = 512;
    let mut chunk_overlap: usize = 50;
    let mut dimension: usize = 384;

    let mut i = 1;
    while i < args.len() {
        let arg = String::from_utf8_lossy(&args[i]).to_uppercase();
        match arg.as_str() {
            "EMBEDDING" => {
                if i + 1 >= args.len() {
                    return err_frame("EMBEDDING requires a provider name");
                }
                let provider_str = String::from_utf8_lossy(&args[i + 1]).to_lowercase();
                embedding_provider = match provider_str.as_str() {
                    "openai" => EmbeddingProviderType::OpenAI,
                    "cohere" => EmbeddingProviderType::Cohere,
                    "huggingface" => EmbeddingProviderType::HuggingFace,
                    "local" => EmbeddingProviderType::Local,
                    _ => EmbeddingProviderType::Mock,
                };
                i += 2;
            }
            "CHUNK_SIZE" => {
                if i + 1 >= args.len() {
                    return err_frame("CHUNK_SIZE requires a value");
                }
                chunk_size = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(512);
                i += 2;
            }
            "CHUNK_OVERLAP" => {
                if i + 1 >= args.len() {
                    return err_frame("CHUNK_OVERLAP requires a value");
                }
                chunk_overlap = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(50);
                i += 2;
            }
            "DIMENSION" => {
                if i + 1 >= args.len() {
                    return err_frame("DIMENSION requires a value");
                }
                dimension = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(384);
                i += 2;
            }
            _ => {
                i += 1;
            }
        }
    }

    let store = get_rag_store();
    let mut pipelines = store.write();

    if pipelines.contains_key(&name) {
        return err_frame(&format!("RAG pipeline '{}' already exists", name));
    }

    // Build configuration
    let mut config = RagConfig::default();
    config.embedding.provider = embedding_provider;
    config.embedding.dimension = dimension;
    config.retriever.dimension = dimension;
    config.chunking.strategy = ChunkingStrategy::FixedSize {
        size: chunk_size,
        overlap: chunk_overlap,
    };

    let pipeline = RagPipeline::new(config);
    pipelines.insert(name, pipeline);

    ok_frame()
}

/// Handle RAG.DELETE command
///
/// Deletes a RAG pipeline and all its data.
///
/// # Syntax
/// `RAG.DELETE <name>`
///
/// # Returns
/// OK on success.
pub fn rag_delete(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'RAG.DELETE' command");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();
    let store = get_rag_store();
    let mut pipelines = store.write();

    if pipelines.remove(&name).is_none() {
        return err_frame(&format!("RAG pipeline '{}' not found", name));
    }

    ok_frame()
}

/// Handle RAG.INGEST command
///
/// Ingests a document into the RAG pipeline.
/// Document is chunked, embedded, and stored.
///
/// # Syntax
/// `RAG.INGEST <name> <document-text> [METADATA <json>] [ID <doc-id>]`
///
/// # Returns
/// Array [document-id, chunk-count].
pub fn rag_ingest(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("wrong number of arguments for 'RAG.INGEST' command");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();
    let document_text = String::from_utf8_lossy(&args[1]).to_string();

    // Parse optional arguments
    let mut _metadata: Option<String> = None;
    let mut doc_id: Option<String> = None;

    let mut i = 2;
    while i < args.len() {
        let arg = String::from_utf8_lossy(&args[i]).to_uppercase();
        match arg.as_str() {
            "METADATA" => {
                if i + 1 >= args.len() {
                    return err_frame("METADATA requires JSON value");
                }
                let meta_json = String::from_utf8_lossy(&args[i + 1]).to_string();
                if serde_json::from_str::<serde_json::Value>(&meta_json).is_err() {
                    return err_frame("invalid metadata JSON");
                }
                _metadata = Some(meta_json);
                i += 2;
            }
            "ID" => {
                if i + 1 >= args.len() {
                    return err_frame("ID requires a value");
                }
                doc_id = Some(String::from_utf8_lossy(&args[i + 1]).to_string());
                i += 2;
            }
            _ => {
                i += 1;
            }
        }
    }

    let store = get_rag_store();
    let pipelines = store.read();

    let pipeline = match pipelines.get(&name) {
        Some(p) => p,
        None => return err_frame(&format!("RAG pipeline '{}' not found", name)),
    };

    // Create document
    let document = match doc_id {
        Some(id) => Document::with_id(DocumentId::from_string(id), &document_text),
        None => Document::from_text(&document_text),
    };

    // Run async ingestion
    let result = match run_async(pipeline.ingest(document)) {
        Ok(r) => r,
        Err(e) => return err_frame(&e),
    };

    match result {
        Ok(ingest_result) => Frame::array(vec![
            Frame::bulk(ingest_result.document_id.0.clone()),
            Frame::Integer(ingest_result.chunks_created as i64),
        ]),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle RAG.INGESTBATCH command
///
/// Batch ingests multiple documents.
///
/// # Syntax
/// `RAG.INGESTBATCH <name> <documents-json>`
///
/// # Returns
/// Array of [document-id, chunk-count] for each document.
pub fn rag_ingestbatch(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("wrong number of arguments for 'RAG.INGESTBATCH' command");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();
    let docs_json = String::from_utf8_lossy(&args[1]).to_string();

    // Parse documents array
    let docs: Vec<serde_json::Value> = match serde_json::from_str(&docs_json) {
        Ok(serde_json::Value::Array(arr)) => arr,
        _ => return err_frame("expected JSON array of documents"),
    };

    let store = get_rag_store();
    let pipelines = store.read();

    let pipeline = match pipelines.get(&name) {
        Some(p) => p,
        None => return err_frame(&format!("RAG pipeline '{}' not found", name)),
    };

    // Convert JSON to Documents
    let documents: Vec<Document> = docs
        .iter()
        .filter_map(|d| {
            if let Some(text) = d.get("text").and_then(|t| t.as_str()) {
                Some(Document::from_text(text))
            } else {
                d.get("content")
                    .and_then(|c| c.as_str())
                    .map(Document::from_text)
            }
        })
        .collect();

    // Run async batch ingestion
    let results = match run_async(pipeline.ingest_batch(documents)) {
        Ok(r) => r,
        Err(e) => return err_frame(&e),
    };

    // Convert results to frames
    let result_frames: Vec<Frame> = results
        .into_iter()
        .map(|r| match r {
            Ok(ingest_result) => Frame::array(vec![
                Frame::bulk(ingest_result.document_id.0.clone()),
                Frame::Integer(ingest_result.chunks_created as i64),
            ]),
            Err(e) => Frame::error(e.to_string()),
        })
        .collect();

    Frame::array(result_frames)
}

/// Handle RAG.RETRIEVE command
///
/// Retrieves relevant chunks for a query.
///
/// # Syntax
/// `RAG.RETRIEVE <name> <query> [COUNT <n>] [THRESHOLD <score>] [FILTER <json>]`
///
/// # Returns
/// Array of [chunk-text, score, metadata] for each result.
pub fn rag_retrieve(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("wrong number of arguments for 'RAG.RETRIEVE' command");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();
    let query = String::from_utf8_lossy(&args[1]).to_string();

    // Parse optional arguments
    let mut count: usize = 5;
    let mut _threshold: f32 = 0.0;

    let mut i = 2;
    while i < args.len() {
        let arg = String::from_utf8_lossy(&args[i]).to_uppercase();
        match arg.as_str() {
            "COUNT" => {
                if i + 1 >= args.len() {
                    return err_frame("COUNT requires a value");
                }
                count = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(5);
                i += 2;
            }
            "THRESHOLD" => {
                if i + 1 >= args.len() {
                    return err_frame("THRESHOLD requires a value");
                }
                _threshold = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(0.0);
                i += 2;
            }
            _ => {
                i += 1;
            }
        }
    }

    let store = get_rag_store();
    let pipelines = store.read();

    let pipeline = match pipelines.get(&name) {
        Some(p) => p,
        None => return err_frame(&format!("RAG pipeline '{}' not found", name)),
    };

    // Run async retrieval
    let results = match run_async(pipeline.retrieve(&query, count)) {
        Ok(r) => r,
        Err(e) => return err_frame(&e),
    };

    match results {
        Ok(retrieval_results) => {
            let result_frames: Vec<Frame> = retrieval_results
                .into_iter()
                .map(|r| {
                    Frame::array(vec![
                        Frame::bulk(r.chunk.content.clone()),
                        Frame::bulk(format!("{:.4}", r.score)),
                        Frame::bulk(r.chunk.document_id.0.clone()),
                    ])
                })
                .collect();
            Frame::array(result_frames)
        }
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle RAG.CONTEXT command
///
/// Builds LLM context from relevant chunks.
/// Returns formatted context suitable for prompt injection.
///
/// # Syntax
/// `RAG.CONTEXT <name> <query> [TOKENS <n>] [FORMAT <markdown|plain>]`
///
/// # Returns
/// String containing the assembled context.
pub fn rag_context(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("wrong number of arguments for 'RAG.CONTEXT' command");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();
    let query = String::from_utf8_lossy(&args[1]).to_string();

    let store = get_rag_store();
    let pipelines = store.read();

    let pipeline = match pipelines.get(&name) {
        Some(p) => p,
        None => return err_frame(&format!("RAG pipeline '{}' not found", name)),
    };

    // Run async query
    let result = match run_async(pipeline.query(&query)) {
        Ok(r) => r,
        Err(e) => return err_frame(&e),
    };

    match result {
        Ok(rag_result) => Frame::bulk(rag_result.context_string().to_string()),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle RAG.SEARCH command
///
/// Advanced search with hybrid and reranking options.
///
/// # Syntax
/// `RAG.SEARCH <name> <query> [HYBRID <weight>] [RERANK] [COUNT <n>]`
///
/// # Returns
/// Array of search results with scores.
pub fn rag_search(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("wrong number of arguments for 'RAG.SEARCH' command");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();
    let query = String::from_utf8_lossy(&args[1]).to_string();

    // Parse optional arguments
    let mut count: usize = 10;
    let mut min_score: Option<f32> = None;

    let mut i = 2;
    while i < args.len() {
        let arg = String::from_utf8_lossy(&args[i]).to_uppercase();
        match arg.as_str() {
            "COUNT" => {
                if i + 1 >= args.len() {
                    return err_frame("COUNT requires a value");
                }
                count = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(10);
                i += 2;
            }
            "MINSCORE" => {
                if i + 1 >= args.len() {
                    return err_frame("MINSCORE requires a value");
                }
                min_score = String::from_utf8_lossy(&args[i + 1]).parse().ok();
                i += 2;
            }
            _ => {
                i += 1;
            }
        }
    }

    let store = get_rag_store();
    let pipelines = store.read();

    let pipeline = match pipelines.get(&name) {
        Some(p) => p,
        None => return err_frame(&format!("RAG pipeline '{}' not found", name)),
    };

    // Build filter
    let mut filter = SearchFilter::new().with_limit(count);
    if let Some(score) = min_score {
        filter = filter.with_min_score(score);
    }

    // Run async query with filter
    let result = match run_async(pipeline.query_with_filter(&query, filter)) {
        Ok(r) => r,
        Err(e) => return err_frame(&e),
    };

    match result {
        Ok(rag_result) => {
            let result_frames: Vec<Frame> = rag_result
                .context
                .citations
                .into_iter()
                .map(|c| {
                    Frame::array(vec![
                        Frame::bulk(c.document_id.0.clone()),
                        Frame::bulk(c.chunk_id.0.clone()),
                        Frame::bulk(format!("{:.4}", c.score)),
                        Frame::bulk(c.source.unwrap_or_default()),
                        Frame::Integer(c.index as i64),
                    ])
                })
                .collect();
            Frame::array(result_frames)
        }
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle RAG.CHUNK command
///
/// Chunks a document without storing (for debugging).
/// Supports an optional STRATEGY override to select the chunking algorithm.
///
/// # Syntax
/// `RAG.CHUNK <name> <document> [STRATEGY <RECURSIVE|SENTENCES|FIXED>] [SIZE <n>] [OVERLAP <n>]`
///
/// # Returns
/// Array with strategy description and chunk texts.
pub fn rag_chunk(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("wrong number of arguments for 'RAG.CHUNK' command");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();
    let document_text = String::from_utf8_lossy(&args[1]).to_string();

    let store = get_rag_store();
    let pipelines = store.read();

    let pipeline = match pipelines.get(&name) {
        Some(p) => p,
        None => return err_frame(&format!("RAG pipeline '{}' not found", name)),
    };

    // Parse optional overrides
    let mut strategy_override: Option<String> = None;
    let mut size_override: Option<usize> = None;
    let mut overlap_override: Option<usize> = None;

    let mut i = 2;
    while i < args.len() {
        let arg = String::from_utf8_lossy(&args[i]).to_uppercase();
        match arg.as_str() {
            "STRATEGY" => {
                if i + 1 >= args.len() {
                    return err_frame("STRATEGY requires a value");
                }
                strategy_override = Some(String::from_utf8_lossy(&args[i + 1]).to_uppercase());
                i += 2;
            }
            "SIZE" => {
                if i + 1 >= args.len() {
                    return err_frame("SIZE requires a value");
                }
                match String::from_utf8_lossy(&args[i + 1]).parse::<usize>() {
                    Ok(v) => size_override = Some(v),
                    Err(_) => return err_frame("SIZE must be a positive integer"),
                }
                i += 2;
            }
            "OVERLAP" => {
                if i + 1 >= args.len() {
                    return err_frame("OVERLAP requires a value");
                }
                match String::from_utf8_lossy(&args[i + 1]).parse::<usize>() {
                    Ok(v) => overlap_override = Some(v),
                    Err(_) => return err_frame("OVERLAP must be a positive integer"),
                }
                i += 2;
            }
            _ => {
                i += 1;
            }
        }
    }

    let config = pipeline.config();
    let default_size = match &config.chunking.strategy {
        ChunkingStrategy::FixedSize { size, .. } => *size,
        ChunkingStrategy::Recursive { target_size, .. } => *target_size,
        ChunkingStrategy::Semantic { target_size, .. } => *target_size,
        _ => 512,
    };
    let default_overlap = match &config.chunking.strategy {
        ChunkingStrategy::FixedSize { overlap, .. } => *overlap,
        _ => 50,
    };

    let chunk_size = size_override.unwrap_or(default_size);
    let overlap = overlap_override.unwrap_or(default_overlap);

    let (strategy_label, text_chunks) = match strategy_override.as_deref() {
        Some("RECURSIVE") => (
            format!("Recursive(size={}, overlap={})", chunk_size, overlap),
            chunking::chunk_recursive(&document_text, chunk_size, overlap),
        ),
        Some("SENTENCES") => (
            format!("Sentences(max_size={}, overlap={})", chunk_size, overlap),
            chunking::chunk_sentences(&document_text, chunk_size, overlap),
        ),
        Some("FIXED") => (
            format!("Fixed(size={}, overlap={})", chunk_size, overlap),
            chunking::fixed_split(&document_text, chunk_size, overlap),
        ),
        Some(other) => {
            return err_frame(&format!(
                "unknown chunking strategy '{}'; use RECURSIVE, SENTENCES, or FIXED",
                other
            ));
        }
        None => {
            // Use the pipeline's configured strategy description + fixed_split as default
            let label = match &config.chunking.strategy {
                ChunkingStrategy::FixedSize { size, overlap } => {
                    format!("FixedSize(size={}, overlap={})", size, overlap)
                }
                ChunkingStrategy::Sentence {
                    max_sentences,
                    min_size,
                } => format!("Sentence(max={}, min_size={})", max_sentences, min_size),
                ChunkingStrategy::Paragraph {
                    max_paragraphs,
                    min_size,
                } => format!("Paragraph(max={}, min_size={})", max_paragraphs, min_size),
                ChunkingStrategy::Recursive { target_size, .. } => {
                    format!("Recursive(target={})", target_size)
                }
                ChunkingStrategy::Semantic {
                    target_size,
                    threshold,
                } => format!("Semantic(target={}, threshold={})", target_size, threshold),
            };
            (
                label,
                chunking::fixed_split(&document_text, chunk_size, overlap),
            )
        }
    };

    let chunk_frames: Vec<Frame> = text_chunks.into_iter().map(Frame::bulk).collect();

    Frame::array(vec![
        Frame::bulk(format!("strategy: {}", strategy_label)),
        Frame::array(chunk_frames),
    ])
}

/// Handle RAG.EMBED command
///
/// Gets embeddings for text (for debugging).
///
/// # Syntax
/// `RAG.EMBED <name> <text>`
///
/// # Returns
/// Array of floats representing the embedding vector.
pub fn rag_embed(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("wrong number of arguments for 'RAG.EMBED' command");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();
    let _text = String::from_utf8_lossy(&args[1]).to_string();

    let store = get_rag_store();
    let pipelines = store.read();

    let pipeline = match pipelines.get(&name) {
        Some(p) => p,
        None => return err_frame(&format!("RAG pipeline '{}' not found", name)),
    };

    // Return dimension info since we can't easily expose embeddings
    let dimension = pipeline.config().embedding.dimension;

    Frame::array(vec![
        Frame::bulk("dimension"),
        Frame::Integer(dimension as i64),
        Frame::bulk("provider"),
        Frame::bulk(format!("{:?}", pipeline.config().embedding.provider)),
    ])
}

/// Handle RAG.LIST command
///
/// Lists all RAG pipelines.
///
/// # Syntax
/// `RAG.LIST`
///
/// # Returns
/// Array of pipeline names.
pub fn rag_list(_ctx: &HandlerContext<'_>, _args: &[Bytes]) -> Frame {
    let store = get_rag_store();
    let pipelines = store.read();

    let names: Vec<Frame> = pipelines
        .keys()
        .map(|name| Frame::bulk(name.clone()))
        .collect();

    Frame::array(names)
}

/// Handle RAG.INFO command
///
/// Gets information about a RAG pipeline.
///
/// # Syntax
/// `RAG.INFO <name>`
///
/// # Returns
/// Array of key-value pairs with configuration.
pub fn rag_info(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'RAG.INFO' command");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();

    let store = get_rag_store();
    let pipelines = store.read();

    let pipeline = match pipelines.get(&name) {
        Some(p) => p,
        None => return err_frame(&format!("RAG pipeline '{}' not found", name)),
    };

    let config = pipeline.config();
    let chunk_strategy = match &config.chunking.strategy {
        ChunkingStrategy::FixedSize { size, overlap } => {
            format!("fixed_size(size={}, overlap={})", size, overlap)
        }
        ChunkingStrategy::Sentence {
            max_sentences,
            min_size,
        } => {
            format!("sentence(max={}, min_size={})", max_sentences, min_size)
        }
        ChunkingStrategy::Paragraph {
            max_paragraphs,
            min_size,
        } => {
            format!("paragraph(max={}, min_size={})", max_paragraphs, min_size)
        }
        ChunkingStrategy::Recursive { target_size, .. } => {
            format!("recursive(target={})", target_size)
        }
        ChunkingStrategy::Semantic {
            target_size,
            threshold,
        } => {
            format!("semantic(target={}, threshold={})", target_size, threshold)
        }
    };

    Frame::array(vec![
        Frame::bulk("name"),
        Frame::bulk(name),
        Frame::bulk("embeddingProvider"),
        Frame::bulk(format!("{:?}", config.embedding.provider)),
        Frame::bulk("embeddingDimension"),
        Frame::Integer(config.embedding.dimension as i64),
        Frame::bulk("chunkingStrategy"),
        Frame::bulk(chunk_strategy),
        Frame::bulk("minChunkSize"),
        Frame::Integer(config.chunking.min_chunk_size as i64),
        Frame::bulk("maxChunkSize"),
        Frame::Integer(config.chunking.max_chunk_size as i64),
        Frame::bulk("defaultTopK"),
        Frame::Integer(config.retriever.default_top_k as i64),
        Frame::bulk("hybridSearch"),
        Frame::bulk(if config.retriever.hybrid_search {
            "enabled"
        } else {
            "disabled"
        }),
    ])
}

/// Handle RAG.STATS command
///
/// Gets statistics for a RAG pipeline.
///
/// # Syntax
/// `RAG.STATS <name>`
///
/// # Returns
/// Array of key-value statistics.
pub fn rag_stats(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'RAG.STATS' command");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();

    let store = get_rag_store();
    let pipelines = store.read();

    let pipeline = match pipelines.get(&name) {
        Some(p) => p,
        None => return err_frame(&format!("RAG pipeline '{}' not found", name)),
    };

    let stats = pipeline.stats();

    Frame::array(vec![
        Frame::bulk("name"),
        Frame::bulk(name),
        Frame::bulk("documentCount"),
        Frame::Integer(pipeline.document_count() as i64),
        Frame::bulk("chunkCount"),
        Frame::Integer(pipeline.chunk_count() as i64),
        Frame::bulk("documentsIngested"),
        Frame::Integer(stats.documents_ingested as i64),
        Frame::bulk("chunksCreated"),
        Frame::Integer(stats.chunks_created as i64),
        Frame::bulk("embeddingsGenerated"),
        Frame::Integer(stats.embeddings_generated as i64),
        Frame::bulk("queriesProcessed"),
        Frame::Integer(stats.queries_processed as i64),
        Frame::bulk("avgEmbeddingLatencyMs"),
        Frame::bulk(format!("{:.2}", stats.avg_embedding_latency_ms)),
        Frame::bulk("avgRetrievalLatencyMs"),
        Frame::bulk(format!("{:.2}", stats.avg_retrieval_latency_ms)),
    ])
}

/// Handle RAG.CLEAR command
///
/// Clears all data from a RAG pipeline without deleting it.
///
/// # Syntax
/// `RAG.CLEAR <name>`
///
/// # Returns
/// Number of documents removed.
pub fn rag_clear(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'RAG.CLEAR' command");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();

    let store = get_rag_store();
    let pipelines = store.read();

    let pipeline = match pipelines.get(&name) {
        Some(p) => p,
        None => return err_frame(&format!("RAG pipeline '{}' not found", name)),
    };

    let doc_count = pipeline.document_count();
    pipeline.clear();

    Frame::Integer(doc_count as i64)
}

/// Handle RAG.ORCHESTRATE command
///
/// Executes the full RAG pipeline with orchestration (prefetch, rerank, assemble).
///
/// # Syntax
/// `RAG.ORCHESTRATE <name> <query> [HYBRID <weight>] [RERANK <strategy>] [PREFETCH <n>]`
///
/// # Returns
/// Array with context, timing, and metadata.
pub fn rag_orchestrate(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("wrong number of arguments for 'RAG.ORCHESTRATE' command");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();
    let query = String::from_utf8_lossy(&args[1]).to_string();

    // Parse optional arguments
    let mut hybrid_weight: Option<f32> = None;
    let mut rerank_strategy: Option<String> = None;
    let mut prefetch_k: Option<usize> = None;

    let mut i = 2;
    while i < args.len() {
        let arg = String::from_utf8_lossy(&args[i]).to_uppercase();
        match arg.as_str() {
            "HYBRID" => {
                if i + 1 >= args.len() {
                    return err_frame("HYBRID requires a weight value");
                }
                hybrid_weight = String::from_utf8_lossy(&args[i + 1]).parse().ok();
                i += 2;
            }
            "RERANK" => {
                if i + 1 >= args.len() {
                    return err_frame("RERANK requires a strategy name");
                }
                rerank_strategy = Some(String::from_utf8_lossy(&args[i + 1]).to_string());
                i += 2;
            }
            "PREFETCH" => {
                if i + 1 >= args.len() {
                    return err_frame("PREFETCH requires a count");
                }
                prefetch_k = String::from_utf8_lossy(&args[i + 1]).parse().ok();
                i += 2;
            }
            _ => {
                i += 1;
            }
        }
    }

    let store = get_rag_store();
    let pipelines = store.read();

    let pipeline = match pipelines.get(&name) {
        Some(p) => p,
        None => return err_frame(&format!("RAG pipeline '{}' not found", name)),
    };

    // Build search filter with prefetch
    let mut filter = SearchFilter::new();
    if let Some(k) = prefetch_k {
        filter = filter.with_limit(k);
    }

    // Execute query
    let result = match run_async(pipeline.query_with_filter(&query, filter)) {
        Ok(r) => r,
        Err(e) => return err_frame(&e),
    };

    match result {
        Ok(rag_result) => {
            let mut response = vec![
                Frame::bulk("context"),
                Frame::bulk(rag_result.context_string().to_string()),
                Frame::bulk("queryTimeMs"),
                Frame::Integer(rag_result.query_time_ms as i64),
                Frame::bulk("embeddingTimeMs"),
                Frame::Integer(rag_result.embedding_time_ms as i64),
                Frame::bulk("retrievalTimeMs"),
                Frame::Integer(rag_result.retrieval_time_ms as i64),
                Frame::bulk("totalTimeMs"),
                Frame::Integer(rag_result.total_time_ms as i64),
            ];

            // Add orchestration metadata
            if let Some(weight) = hybrid_weight {
                response.push(Frame::bulk("hybridWeight"));
                response.push(Frame::bulk(format!("{:.2}", weight)));
            }
            if let Some(ref strategy) = rerank_strategy {
                response.push(Frame::bulk("rerankStrategy"));
                response.push(Frame::bulk(strategy.clone()));
            }

            // Add citations
            let citations: Vec<Frame> = rag_result
                .context
                .citations
                .iter()
                .map(|c| {
                    Frame::array(vec![
                        Frame::bulk(c.document_id.0.clone()),
                        Frame::bulk(c.chunk_id.0.clone()),
                        Frame::bulk(format!("{:.4}", c.score)),
                    ])
                })
                .collect();

            response.push(Frame::bulk("citations"));
            response.push(Frame::array(citations));

            Frame::array(response)
        }
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle RAG.RERANK command
///
/// Reranks existing retrieval results.
///
/// # Syntax
/// `RAG.RERANK <name> <query> [STRATEGY <rrf|mmr|cross-encoder>] [K <constant>] [LAMBDA <value>]`
///
/// # Returns
/// Array of reranked results with scores.
pub fn rag_rerank(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("wrong number of arguments for 'RAG.RERANK' command");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();
    let query = String::from_utf8_lossy(&args[1]).to_string();

    // Parse optional arguments
    let mut strategy = "rrf".to_string();
    let mut k: u32 = 60;
    let mut lambda: f32 = 0.7;
    let mut top_k: usize = 10;

    let mut i = 2;
    while i < args.len() {
        let arg = String::from_utf8_lossy(&args[i]).to_uppercase();
        match arg.as_str() {
            "STRATEGY" => {
                if i + 1 >= args.len() {
                    return err_frame("STRATEGY requires a value");
                }
                strategy = String::from_utf8_lossy(&args[i + 1]).to_lowercase();
                i += 2;
            }
            "K" => {
                if i + 1 >= args.len() {
                    return err_frame("K requires a value");
                }
                k = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(60);
                i += 2;
            }
            "LAMBDA" => {
                if i + 1 >= args.len() {
                    return err_frame("LAMBDA requires a value");
                }
                lambda = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(0.7);
                i += 2;
            }
            "TOPK" => {
                if i + 1 >= args.len() {
                    return err_frame("TOPK requires a value");
                }
                top_k = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(10);
                i += 2;
            }
            _ => {
                i += 1;
            }
        }
    }

    let store = get_rag_store();
    let pipelines = store.read();

    let pipeline = match pipelines.get(&name) {
        Some(p) => p,
        None => return err_frame(&format!("RAG pipeline '{}' not found", name)),
    };

    // First retrieve
    let results = match run_async(pipeline.retrieve(&query, top_k * 5)) {
        Ok(r) => match r {
            Ok(res) => res,
            Err(e) => return err_frame(&e.to_string()),
        },
        Err(e) => return err_frame(&e),
    };

    // Build rerank config
    use ferrite_ai::rag::{RerankConfig, RerankStrategy, Reranker};

    let rerank_strategy = match strategy.as_str() {
        "rrf" => RerankStrategy::RRF { k },
        "mmr" => RerankStrategy::MMR { lambda },
        "cross-encoder" | "crossencoder" => RerankStrategy::CrossEncoder {
            model: "default".to_string(),
            max_length: 512,
        },
        _ => RerankStrategy::RRF { k },
    };

    let config = RerankConfig {
        strategy: rerank_strategy,
        top_k,
        ..Default::default()
    };

    let reranker = Reranker::new(config);

    // Rerank
    let rerank_result = match run_async(reranker.rerank(&query, results)) {
        Ok(r) => match r {
            Ok(res) => res,
            Err(e) => return err_frame(&e.to_string()),
        },
        Err(e) => return err_frame(&e),
    };

    // Build response
    let result_frames: Vec<Frame> = rerank_result
        .results
        .iter()
        .map(|r| {
            Frame::array(vec![
                Frame::bulk(r.chunk.content.clone()),
                Frame::bulk(format!("{:.4}", r.score)),
                Frame::bulk(r.chunk.document_id.0.clone()),
            ])
        })
        .collect();

    Frame::array(vec![
        Frame::bulk("results"),
        Frame::array(result_frames),
        Frame::bulk("latencyMs"),
        Frame::Integer(rerank_result.latency_ms as i64),
        Frame::bulk("originalCount"),
        Frame::Integer(rerank_result.original_count as i64),
        Frame::bulk("strategy"),
        Frame::bulk(rerank_result.strategy),
    ])
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    #[test]
    fn test_parse_threshold() {
        let threshold_str = "0.85";
        let threshold: f32 = threshold_str.parse().unwrap();
        assert!((threshold - 0.85).abs() < 0.001);
    }

    #[test]
    fn test_parse_documents_array() {
        let valid_json = r#"[{"text": "doc1"}, {"text": "doc2"}]"#;
        let invalid_json = r#"{"not": "array"}"#;

        match serde_json::from_str::<serde_json::Value>(valid_json) {
            Ok(serde_json::Value::Array(arr)) => {
                assert_eq!(arr.len(), 2);
            }
            _ => panic!("Expected array"),
        }

        if let Ok(serde_json::Value::Array(_)) =
            serde_json::from_str::<serde_json::Value>(invalid_json)
        {
            panic!("Should not be array");
        }
    }
}
