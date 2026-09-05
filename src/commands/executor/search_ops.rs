//! Search and semantic caching command implementations on CommandExecutor.

use bytes::Bytes;

use crate::protocol::Frame;

use super::CommandExecutor;

impl CommandExecutor {
    // Key management helper methods

    /// DUMP command - serialize a key's value
    pub(super) async fn ft_create(
        &self,
        index: &Bytes,
        schema: &[(String, String)],
        index_type: Option<&str>,
        dimension: Option<usize>,
        metric: Option<&str>,
    ) -> Frame {
        use ferrite_ai::vector::{DistanceMetric, VectorIndexConfig, VectorStore};

        let index_name = String::from_utf8_lossy(index).to_string();
        let dim = dimension.unwrap_or(128);

        let distance_metric = match metric {
            Some("COSINE") | Some("cosine") => DistanceMetric::Cosine,
            Some("L2") | Some("l2") | Some("EUCLIDEAN") => DistanceMetric::Euclidean,
            Some("IP") | Some("ip") | Some("DOT") => DistanceMetric::DotProduct,
            _ => DistanceMetric::Cosine,
        };

        let idx_type = match index_type {
            Some("FLAT") | Some("flat") => ferrite_ai::vector::IndexType::Flat,
            _ => ferrite_ai::vector::IndexType::Hnsw,
        };

        let config = match idx_type {
            ferrite_ai::vector::IndexType::Flat => {
                VectorIndexConfig::flat(&index_name, dim).with_metric(distance_metric)
            }
            _ => VectorIndexConfig::hnsw(&index_name, dim).with_metric(distance_metric),
        };

        let store = VectorStore::new();
        match store.create_index(config) {
            Ok(_) => {
                let mut info = vec![
                    Frame::bulk("OK"),
                    Frame::bulk("index_name"),
                    Frame::bulk(Bytes::from(index_name.clone())),
                    Frame::bulk("dimension"),
                    Frame::Integer(dim as i64),
                ];
                for (field, ftype) in schema {
                    info.push(Frame::bulk(format!("field:{}", field)));
                    info.push(Frame::bulk(Bytes::from(ftype.clone())));
                }
                Frame::array(info)
            }
            Err(e) => Frame::error(format!("ERR {}", e)),
        }
    }

    pub(super) async fn ft_dropindex(&self, index: &Bytes, _delete_docs: bool) -> Frame {
        use ferrite_ai::vector::VectorStore;

        let index_name = String::from_utf8_lossy(index).to_string();
        let store = VectorStore::new();

        match store.drop_index(&index_name) {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::error(format!("ERR {}", e)),
        }
    }

    pub(super) async fn ft_add(
        &self,
        index: &Bytes,
        key: &Bytes,
        vector: &[f32],
        _payload: Option<&Bytes>,
    ) -> Frame {
        use ferrite_ai::vector::{VectorId, VectorIndex, VectorStore};

        let index_name = String::from_utf8_lossy(index).to_string();
        let vector_id = VectorId::from(String::from_utf8_lossy(key).to_string());

        let store = VectorStore::new();
        match store.get_index(&index_name) {
            Some(idx) => match idx.add(vector_id, vector) {
                Ok(()) => Frame::simple("OK"),
                Err(e) => Frame::error(format!("ERR {}", e)),
            },
            None => Frame::error(format!("ERR Unknown index: {}", index_name)),
        }
    }

    pub(super) async fn ft_del(&self, index: &Bytes, key: &Bytes) -> Frame {
        use ferrite_ai::vector::{VectorId, VectorIndex, VectorStore};

        let index_name = String::from_utf8_lossy(index).to_string();
        let vector_id = VectorId::from(String::from_utf8_lossy(key).to_string());

        let store = VectorStore::new();
        match store.get_index(&index_name) {
            Some(idx) => match idx.remove(&vector_id) {
                Ok(true) => Frame::Integer(1),
                Ok(false) => Frame::Integer(0),
                Err(e) => Frame::error(format!("ERR {}", e)),
            },
            None => Frame::error(format!("ERR Unknown index: {}", index_name)),
        }
    }

    pub(super) async fn ft_search(
        &self,
        index: &Bytes,
        query: &[f32],
        k: usize,
        _return_fields: &[String],
        _filter: Option<&str>,
    ) -> Frame {
        use ferrite_ai::vector::{VectorIndex, VectorStore};

        let index_name = String::from_utf8_lossy(index).to_string();
        let store = VectorStore::new();

        match store.get_index(&index_name) {
            Some(idx) => match idx.search(query, k) {
                Ok(results) => {
                    let mut response = vec![Frame::Integer(results.len() as i64)];
                    for result in results {
                        response.push(Frame::bulk(Bytes::from(result.id.to_string())));
                        response.push(Frame::bulk(format!("{:.6}", result.score)));
                    }
                    Frame::array(response)
                }
                Err(e) => Frame::error(format!("ERR {}", e)),
            },
            None => Frame::error(format!("ERR Unknown index: {}", index_name)),
        }
    }

    pub(super) async fn ft_info(&self, index: &Bytes) -> Frame {
        use ferrite_ai::vector::{VectorIndex, VectorStore};

        let index_name = String::from_utf8_lossy(index).to_string();
        let store = VectorStore::new();

        match store.get_index(&index_name) {
            Some(idx) => Frame::array(vec![
                Frame::bulk("index_name"),
                Frame::bulk(Bytes::from(index_name.clone())),
                Frame::bulk("dimension"),
                Frame::Integer(idx.dimension() as i64),
                Frame::bulk("num_vectors"),
                Frame::Integer(idx.len() as i64),
                Frame::bulk("metric"),
                Frame::bulk(format!("{:?}", idx.metric())),
            ]),
            None => Frame::error(format!("ERR Unknown index: {}", index_name)),
        }
    }

    pub(super) async fn ft_list(&self) -> Frame {
        use ferrite_ai::vector::VectorStore;

        let store = VectorStore::new();
        let indexes = store.list_indexes();

        Frame::array(indexes.into_iter().map(Frame::bulk).collect())
    }

    // Hybrid vector search commands

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn vector_hybrid_search(
        &self,
        _db: u8,
        index: &Bytes,
        query_vector: &[f32],
        query_text: &str,
        top_k: usize,
        alpha: f64,
        strategy: &str,
    ) -> Frame {
        use ferrite_ai::hybrid::fusion::{
            FusionStrategy, LinearCombination, ReciprocalRankFusion, ScoredResult,
        };
        use ferrite_ai::vector::{VectorIndex, VectorStore};
        use ferrite_search::bm25::Bm25Index;

        let index_name = String::from_utf8_lossy(index).to_string();

        // Dense retrieval from vector store
        let store = VectorStore::new();
        let dense_results: Vec<ScoredResult> = match store.get_index(&index_name) {
            Some(idx) => match idx.search(query_vector, top_k * 2) {
                Ok(results) => results
                    .into_iter()
                    .map(|r| ScoredResult {
                        doc_id: r.id.to_string(),
                        score: r.score as f64,
                    })
                    .collect(),
                Err(e) => return Frame::error(format!("ERR dense search failed: {}", e)),
            },
            None => return Frame::error(format!("ERR Unknown index: {}", index_name)),
        };

        // Sparse retrieval using BM25
        let bm25 = Bm25Index::default();
        let sparse_results: Vec<ScoredResult> = bm25
            .search(query_text, top_k * 2)
            .into_iter()
            .map(|r| ScoredResult {
                doc_id: r.doc_id,
                score: r.score,
            })
            .collect();

        // Parse strategy
        let fusion_strategy = match strategy {
            "linear" => FusionStrategy::Linear,
            "dense" => FusionStrategy::DenseOnly,
            "sparse" => FusionStrategy::SparseOnly,
            _ => FusionStrategy::RRF,
        };

        // Fuse results
        let fused = match fusion_strategy {
            FusionStrategy::RRF => {
                ReciprocalRankFusion::fuse(&dense_results, &sparse_results, 60, top_k)
            }
            FusionStrategy::Linear => {
                LinearCombination::fuse(&dense_results, &sparse_results, alpha, top_k)
            }
            FusionStrategy::DenseOnly => ReciprocalRankFusion::fuse(&dense_results, &[], 60, top_k),
            FusionStrategy::SparseOnly => {
                ReciprocalRankFusion::fuse(&[], &sparse_results, 60, top_k)
            }
        };

        let mut response = vec![Frame::Integer(fused.len() as i64)];
        for result in fused {
            response.push(Frame::bulk(Bytes::from(result.doc_id)));
            response.push(Frame::bulk(format!("{:.6}", result.fused_score)));
        }
        Frame::array(response)
    }

    pub(super) async fn vector_rerank(
        &self,
        _db: u8,
        _index: &Bytes,
        query_text: &str,
        doc_ids: &[String],
        top_k: usize,
    ) -> Frame {
        use ferrite_ai::hybrid::reranker::{Document, Reranker, SimpleReranker};

        let documents: Vec<Document> = doc_ids
            .iter()
            .enumerate()
            .map(|(i, id)| Document {
                id: id.clone(),
                text: id.clone(), // Use doc_id as placeholder text
                original_score: 1.0 - (i as f64 * 0.01),
            })
            .collect();

        let reranker = SimpleReranker;
        let ranked = reranker.rerank(query_text, &documents, top_k);

        let mut response = vec![Frame::Integer(ranked.len() as i64)];
        for doc in ranked {
            response.push(Frame::bulk(Bytes::from(doc.doc_id)));
            response.push(Frame::bulk(format!("{:.6}", doc.reranked_score)));
            response.push(Frame::Integer(doc.rank as i64));
        }
        Frame::array(response)
    }

    // Semantic Caching commands

    #[cfg(feature = "cloud")]
    pub(super) async fn semantic_set(
        &self,
        query: &Bytes,
        value: &Bytes,
        embedding: &[f32],
        ttl_secs: Option<u64>,
    ) -> Frame {
        use ferrite_ai::semantic::SemanticCache;

        let cache = SemanticCache::with_defaults();
        let query_str = String::from_utf8_lossy(query).to_string();

        match cache.set(&query_str, value.clone(), embedding, ttl_secs) {
            Ok(id) => Frame::Integer(id as i64),
            Err(e) => Frame::error(format!("ERR {}", e)),
        }
    }

    #[cfg(not(feature = "cloud"))]
    pub(super) async fn semantic_set(
        &self,
        _query: &Bytes,
        _value: &Bytes,
        _embedding: &[f32],
        _ttl_secs: Option<u64>,
    ) -> Frame {
        Frame::error("ERR semantic caching requires the 'cloud' feature")
    }

    #[cfg(feature = "cloud")]
    pub(super) async fn semantic_get(
        &self,
        embedding: &[f32],
        threshold: Option<f32>,
        count: Option<usize>,
    ) -> Frame {
        use ferrite_ai::semantic::SemanticCache;

        let cache = SemanticCache::with_defaults();

        if let Some(count) = count {
            // Return multiple results
            match cache.get_many(embedding, count, threshold) {
                Ok(results) => {
                    if results.is_empty() {
                        Frame::Null
                    } else {
                        let items: Vec<Frame> = results
                            .iter()
                            .map(|r| {
                                Frame::array(vec![
                                    Frame::bulk("id"),
                                    Frame::Integer(r.id as i64),
                                    Frame::bulk("query"),
                                    Frame::bulk(Bytes::from(r.entry.query.clone())),
                                    Frame::bulk("value"),
                                    Frame::bulk(r.entry.value.clone()),
                                    Frame::bulk("similarity"),
                                    Frame::Double(r.similarity as f64),
                                ])
                            })
                            .collect();
                        Frame::array(items)
                    }
                }
                Err(e) => Frame::error(format!("ERR {}", e)),
            }
        } else {
            // Return single best result
            match cache.get(embedding, threshold) {
                Ok(Some(result)) => Frame::array(vec![
                    Frame::bulk("id"),
                    Frame::Integer(result.id as i64),
                    Frame::bulk("query"),
                    Frame::bulk(Bytes::from(result.entry.query.clone())),
                    Frame::bulk("value"),
                    Frame::bulk(result.entry.value.clone()),
                    Frame::bulk("similarity"),
                    Frame::Double(result.similarity as f64),
                ]),
                Ok(None) => Frame::Null,
                Err(e) => Frame::error(format!("ERR {}", e)),
            }
        }
    }

    #[cfg(not(feature = "cloud"))]
    pub(super) async fn semantic_get(
        &self,
        _embedding: &[f32],
        _threshold: Option<f32>,
        _count: Option<usize>,
    ) -> Frame {
        Frame::error("ERR semantic caching requires the 'cloud' feature")
    }

    pub(super) async fn semantic_gettext(
        &self,
        _query: &Bytes,
        _threshold: Option<f32>,
        _count: Option<usize>,
    ) -> Frame {
        // This requires auto-embed to be enabled with a model
        // For now, return an error indicating this feature needs configuration
        Frame::error(
            "ERR SEMANTIC.GETTEXT requires auto_embed to be enabled with an embedding model",
        )
    }

    #[cfg(feature = "cloud")]
    pub(super) async fn semantic_del(&self, id: u64) -> Frame {
        use ferrite_ai::semantic::SemanticCache;

        let cache = SemanticCache::with_defaults();

        if cache.remove(id) {
            Frame::Integer(1)
        } else {
            Frame::Integer(0)
        }
    }

    #[cfg(not(feature = "cloud"))]
    pub(super) async fn semantic_del(&self, _id: u64) -> Frame {
        Frame::error("ERR semantic caching requires the 'cloud' feature")
    }

    #[cfg(feature = "cloud")]
    pub(super) async fn semantic_clear(&self) -> Frame {
        use ferrite_ai::semantic::SemanticCache;

        let cache = SemanticCache::with_defaults();
        cache.clear();

        Frame::simple("OK")
    }

    #[cfg(not(feature = "cloud"))]
    pub(super) async fn semantic_clear(&self) -> Frame {
        Frame::error("ERR semantic caching requires the 'cloud' feature")
    }

    #[cfg(feature = "cloud")]
    pub(super) async fn semantic_info(&self) -> Frame {
        use ferrite_ai::semantic::SemanticConfig;

        let config = SemanticConfig::default();

        Frame::array(vec![
            Frame::bulk("semantic_enabled"),
            Frame::bulk(if config.enabled { "yes" } else { "no" }),
            Frame::bulk("embedding_dim"),
            Frame::Integer(config.embedding_dim as i64),
            Frame::bulk("default_threshold"),
            Frame::Double(config.default_threshold as f64),
            Frame::bulk("max_entries"),
            Frame::Integer(config.max_entries as i64),
            Frame::bulk("default_ttl_secs"),
            Frame::Integer(config.default_ttl_secs as i64),
            Frame::bulk("index_type"),
            Frame::bulk(Bytes::from(format!("{:?}", config.index_type))),
            Frame::bulk("distance_metric"),
            Frame::bulk(Bytes::from(format!("{:?}", config.distance_metric))),
            Frame::bulk("auto_embed"),
            Frame::bulk(if config.auto_embed { "yes" } else { "no" }),
        ])
    }

    #[cfg(not(feature = "cloud"))]
    pub(super) async fn semantic_info(&self) -> Frame {
        Frame::error("ERR semantic caching requires the 'cloud' feature")
    }

    #[cfg(feature = "cloud")]
    pub(super) async fn semantic_stats(&self) -> Frame {
        use ferrite_ai::semantic::SemanticCache;

        let cache = SemanticCache::with_defaults();
        let stats = cache.stats();

        Frame::array(vec![
            Frame::bulk("entries"),
            Frame::Integer(stats.entries as i64),
            Frame::bulk("hits"),
            Frame::Integer(stats.hits as i64),
            Frame::bulk("misses"),
            Frame::Integer(stats.misses as i64),
            Frame::bulk("sets"),
            Frame::Integer(stats.sets as i64),
            Frame::bulk("evictions"),
            Frame::Integer(stats.evictions as i64),
            Frame::bulk("hit_rate"),
            Frame::Double(stats.hit_rate),
        ])
    }

    #[cfg(not(feature = "cloud"))]
    pub(super) async fn semantic_stats(&self) -> Frame {
        Frame::error("ERR semantic caching requires the 'cloud' feature")
    }

    #[cfg(feature = "cloud")]
    pub(super) async fn semantic_config(
        &self,
        operation: &Bytes,
        param: Option<&Bytes>,
        _value: Option<&Bytes>,
    ) -> Frame {
        use ferrite_ai::semantic::SemanticConfig;

        let op = String::from_utf8_lossy(operation).to_uppercase();
        let config = SemanticConfig::default();

        match op.as_str() {
            "GET" => {
                if let Some(p) = param {
                    let param_name = String::from_utf8_lossy(p).to_lowercase();
                    match param_name.as_str() {
                        "enabled" => Frame::bulk(if config.enabled { "yes" } else { "no" }),
                        "default_threshold" => Frame::Double(config.default_threshold as f64),
                        "embedding_dim" => Frame::Integer(config.embedding_dim as i64),
                        "max_entries" => Frame::Integer(config.max_entries as i64),
                        "default_ttl_secs" => Frame::Integer(config.default_ttl_secs as i64),
                        "auto_embed" => Frame::bulk(if config.auto_embed { "yes" } else { "no" }),
                        _ => Frame::error(format!("ERR Unknown config parameter: {}", param_name)),
                    }
                } else {
                    // Return all config
                    self.semantic_info().await
                }
            }
            "SET" => {
                // Config SET not implemented yet - would require runtime config modification
                Frame::error("ERR SEMANTIC.CONFIG SET not implemented. Use config file.")
            }
            _ => Frame::error(format!("ERR Unknown operation: {}. Use GET or SET.", op)),
        }
    }

    #[cfg(not(feature = "cloud"))]
    pub(super) async fn semantic_config(
        &self,
        _operation: &Bytes,
        _param: Option<&Bytes>,
        _value: Option<&Bytes>,
    ) -> Frame {
        Frame::error("ERR semantic caching requires the 'cloud' feature")
    }
}
