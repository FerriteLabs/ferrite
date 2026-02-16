//! Vector search (FT.*) command handlers
//!
//! Migration target: `ferrite-ai` crate (crates/ferrite-ai)
//!
//! This module contains handlers for vector search operations:
//! - FT.CREATE (create vector index)
//! - FT.DROPINDEX (drop index)
//! - FT.ADD (add vector)
//! - FT.DEL (delete vector)
//! - FT.SEARCH (search by vector)
//! - FT.INFO (index information)
//! - FT._LIST (list indexes)

use bytes::Bytes;
use std::sync::OnceLock;

use crate::protocol::Frame;
use ferrite_ai::vector::VectorStore;

/// Global vector store singleton
static VECTOR_STORE: OnceLock<VectorStore> = OnceLock::new();

/// Get or initialize the global vector store
fn get_store() -> &'static VectorStore {
    VECTOR_STORE.get_or_init(VectorStore::new)
}

/// Handle FT.CREATE command
pub async fn create(
    index: &Bytes,
    schema: &[(String, String)],
    index_type: Option<&str>,
    dimension: Option<usize>,
    metric: Option<&str>,
) -> Frame {
    use ferrite_ai::vector::{DistanceMetric, VectorIndexConfig};

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

    let store = get_store();
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

/// Handle FT.DROPINDEX command
pub async fn dropindex(index: &Bytes, _delete_docs: bool) -> Frame {
    let index_name = String::from_utf8_lossy(index).to_string();
    let store = get_store();

    match store.drop_index(&index_name) {
        Ok(()) => Frame::simple("OK"),
        Err(e) => Frame::error(format!("ERR {}", e)),
    }
}

/// Handle FT.ADD command
pub async fn add(index: &Bytes, key: &Bytes, vector: &[f32], _payload: Option<&Bytes>) -> Frame {
    use ferrite_ai::vector::{VectorId, VectorIndex};

    let index_name = String::from_utf8_lossy(index).to_string();
    let vector_id = VectorId::from(String::from_utf8_lossy(key).to_string());

    let store = get_store();
    match store.get_index(&index_name) {
        Some(idx) => match idx.add(vector_id, vector) {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::error(format!("ERR {}", e)),
        },
        None => Frame::error(format!("ERR Unknown index: {}", index_name)),
    }
}

/// Handle FT.DEL command
pub async fn del(index: &Bytes, key: &Bytes) -> Frame {
    use ferrite_ai::vector::{VectorId, VectorIndex};

    let index_name = String::from_utf8_lossy(index).to_string();
    let vector_id = VectorId::from(String::from_utf8_lossy(key).to_string());

    let store = get_store();
    match store.get_index(&index_name) {
        Some(idx) => match idx.remove(&vector_id) {
            Ok(true) => Frame::Integer(1),
            Ok(false) => Frame::Integer(0),
            Err(e) => Frame::error(format!("ERR {}", e)),
        },
        None => Frame::error(format!("ERR Unknown index: {}", index_name)),
    }
}

/// Handle FT.SEARCH command
pub async fn search(
    index: &Bytes,
    query: &[f32],
    k: usize,
    _return_fields: &[String],
    _filter: Option<&str>,
) -> Frame {
    use ferrite_ai::vector::VectorIndex;

    let index_name = String::from_utf8_lossy(index).to_string();
    let store = get_store();

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

/// Handle FT.INFO command
pub async fn info(index: &Bytes) -> Frame {
    use ferrite_ai::vector::VectorIndex;

    let index_name = String::from_utf8_lossy(index).to_string();
    let store = get_store();

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

/// Handle FT._LIST command
pub async fn list() -> Frame {
    let store = get_store();
    let indexes = store.list_indexes();

    Frame::array(indexes.into_iter().map(Frame::bulk).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_list_indexes() {
        let result = list().await;
        assert!(matches!(result, Frame::Array(Some(_))));
    }

    #[tokio::test]
    async fn test_info_unknown_index() {
        let result = info(&Bytes::from("unknown_index")).await;
        assert!(matches!(result, Frame::Error(_)));
    }

    #[tokio::test]
    async fn test_del_unknown_index() {
        let result = del(&Bytes::from("unknown_index"), &Bytes::from("key")).await;
        assert!(matches!(result, Frame::Error(_)));
    }

    #[tokio::test]
    async fn test_add_unknown_index() {
        let result = add(
            &Bytes::from("unknown_index"),
            &Bytes::from("key"),
            &[0.1, 0.2],
            None,
        )
        .await;
        assert!(matches!(result, Frame::Error(_)));
    }

    #[tokio::test]
    async fn test_search_unknown_index() {
        let result = search(&Bytes::from("unknown_index"), &[0.1, 0.2], 10, &[], None).await;
        assert!(matches!(result, Frame::Error(_)));
    }
}
