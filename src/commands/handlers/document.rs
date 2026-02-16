//! Document Database Command Handlers
//!
//! Migration target: `ferrite-document` crate (crates/ferrite-document)
//!
//! Redis-compatible command bindings for the document database engine.
//! Provides MongoDB-like document operations over RESP protocol.
//!
//! # Commands
//!
//! - `DOC.CREATE <collection>` - Create a collection
//! - `DOC.DROP <collection>` - Drop a collection
//! - `DOC.INSERT <collection> <json>` - Insert document
//! - `DOC.INSERTMANY <collection> <json-array>` - Insert multiple documents
//! - `DOC.FIND <collection> <query>` - Find documents
//! - `DOC.FINDONE <collection> <query>` - Find single document
//! - `DOC.UPDATE <collection> <query> <update>` - Update documents
//! - `DOC.DELETE <collection> <query>` - Delete documents
//! - `DOC.COUNT <collection> [query]` - Count documents
//! - `DOC.DISTINCT <collection> <field> [query]` - Get distinct values
//! - `DOC.AGGREGATE <collection> <pipeline>` - Run aggregation
//! - `DOC.CREATEINDEX <collection> <keys> [options]` - Create index
//! - `DOC.DROPINDEX <collection> <name>` - Drop index
//! - `DOC.LISTCOLLECTIONS` - List all collections

use bytes::Bytes;
use std::sync::OnceLock;

use ferrite_document::document::{CollectionOptions, DocumentStore, DocumentStoreConfig, IndexOptions};
use crate::protocol::Frame;

use super::{err_frame, ok_frame, HandlerContext};

/// Global document store instance
static DOCUMENT_STORE: OnceLock<DocumentStore> = OnceLock::new();

/// Get or initialize the global document store
fn get_document_store() -> &'static DocumentStore {
    DOCUMENT_STORE.get_or_init(|| DocumentStore::new(DocumentStoreConfig::default()))
}

/// Handle DOC.CREATE command
///
/// Creates a new document collection.
///
/// # Syntax
/// `DOC.CREATE <collection> [SCHEMA <json-schema>] [CAPPED <max-docs>]`
///
/// # Returns
/// OK on success, error if collection exists.
pub fn doc_create(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'DOC.CREATE' command");
    }

    let collection = String::from_utf8_lossy(&args[0]).to_string();

    // Parse optional arguments
    let mut _schema: Option<String> = None;
    let mut capped: Option<usize> = None;

    let mut i = 1;
    while i < args.len() {
        let arg = String::from_utf8_lossy(&args[i]).to_uppercase();
        match arg.as_str() {
            "SCHEMA" => {
                if i + 1 >= args.len() {
                    return err_frame("SCHEMA requires a JSON schema");
                }
                _schema = Some(String::from_utf8_lossy(&args[i + 1]).to_string());
                i += 2;
            }
            "CAPPED" => {
                if i + 1 >= args.len() {
                    return err_frame("CAPPED requires a max document count");
                }
                capped = String::from_utf8_lossy(&args[i + 1]).parse().ok();
                i += 2;
            }
            _ => {
                i += 1;
            }
        }
    }

    let store = get_document_store();

    // Create collection options
    let options = if capped.is_some() {
        Some(CollectionOptions {
            capped_max_documents: capped.map(|c| c as u64),
            ..Default::default()
        })
    } else {
        None
    };

    // Run async operation synchronously
    let rt = tokio::runtime::Handle::try_current();
    let result = match rt {
        Ok(handle) => handle.block_on(store.create_collection(&collection, options)),
        Err(_) => {
            // Create a new runtime if we're not in one
            match tokio::runtime::Runtime::new() {
                Ok(rt) => rt.block_on(store.create_collection(&collection, options)),
                Err(e) => return err_frame(&format!("runtime error: {}", e)),
            }
        }
    };

    match result {
        Ok(_) => ok_frame(),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle DOC.DROP command
///
/// Drops a document collection and all its documents.
///
/// # Syntax
/// `DOC.DROP <collection>`
///
/// # Returns
/// OK on success, error if collection doesn't exist.
pub fn doc_drop(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'DOC.DROP' command");
    }

    let collection = String::from_utf8_lossy(&args[0]).to_string();
    let store = get_document_store();

    let rt = tokio::runtime::Handle::try_current();
    let result = match rt {
        Ok(handle) => handle.block_on(store.drop_collection(&collection)),
        Err(_) => match tokio::runtime::Runtime::new() {
            Ok(rt) => rt.block_on(store.drop_collection(&collection)),
            Err(e) => return err_frame(&format!("runtime error: {}", e)),
        },
    };

    match result {
        Ok(_) => ok_frame(),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle DOC.INSERT command
///
/// Inserts a single document into a collection.
///
/// # Syntax
/// `DOC.INSERT <collection> <json-document>`
///
/// # Returns
/// The inserted document's ID.
pub fn doc_insert(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("wrong number of arguments for 'DOC.INSERT' command");
    }

    let collection = String::from_utf8_lossy(&args[0]).to_string();
    let doc_json = String::from_utf8_lossy(&args[1]).to_string();

    // Parse JSON
    let doc: serde_json::Value = match serde_json::from_str(&doc_json) {
        Ok(v) => v,
        Err(_) => return err_frame("invalid JSON document"),
    };

    let store = get_document_store();

    let rt = tokio::runtime::Handle::try_current();
    let result = match rt {
        Ok(handle) => handle.block_on(store.insert_one(&collection, doc)),
        Err(_) => match tokio::runtime::Runtime::new() {
            Ok(rt) => rt.block_on(store.insert_one(&collection, doc)),
            Err(e) => return err_frame(&format!("runtime error: {}", e)),
        },
    };

    match result {
        Ok(id) => Frame::bulk(id.to_string()),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle DOC.INSERTMANY command
///
/// Inserts multiple documents into a collection.
///
/// # Syntax
/// `DOC.INSERTMANY <collection> <json-array>`
///
/// # Returns
/// Array of inserted document IDs.
pub fn doc_insertmany(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("wrong number of arguments for 'DOC.INSERTMANY' command");
    }

    let collection = String::from_utf8_lossy(&args[0]).to_string();
    let docs_json = String::from_utf8_lossy(&args[1]).to_string();

    // Parse and validate JSON array
    let docs: Vec<serde_json::Value> = match serde_json::from_str(&docs_json) {
        Ok(serde_json::Value::Array(arr)) => arr,
        _ => return err_frame("expected JSON array of documents"),
    };

    let store = get_document_store();

    let rt = tokio::runtime::Handle::try_current();
    let result = match rt {
        Ok(handle) => handle.block_on(store.insert_many(&collection, docs)),
        Err(_) => match tokio::runtime::Runtime::new() {
            Ok(rt) => rt.block_on(store.insert_many(&collection, docs)),
            Err(e) => return err_frame(&format!("runtime error: {}", e)),
        },
    };

    match result {
        Ok(ids) => {
            let frames: Vec<Frame> = ids.iter().map(|id| Frame::bulk(id.to_string())).collect();
            Frame::array(frames)
        }
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle DOC.FIND command
///
/// Finds documents matching a query.
///
/// # Syntax
/// `DOC.FIND <collection> <query> [LIMIT <n>] [SKIP <n>] [SORT <field> <asc|desc>]`
///
/// # Returns
/// Array of matching documents as JSON strings.
pub fn doc_find(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("wrong number of arguments for 'DOC.FIND' command");
    }

    let collection = String::from_utf8_lossy(&args[0]).to_string();
    let query_json = String::from_utf8_lossy(&args[1]).to_string();

    // Parse query JSON
    let query: serde_json::Value = match serde_json::from_str(&query_json) {
        Ok(v) => v,
        Err(_) => return err_frame("invalid query JSON"),
    };

    // Parse optional arguments
    let mut limit: Option<usize> = None;
    let mut skip: Option<usize> = None;

    let mut i = 2;
    while i < args.len() {
        let arg = String::from_utf8_lossy(&args[i]).to_uppercase();
        match arg.as_str() {
            "LIMIT" => {
                if i + 1 >= args.len() {
                    return err_frame("LIMIT requires a value");
                }
                limit = String::from_utf8_lossy(&args[i + 1]).parse().ok();
                i += 2;
            }
            "SKIP" => {
                if i + 1 >= args.len() {
                    return err_frame("SKIP requires a value");
                }
                skip = String::from_utf8_lossy(&args[i + 1]).parse().ok();
                i += 2;
            }
            "SORT" => {
                if i + 2 >= args.len() {
                    return err_frame("SORT requires field and order");
                }
                // TODO: Implement sort when DocumentStore supports it
                i += 3;
            }
            _ => {
                i += 1;
            }
        }
    }

    let store = get_document_store();

    let rt = tokio::runtime::Handle::try_current();
    let result = match rt {
        Ok(handle) => handle.block_on(store.find(&collection, query)),
        Err(_) => match tokio::runtime::Runtime::new() {
            Ok(rt) => rt.block_on(store.find(&collection, query)),
            Err(e) => return err_frame(&format!("runtime error: {}", e)),
        },
    };

    match result {
        Ok(docs) => {
            let mut results: Vec<_> = docs.into_iter().collect();

            // Apply skip
            if let Some(s) = skip {
                if s < results.len() {
                    results = results.into_iter().skip(s).collect();
                } else {
                    results.clear();
                }
            }

            // Apply limit
            if let Some(l) = limit {
                results.truncate(l);
            }

            let frames: Vec<Frame> = results
                .iter()
                .map(|doc| Frame::bulk(doc.to_json().to_string()))
                .collect();
            Frame::array(frames)
        }
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle DOC.FINDONE command
///
/// Finds a single document matching a query.
///
/// # Syntax
/// `DOC.FINDONE <collection> <query>`
///
/// # Returns
/// The document as JSON string, or nil if not found.
pub fn doc_findone(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("wrong number of arguments for 'DOC.FINDONE' command");
    }

    let collection = String::from_utf8_lossy(&args[0]).to_string();
    let query_json = String::from_utf8_lossy(&args[1]).to_string();

    // Parse query JSON
    let query: serde_json::Value = match serde_json::from_str(&query_json) {
        Ok(v) => v,
        Err(_) => return err_frame("invalid query JSON"),
    };

    let store = get_document_store();

    let rt = tokio::runtime::Handle::try_current();
    let result = match rt {
        Ok(handle) => handle.block_on(store.find_one(&collection, query)),
        Err(_) => match tokio::runtime::Runtime::new() {
            Ok(rt) => rt.block_on(store.find_one(&collection, query)),
            Err(e) => return err_frame(&format!("runtime error: {}", e)),
        },
    };

    match result {
        Ok(Some(doc)) => Frame::bulk(doc.to_json().to_string()),
        Ok(None) => Frame::Null,
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle DOC.UPDATE command
///
/// Updates documents matching a query.
///
/// # Syntax
/// `DOC.UPDATE <collection> <query> <update> [UPSERT]`
///
/// # Returns
/// Number of modified documents.
pub fn doc_update(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 3 {
        return err_frame("wrong number of arguments for 'DOC.UPDATE' command");
    }

    let collection = String::from_utf8_lossy(&args[0]).to_string();
    let query_json = String::from_utf8_lossy(&args[1]).to_string();
    let update_json = String::from_utf8_lossy(&args[2]).to_string();

    // Parse query JSON
    let query: serde_json::Value = match serde_json::from_str(&query_json) {
        Ok(v) => v,
        Err(_) => return err_frame("invalid query JSON"),
    };

    // Parse update JSON
    let update: serde_json::Value = match serde_json::from_str(&update_json) {
        Ok(v) => v,
        Err(_) => return err_frame("invalid update JSON"),
    };

    // Check for UPSERT flag (not yet supported)
    let _upsert = args.len() > 3 && String::from_utf8_lossy(&args[3]).to_uppercase() == "UPSERT";

    let store = get_document_store();

    let rt = tokio::runtime::Handle::try_current();
    let result = match rt {
        Ok(handle) => handle.block_on(store.update_many(&collection, query, update)),
        Err(_) => match tokio::runtime::Runtime::new() {
            Ok(rt) => rt.block_on(store.update_many(&collection, query, update)),
            Err(e) => return err_frame(&format!("runtime error: {}", e)),
        },
    };

    match result {
        Ok(update_result) => Frame::Integer(update_result.modified_count as i64),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle DOC.DELETE command
///
/// Deletes documents matching a query.
///
/// # Syntax
/// `DOC.DELETE <collection> <query>`
///
/// # Returns
/// Number of deleted documents.
pub fn doc_delete(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("wrong number of arguments for 'DOC.DELETE' command");
    }

    let collection = String::from_utf8_lossy(&args[0]).to_string();
    let query_json = String::from_utf8_lossy(&args[1]).to_string();

    // Parse query JSON
    let query: serde_json::Value = match serde_json::from_str(&query_json) {
        Ok(v) => v,
        Err(_) => return err_frame("invalid query JSON"),
    };

    let store = get_document_store();

    let rt = tokio::runtime::Handle::try_current();
    let result = match rt {
        Ok(handle) => handle.block_on(store.delete_many(&collection, query)),
        Err(_) => match tokio::runtime::Runtime::new() {
            Ok(rt) => rt.block_on(store.delete_many(&collection, query)),
            Err(e) => return err_frame(&format!("runtime error: {}", e)),
        },
    };

    match result {
        Ok(delete_result) => Frame::Integer(delete_result.deleted_count as i64),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle DOC.COUNT command
///
/// Counts documents matching a query.
///
/// # Syntax
/// `DOC.COUNT <collection> [query]`
///
/// # Returns
/// Count of matching documents.
pub fn doc_count(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'DOC.COUNT' command");
    }

    let collection = String::from_utf8_lossy(&args[0]).to_string();

    // Optional query (default to empty object to match all)
    let query: serde_json::Value = if args.len() > 1 {
        let query_json = String::from_utf8_lossy(&args[1]).to_string();
        match serde_json::from_str(&query_json) {
            Ok(v) => v,
            Err(_) => return err_frame("invalid query JSON"),
        }
    } else {
        serde_json::json!({})
    };

    let store = get_document_store();

    let rt = tokio::runtime::Handle::try_current();
    let result = match rt {
        Ok(handle) => handle.block_on(store.count(&collection, query)),
        Err(_) => match tokio::runtime::Runtime::new() {
            Ok(rt) => rt.block_on(store.count(&collection, query)),
            Err(e) => return err_frame(&format!("runtime error: {}", e)),
        },
    };

    match result {
        Ok(count) => Frame::Integer(count as i64),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle DOC.DISTINCT command
///
/// Gets distinct values for a field.
///
/// # Syntax
/// `DOC.DISTINCT <collection> <field> [query]`
///
/// # Returns
/// Array of distinct values.
pub fn doc_distinct(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("wrong number of arguments for 'DOC.DISTINCT' command");
    }

    let collection = String::from_utf8_lossy(&args[0]).to_string();
    let field = String::from_utf8_lossy(&args[1]).to_string();

    // Optional query
    let query: Option<serde_json::Value> = if args.len() > 2 {
        let query_json = String::from_utf8_lossy(&args[2]).to_string();
        match serde_json::from_str(&query_json) {
            Ok(v) => Some(v),
            Err(_) => return err_frame("invalid query JSON"),
        }
    } else {
        None
    };

    let store = get_document_store();

    let rt = tokio::runtime::Handle::try_current();
    let result = match rt {
        Ok(handle) => handle.block_on(store.distinct(&collection, &field, query)),
        Err(_) => match tokio::runtime::Runtime::new() {
            Ok(rt) => rt.block_on(store.distinct(&collection, &field, query)),
            Err(e) => return err_frame(&format!("runtime error: {}", e)),
        },
    };

    match result {
        Ok(values) => {
            let frames: Vec<Frame> = values.iter().map(|v| Frame::bulk(v.to_string())).collect();
            Frame::array(frames)
        }
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle DOC.AGGREGATE command
///
/// Runs an aggregation pipeline.
///
/// # Syntax
/// `DOC.AGGREGATE <collection> <pipeline-json>`
///
/// # Returns
/// Array of aggregation results.
pub fn doc_aggregate(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("wrong number of arguments for 'DOC.AGGREGATE' command");
    }

    let collection = String::from_utf8_lossy(&args[0]).to_string();
    let pipeline_json = String::from_utf8_lossy(&args[1]).to_string();

    // Parse pipeline JSON (should be array of stages)
    let pipeline: Vec<serde_json::Value> = match serde_json::from_str(&pipeline_json) {
        Ok(serde_json::Value::Array(arr)) => arr,
        _ => return err_frame("pipeline must be a JSON array of stages"),
    };

    let store = get_document_store();

    let rt = tokio::runtime::Handle::try_current();
    let result = match rt {
        Ok(handle) => handle.block_on(store.aggregate(&collection, pipeline)),
        Err(_) => match tokio::runtime::Runtime::new() {
            Ok(rt) => rt.block_on(store.aggregate(&collection, pipeline)),
            Err(e) => return err_frame(&format!("runtime error: {}", e)),
        },
    };

    match result {
        Ok(docs) => {
            let frames: Vec<Frame> = docs
                .iter()
                .map(|doc| Frame::bulk(doc.to_json().to_string()))
                .collect();
            Frame::array(frames)
        }
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle DOC.CREATEINDEX command
///
/// Creates an index on a collection.
///
/// # Syntax
/// `DOC.CREATEINDEX <collection> <keys-json> [NAME <name>] [UNIQUE] [SPARSE]`
///
/// # Returns
/// OK on success.
pub fn doc_createindex(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("wrong number of arguments for 'DOC.CREATEINDEX' command");
    }

    let collection = String::from_utf8_lossy(&args[0]).to_string();
    let keys_json = String::from_utf8_lossy(&args[1]).to_string();

    // Parse keys JSON
    let keys: serde_json::Value = match serde_json::from_str(&keys_json) {
        Ok(v) => v,
        Err(_) => return err_frame("invalid keys JSON"),
    };

    // Parse optional arguments
    let mut name: Option<String> = None;
    let mut unique = false;
    let mut sparse = false;

    let mut i = 2;
    while i < args.len() {
        let arg = String::from_utf8_lossy(&args[i]).to_uppercase();
        match arg.as_str() {
            "NAME" => {
                if i + 1 >= args.len() {
                    return err_frame("NAME requires a value");
                }
                name = Some(String::from_utf8_lossy(&args[i + 1]).to_string());
                i += 2;
            }
            "UNIQUE" => {
                unique = true;
                i += 1;
            }
            "SPARSE" => {
                sparse = true;
                i += 1;
            }
            _ => {
                i += 1;
            }
        }
    }

    let options = IndexOptions {
        name,
        unique,
        sparse,
        ..Default::default()
    };

    let store = get_document_store();

    let rt = tokio::runtime::Handle::try_current();
    let result = match rt {
        Ok(handle) => handle.block_on(store.create_index(&collection, keys, Some(options))),
        Err(_) => match tokio::runtime::Runtime::new() {
            Ok(rt) => rt.block_on(store.create_index(&collection, keys, Some(options))),
            Err(e) => return err_frame(&format!("runtime error: {}", e)),
        },
    };

    match result {
        Ok(index_name) => Frame::bulk(index_name),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle DOC.DROPINDEX command
///
/// Drops an index from a collection.
///
/// # Syntax
/// `DOC.DROPINDEX <collection> <index-name>`
///
/// # Returns
/// OK on success.
pub fn doc_dropindex(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("wrong number of arguments for 'DOC.DROPINDEX' command");
    }

    let collection = String::from_utf8_lossy(&args[0]).to_string();
    let index_name = String::from_utf8_lossy(&args[1]).to_string();

    let store = get_document_store();

    let rt = tokio::runtime::Handle::try_current();
    let result = match rt {
        Ok(handle) => handle.block_on(store.drop_index(&collection, &index_name)),
        Err(_) => match tokio::runtime::Runtime::new() {
            Ok(rt) => rt.block_on(store.drop_index(&collection, &index_name)),
            Err(e) => return err_frame(&format!("runtime error: {}", e)),
        },
    };

    match result {
        Ok(_) => ok_frame(),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle DOC.LISTCOLLECTIONS command
///
/// Lists all document collections.
///
/// # Syntax
/// `DOC.LISTCOLLECTIONS`
///
/// # Returns
/// Array of collection names.
pub fn doc_listcollections(_ctx: &HandlerContext<'_>, _args: &[Bytes]) -> Frame {
    let store = get_document_store();

    let rt = tokio::runtime::Handle::try_current();
    let collections = match rt {
        Ok(handle) => handle.block_on(store.list_collections()),
        Err(_) => match tokio::runtime::Runtime::new() {
            Ok(rt) => rt.block_on(store.list_collections()),
            Err(_) => return Frame::array(Vec::new()),
        },
    };

    let frames: Vec<Frame> = collections.iter().map(|c| Frame::bulk(c.clone())).collect();
    Frame::array(frames)
}

/// Handle DOC.STATS command
///
/// Gets statistics for a collection.
///
/// # Syntax
/// `DOC.STATS <collection>`
///
/// # Returns
/// Array of key-value statistics.
pub fn doc_stats(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'DOC.STATS' command");
    }

    let collection = String::from_utf8_lossy(&args[0]).to_string();
    let store = get_document_store();

    // Get metrics
    let rt = tokio::runtime::Handle::try_current();
    let metrics = match rt {
        Ok(handle) => handle.block_on(store.metrics()),
        Err(_) => match tokio::runtime::Runtime::new() {
            Ok(rt) => rt.block_on(store.metrics()),
            Err(_) => {
                return Frame::array(vec![
                    Frame::bulk("name"),
                    Frame::bulk(collection),
                    Frame::bulk("documentCount"),
                    Frame::Integer(0),
                ])
            }
        },
    };

    // Get document count for this collection
    let doc_count = {
        let query = serde_json::json!({});
        let rt = tokio::runtime::Handle::try_current();
        match rt {
            Ok(handle) => handle
                .block_on(store.count(&collection, query))
                .unwrap_or(0),
            Err(_) => 0,
        }
    };

    Frame::array(vec![
        Frame::bulk("name"),
        Frame::bulk(collection),
        Frame::bulk("documentCount"),
        Frame::Integer(doc_count as i64),
        Frame::bulk("storageSize"),
        Frame::Integer(0), // Would need to implement size tracking
        Frame::bulk("indexCount"),
        Frame::Integer(metrics.total_indexes as i64),
        Frame::bulk("avgDocumentSize"),
        Frame::Integer(0),
        Frame::bulk("totalInserts"),
        Frame::Integer(metrics.inserts as i64),
        Frame::bulk("totalUpdates"),
        Frame::Integer(metrics.updates as i64),
        Frame::bulk("totalDeletes"),
        Frame::Integer(metrics.deletes as i64),
        Frame::bulk("totalQueries"),
        Frame::Integer(metrics.queries as i64),
    ])
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn test_json_validation() {
        let valid_json = r#"{"name": "test"}"#;
        let invalid_json = r#"{"name": }"#;

        assert!(serde_json::from_str::<serde_json::Value>(valid_json).is_ok());
        assert!(serde_json::from_str::<serde_json::Value>(invalid_json).is_err());
    }

    #[test]
    fn test_json_array_validation() {
        let valid_array = r#"[{"a": 1}, {"b": 2}]"#;
        let invalid_array = r#"{"not": "array"}"#;

        match serde_json::from_str::<serde_json::Value>(valid_array) {
            Ok(serde_json::Value::Array(_)) => {}
            _ => panic!("Expected array"),
        }

        if let Ok(serde_json::Value::Array(_)) =
            serde_json::from_str::<serde_json::Value>(invalid_array)
        {
            panic!("Should not be array");
        }
    }

    #[test]
    fn test_get_document_store() {
        // Should initialize and return the same instance
        let store1 = get_document_store();
        let store2 = get_document_store();
        assert!(std::ptr::eq(store1, store2));
    }
}
