//! Semantic caching command handlers
//!
//! Migration target: `ferrite-ai` crate (crates/ferrite-ai)
//!
//! This module contains handlers for semantic cache operations:
//! - SEMANTIC.SET (store with embedding)
//! - SEMANTIC.GET (retrieve by embedding similarity)
//! - SEMANTIC.GETTEXT (retrieve by text - requires auto-embed)
//! - SEMANTIC.DEL (delete entry)
//! - SEMANTIC.CLEAR (clear cache)
//! - SEMANTIC.INFO (configuration info)
//! - SEMANTIC.STATS (usage statistics)
//! - SEMANTIC.CONFIG (get/set configuration)
//!
//! And function cache operations (FCACHE.*):
//! - FCACHE.SET (cache a function call result)
//! - FCACHE.GET (retrieve cached function call result)
//! - FCACHE.INVALIDATE (invalidate by function name or ID)
//! - FCACHE.STATS (function cache statistics)
//! - FCACHE.INFO (function cache configuration)

use bytes::Bytes;

use crate::protocol::Frame;

/// Handle SEMANTIC.SET command
pub async fn set(query: &Bytes, value: &Bytes, embedding: &[f32], ttl_secs: Option<u64>) -> Frame {
    use ferrite_ai::semantic::SemanticCache;

    let cache = SemanticCache::with_defaults();
    let query_str = String::from_utf8_lossy(query).to_string();

    match cache.set(&query_str, value.clone(), embedding, ttl_secs) {
        Ok(id) => Frame::Integer(id as i64),
        Err(e) => Frame::error(format!("ERR {}", e)),
    }
}

/// Handle SEMANTIC.GET command
pub async fn get(embedding: &[f32], threshold: Option<f32>, count: Option<usize>) -> Frame {
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

/// Handle SEMANTIC.GETTEXT command
pub async fn gettext(_query: &Bytes, _threshold: Option<f32>, _count: Option<usize>) -> Frame {
    // This requires auto-embed to be enabled with a model
    // For now, return an error indicating this feature needs configuration
    Frame::error("ERR SEMANTIC.GETTEXT requires auto_embed to be enabled with an embedding model")
}

/// Handle SEMANTIC.DEL command
pub async fn del(id: u64) -> Frame {
    use ferrite_ai::semantic::SemanticCache;

    let cache = SemanticCache::with_defaults();

    if cache.remove(id) {
        Frame::Integer(1)
    } else {
        Frame::Integer(0)
    }
}

/// Handle SEMANTIC.CLEAR command
pub async fn clear() -> Frame {
    use ferrite_ai::semantic::SemanticCache;

    let cache = SemanticCache::with_defaults();
    cache.clear();

    Frame::simple("OK")
}

/// Handle SEMANTIC.INFO command
pub async fn info() -> Frame {
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

/// Handle SEMANTIC.STATS command
pub async fn stats() -> Frame {
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

/// Handle SEMANTIC.CONFIG command
pub async fn config(operation: &Bytes, param: Option<&Bytes>, _value: Option<&Bytes>) -> Frame {
    use ferrite_ai::semantic::SemanticConfig;

    let op = String::from_utf8_lossy(operation).to_uppercase();
    let semantic_config = SemanticConfig::default();

    match op.as_str() {
        "GET" => {
            if let Some(p) = param {
                let param_name = String::from_utf8_lossy(p).to_lowercase();
                match param_name.as_str() {
                    "enabled" => Frame::bulk(if semantic_config.enabled { "yes" } else { "no" }),
                    "default_threshold" => Frame::Double(semantic_config.default_threshold as f64),
                    "embedding_dim" => Frame::Integer(semantic_config.embedding_dim as i64),
                    "max_entries" => Frame::Integer(semantic_config.max_entries as i64),
                    "default_ttl_secs" => Frame::Integer(semantic_config.default_ttl_secs as i64),
                    "auto_embed" => Frame::bulk(if semantic_config.auto_embed {
                        "yes"
                    } else {
                        "no"
                    }),
                    _ => Frame::error(format!("ERR Unknown config parameter: {}", param_name)),
                }
            } else {
                // Return all config
                info().await
            }
        }
        "SET" => {
            // Config SET not implemented yet - would require runtime config modification
            Frame::error("ERR SEMANTIC.CONFIG SET not implemented. Use config file.")
        }
        _ => Frame::error(format!("ERR Unknown operation: {}. Use GET or SET.", op)),
    }
}

// =============================================================================
// Function Cache (FCACHE.*) Command Handlers
// =============================================================================

use parking_lot::RwLock;
use std::sync::OnceLock;

// Global function cache instance (lazy initialized)
static FUNCTION_CACHE: OnceLock<RwLock<Option<ferrite_ai::semantic::FunctionCache>>> = OnceLock::new();

fn get_or_init_function_cache() -> &'static RwLock<Option<ferrite_ai::semantic::FunctionCache>> {
    FUNCTION_CACHE.get_or_init(|| RwLock::new(ferrite_ai::semantic::FunctionCache::with_defaults().ok()))
}

/// Handle FCACHE.SET command
/// FCACHE.SET <function_name> <arguments_json> <result_json> [NAMESPACE ns] [TTL secs]
pub async fn fcache_set(
    function_name: &Bytes,
    arguments_json: &Bytes,
    result_json: &Bytes,
    namespace: Option<&Bytes>,
    ttl_secs: Option<u64>,
) -> Frame {
    use ferrite_ai::semantic::FunctionCall;
    use serde_json::Value;

    let cache_lock = get_or_init_function_cache();
    let cache_opt = cache_lock.read();

    let cache = match cache_opt.as_ref() {
        Some(c) => c,
        None => return Frame::error("ERR Function cache not initialized"),
    };

    // Parse function name
    let name = String::from_utf8_lossy(function_name).to_string();

    // Parse arguments JSON
    let arguments: Value = match serde_json::from_slice(arguments_json) {
        Ok(v) => v,
        Err(e) => return Frame::error(format!("ERR Invalid arguments JSON: {}", e)),
    };

    // Parse result JSON
    let result: Value = match serde_json::from_slice(result_json) {
        Ok(v) => v,
        Err(e) => return Frame::error(format!("ERR Invalid result JSON: {}", e)),
    };

    // Build function call
    let mut call = FunctionCall::new(name, arguments);
    if let Some(ns) = namespace {
        call = call.with_namespace(String::from_utf8_lossy(ns).to_string());
    }

    // Store in cache
    match cache.set_with_options(&call, result, ttl_secs, Vec::new()) {
        Ok(id) => Frame::Integer(id as i64),
        Err(e) => Frame::error(format!("ERR {}", e)),
    }
}

/// Handle FCACHE.GET command
/// FCACHE.GET <function_name> <arguments_json> [NAMESPACE ns] [THRESHOLD thresh]
pub async fn fcache_get(
    function_name: &Bytes,
    arguments_json: &Bytes,
    namespace: Option<&Bytes>,
    threshold: Option<f32>,
) -> Frame {
    use ferrite_ai::semantic::FunctionCall;
    use serde_json::Value;

    let cache_lock = get_or_init_function_cache();
    let cache_opt = cache_lock.read();

    let cache = match cache_opt.as_ref() {
        Some(c) => c,
        None => return Frame::error("ERR Function cache not initialized"),
    };

    // Parse function name
    let name = String::from_utf8_lossy(function_name).to_string();

    // Parse arguments JSON
    let arguments: Value = match serde_json::from_slice(arguments_json) {
        Ok(v) => v,
        Err(e) => return Frame::error(format!("ERR Invalid arguments JSON: {}", e)),
    };

    // Build function call
    let mut call = FunctionCall::new(name, arguments);
    if let Some(ns) = namespace {
        call = call.with_namespace(String::from_utf8_lossy(ns).to_string());
    }

    // Look up in cache
    match cache.get_with_threshold(&call, threshold) {
        Ok(Some(result)) => {
            let result_str = serde_json::to_string(&result.result).unwrap_or_default();
            let original_args =
                serde_json::to_string(&result.original_call.arguments).unwrap_or_default();

            Frame::array(vec![
                Frame::bulk("result"),
                Frame::bulk(Bytes::from(result_str)),
                Frame::bulk("similarity"),
                Frame::Double(result.similarity as f64),
                Frame::bulk("exact_match"),
                Frame::bulk(if result.exact_match { "true" } else { "false" }),
                Frame::bulk("cache_id"),
                Frame::Integer(result.cache_id as i64),
                Frame::bulk("cost_saved"),
                Frame::Double(result.cost_saved),
                Frame::bulk("time_saved_ms"),
                Frame::Integer(result.time_saved_ms as i64),
                Frame::bulk("original_function"),
                Frame::bulk(Bytes::from(result.original_call.name.clone())),
                Frame::bulk("original_arguments"),
                Frame::bulk(Bytes::from(original_args)),
                Frame::bulk("age_secs"),
                Frame::Integer(result.age_secs as i64),
            ])
        }
        Ok(None) => Frame::Null,
        Err(e) => Frame::error(format!("ERR {}", e)),
    }
}

/// Handle FCACHE.INVALIDATE command
/// FCACHE.INVALIDATE <function_name | ID id>
pub async fn fcache_invalidate(target: &Bytes, is_id: bool) -> Frame {
    let cache_lock = get_or_init_function_cache();
    let cache_opt = cache_lock.read();

    let cache = match cache_opt.as_ref() {
        Some(c) => c,
        None => return Frame::error("ERR Function cache not initialized"),
    };

    if is_id {
        // Invalidate by ID
        let id_str = String::from_utf8_lossy(target);
        match id_str.parse::<u64>() {
            Ok(id) => {
                if cache.invalidate(id) {
                    Frame::Integer(1)
                } else {
                    Frame::Integer(0)
                }
            }
            Err(_) => Frame::error("ERR Invalid cache ID"),
        }
    } else {
        // Invalidate by function name
        let function_name = String::from_utf8_lossy(target);
        let count = cache.invalidate_function(&function_name);
        Frame::Integer(count as i64)
    }
}

/// Handle FCACHE.STATS command
pub async fn fcache_stats() -> Frame {
    let cache_lock = get_or_init_function_cache();
    let cache_opt = cache_lock.read();

    let cache = match cache_opt.as_ref() {
        Some(c) => c,
        None => return Frame::error("ERR Function cache not initialized"),
    };

    let stats = cache.stats();

    // Build per-function stats array
    let per_function: Vec<Frame> = stats
        .per_function
        .iter()
        .map(|(name, fs)| {
            Frame::array(vec![
                Frame::bulk("name"),
                Frame::bulk(Bytes::from(name.clone())),
                Frame::bulk("entries"),
                Frame::Integer(fs.entries as i64),
                Frame::bulk("hits"),
                Frame::Integer(fs.hits as i64),
                Frame::bulk("misses"),
                Frame::Integer(fs.misses as i64),
                Frame::bulk("hit_rate"),
                Frame::Double(fs.hit_rate),
                Frame::bulk("cost_saved"),
                Frame::Double(fs.cost_saved),
            ])
        })
        .collect();

    Frame::array(vec![
        Frame::bulk("entries"),
        Frame::Integer(stats.entries as i64),
        Frame::bulk("lookups"),
        Frame::Integer(stats.lookups as i64),
        Frame::bulk("hits"),
        Frame::Integer(stats.hits as i64),
        Frame::bulk("misses"),
        Frame::Integer(stats.misses as i64),
        Frame::bulk("exact_hits"),
        Frame::Integer(stats.exact_hits as i64),
        Frame::bulk("semantic_hits"),
        Frame::Integer(stats.semantic_hits as i64),
        Frame::bulk("hit_rate"),
        Frame::Double(stats.hit_rate),
        Frame::bulk("avg_similarity"),
        Frame::Double(stats.avg_similarity),
        Frame::bulk("total_cost_saved"),
        Frame::Double(stats.total_cost_saved),
        Frame::bulk("total_time_saved_ms"),
        Frame::Integer(stats.total_time_saved_ms as i64),
        Frame::bulk("invalidations"),
        Frame::Integer(stats.invalidations as i64),
        Frame::bulk("per_function"),
        Frame::array(per_function),
    ])
}

/// Handle FCACHE.INFO command
pub async fn fcache_info() -> Frame {
    use ferrite_ai::semantic::FunctionCacheConfig;

    let config = FunctionCacheConfig::default();

    Frame::array(vec![
        Frame::bulk("enabled"),
        Frame::bulk(if config.enabled { "yes" } else { "no" }),
        Frame::bulk("default_threshold"),
        Frame::Double(config.default_threshold as f64),
        Frame::bulk("max_entries"),
        Frame::Integer(config.max_entries as i64),
        Frame::bulk("default_ttl_secs"),
        Frame::Integer(config.default_ttl_secs as i64),
        Frame::bulk("embedding_dim"),
        Frame::Integer(config.embedding_dim as i64),
        Frame::bulk("normalize_arguments"),
        Frame::bulk(if config.normalize_arguments {
            "yes"
        } else {
            "no"
        }),
        Frame::bulk("strict_function_names"),
        Frame::bulk(if config.strict_function_names {
            "yes"
        } else {
            "no"
        }),
        Frame::bulk("cost_tracking_enabled"),
        Frame::bulk(if config.cost_tracking.enabled {
            "yes"
        } else {
            "no"
        }),
        Frame::bulk("default_cost_per_call"),
        Frame::Double(config.cost_tracking.default_cost_per_call),
        Frame::bulk("dependency_tracking"),
        Frame::bulk(if config.invalidation.dependency_tracking {
            "yes"
        } else {
            "no"
        }),
    ])
}

/// Handle FCACHE.CLEAR command
pub async fn fcache_clear() -> Frame {
    let cache_lock = get_or_init_function_cache();
    let cache_opt = cache_lock.read();

    match cache_opt.as_ref() {
        Some(c) => {
            c.clear();
            Frame::simple("OK")
        }
        None => Frame::error("ERR Function cache not initialized"),
    }
}

/// Handle FCACHE.THRESHOLD command
/// FCACHE.THRESHOLD <function_name> <threshold>
pub async fn fcache_threshold(function_name: &Bytes, threshold: f32) -> Frame {
    let cache_lock = get_or_init_function_cache();
    let mut cache_opt = cache_lock.write();

    match cache_opt.as_mut() {
        Some(c) => {
            let name = String::from_utf8_lossy(function_name).to_string();
            c.set_function_threshold(&name, threshold);
            Frame::simple("OK")
        }
        None => Frame::error("ERR Function cache not initialized"),
    }
}

/// Handle FCACHE.TTL command
/// FCACHE.TTL <function_name> <ttl_secs>
pub async fn fcache_ttl(function_name: &Bytes, ttl_secs: u64) -> Frame {
    let cache_lock = get_or_init_function_cache();
    let mut cache_opt = cache_lock.write();

    match cache_opt.as_mut() {
        Some(c) => {
            let name = String::from_utf8_lossy(function_name).to_string();
            c.set_function_ttl(&name, ttl_secs);
            Frame::simple("OK")
        }
        None => Frame::error("ERR Function cache not initialized"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_info() {
        let result = info().await;
        assert!(matches!(result, Frame::Array(Some(_))));
    }

    #[tokio::test]
    async fn test_stats() {
        let result = stats().await;
        assert!(matches!(result, Frame::Array(Some(_))));
    }

    #[tokio::test]
    async fn test_clear() {
        let result = clear().await;
        assert!(matches!(result, Frame::Simple(_)));
    }

    #[tokio::test]
    async fn test_gettext_returns_error() {
        let result = gettext(&Bytes::from("query"), None, None).await;
        assert!(matches!(result, Frame::Error(_)));
    }

    #[tokio::test]
    async fn test_config_get_enabled() {
        let result = config(&Bytes::from("GET"), Some(&Bytes::from("enabled")), None).await;
        assert!(matches!(result, Frame::Bulk(_)));
    }

    #[tokio::test]
    async fn test_config_set_not_implemented() {
        let result = config(&Bytes::from("SET"), None, None).await;
        assert!(matches!(result, Frame::Error(_)));
    }

    #[tokio::test]
    async fn test_config_unknown_operation() {
        let result = config(&Bytes::from("UNKNOWN"), None, None).await;
        assert!(matches!(result, Frame::Error(_)));
    }
}
