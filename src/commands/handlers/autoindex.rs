//! AI-Powered Auto-Indexing command handlers
//!
//! Migration target: `ferrite-search` crate (crates/ferrite-search)
//!
//! This module contains handlers for auto-index operations:
//! - AUTOINDEX.RECORD (record an access pattern)
//! - AUTOINDEX.ANALYZE (analyze patterns and get recommendations)
//! - AUTOINDEX.RECOMMEND (get current recommendations)
//! - AUTOINDEX.APPLY (apply a recommendation)
//! - AUTOINDEX.LIST (list active auto-created indexes)
//! - AUTOINDEX.REMOVE (remove an auto-created index)
//! - AUTOINDEX.STATS (get auto-indexing statistics)
//! - AUTOINDEX.INFO (get configuration info)
//! - AUTOINDEX.CLEANUP (clean up unused indexes)
//! - AUTOINDEX.ABTEST.START (start an A/B test for a recommendation)
//! - AUTOINDEX.ABTEST.STATUS (check A/B test status)
//! - AUTOINDEX.ABTEST.COMPLETE (complete an A/B test)

use bytes::Bytes;
use parking_lot::RwLock;
use std::sync::OnceLock;
use std::time::Duration;

use crate::protocol::Frame;
use ferrite_search::autoindex::{
    AccessType, AutoIndexConfig, AutoIndexEngine, IndexRecommendation, IndexType,
    RecommendationConfidence,
};

// Global auto-index engine instance (lazy initialized)
static AUTO_INDEX_ENGINE: OnceLock<RwLock<Option<AutoIndexEngine>>> = OnceLock::new();

fn get_or_init_engine() -> &'static RwLock<Option<AutoIndexEngine>> {
    AUTO_INDEX_ENGINE
        .get_or_init(|| RwLock::new(Some(AutoIndexEngine::new(AutoIndexConfig::default()))))
}

/// Handle AUTOINDEX.RECORD command
/// AUTOINDEX.RECORD <pattern> <access_type> <latency_us> [FIELDS field1 field2 ...]
pub async fn record(
    pattern: &Bytes,
    access_type: &Bytes,
    latency_us: u64,
    fields: Option<Vec<String>>,
) -> Frame {
    let engine_lock = get_or_init_engine();
    let engine_opt = engine_lock.read();

    let engine = match engine_opt.as_ref() {
        Some(e) => e,
        None => return Frame::error("ERR Auto-index engine not initialized"),
    };

    let pattern_str = String::from_utf8_lossy(pattern).to_string();
    let access_type_str = String::from_utf8_lossy(access_type).to_uppercase();

    let access = match access_type_str.as_str() {
        "READ" => AccessType::Read,
        "WRITE" => AccessType::Write,
        "SCAN" => AccessType::Scan,
        "RANGE" => AccessType::Range,
        "DELETE" => AccessType::Delete,
        _ => {
            return Frame::error(format!(
                "ERR Unknown access type: {}. Use READ, WRITE, SCAN, RANGE, or DELETE",
                access_type_str
            ))
        }
    };

    let latency = Duration::from_micros(latency_us);

    engine.record_access(&pattern_str, access, latency, fields.as_deref());

    Frame::simple("OK")
}

/// Handle AUTOINDEX.ANALYZE command
/// AUTOINDEX.ANALYZE
pub async fn analyze() -> Frame {
    let engine_lock = get_or_init_engine();
    let engine_opt = engine_lock.read();

    let engine = match engine_opt.as_ref() {
        Some(e) => e,
        None => return Frame::error("ERR Auto-index engine not initialized"),
    };

    let recommendations = engine.analyze();

    if recommendations.is_empty() {
        return Frame::array(vec![]);
    }

    let items: Vec<Frame> = recommendations
        .iter()
        .map(recommendation_to_frame)
        .collect();

    Frame::array(items)
}

/// Handle AUTOINDEX.RECOMMEND command
/// AUTOINDEX.RECOMMEND [COUNT count]
pub async fn recommend(count: Option<usize>) -> Frame {
    let engine_lock = get_or_init_engine();
    let engine_opt = engine_lock.read();

    let engine = match engine_opt.as_ref() {
        Some(e) => e,
        None => return Frame::error("ERR Auto-index engine not initialized"),
    };

    let mut recommendations = engine.get_recommendations();

    if let Some(c) = count {
        recommendations.truncate(c);
    }

    if recommendations.is_empty() {
        return Frame::array(vec![]);
    }

    let items: Vec<Frame> = recommendations
        .iter()
        .map(recommendation_to_frame)
        .collect();

    Frame::array(items)
}

/// Handle AUTOINDEX.APPLY command
/// AUTOINDEX.APPLY <pattern> <index_type> [FIELDS field1 field2 ...]
pub async fn apply(pattern: &Bytes, index_type: &Bytes, fields: Option<Vec<String>>) -> Frame {
    let engine_lock = get_or_init_engine();
    let engine_opt = engine_lock.read();

    let engine = match engine_opt.as_ref() {
        Some(e) => e,
        None => return Frame::error("ERR Auto-index engine not initialized"),
    };

    let pattern_str = String::from_utf8_lossy(pattern).to_string();
    let type_str = String::from_utf8_lossy(index_type).to_uppercase();

    let idx_type = match parse_index_type(&type_str, &fields) {
        Ok(t) => t,
        Err(e) => return Frame::error(e),
    };

    let rec = IndexRecommendation {
        pattern: pattern_str,
        index_type: idx_type,
        fields: fields.unwrap_or_default(),
        confidence: RecommendationConfidence::high(vec!["manual apply".to_string()]),
        estimated_storage_bytes: 0, // Will be calculated
        estimated_latency_improvement: 0.0,
    };

    match engine.apply_recommendation(&rec) {
        Ok(()) => Frame::simple("OK"),
        Err(e) => Frame::error(format!("ERR {}", e)),
    }
}

/// Handle AUTOINDEX.LIST command
/// AUTOINDEX.LIST
pub async fn list() -> Frame {
    let engine_lock = get_or_init_engine();
    let engine_opt = engine_lock.read();

    let engine = match engine_opt.as_ref() {
        Some(e) => e,
        None => return Frame::error("ERR Auto-index engine not initialized"),
    };

    let indexes = engine.get_active_indexes();

    if indexes.is_empty() {
        return Frame::array(vec![]);
    }

    let items: Vec<Frame> = indexes
        .iter()
        .map(|idx| {
            Frame::array(vec![
                Frame::bulk("pattern"),
                Frame::bulk(Bytes::from(idx.pattern.clone())),
                Frame::bulk("index_type"),
                Frame::bulk(Bytes::from(format!("{:?}", idx.index_type))),
                Frame::bulk("fields"),
                Frame::array(
                    idx.fields
                        .iter()
                        .map(|f| Frame::bulk(Bytes::from(f.clone())))
                        .collect(),
                ),
                Frame::bulk("created_at"),
                Frame::Integer(idx.created_at as i64),
                Frame::bulk("access_count"),
                Frame::Integer(idx.access_count as i64),
                Frame::bulk("storage_bytes"),
                Frame::Integer(idx.storage_bytes as i64),
                Frame::bulk("latency_improvement_percent"),
                Frame::Double(idx.latency_improvement_percent),
                Frame::bulk("from_ab_test"),
                Frame::bulk(if idx.from_ab_test { "true" } else { "false" }),
            ])
        })
        .collect();

    Frame::array(items)
}

/// Handle AUTOINDEX.REMOVE command
/// AUTOINDEX.REMOVE <pattern> <index_type>
pub async fn remove(pattern: &Bytes, index_type: &Bytes) -> Frame {
    let engine_lock = get_or_init_engine();
    let engine_opt = engine_lock.read();

    let engine = match engine_opt.as_ref() {
        Some(e) => e,
        None => return Frame::error("ERR Auto-index engine not initialized"),
    };

    let pattern_str = String::from_utf8_lossy(pattern).to_string();
    let type_str = String::from_utf8_lossy(index_type).to_uppercase();

    let idx_type = match parse_index_type(&type_str, &None) {
        Ok(t) => t,
        Err(e) => return Frame::error(e),
    };

    if engine.remove_index(&pattern_str, &idx_type) {
        Frame::Integer(1)
    } else {
        Frame::Integer(0)
    }
}

/// Handle AUTOINDEX.STATS command
/// AUTOINDEX.STATS
pub async fn stats() -> Frame {
    let engine_lock = get_or_init_engine();
    let engine_opt = engine_lock.read();

    let engine = match engine_opt.as_ref() {
        Some(e) => e,
        None => return Frame::error("ERR Auto-index engine not initialized"),
    };

    let stats = engine.stats();

    Frame::array(vec![
        Frame::bulk("patterns_analyzed"),
        Frame::Integer(stats.patterns_analyzed as i64),
        Frame::bulk("recommendations_generated"),
        Frame::Integer(stats.recommendations_generated as i64),
        Frame::bulk("indexes_created"),
        Frame::Integer(stats.indexes_created as i64),
        Frame::bulk("indexes_removed"),
        Frame::Integer(stats.indexes_removed as i64),
        Frame::bulk("ab_tests_conducted"),
        Frame::Integer(stats.ab_tests_conducted as i64),
        Frame::bulk("ab_tests_passed"),
        Frame::Integer(stats.ab_tests_passed as i64),
        Frame::bulk("storage_used_bytes"),
        Frame::Integer(stats.storage_used_bytes as i64),
        Frame::bulk("total_latency_improvement_percent"),
        Frame::Double(stats.total_latency_improvement_percent),
    ])
}

/// Handle AUTOINDEX.INFO command
/// AUTOINDEX.INFO
pub async fn info() -> Frame {
    let engine_lock = get_or_init_engine();
    let engine_opt = engine_lock.read();

    let engine = match engine_opt.as_ref() {
        Some(e) => e,
        None => return Frame::error("ERR Auto-index engine not initialized"),
    };

    let config = engine.config();

    Frame::array(vec![
        Frame::bulk("enabled"),
        Frame::bulk(if config.enabled { "yes" } else { "no" }),
        Frame::bulk("min_access_count"),
        Frame::Integer(config.min_access_count as i64),
        Frame::bulk("analysis_interval_secs"),
        Frame::Integer(config.analysis_interval_secs as i64),
        Frame::bulk("confidence_threshold"),
        Frame::Double(config.confidence_threshold),
        Frame::bulk("max_indexes_per_pattern"),
        Frame::Integer(config.max_indexes_per_pattern as i64),
        Frame::bulk("max_total_indexes"),
        Frame::Integer(config.max_total_indexes as i64),
        Frame::bulk("auto_apply"),
        Frame::bulk(if config.auto_apply { "yes" } else { "no" }),
        Frame::bulk("storage_budget_bytes"),
        Frame::Integer(config.storage_budget_bytes as i64),
        Frame::bulk("max_write_amplification"),
        Frame::Double(config.max_write_amplification),
        Frame::bulk("ab_testing_enabled"),
        Frame::bulk(if config.ab_testing_enabled {
            "yes"
        } else {
            "no"
        }),
        Frame::bulk("ab_test_duration_secs"),
        Frame::Integer(config.ab_test_duration_secs as i64),
        Frame::bulk("history_retention_hours"),
        Frame::Integer(config.history_retention_hours as i64),
        Frame::bulk("ml_predictor_enabled"),
        Frame::bulk(if config.ml_predictor_enabled {
            "yes"
        } else {
            "no"
        }),
    ])
}

/// Handle AUTOINDEX.CLEANUP command
/// AUTOINDEX.CLEANUP [MIN_ACCESS_PER_DAY count]
pub async fn cleanup(min_access_per_day: Option<u64>) -> Frame {
    let engine_lock = get_or_init_engine();
    let engine_opt = engine_lock.read();

    let engine = match engine_opt.as_ref() {
        Some(e) => e,
        None => return Frame::error("ERR Auto-index engine not initialized"),
    };

    let threshold = min_access_per_day.unwrap_or(10);
    let removed = engine.cleanup_unused(threshold);

    Frame::Integer(removed as i64)
}

/// Handle AUTOINDEX.ABTEST.START command
/// AUTOINDEX.ABTEST.START <pattern> <index_type> [FIELDS field1 field2 ...]
pub async fn abtest_start(
    pattern: &Bytes,
    index_type: &Bytes,
    fields: Option<Vec<String>>,
) -> Frame {
    let engine_lock = get_or_init_engine();
    let engine_opt = engine_lock.read();

    let engine = match engine_opt.as_ref() {
        Some(e) => e,
        None => return Frame::error("ERR Auto-index engine not initialized"),
    };

    let pattern_str = String::from_utf8_lossy(pattern).to_string();
    let type_str = String::from_utf8_lossy(index_type).to_uppercase();

    let idx_type = match parse_index_type(&type_str, &fields) {
        Ok(t) => t,
        Err(e) => return Frame::error(e),
    };

    let rec = IndexRecommendation {
        pattern: pattern_str,
        index_type: idx_type,
        fields: fields.unwrap_or_default(),
        confidence: RecommendationConfidence::medium(vec!["A/B test".to_string()]),
        estimated_storage_bytes: 0,
        estimated_latency_improvement: 0.0,
    };

    match engine.start_ab_test(rec) {
        Ok(test_id) => Frame::bulk(Bytes::from(test_id)),
        Err(e) => Frame::error(format!("ERR {}", e)),
    }
}

/// Handle AUTOINDEX.ABTEST.COMPLETE command
/// AUTOINDEX.ABTEST.COMPLETE <test_id>
pub async fn abtest_complete(test_id: &Bytes) -> Frame {
    let engine_lock = get_or_init_engine();
    let engine_opt = engine_lock.read();

    let engine = match engine_opt.as_ref() {
        Some(e) => e,
        None => return Frame::error("ERR Auto-index engine not initialized"),
    };

    let id = String::from_utf8_lossy(test_id).to_string();

    match engine.complete_ab_test(&id) {
        Ok(accepted) => Frame::array(vec![
            Frame::bulk("result"),
            Frame::bulk(if accepted { "accepted" } else { "rejected" }),
        ]),
        Err(e) => Frame::error(format!("ERR {}", e)),
    }
}

/// Handle AUTOINDEX.HAS command
/// AUTOINDEX.HAS <pattern> <index_type>
pub async fn has(pattern: &Bytes, index_type: &Bytes) -> Frame {
    let engine_lock = get_or_init_engine();
    let engine_opt = engine_lock.read();

    let engine = match engine_opt.as_ref() {
        Some(e) => e,
        None => return Frame::error("ERR Auto-index engine not initialized"),
    };

    let pattern_str = String::from_utf8_lossy(pattern).to_string();
    let type_str = String::from_utf8_lossy(index_type).to_uppercase();

    let idx_type = match parse_index_type(&type_str, &None) {
        Ok(t) => t,
        Err(e) => return Frame::error(e),
    };

    if engine.has_index(&pattern_str, &idx_type) {
        Frame::Integer(1)
    } else {
        Frame::Integer(0)
    }
}

/// Handle AUTOINDEX.USAGE command
/// AUTOINDEX.USAGE <pattern> <index_type>
pub async fn usage(pattern: &Bytes, index_type: &Bytes) -> Frame {
    let engine_lock = get_or_init_engine();
    let engine_opt = engine_lock.read();

    let engine = match engine_opt.as_ref() {
        Some(e) => e,
        None => return Frame::error("ERR Auto-index engine not initialized"),
    };

    let pattern_str = String::from_utf8_lossy(pattern).to_string();
    let type_str = String::from_utf8_lossy(index_type).to_uppercase();

    let idx_type = match parse_index_type(&type_str, &None) {
        Ok(t) => t,
        Err(e) => return Frame::error(e),
    };

    engine.record_index_usage(&pattern_str, &idx_type);

    Frame::simple("OK")
}

// Helper functions

fn recommendation_to_frame(rec: &IndexRecommendation) -> Frame {
    Frame::array(vec![
        Frame::bulk("pattern"),
        Frame::bulk(Bytes::from(rec.pattern.clone())),
        Frame::bulk("index_type"),
        Frame::bulk(Bytes::from(format!("{:?}", rec.index_type))),
        Frame::bulk("fields"),
        Frame::array(
            rec.fields
                .iter()
                .map(|f| Frame::bulk(Bytes::from(f.clone())))
                .collect(),
        ),
        Frame::bulk("confidence_score"),
        Frame::Double(rec.confidence.score),
        Frame::bulk("confidence_reasons"),
        Frame::array(
            rec.confidence
                .reasons
                .iter()
                .map(|r| Frame::bulk(Bytes::from(r.clone())))
                .collect(),
        ),
        Frame::bulk("estimated_storage_bytes"),
        Frame::Integer(rec.estimated_storage_bytes as i64),
        Frame::bulk("estimated_latency_improvement"),
        Frame::Double(rec.estimated_latency_improvement),
    ])
}

fn parse_index_type(type_str: &str, fields: &Option<Vec<String>>) -> Result<IndexType, String> {
    match type_str {
        "BTREE" => Ok(IndexType::BTree),
        "HASH" => Ok(IndexType::Hash),
        "SECONDARY" => {
            if let Some(f) = fields {
                if let Some(field) = f.first() {
                    Ok(IndexType::Secondary(field.clone()))
                } else {
                    Err("ERR SECONDARY index requires at least one field".to_string())
                }
            } else {
                Err("ERR SECONDARY index requires at least one field".to_string())
            }
        }
        "COMPOUND" => {
            if let Some(f) = fields {
                if f.len() >= 2 {
                    Ok(IndexType::Compound(f.clone()))
                } else {
                    Err("ERR COMPOUND index requires at least two fields".to_string())
                }
            } else {
                Err("ERR COMPOUND index requires at least two fields".to_string())
            }
        }
        "GEOSPATIAL" => Ok(IndexType::Geospatial),
        "FULLTEXT" => Ok(IndexType::FullText),
        "PREFIX" => Ok(IndexType::Prefix),
        _ => Err(format!(
            "ERR Unknown index type: {}. Use BTREE, HASH, SECONDARY, COMPOUND, GEOSPATIAL, FULLTEXT, or PREFIX",
            type_str
        )),
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
    async fn test_list_empty() {
        let result = list().await;
        assert!(matches!(result, Frame::Array(Some(_))));
    }

    #[tokio::test]
    async fn test_record() {
        let result = record(&Bytes::from("users:*"), &Bytes::from("READ"), 100, None).await;
        assert!(matches!(result, Frame::Simple(_)));
    }

    #[tokio::test]
    async fn test_record_invalid_type() {
        let result = record(&Bytes::from("users:*"), &Bytes::from("INVALID"), 100, None).await;
        assert!(matches!(result, Frame::Error(_)));
    }

    #[tokio::test]
    async fn test_analyze() {
        let result = analyze().await;
        assert!(matches!(result, Frame::Array(Some(_))));
    }

    #[tokio::test]
    async fn test_recommend() {
        let result = recommend(Some(5)).await;
        assert!(matches!(result, Frame::Array(Some(_))));
    }

    #[tokio::test]
    async fn test_parse_index_type_btree() {
        let result = parse_index_type("BTREE", &None);
        assert!(matches!(result, Ok(IndexType::BTree)));
    }

    #[tokio::test]
    async fn test_parse_index_type_secondary_no_field() {
        let result = parse_index_type("SECONDARY", &None);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_parse_index_type_secondary_with_field() {
        let fields = Some(vec!["name".to_string()]);
        let result = parse_index_type("SECONDARY", &fields);
        assert!(matches!(result, Ok(IndexType::Secondary(_))));
    }

    #[tokio::test]
    async fn test_cleanup() {
        let result = cleanup(Some(10)).await;
        assert!(matches!(result, Frame::Integer(_)));
    }
}
