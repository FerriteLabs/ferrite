//! AI Agent memory command handlers
//!
//! AGENT.REMEMBER, AGENT.RECALL, AGENT.FORGET, AGENT.CHECKPOINT, AGENT.STATS
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;

use bytes::Bytes;

use crate::protocol::Frame;
use ferrite_ai::agent_memory::store::{
    AgentMemoryConfig, AgentMemoryStats, MemoryError, PersistentAgentMemoryStore,
    StoreMemoryEntry, StoreMemoryType,
};

use super::err_frame;

/// Global agent memory store singleton.
static AGENT_STORE: OnceLock<PersistentAgentMemoryStore> = OnceLock::new();

/// Get or initialize the global agent memory store.
fn get_store() -> &'static PersistentAgentMemoryStore {
    AGENT_STORE.get_or_init(|| PersistentAgentMemoryStore::new(AgentMemoryConfig::default()))
}

/// Dispatch an `AGENT` subcommand.
pub fn agent_command(subcommand: &str, args: &[String]) -> Frame {
    match subcommand.to_uppercase().as_str() {
        "REMEMBER" => agent_remember(args),
        "RECALL" => agent_recall(args),
        "RECALL.RECENT" => agent_recall_recent(args),
        "FORGET" => agent_forget(args),
        "FORGET.ALL" => agent_forget_all(args),
        "FORGET.BEFORE" => agent_forget_before(args),
        "CHECKPOINT" => agent_checkpoint(args),
        "RESTORE" => agent_restore(args),
        "STATS" => agent_stats(args),
        "HELP" => agent_help(),
        _ => err_frame(&format!("unknown AGENT subcommand '{}'", subcommand)),
    }
}

/// AGENT.REMEMBER agent_id content [TYPE type] [IMPORTANCE 0.0-1.0] [TTL seconds] [META key value ...]
fn agent_remember(args: &[String]) -> Frame {
    if args.len() < 2 {
        return err_frame("AGENT.REMEMBER requires at least: agent_id content");
    }

    let agent_id = &args[0];
    let content = &args[1];
    let mut memory_type = StoreMemoryType::Conversation;
    let mut importance: Option<f32> = None;
    let mut _ttl: Option<Duration> = None;
    let mut metadata = HashMap::new();

    let mut i = 2;
    while i < args.len() {
        match args[i].to_uppercase().as_str() {
            "TYPE" => {
                if i + 1 < args.len() {
                    if let Some(mt) = StoreMemoryType::from_str_loose(&args[i + 1]) {
                        memory_type = mt;
                    } else {
                        return err_frame(&format!("unknown memory type '{}'", args[i + 1]));
                    }
                    i += 2;
                } else {
                    return err_frame("TYPE requires a value");
                }
            }
            "IMPORTANCE" => {
                if i + 1 < args.len() {
                    match args[i + 1].parse::<f32>() {
                        Ok(v) => importance = Some(v),
                        Err(_) => return err_frame("IMPORTANCE must be a float"),
                    }
                    i += 2;
                } else {
                    return err_frame("IMPORTANCE requires a value");
                }
            }
            "TTL" => {
                if i + 1 < args.len() {
                    match args[i + 1].parse::<u64>() {
                        Ok(v) => _ttl = Some(Duration::from_secs(v)),
                        Err(_) => return err_frame("TTL must be an integer (seconds)"),
                    }
                    i += 2;
                } else {
                    return err_frame("TTL requires a value");
                }
            }
            "META" => {
                i += 1;
                while i + 1 < args.len() {
                    let k = args[i].clone();
                    let v = args[i + 1].clone();
                    // Check if next key is a known keyword
                    if matches!(
                        k.to_uppercase().as_str(),
                        "TYPE" | "IMPORTANCE" | "TTL" | "META"
                    ) {
                        break;
                    }
                    metadata.insert(k, serde_json::Value::String(v));
                    i += 2;
                }
            }
            _ => {
                i += 1;
            }
        }
    }

    let store = get_store();
    match store.remember(agent_id, content, memory_type, metadata) {
        Ok(id) => {
            // Update importance if specified
            if let Some(imp) = importance {
                let _ = store.update_importance(&id, imp);
            }
            Frame::Bulk(Some(Bytes::from(id)))
        }
        Err(e) => err_frame(&e.to_string()),
    }
}

/// AGENT.RECALL agent_id query [TOP k] [TYPE type]
fn agent_recall(args: &[String]) -> Frame {
    if args.len() < 2 {
        return err_frame("AGENT.RECALL requires: agent_id query");
    }

    let agent_id = &args[0];
    let query = &args[1];
    let mut top_k = 10usize;
    let mut filter_type: Option<StoreMemoryType> = None;

    let mut i = 2;
    while i < args.len() {
        match args[i].to_uppercase().as_str() {
            "TOP" => {
                if i + 1 < args.len() {
                    top_k = args[i + 1].parse().unwrap_or(10);
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "TYPE" => {
                if i + 1 < args.len() {
                    filter_type = StoreMemoryType::from_str_loose(&args[i + 1]);
                    i += 2;
                } else {
                    i += 1;
                }
            }
            _ => i += 1,
        }
    }

    let store = get_store();
    let results = if let Some(mt) = filter_type {
        store.recall_by_type(agent_id, mt, top_k)
    } else {
        store.recall(agent_id, query, top_k)
    };

    let frames: Vec<Frame> = results.into_iter().map(memory_to_frame).collect();
    Frame::Array(Some(frames))
}

/// AGENT.RECALL.RECENT agent_id [LIMIT n]
fn agent_recall_recent(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("AGENT.RECALL.RECENT requires: agent_id");
    }

    let agent_id = &args[0];
    let mut limit = 10usize;

    if args.len() >= 3 && args[1].to_uppercase() == "LIMIT" {
        limit = args[2].parse().unwrap_or(10);
    }

    let store = get_store();
    let results = store.recall_recent(agent_id, limit);
    let frames: Vec<Frame> = results.into_iter().map(memory_to_frame).collect();
    Frame::Array(Some(frames))
}

/// AGENT.FORGET memory_id
fn agent_forget(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("AGENT.FORGET requires: memory_id");
    }

    let store = get_store();
    match store.forget(&args[0]) {
        Ok(()) => Frame::Simple(Bytes::from("OK")),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// AGENT.FORGET.ALL agent_id
fn agent_forget_all(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("AGENT.FORGET.ALL requires: agent_id");
    }

    let store = get_store();
    let count = store.forget_agent(&args[0]);
    Frame::Integer(count as i64)
}

/// AGENT.FORGET.BEFORE agent_id timestamp_ms
fn agent_forget_before(args: &[String]) -> Frame {
    if args.len() < 2 {
        return err_frame("AGENT.FORGET.BEFORE requires: agent_id timestamp_ms");
    }

    let agent_id = &args[0];
    let before: u64 = match args[1].parse() {
        Ok(v) => v,
        Err(_) => return err_frame("timestamp must be an integer (epoch ms)"),
    };

    let store = get_store();
    let count = store.forget_before(agent_id, before);
    Frame::Integer(count as i64)
}

/// AGENT.CHECKPOINT agent_id json_state
fn agent_checkpoint(args: &[String]) -> Frame {
    if args.len() < 2 {
        return err_frame("AGENT.CHECKPOINT requires: agent_id json_state");
    }

    let agent_id = &args[0];
    let state: serde_json::Value = match serde_json::from_str(&args[1]) {
        Ok(v) => v,
        Err(e) => return err_frame(&format!("invalid JSON: {}", e)),
    };

    let store = get_store();
    match store.checkpoint(agent_id, state) {
        Ok(id) => Frame::Bulk(Some(Bytes::from(id))),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// AGENT.RESTORE checkpoint_id
fn agent_restore(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("AGENT.RESTORE requires: checkpoint_id");
    }

    let store = get_store();
    match store.restore_checkpoint(&args[0]) {
        Some(value) => {
            let json = serde_json::to_string(&value).unwrap_or_default();
            Frame::Bulk(Some(Bytes::from(json)))
        }
        None => Frame::Null,
    }
}

/// AGENT.STATS agent_id
fn agent_stats(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("AGENT.STATS requires: agent_id");
    }

    let store = get_store();
    let stats = store.stats(&args[0]);

    let mut map = HashMap::new();
    map.insert(
        Bytes::from("total_memories"),
        Frame::Integer(stats.total_memories as i64),
    );
    map.insert(
        Bytes::from("total_bytes"),
        Frame::Integer(stats.total_bytes as i64),
    );

    if let Some(oldest) = stats.oldest_memory {
        map.insert(Bytes::from("oldest_memory"), Frame::Integer(oldest as i64));
    }
    if let Some(newest) = stats.newest_memory {
        map.insert(Bytes::from("newest_memory"), Frame::Integer(newest as i64));
    }

    // Add per-type counts
    for (mt, count) in &stats.by_type {
        map.insert(
            Bytes::from(format!("type:{}", mt)),
            Frame::Integer(*count as i64),
        );
    }

    Frame::Map(map)
}

/// AGENT.HELP
fn agent_help() -> Frame {
    let help = vec![
        "AGENT.REMEMBER agent_id content [TYPE type] [IMPORTANCE 0.0-1.0] [TTL seconds] [META key value ...] - Store a memory",
        "AGENT.RECALL agent_id query [TOP k] [TYPE type] - Semantic recall",
        "AGENT.RECALL.RECENT agent_id [LIMIT n] - Recent memories",
        "AGENT.FORGET memory_id - Delete a memory",
        "AGENT.FORGET.ALL agent_id - Delete all agent memories",
        "AGENT.FORGET.BEFORE agent_id timestamp_ms - Delete old memories",
        "AGENT.CHECKPOINT agent_id json_state - Save agent state",
        "AGENT.RESTORE checkpoint_id - Restore agent state",
        "AGENT.STATS agent_id - Memory statistics",
        "AGENT.HELP - This help text",
    ];

    Frame::Array(Some(
        help.into_iter()
            .map(|s| Frame::Bulk(Some(Bytes::from(s.to_string()))))
            .collect(),
    ))
}

/// Convert a StoreMemoryEntry to a Frame (array of key-value pairs).
fn memory_to_frame(entry: StoreMemoryEntry) -> Frame {
    let fields = vec![
        Frame::Bulk(Some(Bytes::from("id"))),
        Frame::Bulk(Some(Bytes::from(entry.id))),
        Frame::Bulk(Some(Bytes::from("agent_id"))),
        Frame::Bulk(Some(Bytes::from(entry.agent_id))),
        Frame::Bulk(Some(Bytes::from("content"))),
        Frame::Bulk(Some(Bytes::from(entry.content))),
        Frame::Bulk(Some(Bytes::from("type"))),
        Frame::Bulk(Some(Bytes::from(entry.memory_type.to_string()))),
        Frame::Bulk(Some(Bytes::from("importance"))),
        Frame::Bulk(Some(Bytes::from(format!("{}", entry.importance)))),
        Frame::Bulk(Some(Bytes::from("created_at"))),
        Frame::Integer(entry.created_at as i64),
        Frame::Bulk(Some(Bytes::from("access_count"))),
        Frame::Integer(entry.access_count as i64),
    ];
    Frame::Array(Some(fields))
}
