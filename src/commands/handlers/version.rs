//! VERSION.* command handlers
//!
//! Time-indexed data versioning with branch, tag, merge, compaction, and
//! point-in-time read support.
#![allow(dead_code)]

use std::sync::OnceLock;

use bytes::Bytes;

use crate::protocol::Frame;
use ferrite_core::temporal::versioning::{VersionConfig, VersionManager};

use super::err_frame;

/// Global version manager singleton.
static VERSION_MGR: OnceLock<VersionManager> = OnceLock::new();

fn get_manager() -> &'static VersionManager {
    VERSION_MGR.get_or_init(|| VersionManager::new(VersionConfig::default()))
}

/// Dispatch a `VERSION` subcommand.
pub fn version_command(subcommand: &str, args: &[String]) -> Frame {
    match subcommand.to_uppercase().as_str() {
        "WRITE" => version_write(args),
        "READ" => version_read(args),
        "HISTORY" => version_history(args),
        "DIFF" => version_diff(args),
        "BRANCH.CREATE" => version_branch_create(args),
        "BRANCH.DELETE" => version_branch_delete(args),
        "BRANCH.LIST" => version_branch_list(),
        "BRANCH.SWITCH" => version_branch_switch(args),
        "MERGE" => version_merge(args),
        "TAG" => version_tag(args),
        "TAG.LIST" => version_tag_list(),
        "COMPACT" => version_compact(args),
        "STATS" => version_stats(),
        "HELP" => version_help(),
        _ => err_frame(&format!("unknown VERSION subcommand '{}'", subcommand)),
    }
}

/// VERSION.WRITE key value [BRANCH name] [AUTHOR name] [MESSAGE msg]
fn version_write(args: &[String]) -> Frame {
    if args.len() < 2 {
        return err_frame("VERSION.WRITE requires at least: key value");
    }

    let key = &args[0];
    let value = args[1].as_bytes();
    let mut branch = "main".to_string();
    let mut author = "anonymous".to_string();
    let mut _message: Option<String> = None;

    let mut i = 2;
    while i < args.len() {
        match args[i].to_uppercase().as_str() {
            "BRANCH" if i + 1 < args.len() => {
                branch = args[i + 1].clone();
                i += 2;
            }
            "AUTHOR" if i + 1 < args.len() => {
                author = args[i + 1].clone();
                i += 2;
            }
            "MESSAGE" if i + 1 < args.len() => {
                _message = Some(args[i + 1].clone());
                i += 2;
            }
            _ => {
                i += 1;
            }
        }
    }

    match get_manager().write(key, value, &branch, &author) {
        Ok(vid) => Frame::Integer(vid as i64),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// VERSION.READ key [BRANCH name] [AT timestamp_ms] [VERSION id]
fn version_read(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("VERSION.READ requires at least: key");
    }

    let key = &args[0];
    let mut branch: Option<String> = None;
    let mut at_ts: Option<u64> = None;
    let mut version_id: Option<u64> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].to_uppercase().as_str() {
            "BRANCH" if i + 1 < args.len() => {
                branch = Some(args[i + 1].clone());
                i += 2;
            }
            "AT" if i + 1 < args.len() => {
                at_ts = args[i + 1].parse().ok();
                i += 2;
            }
            "VERSION" if i + 1 < args.len() => {
                version_id = args[i + 1].parse().ok();
                i += 2;
            }
            _ => {
                i += 1;
            }
        }
    }

    let mgr = get_manager();

    let version = if let Some(vid) = version_id {
        mgr.read_version(key, vid)
    } else if let Some(ts) = at_ts {
        mgr.read_at(key, ts)
    } else {
        let br = branch.as_deref().unwrap_or("main");
        mgr.read(key, br)
    };

    match version {
        Some(v) => Frame::bulk(Bytes::from(v.value)),
        None => Frame::Null,
    }
}

/// VERSION.HISTORY key [LIMIT n]
fn version_history(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("VERSION.HISTORY requires at least: key");
    }

    let key = &args[0];
    let mut limit = 50usize;

    let mut i = 1;
    while i < args.len() {
        if args[i].to_uppercase() == "LIMIT" && i + 1 < args.len() {
            limit = args[i + 1].parse().unwrap_or(50);
            i += 2;
        } else {
            i += 1;
        }
    }

    let versions = get_manager().history(key, limit);
    let items: Vec<Frame> = versions
        .iter()
        .map(|v| {
            Frame::array(vec![
                Frame::bulk(format!("id:{}", v.id)),
                Frame::bulk(format!("timestamp:{}", v.timestamp)),
                Frame::bulk(format!("author:{}", v.author)),
                Frame::bulk(Bytes::from(v.value.clone())),
            ])
        })
        .collect();
    Frame::Array(Some(items))
}

/// VERSION.DIFF key version1 version2
fn version_diff(args: &[String]) -> Frame {
    if args.len() < 3 {
        return err_frame("VERSION.DIFF requires: key version1 version2");
    }

    let key = &args[0];
    let v1: u64 = match args[1].parse() {
        Ok(v) => v,
        Err(_) => return err_frame("invalid version1 id"),
    };
    let v2: u64 = match args[2].parse() {
        Ok(v) => v,
        Err(_) => return err_frame("invalid version2 id"),
    };

    match get_manager().diff(key, v1, v2) {
        Some(diff) => Frame::array(vec![
            Frame::bulk(format!("key:{}", diff.key)),
            Frame::bulk(format!("old_version:{}", diff.old_version)),
            Frame::bulk(format!("new_version:{}", diff.new_version)),
            Frame::bulk(format!("changed:{}", diff.changed)),
        ]),
        None => Frame::Null,
    }
}

/// VERSION.BRANCH.CREATE name [FROM branch]
fn version_branch_create(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("VERSION.BRANCH.CREATE requires: name [FROM branch]");
    }

    let name = &args[0];
    let mut from = "main".to_string();

    if args.len() >= 3 && args[1].to_uppercase() == "FROM" {
        from = args[2].clone();
    }

    match get_manager().create_branch(name, &from) {
        Ok(_) => Frame::Simple(Bytes::from("OK")),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// VERSION.BRANCH.DELETE name
fn version_branch_delete(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("VERSION.BRANCH.DELETE requires: name");
    }

    match get_manager().delete_branch(&args[0]) {
        Ok(()) => Frame::Simple(Bytes::from("OK")),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// VERSION.BRANCH.LIST
fn version_branch_list() -> Frame {
    let branches = get_manager().list_branches();
    let items: Vec<Frame> = branches
        .iter()
        .map(|b| {
            Frame::array(vec![
                Frame::bulk(format!("name:{}", b.name)),
                Frame::bulk(format!("head:{}", b.head)),
                Frame::bulk(format!("protected:{}", b.protected)),
            ])
        })
        .collect();
    Frame::Array(Some(items))
}

/// VERSION.BRANCH.SWITCH name
fn version_branch_switch(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("VERSION.BRANCH.SWITCH requires: name");
    }

    match get_manager().switch_branch(&args[0]) {
        Ok(()) => Frame::Simple(Bytes::from("OK")),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// VERSION.MERGE source target
fn version_merge(args: &[String]) -> Frame {
    if args.len() < 2 {
        return err_frame("VERSION.MERGE requires: source target");
    }

    match get_manager().merge(&args[0], &args[1]) {
        Ok(result) => Frame::array(vec![
            Frame::bulk(format!("merged_keys:{}", result.merged_keys)),
            Frame::bulk(format!("conflicts:{}", result.conflicts.len())),
            Frame::bulk(format!("source:{}", result.source_branch)),
            Frame::bulk(format!("target:{}", result.target_branch)),
        ]),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// VERSION.TAG name version_id [MESSAGE msg]
fn version_tag(args: &[String]) -> Frame {
    if args.len() < 2 {
        return err_frame("VERSION.TAG requires: name version_id [MESSAGE msg]");
    }

    let name = &args[0];
    let version_id: u64 = match args[1].parse() {
        Ok(v) => v,
        Err(_) => return err_frame("invalid version_id"),
    };

    let message = if args.len() >= 4 && args[2].to_uppercase() == "MESSAGE" {
        Some(args[3].clone())
    } else {
        None
    };

    match get_manager().create_tag(name, version_id, message) {
        Ok(_) => Frame::Simple(Bytes::from("OK")),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// VERSION.TAG.LIST
fn version_tag_list() -> Frame {
    let tags = get_manager().list_tags();
    let items: Vec<Frame> = tags
        .iter()
        .map(|t| {
            Frame::array(vec![
                Frame::bulk(format!("name:{}", t.name)),
                Frame::bulk(format!("version_id:{}", t.version_id)),
                Frame::bulk(format!("created_at:{}", t.created_at)),
            ])
        })
        .collect();
    Frame::Array(Some(items))
}

/// VERSION.COMPACT key [KEEP n]
fn version_compact(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("VERSION.COMPACT requires: key [KEEP n]");
    }

    let key = &args[0];
    let mut keep = 10usize;

    if args.len() >= 3 && args[1].to_uppercase() == "KEEP" {
        keep = args[2].parse().unwrap_or(10);
    }

    let removed = get_manager().compact(key, keep);
    Frame::Integer(removed as i64)
}

/// VERSION.STATS
fn version_stats() -> Frame {
    let stats = get_manager().stats();
    Frame::array(vec![
        Frame::bulk(format!("total_versions:{}", stats.total_versions)),
        Frame::bulk(format!("total_keys:{}", stats.total_keys)),
        Frame::bulk(format!("branches:{}", stats.branches)),
        Frame::bulk(format!("tags:{}", stats.tags)),
        Frame::bulk(format!("storage_bytes:{}", stats.storage_bytes)),
    ])
}

/// VERSION.HELP
fn version_help() -> Frame {
    let lines = vec![
        "VERSION.WRITE key value [BRANCH name] [AUTHOR name] [MESSAGE msg] - Create a new version",
        "VERSION.READ key [BRANCH name] [AT timestamp_ms] [VERSION id] - Read a version",
        "VERSION.HISTORY key [LIMIT n] - List version history",
        "VERSION.DIFF key version1 version2 - Compare two versions",
        "VERSION.BRANCH.CREATE name [FROM branch] - Create a branch",
        "VERSION.BRANCH.DELETE name - Delete a branch",
        "VERSION.BRANCH.LIST - List all branches",
        "VERSION.BRANCH.SWITCH name - Switch current branch",
        "VERSION.MERGE source target - Merge branches",
        "VERSION.TAG name version_id [MESSAGE msg] - Create a tag",
        "VERSION.TAG.LIST - List all tags",
        "VERSION.COMPACT key [KEEP n] - Compact old versions",
        "VERSION.STATS - Show version store statistics",
    ];
    Frame::Array(Some(
        lines
            .into_iter()
            .map(|l| Frame::bulk(l.to_string()))
            .collect(),
    ))
}
