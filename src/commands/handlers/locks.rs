//! LOCK.* command handlers
//!
//! LOCK.ACQUIRE, LOCK.RELEASE, LOCK.EXTEND, LOCK.TRY, LOCK.INFO,
//! LOCK.OWNER, LOCK.DEADLOCKS, LOCK.EXPIRE, LOCK.STATS, LOCK.HELP
#![allow(dead_code)]

use std::sync::OnceLock;
use std::time::Duration;

use bytes::Bytes;

use crate::protocol::Frame;
use ferrite_core::cluster::distributed_lock::{
    DistributedLockManager, LockConfig, LockType,
};

use super::err_frame;

/// Global lock manager singleton.
static LOCK_MANAGER: OnceLock<DistributedLockManager> = OnceLock::new();

fn get_lock_manager() -> &'static DistributedLockManager {
    LOCK_MANAGER.get_or_init(|| DistributedLockManager::new(LockConfig::default()))
}

/// Helper to create a bulk frame from an owned String.
fn bulk(s: impl Into<String>) -> Frame {
    Frame::Bulk(Some(Bytes::from(s.into())))
}

/// Dispatch a `LOCK` subcommand.
pub fn lock_command(subcommand: &str, args: &[String]) -> Frame {
    match subcommand.to_uppercase().as_str() {
        "ACQUIRE" => lock_acquire(args),
        "RELEASE" => lock_release(args),
        "EXTEND" => lock_extend(args),
        "TRY" => lock_try(args),
        "INFO" => lock_info(args),
        "OWNER" => lock_owner(args),
        "DEADLOCKS" => lock_deadlocks(),
        "EXPIRE" => lock_expire(),
        "STATS" => lock_stats(),
        "HELP" => lock_help(),
        _ => err_frame(&format!("unknown LOCK subcommand '{}'", subcommand)),
    }
}

/// LOCK.ACQUIRE key owner [TTL ms] [TYPE exclusive|read|write] [TIMEOUT ms]
fn lock_acquire(args: &[String]) -> Frame {
    if args.len() < 2 {
        return err_frame("usage: LOCK.ACQUIRE key owner [TTL ms] [TYPE exclusive|read|write]");
    }

    let key = &args[0];
    let owner = &args[1];
    let ttl = parse_ttl_arg(args, 2).unwrap_or(Duration::from_secs(30));
    let lock_type = parse_lock_type_arg(args);

    match get_lock_manager().acquire(key, owner, ttl, lock_type) {
        Ok(grant) => {
            let mut items = Vec::new();
            items.push(bulk("token"));
            items.push(Frame::Integer(grant.token as i64));
            items.push(bulk("expires_in_ms"));
            items.push(Frame::Integer(
                grant
                    .expires_at
                    .duration_since(std::time::Instant::now())
                    .as_millis() as i64,
            ));
            if let Some(pos) = grant.queue_position {
                items.push(bulk("status"));
                items.push(bulk("QUEUED"));
                items.push(bulk("queue_position"));
                items.push(Frame::Integer(pos as i64));
            } else {
                items.push(bulk("status"));
                items.push(bulk("OK"));
            }
            Frame::Array(Some(items))
        }
        Err(e) => err_frame(&e.to_string()),
    }
}

/// LOCK.RELEASE key token
fn lock_release(args: &[String]) -> Frame {
    if args.len() < 2 {
        return err_frame("usage: LOCK.RELEASE key token");
    }

    let key = &args[0];
    let token: u64 = match args[1].parse() {
        Ok(t) => t,
        Err(_) => return err_frame("invalid token: must be a positive integer"),
    };

    match get_lock_manager().release(key, token) {
        Ok(()) => bulk("OK"),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// LOCK.EXTEND key token ttl_ms
fn lock_extend(args: &[String]) -> Frame {
    if args.len() < 3 {
        return err_frame("usage: LOCK.EXTEND key token ttl_ms");
    }

    let key = &args[0];
    let token: u64 = match args[1].parse() {
        Ok(t) => t,
        Err(_) => return err_frame("invalid token"),
    };
    let ttl_ms: u64 = match args[2].parse() {
        Ok(t) => t,
        Err(_) => return err_frame("invalid TTL"),
    };

    match get_lock_manager().extend(key, token, Duration::from_millis(ttl_ms)) {
        Ok(grant) => {
            let mut items = Vec::new();
            items.push(bulk("token"));
            items.push(Frame::Integer(grant.token as i64));
            items.push(bulk("new_expires_in_ms"));
            items.push(Frame::Integer(
                grant
                    .expires_at
                    .duration_since(std::time::Instant::now())
                    .as_millis() as i64,
            ));
            Frame::Array(Some(items))
        }
        Err(e) => err_frame(&e.to_string()),
    }
}

/// LOCK.TRY key owner [TTL ms]
fn lock_try(args: &[String]) -> Frame {
    if args.len() < 2 {
        return err_frame("usage: LOCK.TRY key owner [TTL ms]");
    }

    let key = &args[0];
    let owner = &args[1];
    let ttl = parse_ttl_arg(args, 2).unwrap_or(Duration::from_secs(30));

    match get_lock_manager().try_acquire(key, owner, ttl) {
        Ok(Some(grant)) => {
            let mut items = Vec::new();
            items.push(bulk("token"));
            items.push(Frame::Integer(grant.token as i64));
            Frame::Array(Some(items))
        }
        Ok(None) => Frame::Null,
        Err(e) => err_frame(&e.to_string()),
    }
}

/// LOCK.INFO key
fn lock_info(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("usage: LOCK.INFO key");
    }

    match get_lock_manager().lock_info(&args[0]) {
        Some(info) => {
            let mut items = Vec::new();
            items.push(bulk("key"));
            items.push(bulk(info.key));
            items.push(bulk("owner"));
            items.push(bulk(info.owner));
            items.push(bulk("token"));
            items.push(Frame::Integer(info.token as i64));
            items.push(bulk("held_for_ms"));
            items.push(Frame::Integer(info.held_for_ms as i64));
            items.push(bulk("expires_in_ms"));
            items.push(Frame::Integer(info.expires_in_ms as i64));
            items.push(bulk("lock_type"));
            items.push(bulk(info.lock_type));
            items.push(bulk("queue_depth"));
            items.push(Frame::Integer(info.queue_depth as i64));
            Frame::Array(Some(items))
        }
        None => Frame::Null,
    }
}

/// LOCK.OWNER owner
fn lock_owner(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("usage: LOCK.OWNER owner");
    }

    let locks = get_lock_manager().owner_locks(&args[0]);
    let items: Vec<Frame> = locks
        .into_iter()
        .map(|info| {
            let mut entry = Vec::new();
            entry.push(bulk(info.key));
            entry.push(Frame::Integer(info.token as i64));
            entry.push(bulk(info.lock_type));
            Frame::Array(Some(entry))
        })
        .collect();
    Frame::Array(Some(items))
}

/// LOCK.DEADLOCKS
fn lock_deadlocks() -> Frame {
    let cycles = get_lock_manager().detect_deadlocks();
    if cycles.is_empty() {
        return Frame::Array(Some(vec![bulk("(none)")]));
    }

    let items: Vec<Frame> = cycles
        .iter()
        .map(|c| {
            let mut entry = Vec::new();
            entry.push(bulk("participants"));
            entry.push(bulk(c.participants.join(", ")));
            entry.push(bulk("keys"));
            entry.push(bulk(c.keys.join(", ")));
            Frame::Array(Some(entry))
        })
        .collect();
    Frame::Array(Some(items))
}

/// LOCK.EXPIRE — clean up stale locks.
fn lock_expire() -> Frame {
    let count = get_lock_manager().expire_stale();
    Frame::Integer(count as i64)
}

/// LOCK.STATS
fn lock_stats() -> Frame {
    let stats = get_lock_manager().stats();
    let mut items = Vec::new();

    items.push(bulk("active_locks"));
    items.push(Frame::Integer(stats.active_locks as i64));
    items.push(bulk("queued_waiters"));
    items.push(Frame::Integer(stats.queued_waiters as i64));
    items.push(bulk("total_acquired"));
    items.push(Frame::Integer(stats.total_acquired as i64));
    items.push(bulk("total_released"));
    items.push(Frame::Integer(stats.total_released as i64));
    items.push(bulk("total_expired"));
    items.push(Frame::Integer(stats.total_expired as i64));
    items.push(bulk("deadlocks_detected"));
    items.push(Frame::Integer(stats.deadlocks_detected as i64));
    items.push(bulk("avg_hold_time_ms"));
    items.push(bulk(format!("{:.2}", stats.avg_hold_time_ms)));
    items.push(bulk("contention_rate"));
    items.push(bulk(format!("{:.4}", stats.contention_rate)));

    Frame::Array(Some(items))
}

/// LOCK.HELP
fn lock_help() -> Frame {
    let help = vec![
        "LOCK.ACQUIRE key owner [TTL ms] [TYPE exclusive|read|write] [TIMEOUT ms] - Acquire a lock",
        "LOCK.RELEASE key token - Release a lock by fencing token",
        "LOCK.EXTEND key token ttl_ms - Extend lock TTL",
        "LOCK.TRY key owner [TTL ms] - Non-blocking lock attempt",
        "LOCK.INFO key - Get lock information",
        "LOCK.OWNER owner - List all locks held by owner",
        "LOCK.DEADLOCKS - Detect deadlock cycles",
        "LOCK.EXPIRE - Clean up expired locks",
        "LOCK.STATS - Show lock statistics",
        "LOCK.HELP - Show this help",
    ];
    let items: Vec<Frame> = help.iter().map(|s| bulk(s.to_string())).collect();
    Frame::Array(Some(items))
}

// ── Argument Helpers ─────────────────────────────────────────────────────────

/// Parse a TTL argument from "TTL" keyword followed by millisecond value.
fn parse_ttl_arg(args: &[String], start: usize) -> Option<Duration> {
    let mut i = start;
    while i + 1 < args.len() {
        if args[i].to_uppercase() == "TTL" {
            if let Ok(ms) = args[i + 1].parse::<u64>() {
                return Some(Duration::from_millis(ms));
            }
        }
        i += 1;
    }
    None
}

/// Parse lock type from "TYPE" keyword.
fn parse_lock_type_arg(args: &[String]) -> LockType {
    for i in 0..args.len().saturating_sub(1) {
        if args[i].to_uppercase() == "TYPE" {
            return match args[i + 1].to_uppercase().as_str() {
                "READ" => LockType::ReadWrite {
                    readers: 1,
                    writer: None,
                },
                "WRITE" => LockType::ReadWrite {
                    readers: 0,
                    writer: Some(args.get(1).cloned().unwrap_or_default()),
                },
                _ => LockType::Exclusive,
            };
        }
    }
    LockType::Exclusive
}
