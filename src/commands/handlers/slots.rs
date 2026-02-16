//! Replication slot command handlers
//!
//! Implements PostgreSQL-style logical replication slot commands.

use std::sync::OnceLock;

use bytes::Bytes;

use crate::protocol::Frame;
use crate::replication::slots::{ChangeOperation, SlotConfig, SlotManager, SlotState};

use super::{err_frame, ok_frame, HandlerContext};

/// Global slot manager instance
static SLOT_MANAGER: OnceLock<SlotManager> = OnceLock::new();

/// Get the global slot manager
fn slot_manager() -> &'static SlotManager {
    SLOT_MANAGER.get_or_init(SlotManager::new)
}

/// Handle SLOT.CREATE command
///
/// SLOT.CREATE slot_name [PLUGIN plugin] [PATTERNS pattern1 pattern2 ...] [DATABASE db]
pub fn handle_slot_create(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("SLOT.CREATE requires slot name");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();
    let mut config = SlotConfig::new(&name);

    let mut i = 1;
    while i < args.len() {
        let arg = String::from_utf8_lossy(&args[i]).to_uppercase();
        match arg.as_str() {
            "PLUGIN" => {
                if i + 1 >= args.len() {
                    return err_frame("PLUGIN requires a value");
                }
                i += 1;
                config = config.with_plugin(String::from_utf8_lossy(&args[i]).to_string());
            }
            "PATTERNS" => {
                i += 1;
                let mut patterns = Vec::new();
                while i < args.len() {
                    let val = String::from_utf8_lossy(&args[i]).to_string();
                    if val.starts_with(char::is_uppercase) {
                        break;
                    }
                    patterns.push(val);
                    i += 1;
                }
                if !patterns.is_empty() {
                    config = config.with_patterns(patterns);
                }
                continue;
            }
            "DATABASE" => {
                if i + 1 >= args.len() {
                    return err_frame("DATABASE requires a value");
                }
                i += 1;
                let db_str = String::from_utf8_lossy(&args[i]);
                match db_str.parse::<u8>() {
                    Ok(db) => config = config.with_database(db),
                    Err(_) => return err_frame("invalid database number"),
                }
            }
            _ => {
                return err_frame(&format!("unknown option: {}", arg));
            }
        }
        i += 1;
    }

    match slot_manager().create_slot(config) {
        Ok(id) => Frame::bulk(id),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle SLOT.DROP command
///
/// SLOT.DROP slot_name
pub fn handle_slot_drop(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("SLOT.DROP requires slot name");
    }

    let name = String::from_utf8_lossy(&args[0]);

    match slot_manager().drop_slot(&name) {
        Ok(()) => ok_frame(),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle SLOT.GET command
///
/// SLOT.GET slot_name
pub fn handle_slot_get(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("SLOT.GET requires slot name");
    }

    let name = String::from_utf8_lossy(&args[0]);

    match slot_manager().get_slot(&name) {
        Some(stats) => {
            let fields = vec![
                Frame::bulk("name"),
                Frame::bulk(stats.name),
                Frame::bulk("state"),
                Frame::bulk(format!("{:?}", stats.state)),
                Frame::bulk("current_lsn"),
                Frame::Integer(stats.current_lsn as i64),
                Frame::bulk("confirmed_lsn"),
                Frame::Integer(stats.confirmed_lsn as i64),
                Frame::bulk("lag_bytes"),
                Frame::Integer(stats.lag_bytes as i64),
                Frame::bulk("active_consumers"),
                Frame::Integer(stats.active_consumers as i64),
                Frame::bulk("bytes_sent"),
                Frame::Integer(stats.bytes_sent as i64),
                Frame::bulk("changes_sent"),
                Frame::Integer(stats.changes_sent as i64),
            ];
            Frame::array(fields)
        }
        None => Frame::Null,
    }
}

/// Handle SLOT.LIST command
///
/// SLOT.LIST
pub fn handle_slot_list(_ctx: &HandlerContext<'_>, _args: &[Bytes]) -> Frame {
    let slots = slot_manager().list_slots();

    if slots.is_empty() {
        return Frame::array(vec![]);
    }

    let items: Vec<Frame> = slots
        .into_iter()
        .map(|s| {
            Frame::array(vec![
                Frame::bulk(s.name),
                Frame::bulk(format!("{:?}", s.state)),
                Frame::Integer(s.lag_bytes as i64),
            ])
        })
        .collect();

    Frame::array(items)
}

/// Handle SLOT.START command
///
/// SLOT.START slot_name [FROM lsn]
pub fn handle_slot_start(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("SLOT.START requires slot name");
    }

    let name = String::from_utf8_lossy(&args[0]);
    let mut start_lsn = None;

    if args.len() >= 3 {
        let keyword = String::from_utf8_lossy(&args[1]).to_uppercase();
        if keyword == "FROM" {
            let lsn_str = String::from_utf8_lossy(&args[2]);
            match lsn_str.parse::<u64>() {
                Ok(lsn) => start_lsn = Some(lsn),
                Err(_) => return err_frame("invalid LSN"),
            }
        }
    }

    match slot_manager().start_replication(&name, start_lsn) {
        Ok(_receiver) => {
            // In a real implementation, we'd store the receiver for streaming
            // For now, return success with the current LSN
            let current_lsn = slot_manager().current_lsn();
            Frame::array(vec![
                Frame::bulk("started"),
                Frame::Integer(current_lsn as i64),
            ])
        }
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle SLOT.STOP command
///
/// SLOT.STOP slot_name
pub fn handle_slot_stop(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("SLOT.STOP requires slot name");
    }

    let name = String::from_utf8_lossy(&args[0]);

    match slot_manager().stop_replication(&name) {
        Ok(()) => ok_frame(),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle SLOT.CONFIRM command
///
/// SLOT.CONFIRM slot_name lsn
pub fn handle_slot_confirm(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("SLOT.CONFIRM requires slot name and LSN");
    }

    let name = String::from_utf8_lossy(&args[0]);
    let lsn_str = String::from_utf8_lossy(&args[1]);

    let lsn = match lsn_str.parse::<u64>() {
        Ok(l) => l,
        Err(_) => return err_frame("invalid LSN"),
    };

    match slot_manager().confirm_flush(&name, lsn) {
        Ok(()) => ok_frame(),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle SLOT.STATS command
///
/// SLOT.STATS
pub fn handle_slot_stats(_ctx: &HandlerContext<'_>, _args: &[Bytes]) -> Frame {
    let stats = slot_manager().stats();

    Frame::array(vec![
        Frame::bulk("slot_count"),
        Frame::Integer(stats.slot_count as i64),
        Frame::bulk("active_slots"),
        Frame::Integer(stats.active_slots as i64),
        Frame::bulk("total_consumers"),
        Frame::Integer(stats.total_consumers as i64),
        Frame::bulk("current_lsn"),
        Frame::Integer(stats.current_lsn as i64),
    ])
}

/// Handle SLOT.PUBLISH command (for testing/manual changes)
///
/// SLOT.PUBLISH operation key [value]
pub fn handle_slot_publish(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("SLOT.PUBLISH requires operation and key");
    }

    let op_str = String::from_utf8_lossy(&args[0]).to_uppercase();
    let key = String::from_utf8_lossy(&args[1]).to_string();

    let operation = match op_str.as_str() {
        "INSERT" => ChangeOperation::Insert,
        "UPDATE" => ChangeOperation::Update,
        "DELETE" => ChangeOperation::Delete,
        "EXPIRE" => ChangeOperation::Expire,
        _ => return err_frame("invalid operation (INSERT, UPDATE, DELETE, EXPIRE)"),
    };

    let mut event = crate::replication::slots::ChangeEvent::new(0, operation, key);

    if args.len() >= 3 {
        event = event.with_value(args[2].to_vec());
    }

    slot_manager().publish_change(event);

    Frame::Integer(slot_manager().current_lsn() as i64)
}

/// Get slot state as string
// Reserved for cluster slot management API
#[allow(dead_code)]
fn slot_state_str(state: SlotState) -> &'static str {
    match state {
        SlotState::Inactive => "inactive",
        SlotState::Active => "active",
        SlotState::Paused => "paused",
        SlotState::Dropping => "dropping",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slot_manager_singleton() {
        let m1 = slot_manager();
        let m2 = slot_manager();
        assert!(std::ptr::eq(m1, m2));
    }
}
