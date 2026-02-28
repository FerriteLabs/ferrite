//! CONSENSUS.* command handlers
//!
//! CONSENSUS.CREATE, CONSENSUS.DISSOLVE, CONSENSUS.PROPOSE, CONSENSUS.READ,
//! CONSENSUS.LEADER, CONSENSUS.ELECT, CONSENSUS.MEMBERS, CONSENSUS.MEMBER.ADD,
//! CONSENSUS.MEMBER.REMOVE, CONSENSUS.INFO, CONSENSUS.LIST, CONSENSUS.STATS,
//! CONSENSUS.HELP
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::OnceLock;

use bytes::Bytes;

use crate::protocol::Frame;
use ferrite_core::cluster::consensus_api::{ConsensusConfig, ConsensusManager};

use super::err_frame;

/// Global consensus manager singleton.
static CONSENSUS_MANAGER: OnceLock<ConsensusManager> = OnceLock::new();

fn get_manager() -> &'static ConsensusManager {
    CONSENSUS_MANAGER.get_or_init(|| ConsensusManager::new(ConsensusConfig::default()))
}

/// Dispatch a `CONSENSUS` subcommand.
pub fn consensus_command(subcommand: &str, args: &[String]) -> Frame {
    match subcommand.to_uppercase().as_str() {
        "CREATE" => handle_create(args),
        "DISSOLVE" => handle_dissolve(args),
        "PROPOSE" => handle_propose(args),
        "READ" => handle_read(args),
        "LEADER" => handle_leader(args),
        "ELECT" => handle_elect(args),
        "MEMBERS" => handle_members(args),
        "MEMBER.ADD" => handle_member_add(args),
        "MEMBER.REMOVE" => handle_member_remove(args),
        "INFO" => handle_info(args),
        "LIST" => handle_list(),
        "STATS" => handle_stats(),
        "HELP" | "" => handle_help(),
        other => err_frame(&format!("unknown CONSENSUS subcommand '{}'", other)),
    }
}

/// CONSENSUS.CREATE group [member1 member2 ...]
fn handle_create(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("CONSENSUS.CREATE requires: group [member1 member2 ...]");
    }
    let group_name = &args[0];
    let members: Vec<String> = args[1..].to_vec();

    match get_manager().create_group(group_name, members) {
        Ok(group) => {
            let mut map = HashMap::new();
            map.insert(
                Bytes::from_static(b"name"),
                Frame::Bulk(Some(Bytes::from(group.name))),
            );
            map.insert(
                Bytes::from_static(b"state"),
                Frame::Bulk(Some(Bytes::from(group.state.to_string()))),
            );
            map.insert(
                Bytes::from_static(b"leader"),
                Frame::Bulk(Some(Bytes::from(
                    group.leader.unwrap_or_else(|| "none".to_string()),
                ))),
            );
            map.insert(
                Bytes::from_static(b"members"),
                Frame::Integer(group.members.len() as i64),
            );
            map.insert(
                Bytes::from_static(b"epoch"),
                Frame::Integer(group.epoch as i64),
            );
            Frame::Map(map)
        }
        Err(e) => err_frame(&e.to_string()),
    }
}

/// CONSENSUS.DISSOLVE group
fn handle_dissolve(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("CONSENSUS.DISSOLVE requires: group");
    }
    match get_manager().dissolve_group(&args[0]) {
        Ok(()) => Frame::Simple(Bytes::from("OK")),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// CONSENSUS.PROPOSE group key value
fn handle_propose(args: &[String]) -> Frame {
    if args.len() < 3 {
        return err_frame("CONSENSUS.PROPOSE requires: group key value");
    }
    match get_manager().propose(&args[0], &args[1], args[2].as_bytes()) {
        Ok(epoch) => Frame::Integer(epoch as i64),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// CONSENSUS.READ group key
fn handle_read(args: &[String]) -> Frame {
    if args.len() < 2 {
        return err_frame("CONSENSUS.READ requires: group key");
    }
    match get_manager().read(&args[0], &args[1]) {
        Ok(Some(entry)) => {
            let mut map = HashMap::new();
            map.insert(
                Bytes::from_static(b"key"),
                Frame::Bulk(Some(Bytes::from(entry.key))),
            );
            map.insert(
                Bytes::from_static(b"value"),
                Frame::Bulk(Some(Bytes::from(entry.value))),
            );
            map.insert(
                Bytes::from_static(b"epoch"),
                Frame::Integer(entry.epoch as i64),
            );
            map.insert(
                Bytes::from_static(b"proposed_by"),
                Frame::Bulk(Some(Bytes::from(entry.proposed_by))),
            );
            map.insert(
                Bytes::from_static(b"timestamp"),
                Frame::Integer(entry.timestamp as i64),
            );
            Frame::Map(map)
        }
        Ok(None) => Frame::Null,
        Err(e) => err_frame(&e.to_string()),
    }
}

/// CONSENSUS.LEADER group
fn handle_leader(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("CONSENSUS.LEADER requires: group");
    }
    match get_manager().leader(&args[0]) {
        Ok(Some(leader)) => Frame::Bulk(Some(Bytes::from(leader))),
        Ok(None) => Frame::Null,
        Err(e) => err_frame(&e.to_string()),
    }
}

/// CONSENSUS.ELECT group
fn handle_elect(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("CONSENSUS.ELECT requires: group");
    }
    match get_manager().elect(&args[0]) {
        Ok(leader) => Frame::Bulk(Some(Bytes::from(leader))),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// CONSENSUS.MEMBERS group
fn handle_members(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("CONSENSUS.MEMBERS requires: group");
    }
    match get_manager().members(&args[0]) {
        Ok(members) => {
            let frames: Vec<Frame> = members
                .into_iter()
                .map(|m| Frame::Bulk(Some(Bytes::from(m))))
                .collect();
            Frame::Array(Some(frames))
        }
        Err(e) => err_frame(&e.to_string()),
    }
}

/// CONSENSUS.MEMBER.ADD group member
fn handle_member_add(args: &[String]) -> Frame {
    if args.len() < 2 {
        return err_frame("CONSENSUS.MEMBER.ADD requires: group member");
    }
    match get_manager().add_member(&args[0], &args[1]) {
        Ok(()) => Frame::Simple(Bytes::from("OK")),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// CONSENSUS.MEMBER.REMOVE group member
fn handle_member_remove(args: &[String]) -> Frame {
    if args.len() < 2 {
        return err_frame("CONSENSUS.MEMBER.REMOVE requires: group member");
    }
    match get_manager().remove_member(&args[0], &args[1]) {
        Ok(()) => Frame::Simple(Bytes::from("OK")),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// CONSENSUS.INFO group
fn handle_info(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("CONSENSUS.INFO requires: group");
    }
    match get_manager().group_info(&args[0]) {
        Some(info) => {
            let mut map = HashMap::new();
            map.insert(
                Bytes::from_static(b"name"),
                Frame::Bulk(Some(Bytes::from(info.name))),
            );
            map.insert(
                Bytes::from_static(b"state"),
                Frame::Bulk(Some(Bytes::from(info.state.to_string()))),
            );
            map.insert(
                Bytes::from_static(b"leader"),
                Frame::Bulk(Some(Bytes::from(
                    info.leader.unwrap_or_else(|| "none".to_string()),
                ))),
            );
            map.insert(
                Bytes::from_static(b"members_count"),
                Frame::Integer(info.members_count as i64),
            );
            map.insert(
                Bytes::from_static(b"epoch"),
                Frame::Integer(info.epoch as i64),
            );
            map.insert(
                Bytes::from_static(b"entries"),
                Frame::Integer(info.entries as i64),
            );
            map.insert(
                Bytes::from_static(b"created_at"),
                Frame::Integer(info.created_at as i64),
            );
            Frame::Map(map)
        }
        None => Frame::Null,
    }
}

/// CONSENSUS.LIST
fn handle_list() -> Frame {
    let groups = get_manager().list_groups();
    let frames: Vec<Frame> = groups
        .into_iter()
        .map(|info| {
            let mut map = HashMap::new();
            map.insert(
                Bytes::from_static(b"name"),
                Frame::Bulk(Some(Bytes::from(info.name))),
            );
            map.insert(
                Bytes::from_static(b"state"),
                Frame::Bulk(Some(Bytes::from(info.state.to_string()))),
            );
            map.insert(
                Bytes::from_static(b"members_count"),
                Frame::Integer(info.members_count as i64),
            );
            map.insert(
                Bytes::from_static(b"epoch"),
                Frame::Integer(info.epoch as i64),
            );
            Frame::Map(map)
        })
        .collect();
    Frame::Array(Some(frames))
}

/// CONSENSUS.STATS
fn handle_stats() -> Frame {
    let stats = get_manager().stats();
    let mut map = HashMap::new();
    map.insert(
        Bytes::from_static(b"groups"),
        Frame::Integer(stats.groups as i64),
    );
    map.insert(
        Bytes::from_static(b"total_proposals"),
        Frame::Integer(stats.total_proposals as i64),
    );
    map.insert(
        Bytes::from_static(b"total_reads"),
        Frame::Integer(stats.total_reads as i64),
    );
    map.insert(
        Bytes::from_static(b"elections"),
        Frame::Integer(stats.elections as i64),
    );
    map.insert(
        Bytes::from_static(b"avg_latency_us"),
        Frame::Double(stats.avg_latency_us),
    );
    Frame::Map(map)
}

/// CONSENSUS.HELP
fn handle_help() -> Frame {
    let lines = vec![
        "CONSENSUS.CREATE group [member ...]   - Create a consensus group",
        "CONSENSUS.DISSOLVE group              - Dissolve a consensus group",
        "CONSENSUS.PROPOSE group key value     - Propose a value",
        "CONSENSUS.READ group key              - Linearizable read",
        "CONSENSUS.LEADER group                - Get current leader",
        "CONSENSUS.ELECT group                 - Trigger election",
        "CONSENSUS.MEMBERS group               - List group members",
        "CONSENSUS.MEMBER.ADD group member     - Add a member",
        "CONSENSUS.MEMBER.REMOVE group member  - Remove a member",
        "CONSENSUS.INFO group                  - Group information",
        "CONSENSUS.LIST                        - List all groups",
        "CONSENSUS.STATS                       - Service statistics",
        "CONSENSUS.HELP                        - Show this help",
    ];
    let frames: Vec<Frame> = lines
        .into_iter()
        .map(|l| Frame::Bulk(Some(Bytes::from(l))))
        .collect();
    Frame::Array(Some(frames))
}
