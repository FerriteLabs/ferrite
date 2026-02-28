//! Cluster protocol MOVED/ASK redirect support
//!
//! When cluster mode is enabled, commands targeting keys in slots not owned
//! by this node are rejected with a MOVED or ASK redirect response,
//! directing the client to the correct node.

use crate::cluster::{ClusterStateManager, RedirectResult};
use crate::commands::Command;
use crate::protocol::Frame;
use bytes::Bytes;
use std::sync::Arc;

/// Check whether a command should be redirected to another node.
/// Returns `Some(Frame)` with a MOVED/ASK error if the command targets
/// a slot not owned by this node, or `None` if it should be handled locally.
pub fn check_slot_redirect(
    cluster_state: &Arc<ClusterStateManager>,
    command: &Command,
) -> Option<Frame> {
    let key = extract_first_key(command)?;
    let result = cluster_state.check_redirect(&key);
    redirect_frame(result)
}

/// Convert a `RedirectResult` into a RESP error Frame for MOVED/ASK.
fn redirect_frame(result: RedirectResult) -> Option<Frame> {
    match result {
        RedirectResult::Ok => None,
        RedirectResult::Moved { slot, addr } => Some(Frame::Error(Bytes::from(format!(
            "MOVED {} {}",
            slot, addr
        )))),
        RedirectResult::Ask { slot, addr } => {
            Some(Frame::Error(Bytes::from(format!("ASK {} {}", slot, addr))))
        }
    }
}

/// Extract the first key argument from a Command for slot routing.
fn extract_first_key(command: &Command) -> Option<Bytes> {
    match command {
        // String commands
        Command::Get { key }
        | Command::StrLen { key }
        | Command::GetDel { key }
        | Command::Incr { key }
        | Command::Decr { key } => Some(key.clone()),
        Command::Set { key, .. }
        | Command::SetNx { key, .. }
        | Command::SetEx { key, .. }
        | Command::PSetEx { key, .. }
        | Command::GetSet { key, .. }
        | Command::GetEx { key, .. }
        | Command::SetRange { key, .. }
        | Command::GetRange { key, .. }
        | Command::IncrBy { key, .. }
        | Command::DecrBy { key, .. }
        | Command::IncrByFloat { key, .. }
        | Command::Append { key, .. } => Some(key.clone()),

        // List commands
        Command::LPush { key, .. }
        | Command::RPush { key, .. }
        | Command::LPop { key, .. }
        | Command::RPop { key, .. }
        | Command::LRange { key, .. }
        | Command::LLen { key }
        | Command::LIndex { key, .. }
        | Command::LSet { key, .. }
        | Command::LRem { key, .. }
        | Command::LInsert { key, .. }
        | Command::LPos { key, .. }
        | Command::LTrim { key, .. } => Some(key.clone()),

        // Hash commands
        Command::HSet { key, .. }
        | Command::HGet { key, .. }
        | Command::HDel { key, .. }
        | Command::HExists { key, .. }
        | Command::HGetAll { key }
        | Command::HKeys { key }
        | Command::HVals { key }
        | Command::HLen { key }
        | Command::HMSet { key, .. }
        | Command::HMGet { key, .. }
        | Command::HIncrBy { key, .. }
        | Command::HIncrByFloat { key, .. }
        | Command::HSetNx { key, .. }
        | Command::HRandField { key, .. }
        | Command::HScan { key, .. } => Some(key.clone()),

        // Set commands
        Command::SAdd { key, .. }
        | Command::SRem { key, .. }
        | Command::SMembers { key }
        | Command::SIsMember { key, .. }
        | Command::SCard { key }
        | Command::SRandMember { key, .. }
        | Command::SPop { key, .. }
        | Command::SMIsMember { key, .. }
        | Command::SScan { key, .. } => Some(key.clone()),

        // Sorted set commands
        Command::ZAdd { key, .. }
        | Command::ZRem { key, .. }
        | Command::ZScore { key, .. }
        | Command::ZCard { key }
        | Command::ZCount { key, .. }
        | Command::ZRank { key, .. }
        | Command::ZRevRank { key, .. }
        | Command::ZRange { key, .. }
        | Command::ZRangeByScore { key, .. }
        | Command::ZRevRangeByScore { key, .. }
        | Command::ZIncrBy { key, .. }
        | Command::ZLexCount { key, .. }
        | Command::ZRangeByLex { key, .. }
        | Command::ZScan { key, .. }
        | Command::ZMScore { key, .. }
        | Command::ZPopMin { key, .. }
        | Command::ZPopMax { key, .. }
        | Command::ZRandMember { key, .. }
        | Command::ZRevRange { key, .. } => Some(key.clone()),

        // Key commands
        Command::Del { keys }
        | Command::Exists { keys }
        | Command::Unlink { keys }
        | Command::Touch { keys } => keys.first().cloned(),
        Command::Type { key }
        | Command::Rename { key, .. }
        | Command::RenameNx { key, .. }
        | Command::Sort { key, .. }
        | Command::Dump { key }
        | Command::Restore { key, .. }
        | Command::Expire { key, .. }
        | Command::PExpire { key, .. }
        | Command::ExpireAt { key, .. }
        | Command::PExpireAt { key, .. }
        | Command::Persist { key }
        | Command::Ttl { key }
        | Command::PTtl { key }
        | Command::ExpireTime { key }
        | Command::PExpireTime { key } => Some(key.clone()),
        Command::Object { key, .. } => key.clone(),
        Command::Copy { source, .. } => Some(source.clone()),

        // Multi-key commands — use first key
        Command::MGet { keys } => keys.first().cloned(),
        Command::MSet { pairs } | Command::MSetNx { pairs } => {
            pairs.first().map(|(k, _)| k.clone())
        }
        Command::Lcs { key1, .. } => Some(key1.clone()),

        // Stream commands
        Command::XAdd { key, .. }
        | Command::XLen { key }
        | Command::XRange { key, .. }
        | Command::XRevRange { key, .. }
        | Command::XDel { key, .. }
        | Command::XTrim { key, .. }
        | Command::XInfo { key, .. }
        | Command::XGroupCreate { key, .. }
        | Command::XGroupDestroy { key, .. }
        | Command::XGroupSetId { key, .. }
        | Command::XGroupDelConsumer { key, .. }
        | Command::XAck { key, .. }
        | Command::XClaim { key, .. }
        | Command::XAutoClaim { key, .. }
        | Command::XPending { key, .. } => Some(key.clone()),

        // Bitmap commands
        Command::GetBit { key, .. }
        | Command::SetBit { key, .. }
        | Command::BitCount { key, .. }
        | Command::BitPos { key, .. }
        | Command::BitField { key, .. }
        | Command::BitFieldRo { key, .. } => Some(key.clone()),
        Command::BitOp { destkey, .. } => Some(destkey.clone()),

        // Geo commands
        Command::GeoAdd { key, .. }
        | Command::GeoPos { key, .. }
        | Command::GeoDist { key, .. }
        | Command::GeoHash { key, .. }
        | Command::GeoRadius { key, .. }
        | Command::GeoRadiusByMember { key, .. }
        | Command::GeoSearch { key, .. } => Some(key.clone()),

        // HyperLogLog
        Command::PfAdd { key, .. } => Some(key.clone()),
        Command::PfCount { keys } => keys.first().cloned(),
        Command::PfMerge { destkey, .. } => Some(destkey.clone()),

        // Administrative/no-key commands return None
        _ => None,
    }
}
