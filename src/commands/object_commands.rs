#![allow(dead_code)]
//! Extended OBJECT command implementations
//!
//! Provides standalone OBJECT FREQ, IDLETIME, and HELP handlers.

use std::sync::Arc;

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::Store;

/// OBJECT FREQ key — Return the LFU access frequency counter for the key.
pub fn object_freq(store: &Arc<Store>, db: u8, key: &Bytes) -> Frame {
    match store.object_freq(db, key) {
        Some(count) => Frame::Integer(count as i64),
        None => Frame::Null,
    }
}

/// OBJECT IDLETIME key — Return seconds since the key was last accessed.
pub fn object_idletime(store: &Arc<Store>, db: u8, key: &Bytes) -> Frame {
    match store.object_idletime(db, key) {
        Some(idle) => Frame::Integer(idle),
        None => Frame::Null,
    }
}

/// OBJECT HELP — Return help text for the OBJECT command.
pub fn object_help() -> Frame {
    Frame::array(vec![
        Frame::bulk("OBJECT <subcommand> [<arg> [value] [opt] ...]"),
        Frame::bulk("ENCODING <key>"),
        Frame::bulk("    Return the encoding of the object stored at <key>."),
        Frame::bulk("FREQ <key>"),
        Frame::bulk("    Return the access frequency index of the key. The returned integer is proportional to the logarithm of the recent access frequency."),
        Frame::bulk("HELP"),
        Frame::bulk("    Return subcommand help summary."),
        Frame::bulk("IDLETIME <key>"),
        Frame::bulk("    Return the idle time of the key, which is the approximated number of seconds elapsed since the last access to the key."),
        Frame::bulk("REFCOUNT <key>"),
        Frame::bulk("    Return the reference count of the object stored at <key>."),
    ])
}
