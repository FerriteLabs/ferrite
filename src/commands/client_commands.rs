#![allow(dead_code)]
//! Extended CLIENT command implementations
//!
//! Provides CLIENT INFO, CLIENT NO-EVICT, and CLIENT NO-TOUCH.

use crate::protocol::Frame;

/// CLIENT INFO — Return information about the current connection.
/// Format: "id=N addr=ip:port laddr=ip:port fd=N name= db=N ..."
pub fn client_info(id: u64, addr: &str, local_addr: &str, name: &str, db: u8) -> Frame {
    let info = format!(
        "id={} addr={} laddr={} fd=0 name={} db={} sub=0 psub=0 multi=-1 \
         qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 obl=0 oll=0 omem=0 \
         tot-mem=0 events=r cmd=client|info user=default lib-name= lib-ver= \
         redir=-1",
        id, addr, local_addr, name, db
    );
    Frame::bulk(info)
}

/// CLIENT NO-EVICT ON|OFF — Exclude/include current client from eviction.
pub fn client_no_evict(on: bool) -> Frame {
    let _ = on; // Acknowledged but no-op in current implementation
    Frame::simple("OK")
}

/// CLIENT NO-TOUCH ON|OFF — Prevent the current client from affecting LRU/LFU of keys.
pub fn client_no_touch(on: bool) -> Frame {
    let _ = on; // Acknowledged but no-op in current implementation
    Frame::simple("OK")
}
