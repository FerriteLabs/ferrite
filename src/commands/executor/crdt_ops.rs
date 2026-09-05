//! CRDT command implementations on CommandExecutor (G-Counter, PN-Counter, LWW-Register, OR-Set).

use bytes::Bytes;

use crate::protocol::Frame;

use super::CommandExecutor;

impl CommandExecutor {
    // CRDT commands

    pub(super) async fn crdt_gcounter(
        &self,
        key: &Bytes,
        subcommand: &str,
        args: &[String],
    ) -> Frame {
        use ferrite_plugins::crdt::{GCounter, SiteId};

        let key_str = String::from_utf8_lossy(key).to_string();
        let site_id = SiteId::new(1); // Default site ID
        let mut counter = GCounter::new();

        match subcommand {
            "INCR" => {
                let delta = args
                    .first()
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(1);
                counter.increment(site_id, delta);
                Frame::Integer(counter.value() as i64)
            }
            "GET" => Frame::Integer(counter.value() as i64),
            "MERGE" => {
                // In production, would merge from serialized state
                Frame::simple("OK")
            }
            "INFO" => Frame::array(vec![
                Frame::bulk("key"),
                Frame::bulk(Bytes::from(key_str.clone())),
                Frame::bulk("type"),
                Frame::bulk("gcounter"),
                Frame::bulk("value"),
                Frame::Integer(counter.value() as i64),
            ]),
            _ => Frame::error(format!(
                "ERR Unknown CRDT.GCOUNTER subcommand: {}",
                subcommand
            )),
        }
    }

    pub(super) async fn crdt_pncounter(
        &self,
        key: &Bytes,
        subcommand: &str,
        args: &[String],
    ) -> Frame {
        use ferrite_plugins::crdt::{PNCounter, SiteId};

        let key_str = String::from_utf8_lossy(key).to_string();
        let site_id = SiteId::new(1);
        let mut counter = PNCounter::new();

        match subcommand {
            "INCR" => {
                let delta = args
                    .first()
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(1);
                counter.increment(site_id, delta);
                Frame::Integer(counter.value())
            }
            "DECR" => {
                let delta = args
                    .first()
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(1);
                counter.decrement(site_id, delta);
                Frame::Integer(counter.value())
            }
            "GET" => Frame::Integer(counter.value()),
            "MERGE" => Frame::simple("OK"),
            "INFO" => Frame::array(vec![
                Frame::bulk("key"),
                Frame::bulk(Bytes::from(key_str.clone())),
                Frame::bulk("type"),
                Frame::bulk("pncounter"),
                Frame::bulk("value"),
                Frame::Integer(counter.value()),
            ]),
            _ => Frame::error(format!(
                "ERR Unknown CRDT.PNCOUNTER subcommand: {}",
                subcommand
            )),
        }
    }

    pub(super) async fn crdt_lwwreg(
        &self,
        key: &Bytes,
        subcommand: &str,
        args: &[String],
    ) -> Frame {
        use ferrite_plugins::crdt::{HybridTimestamp, LwwRegister, SiteId};

        let key_str = String::from_utf8_lossy(key).to_string();
        let mut register: LwwRegister<String> = LwwRegister::new();
        let site_id = SiteId::new(1);
        let timestamp = HybridTimestamp::now(site_id);

        match subcommand {
            "SET" => {
                if let Some(value) = args.first() {
                    register.set(value.clone(), timestamp);
                    Frame::simple("OK")
                } else {
                    Frame::error("ERR CRDT.LWWREG SET requires a value")
                }
            }
            "GET" => match register.value() {
                Some(value) => Frame::bulk(Bytes::from(value.clone())),
                None => Frame::null(),
            },
            "MERGE" => Frame::simple("OK"),
            "INFO" => Frame::array(vec![
                Frame::bulk("key"),
                Frame::bulk(Bytes::from(key_str)),
                Frame::bulk("type"),
                Frame::bulk("lwwregister"),
            ]),
            _ => Frame::error(format!(
                "ERR Unknown CRDT.LWWREG subcommand: {}",
                subcommand
            )),
        }
    }

    pub(super) async fn crdt_orset(&self, key: &Bytes, subcommand: &str, args: &[String]) -> Frame {
        use ferrite_plugins::crdt::{OrSet, SiteId};
        use std::sync::atomic::{AtomicU64, Ordering};

        let key_str = String::from_utf8_lossy(key).to_string();
        let _site_id = SiteId::new(1);
        let set: OrSet<String> = OrSet::new();
        static COUNTER: AtomicU64 = AtomicU64::new(1);

        match subcommand {
            "ADD" => {
                if !args.is_empty() {
                    let _counter = COUNTER.fetch_add(1, Ordering::SeqCst);
                    // In production, would persist to storage
                    Frame::Integer(1)
                } else {
                    Frame::error("ERR CRDT.ORSET ADD requires an element")
                }
            }
            "REMOVE" => {
                if !args.is_empty() {
                    // In production, would remove from storage
                    Frame::Integer(1)
                } else {
                    Frame::error("ERR CRDT.ORSET REMOVE requires an element")
                }
            }
            "CONTAINS" => {
                if let Some(element) = args.first() {
                    Frame::Integer(if set.contains(element) { 1 } else { 0 })
                } else {
                    Frame::error("ERR CRDT.ORSET CONTAINS requires an element")
                }
            }
            "MEMBERS" => {
                let members: Vec<Frame> = set
                    .members()
                    .map(|s| Frame::bulk(Bytes::from(s.clone())))
                    .collect();
                Frame::array(members)
            }
            "CARD" => Frame::Integer(set.len() as i64),
            "MERGE" => Frame::simple("OK"),
            "INFO" => Frame::array(vec![
                Frame::bulk("key"),
                Frame::bulk(Bytes::from(key_str)),
                Frame::bulk("type"),
                Frame::bulk("orset"),
                Frame::bulk("cardinality"),
                Frame::Integer(set.len() as i64),
            ]),
            _ => Frame::error(format!("ERR Unknown CRDT.ORSET subcommand: {}", subcommand)),
        }
    }

    pub(super) async fn crdt_info(&self, key: Option<&Bytes>) -> Frame {
        use ferrite_plugins::crdt::CrdtConfig;

        let config = CrdtConfig::default();

        let mut info = vec![
            Frame::bulk("crdt_enabled"),
            Frame::bulk(if config.enabled { "yes" } else { "no" }),
            Frame::bulk("site_id"),
            Frame::bulk(Bytes::from(config.site_id.clone())),
            Frame::bulk("replication_mode"),
            Frame::bulk(Bytes::from(format!("{:?}", config.mode))),
            Frame::bulk("sync_interval_ms"),
            Frame::Integer(config.sync_interval_ms as i64),
        ];

        if let Some(k) = key {
            info.push(Frame::bulk("key"));
            info.push(Frame::bulk(String::from_utf8_lossy(k).to_string()));
        }

        Frame::array(info)
    }
}
