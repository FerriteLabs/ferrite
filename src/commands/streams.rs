//! Redis Streams commands
//!
//! This module implements Redis Streams commands including consumer groups.
//! Commands: XADD, XREAD, XRANGE, XLEN, XDEL, XTRIM, XINFO,
//! XGROUP, XACK, XCLAIM, XAUTOCLAIM, XPENDING, XREADGROUP.

use std::sync::Arc;

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::{
    Store, Stream, StreamConsumerGroup, StreamEntryId, StreamPendingEntry, Value,
};

/// Execute XADD command
pub fn xadd(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    id: Option<StreamEntryId>,
    fields: Vec<(Bytes, Bytes)>,
) -> Frame {
    // Get or create stream
    let mut stream = match store.get(db, key) {
        Some(Value::Stream(s)) => s,
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => Stream::new(),
    };

    match stream.add(id, fields) {
        Some(new_id) => {
            let id_str = new_id.to_string();
            store.set(db, key.clone(), Value::Stream(stream));
            Frame::bulk(Bytes::from(id_str))
        }
        None => Frame::error(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item",
        ),
    }
}

/// Execute XLEN command
pub fn xlen(store: &Arc<Store>, db: u8, key: &Bytes) -> Frame {
    match store.get(db, key) {
        Some(Value::Stream(s)) => Frame::Integer(s.length as i64),
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::Integer(0),
    }
}

/// Execute XRANGE command
pub fn xrange(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    start: &str,
    end: &str,
    count: Option<usize>,
) -> Frame {
    match store.get(db, key) {
        Some(Value::Stream(s)) => {
            let start_id = if start == "-" {
                StreamEntryId::new(0, 0)
            } else {
                match StreamEntryId::parse(start) {
                    Some(id) => id,
                    None => {
                        return Frame::error(
                            "ERR Invalid stream ID specified as stream command argument",
                        )
                    }
                }
            };

            let end_id = if end == "+" {
                StreamEntryId::new(u64::MAX, u64::MAX)
            } else {
                match StreamEntryId::parse(end) {
                    Some(id) => id,
                    None => {
                        return Frame::error(
                            "ERR Invalid stream ID specified as stream command argument",
                        )
                    }
                }
            };

            let entries = s.range(&start_id, &end_id, count);
            let frames: Vec<Frame> = entries
                .into_iter()
                .map(|entry| {
                    let id_frame = Frame::bulk(Bytes::from(entry.id.to_string()));
                    let fields_frames: Vec<Frame> = entry
                        .fields
                        .into_iter()
                        .flat_map(|(k, v)| vec![Frame::bulk(k), Frame::bulk(v)])
                        .collect();
                    Frame::array(vec![id_frame, Frame::array(fields_frames)])
                })
                .collect();
            Frame::array(frames)
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::array(vec![]),
    }
}

/// Execute XREVRANGE command
pub fn xrevrange(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    end: &str,
    start: &str,
    count: Option<usize>,
) -> Frame {
    match store.get(db, key) {
        Some(Value::Stream(s)) => {
            let start_id = if start == "-" {
                StreamEntryId::new(0, 0)
            } else {
                match StreamEntryId::parse(start) {
                    Some(id) => id,
                    None => {
                        return Frame::error(
                            "ERR Invalid stream ID specified as stream command argument",
                        )
                    }
                }
            };

            let end_id = if end == "+" {
                StreamEntryId::new(u64::MAX, u64::MAX)
            } else {
                match StreamEntryId::parse(end) {
                    Some(id) => id,
                    None => {
                        return Frame::error(
                            "ERR Invalid stream ID specified as stream command argument",
                        )
                    }
                }
            };

            let mut entries = s.range(&start_id, &end_id, None);
            entries.reverse();
            if let Some(c) = count {
                entries.truncate(c);
            }

            let frames: Vec<Frame> = entries
                .into_iter()
                .map(|entry| {
                    let id_frame = Frame::bulk(Bytes::from(entry.id.to_string()));
                    let fields_frames: Vec<Frame> = entry
                        .fields
                        .into_iter()
                        .flat_map(|(k, v)| vec![Frame::bulk(k), Frame::bulk(v)])
                        .collect();
                    Frame::array(vec![id_frame, Frame::array(fields_frames)])
                })
                .collect();
            Frame::array(frames)
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::array(vec![]),
    }
}

/// Execute XDEL command
pub fn xdel(store: &Arc<Store>, db: u8, key: &Bytes, ids: &[StreamEntryId]) -> Frame {
    match store.get(db, key) {
        Some(Value::Stream(mut s)) => {
            let count = s.delete(ids);
            if s.length == 0 {
                store.del(db, &[key.clone()]);
            } else {
                store.set(db, key.clone(), Value::Stream(s));
            }
            Frame::Integer(count as i64)
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::Integer(0),
    }
}

/// Execute XTRIM command (MAXLEN strategy)
pub fn xtrim(store: &Arc<Store>, db: u8, key: &Bytes, maxlen: usize) -> Frame {
    match store.get(db, key) {
        Some(Value::Stream(mut s)) => {
            let removed = s.trim(maxlen);
            store.set(db, key.clone(), Value::Stream(s));
            Frame::Integer(removed as i64)
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::Integer(0),
    }
}

/// Execute XTRIM command (MINID strategy)
pub fn xtrim_by_minid(store: &Arc<Store>, db: u8, key: &Bytes, minid_str: &str) -> Frame {
    let minid = match StreamEntryId::parse(minid_str) {
        Some(id) => id,
        None => {
            return Frame::error("ERR Invalid stream ID specified as stream command argument")
        }
    };

    match store.get(db, key) {
        Some(Value::Stream(mut s)) => {
            let removed = s.trim_by_minid(&minid);
            store.set(db, key.clone(), Value::Stream(s));
            Frame::Integer(removed as i64)
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::Integer(0),
    }
}

/// Execute XREAD command (simplified, non-blocking)
pub fn xread(
    store: &Arc<Store>,
    db: u8,
    streams: &[(Bytes, String)],
    count: Option<usize>,
) -> Frame {
    let mut results = Vec::new();

    for (key, start_id_str) in streams {
        let start_id = if start_id_str == "0" || start_id_str == "0-0" {
            StreamEntryId::new(0, 0)
        } else {
            match StreamEntryId::parse(start_id_str) {
                Some(id) => StreamEntryId::new(id.ms, id.seq + 1), // Exclusive start
                None => continue,
            }
        };

        match store.get(db, key) {
            Some(Value::Stream(s)) => {
                let end_id = StreamEntryId::new(u64::MAX, u64::MAX);
                let entries = s.range(&start_id, &end_id, count);
                if !entries.is_empty() {
                    let entry_frames: Vec<Frame> = entries
                        .into_iter()
                        .map(|entry| {
                            let id_frame = Frame::bulk(Bytes::from(entry.id.to_string()));
                            let fields_frames: Vec<Frame> = entry
                                .fields
                                .into_iter()
                                .flat_map(|(k, v)| vec![Frame::bulk(k), Frame::bulk(v)])
                                .collect();
                            Frame::array(vec![id_frame, Frame::array(fields_frames)])
                        })
                        .collect();
                    results.push(Frame::array(vec![
                        Frame::bulk(key.clone()),
                        Frame::array(entry_frames),
                    ]));
                }
            }
            _ => continue,
        }
    }

    if results.is_empty() {
        Frame::null()
    } else {
        Frame::array(results)
    }
}

/// Execute XINFO STREAM command
pub fn xinfo_stream(store: &Arc<Store>, db: u8, key: &Bytes) -> Frame {
    match store.get(db, key) {
        Some(Value::Stream(s)) => {
            let mut info = vec![
                Frame::bulk("length"),
                Frame::Integer(s.length as i64),
                Frame::bulk("radix-tree-keys"),
                Frame::Integer(s.entries.len() as i64),
                Frame::bulk("radix-tree-nodes"),
                Frame::Integer(s.entries.len() as i64),
                Frame::bulk("last-generated-id"),
                Frame::bulk(Bytes::from(s.last_id.to_string())),
                Frame::bulk("groups"),
                Frame::Integer(s.consumer_groups.len() as i64),
                Frame::bulk("entries-added"),
                Frame::Integer(s.length as i64),
                Frame::bulk("max-deleted-entry-id"),
                Frame::bulk(Bytes::from("0-0")),
                Frame::bulk("recorded-first-entry-id"),
                Frame::bulk(Bytes::from(
                    s.entries
                        .keys()
                        .next()
                        .map(|id| id.to_string())
                        .unwrap_or_else(|| "0-0".to_string()),
                )),
            ];

            // First entry
            if let Some((id, fields)) = s.entries.iter().next() {
                info.push(Frame::bulk("first-entry"));
                let fields_frames: Vec<Frame> = fields
                    .iter()
                    .flat_map(|(k, v)| vec![Frame::bulk(k.clone()), Frame::bulk(v.clone())])
                    .collect();
                info.push(Frame::array(vec![
                    Frame::bulk(Bytes::from(id.to_string())),
                    Frame::array(fields_frames),
                ]));
            }

            // Last entry
            if let Some((id, fields)) = s.entries.iter().last() {
                info.push(Frame::bulk("last-entry"));
                let fields_frames: Vec<Frame> = fields
                    .iter()
                    .flat_map(|(k, v)| vec![Frame::bulk(k.clone()), Frame::bulk(v.clone())])
                    .collect();
                info.push(Frame::array(vec![
                    Frame::bulk(Bytes::from(id.to_string())),
                    Frame::array(fields_frames),
                ]));
            }

            Frame::array(info)
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::error("ERR no such key"),
    }
}

/// XGROUP CREATE subcommand
pub fn xgroup_create(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    group_name: &Bytes,
    id: &str,
    mkstream: bool,
) -> Frame {
    // Get or create stream
    let mut stream = match store.get(db, key) {
        Some(Value::Stream(s)) => s,
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => {
            if mkstream {
                Stream::new()
            } else {
                return Frame::error(
                    "ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.",
                );
            }
        }
    };

    // Parse the start ID
    let start_id = if id == "$" {
        // Start from the last entry
        stream.last_id.clone()
    } else if id == "0" || id == "0-0" {
        StreamEntryId::new(0, 0)
    } else {
        match StreamEntryId::parse(id) {
            Some(parsed_id) => parsed_id,
            None => {
                return Frame::error("ERR Invalid stream ID specified as stream command argument")
            }
        }
    };

    if stream.create_group(group_name.clone(), start_id) {
        store.set(db, key.clone(), Value::Stream(stream));
        Frame::simple("OK")
    } else {
        Frame::error("BUSYGROUP Consumer Group name already exists")
    }
}

/// XGROUP DESTROY subcommand
pub fn xgroup_destroy(store: &Arc<Store>, db: u8, key: &Bytes, group_name: &Bytes) -> Frame {
    match store.get(db, key) {
        Some(Value::Stream(mut s)) => {
            if s.delete_group(group_name) {
                store.set(db, key.clone(), Value::Stream(s));
                Frame::Integer(1)
            } else {
                Frame::Integer(0)
            }
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::Integer(0),
    }
}

/// XGROUP CREATECONSUMER subcommand
pub fn xgroup_createconsumer(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    group_name: &Bytes,
    consumer_name: &Bytes,
) -> Frame {
    match store.get(db, key) {
        Some(Value::Stream(mut s)) => {
            if s.get_group(group_name).is_none() {
                return Frame::error("NOGROUP No such consumer group for key");
            }
            let created = s.create_consumer(group_name, consumer_name);
            store.set(db, key.clone(), Value::Stream(s));
            Frame::Integer(if created { 1 } else { 0 })
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::error("NOGROUP No such consumer group for key"),
    }
}

/// XGROUP DELCONSUMER subcommand
pub fn xgroup_delconsumer(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    group_name: &Bytes,
    consumer_name: &Bytes,
) -> Frame {
    match store.get(db, key) {
        Some(Value::Stream(mut s)) => match s.delete_consumer(group_name, consumer_name) {
            Some(pending_count) => {
                store.set(db, key.clone(), Value::Stream(s));
                Frame::Integer(pending_count as i64)
            }
            None => Frame::error("NOGROUP No such consumer group for key"),
        },
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::error("NOGROUP No such consumer group for key"),
    }
}

/// XGROUP SETID subcommand
pub fn xgroup_setid(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    group_name: &Bytes,
    id: &str,
) -> Frame {
    match store.get(db, key) {
        Some(Value::Stream(mut s)) => {
            let new_id = if id == "$" {
                s.last_id.clone()
            } else if id == "0" || id == "0-0" {
                StreamEntryId::new(0, 0)
            } else {
                match StreamEntryId::parse(id) {
                    Some(parsed_id) => parsed_id,
                    None => {
                        return Frame::error(
                            "ERR Invalid stream ID specified as stream command argument",
                        )
                    }
                }
            };

            if s.set_group_id(group_name, new_id) {
                store.set(db, key.clone(), Value::Stream(s));
                Frame::simple("OK")
            } else {
                Frame::error("NOGROUP No such consumer group for key")
            }
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::error("ERR no such key"),
    }
}

/// Execute XACK command - acknowledge messages
pub fn xack(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    group_name: &Bytes,
    ids: &[StreamEntryId],
) -> Frame {
    match store.get(db, key) {
        Some(Value::Stream(mut s)) => match s.get_group_mut(group_name) {
            Some(group) => {
                let count = group.ack(ids);
                store.set(db, key.clone(), Value::Stream(s));
                Frame::Integer(count as i64)
            }
            None => Frame::error("NOGROUP No such consumer group for key"),
        },
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::error("NOGROUP No such consumer group for key"),
    }
}

/// Execute XPENDING command - get pending entries
#[allow(clippy::too_many_arguments)]
pub fn xpending(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    group_name: &Bytes,
    start: Option<&str>,
    end: Option<&str>,
    count: Option<usize>,
    consumer: Option<&Bytes>,
    idle: Option<u64>,
) -> Frame {
    match store.get(db, key) {
        Some(Value::Stream(s)) => {
            match s.get_group(group_name) {
                Some(group) => {
                    // Summary form (no range specified)
                    if start.is_none() && end.is_none() && count.is_none() {
                        return xpending_summary(group);
                    }

                    // Extended form
                    let start_id = match start {
                        Some("-") => StreamEntryId::new(0, 0),
                        Some(id_str) => match StreamEntryId::parse(id_str) {
                            Some(id) => id,
                            None => return Frame::error("ERR Invalid stream ID"),
                        },
                        None => StreamEntryId::new(0, 0),
                    };

                    let end_id = match end {
                        Some("+") => StreamEntryId::new(u64::MAX, u64::MAX),
                        Some(id_str) => match StreamEntryId::parse(id_str) {
                            Some(id) => id,
                            None => return Frame::error("ERR Invalid stream ID"),
                        },
                        None => StreamEntryId::new(u64::MAX, u64::MAX),
                    };

                    let count = count.unwrap_or(10);
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;

                    let mut entries: Vec<&StreamPendingEntry> = group
                        .pending
                        .values()
                        .filter(|pe| pe.id >= start_id && pe.id <= end_id)
                        .filter(|pe| {
                            // Filter by consumer if specified
                            if let Some(c) = consumer {
                                pe.consumer == c
                            } else {
                                true
                            }
                        })
                        .filter(|pe| {
                            // Filter by idle time if specified
                            if let Some(min_idle) = idle {
                                let idle_time = now.saturating_sub(pe.delivery_time);
                                idle_time >= min_idle
                            } else {
                                true
                            }
                        })
                        .collect();

                    // Sort by ID
                    entries.sort_by(|a, b| a.id.cmp(&b.id));
                    entries.truncate(count);

                    let result: Vec<Frame> = entries
                        .iter()
                        .map(|pe| {
                            let idle_time = now.saturating_sub(pe.delivery_time);
                            Frame::array(vec![
                                Frame::bulk(Bytes::from(pe.id.to_string())),
                                Frame::bulk(pe.consumer.clone()),
                                Frame::Integer(idle_time as i64),
                                Frame::Integer(pe.delivery_count as i64),
                            ])
                        })
                        .collect();

                    Frame::array(result)
                }
                None => Frame::error("NOGROUP No such consumer group for key"),
            }
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::error("NOGROUP No such consumer group for key"),
    }
}

/// XPENDING summary form helper
fn xpending_summary(group: &StreamConsumerGroup) -> Frame {
    if group.pending.is_empty() {
        return Frame::array(vec![
            Frame::Integer(0),
            Frame::Null,
            Frame::Null,
            Frame::Null,
        ]);
    }

    // Find min and max IDs
    let mut min_id: Option<&StreamEntryId> = None;
    let mut max_id: Option<&StreamEntryId> = None;

    for id in group.pending.keys() {
        min_id = Some(match min_id {
            Some(current) if id < current => id,
            Some(current) => current,
            None => id,
        });
        max_id = Some(match max_id {
            Some(current) if id > current => id,
            Some(current) => current,
            None => id,
        });
    }

    // Count per consumer
    let mut consumer_counts: std::collections::HashMap<&Bytes, i64> =
        std::collections::HashMap::new();
    for pe in group.pending.values() {
        *consumer_counts.entry(&pe.consumer).or_insert(0) += 1;
    }

    let consumers: Vec<Frame> = consumer_counts
        .iter()
        .map(|(name, count)| {
            Frame::array(vec![
                Frame::bulk((*name).clone()),
                Frame::bulk(Bytes::from(count.to_string())),
            ])
        })
        .collect();

    // min_id and max_id are guaranteed to be Some since we return early if pending is empty
    let (Some(min), Some(max)) = (min_id, max_id) else {
        // Defensive: should never happen since pending is not empty
        return Frame::array(vec![
            Frame::Integer(0),
            Frame::Null,
            Frame::Null,
            Frame::Null,
        ]);
    };

    Frame::array(vec![
        Frame::Integer(group.pending.len() as i64),
        Frame::bulk(Bytes::from(min.to_string())),
        Frame::bulk(Bytes::from(max.to_string())),
        Frame::array(consumers),
    ])
}

/// Execute XCLAIM command - claim pending messages
#[allow(clippy::too_many_arguments)]
pub fn xclaim(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    group_name: &Bytes,
    consumer_name: &Bytes,
    min_idle_time: u64,
    ids: &[StreamEntryId],
    idle: Option<u64>,
    time: Option<u64>,
    retrycount: Option<u64>,
    force: bool,
    justid: bool,
) -> Frame {
    match store.get(db, key) {
        Some(Value::Stream(mut s)) => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;

            // First, check which entries exist in the stream (before mutable borrow)
            let entries_exist: std::collections::HashSet<StreamEntryId> = ids
                .iter()
                .filter(|id| s.entries.contains_key(*id))
                .cloned()
                .collect();

            // Clone entry fields we'll need for results
            let entry_fields: std::collections::HashMap<StreamEntryId, Vec<(Bytes, Bytes)>> =
                if !justid {
                    ids.iter()
                        .filter_map(|id| {
                            s.entries.get(id).map(|fields| (id.clone(), fields.clone()))
                        })
                        .collect()
                } else {
                    std::collections::HashMap::new()
                };

            match s.get_group_mut(group_name) {
                Some(group) => {
                    let mut claimed_ids: Vec<StreamEntryId> = Vec::new();

                    for id in ids {
                        // Check if entry exists in pending list
                        let should_claim = if let Some(pe) = group.pending.get(id) {
                            let idle_time = now.saturating_sub(pe.delivery_time);
                            idle_time >= min_idle_time
                        } else if force {
                            // With FORCE, claim even if not in PEL (but entry must exist in stream)
                            entries_exist.contains(id)
                        } else {
                            false
                        };

                        if should_claim {
                            // Remove from old consumer if exists
                            if let Some(pe) = group.pending.get(id) {
                                let old_consumer = pe.consumer.clone();
                                if let Some(old_c) = group.consumers.get_mut(&old_consumer) {
                                    old_c.pending.remove(id);
                                }
                            }

                            // Create new pending entry
                            let delivery_time = if let Some(t) = time {
                                t
                            } else if let Some(i) = idle {
                                now.saturating_sub(i)
                            } else {
                                now
                            };

                            let delivery_count = if let Some(rc) = retrycount {
                                rc
                            } else if let Some(pe) = group.pending.get(id) {
                                pe.delivery_count + 1
                            } else {
                                1
                            };

                            let new_pe = StreamPendingEntry {
                                id: id.clone(),
                                consumer: consumer_name.clone(),
                                delivery_time,
                                delivery_count,
                            };

                            group.pending.insert(id.clone(), new_pe);

                            // Add to new consumer
                            let consumer = group.get_or_create_consumer(consumer_name);
                            consumer.pending.insert(id.clone());
                            consumer.seen_time = now;

                            claimed_ids.push(id.clone());
                        }
                    }

                    store.set(db, key.clone(), Value::Stream(s));

                    // Build result frames using pre-collected data
                    let claimed: Vec<Frame> = claimed_ids
                        .iter()
                        .filter_map(|id| {
                            if justid {
                                Some(Frame::bulk(Bytes::from(id.to_string())))
                            } else if let Some(fields) = entry_fields.get(id) {
                                let fields_frames: Vec<Frame> = fields
                                    .iter()
                                    .flat_map(|(k, v)| {
                                        vec![Frame::bulk(k.clone()), Frame::bulk(v.clone())]
                                    })
                                    .collect();
                                Some(Frame::array(vec![
                                    Frame::bulk(Bytes::from(id.to_string())),
                                    Frame::array(fields_frames),
                                ]))
                            } else {
                                None
                            }
                        })
                        .collect();

                    Frame::array(claimed)
                }
                None => Frame::error("NOGROUP No such consumer group for key"),
            }
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::error("NOGROUP No such consumer group for key"),
    }
}

/// Execute XAUTOCLAIM command - auto-claim stale pending messages
#[allow(clippy::too_many_arguments)]
pub fn xautoclaim(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    group_name: &Bytes,
    consumer_name: &Bytes,
    min_idle_time: u64,
    start_id: &str,
    count: Option<usize>,
    justid: bool,
) -> Frame {
    match store.get(db, key) {
        Some(Value::Stream(mut s)) => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;

            let start = if start_id == "0" || start_id == "0-0" {
                StreamEntryId::new(0, 0)
            } else {
                match StreamEntryId::parse(start_id) {
                    Some(id) => id,
                    None => return Frame::error("ERR Invalid stream ID"),
                }
            };

            let count = count.unwrap_or(100);

            // First, collect all entry IDs that exist in the stream (before mutable borrow)
            let entries_exist: std::collections::HashSet<StreamEntryId> =
                s.entries.keys().cloned().collect();

            // Clone entry fields we might need for results
            let entry_fields: std::collections::HashMap<StreamEntryId, Vec<(Bytes, Bytes)>> =
                if !justid {
                    s.entries
                        .iter()
                        .map(|(id, fields)| (id.clone(), fields.clone()))
                        .collect()
                } else {
                    std::collections::HashMap::new()
                };

            match s.get_group_mut(group_name) {
                Some(group) => {
                    // Find pending entries that are idle enough and >= start_id
                    let mut candidates: Vec<StreamEntryId> = group
                        .pending
                        .iter()
                        .filter(|(id, pe)| {
                            let idle_time = now.saturating_sub(pe.delivery_time);
                            **id >= start && idle_time >= min_idle_time
                        })
                        .map(|(id, _)| id.clone())
                        .collect();

                    candidates.sort();
                    candidates.truncate(count);

                    let mut claimed_ids: Vec<StreamEntryId> = Vec::new();
                    let mut deleted_ids: Vec<StreamEntryId> = Vec::new();
                    let mut next_cursor = StreamEntryId::new(0, 0);

                    for id in &candidates {
                        // Check if entry still exists in stream
                        if !entries_exist.contains(id) {
                            // Entry deleted from stream, remove from PEL
                            deleted_ids.push(id.clone());
                            continue;
                        }

                        // Remove from old consumer
                        if let Some(pe) = group.pending.get(id) {
                            let old_consumer = pe.consumer.clone();
                            if let Some(old_c) = group.consumers.get_mut(&old_consumer) {
                                old_c.pending.remove(id);
                            }
                        }

                        // Create new pending entry
                        let delivery_count = group
                            .pending
                            .get(id)
                            .map(|pe| pe.delivery_count + 1)
                            .unwrap_or(1);

                        let new_pe = StreamPendingEntry {
                            id: id.clone(),
                            consumer: consumer_name.clone(),
                            delivery_time: now,
                            delivery_count,
                        };

                        group.pending.insert(id.clone(), new_pe);

                        // Add to new consumer
                        let consumer = group.get_or_create_consumer(consumer_name);
                        consumer.pending.insert(id.clone());
                        consumer.seen_time = now;

                        claimed_ids.push(id.clone());
                        next_cursor = StreamEntryId::new(id.ms, id.seq + 1);
                    }

                    // Remove deleted entries from PEL
                    for id in &deleted_ids {
                        if let Some(pe) = group.pending.remove(id) {
                            if let Some(c) = group.consumers.get_mut(&pe.consumer) {
                                c.pending.remove(id);
                            }
                        }
                    }

                    store.set(db, key.clone(), Value::Stream(s));

                    // Build result frames using pre-collected data
                    let claimed: Vec<Frame> = claimed_ids
                        .iter()
                        .filter_map(|id| {
                            if justid {
                                Some(Frame::bulk(Bytes::from(id.to_string())))
                            } else if let Some(fields) = entry_fields.get(id) {
                                let fields_frames: Vec<Frame> = fields
                                    .iter()
                                    .flat_map(|(k, v)| {
                                        vec![Frame::bulk(k.clone()), Frame::bulk(v.clone())]
                                    })
                                    .collect();
                                Some(Frame::array(vec![
                                    Frame::bulk(Bytes::from(id.to_string())),
                                    Frame::array(fields_frames),
                                ]))
                            } else {
                                None
                            }
                        })
                        .collect();

                    let deleted_frames: Vec<Frame> = deleted_ids
                        .iter()
                        .map(|id| Frame::bulk(Bytes::from(id.to_string())))
                        .collect();

                    Frame::array(vec![
                        Frame::bulk(Bytes::from(next_cursor.to_string())),
                        Frame::array(claimed),
                        Frame::array(deleted_frames),
                    ])
                }
                None => Frame::error("NOGROUP No such consumer group for key"),
            }
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::error("NOGROUP No such consumer group for key"),
    }
}

/// Execute XREADGROUP command - read from stream using consumer group
pub fn xreadgroup(
    store: &Arc<Store>,
    db: u8,
    group_name: &Bytes,
    consumer_name: &Bytes,
    count: Option<usize>,
    noack: bool,
    streams: &[(Bytes, String)],
) -> Frame {
    let mut results = Vec::new();

    for (key, id_str) in streams {
        match store.get(db, key) {
            Some(Value::Stream(mut s)) => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;

                // First, get the group's last_delivered_id (immutable borrow)
                let group_info = s.get_group(group_name).map(|g| {
                    (
                        g.last_delivered_id.clone(),
                        g.consumers.get(consumer_name).map(|c| c.pending.clone()),
                    )
                });

                let (last_delivered_id, consumer_pending) = match group_info {
                    Some((lid, cp)) => (lid, cp),
                    None => continue, // Skip streams without this consumer group
                };

                let entries = if id_str == ">" {
                    // Read new entries (after last_delivered_id)
                    let start = if last_delivered_id == StreamEntryId::new(0, 0) {
                        StreamEntryId::new(0, 0)
                    } else {
                        StreamEntryId::new(last_delivered_id.ms, last_delivered_id.seq + 1)
                    };
                    let end = StreamEntryId::new(u64::MAX, u64::MAX);
                    s.range(&start, &end, count)
                } else {
                    // Read pending entries (history)
                    let start_id = if id_str == "0" || id_str == "0-0" {
                        StreamEntryId::new(0, 0)
                    } else {
                        match StreamEntryId::parse(id_str) {
                            Some(id) => id,
                            None => continue,
                        }
                    };

                    // Get pending entries for this consumer
                    let mut pending_entries: Vec<crate::storage::StreamEntry> = Vec::new();

                    if let Some(pending) = &consumer_pending {
                        let mut pending_ids: Vec<_> = pending
                            .iter()
                            .filter(|id| **id >= start_id)
                            .cloned()
                            .collect();
                        pending_ids.sort();

                        if let Some(cnt) = count {
                            pending_ids.truncate(cnt);
                        }

                        for id in pending_ids {
                            if let Some(fields) = s.entries.get(&id) {
                                pending_entries.push(crate::storage::StreamEntry {
                                    id: id.clone(),
                                    fields: fields.clone(),
                                });
                            }
                        }
                    }

                    pending_entries
                };

                // Now get mutable borrow to update group state
                if !entries.is_empty() {
                    if let Some(group) = s.get_group_mut(group_name) {
                        // Ensure consumer exists
                        group.get_or_create_consumer(consumer_name);

                        if id_str == ">" {
                            // Update last_delivered_id and add to PEL for new entries
                            if !noack {
                                for entry in &entries {
                                    group.add_pending(entry.id.clone(), consumer_name);
                                }
                            }
                            // entries is guaranteed non-empty since we checked above
                            if let Some(last) = entries.last() {
                                group.last_delivered_id = last.id.clone();
                            }
                        } else {
                            // Update delivery time and count for pending entries
                            for entry in &entries {
                                if let Some(pe) = group.pending.get_mut(&entry.id) {
                                    pe.delivery_time = now;
                                    pe.delivery_count += 1;
                                }
                            }
                        }

                        // Update consumer's seen_time
                        if let Some(consumer) = group.consumers.get_mut(consumer_name) {
                            consumer.seen_time = now;
                        }
                    }

                    let entry_frames: Vec<Frame> = entries
                        .into_iter()
                        .map(|entry| {
                            let id_frame = Frame::bulk(Bytes::from(entry.id.to_string()));
                            let fields_frames: Vec<Frame> = entry
                                .fields
                                .into_iter()
                                .flat_map(|(k, v)| vec![Frame::bulk(k), Frame::bulk(v)])
                                .collect();
                            Frame::array(vec![id_frame, Frame::array(fields_frames)])
                        })
                        .collect();
                    results.push(Frame::array(vec![
                        Frame::bulk(key.clone()),
                        Frame::array(entry_frames),
                    ]));
                }

                store.set(db, key.clone(), Value::Stream(s));
            }
            _ => continue,
        }
    }

    if results.is_empty() {
        Frame::null()
    } else {
        Frame::array(results)
    }
}

/// Execute XINFO GROUPS command
pub fn xinfo_groups(store: &Arc<Store>, db: u8, key: &Bytes) -> Frame {
    match store.get(db, key) {
        Some(Value::Stream(s)) => {
            let groups: Vec<Frame> = s
                .consumer_groups
                .values()
                .map(|group| {
                    Frame::array(vec![
                        Frame::bulk("name"),
                        Frame::bulk(group.name.clone()),
                        Frame::bulk("consumers"),
                        Frame::Integer(group.consumers.len() as i64),
                        Frame::bulk("pending"),
                        Frame::Integer(group.pending.len() as i64),
                        Frame::bulk("last-delivered-id"),
                        Frame::bulk(Bytes::from(group.last_delivered_id.to_string())),
                    ])
                })
                .collect();
            Frame::array(groups)
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::error("ERR no such key"),
    }
}

/// Execute XINFO CONSUMERS command
pub fn xinfo_consumers(store: &Arc<Store>, db: u8, key: &Bytes, group_name: &Bytes) -> Frame {
    match store.get(db, key) {
        Some(Value::Stream(s)) => match s.get_group(group_name) {
            Some(group) => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;

                let consumers: Vec<Frame> = group
                    .consumers
                    .values()
                    .map(|consumer| {
                        let idle = now.saturating_sub(consumer.seen_time);
                        Frame::array(vec![
                            Frame::bulk("name"),
                            Frame::bulk(consumer.name.clone()),
                            Frame::bulk("pending"),
                            Frame::Integer(consumer.pending.len() as i64),
                            Frame::bulk("idle"),
                            Frame::Integer(idle as i64),
                        ])
                    })
                    .collect();
                Frame::array(consumers)
            }
            None => Frame::error("NOGROUP No such consumer group for key"),
        },
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::error("ERR no such key"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_store() -> Arc<Store> {
        Arc::new(Store::new(16))
    }

    #[test]
    fn test_xadd_and_xlen() {
        let store = create_store();
        let key = Bytes::from("mystream");
        let fields = vec![
            (Bytes::from("field1"), Bytes::from("value1")),
            (Bytes::from("field2"), Bytes::from("value2")),
        ];

        // Add entry with auto-generated ID
        let result = xadd(&store, 0, &key, None, fields.clone());
        assert!(matches!(result, Frame::Bulk(Some(_))));

        // Check length
        let len = xlen(&store, 0, &key);
        assert!(matches!(len, Frame::Integer(1)));

        // Add another entry
        let _ = xadd(&store, 0, &key, None, fields.clone());
        let len = xlen(&store, 0, &key);
        assert!(matches!(len, Frame::Integer(2)));
    }

    #[test]
    fn test_xrange() {
        let store = create_store();
        let key = Bytes::from("mystream");

        // Add some entries
        for i in 0..3 {
            let fields = vec![(
                Bytes::from(format!("field{}", i)),
                Bytes::from(format!("value{}", i)),
            )];
            xadd(&store, 0, &key, None, fields);
        }

        // Get all entries
        let result = xrange(&store, 0, &key, "-", "+", None);
        if let Frame::Array(Some(entries)) = result {
            assert_eq!(entries.len(), 3);
        } else {
            panic!("Expected array");
        }

        // Get with count limit
        let result = xrange(&store, 0, &key, "-", "+", Some(2));
        if let Frame::Array(Some(entries)) = result {
            assert_eq!(entries.len(), 2);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_xtrim() {
        let store = create_store();
        let key = Bytes::from("mystream");

        // Add 5 entries
        for i in 0..5 {
            let fields = vec![(
                Bytes::from(format!("field{}", i)),
                Bytes::from(format!("value{}", i)),
            )];
            xadd(&store, 0, &key, None, fields);
        }

        // Trim to 3
        let result = xtrim(&store, 0, &key, 3);
        assert!(matches!(result, Frame::Integer(2)));

        // Verify length
        let len = xlen(&store, 0, &key);
        assert!(matches!(len, Frame::Integer(3)));
    }

    #[test]
    fn test_xdel() {
        let store = create_store();
        let key = Bytes::from("mystream");

        // Add entry with explicit ID
        let id = StreamEntryId::new(1000, 0);
        let fields = vec![(Bytes::from("f"), Bytes::from("v"))];
        xadd(&store, 0, &key, Some(id.clone()), fields);

        // Delete it
        let result = xdel(&store, 0, &key, &[id]);
        assert!(matches!(result, Frame::Integer(1)));

        // Verify length
        let len = xlen(&store, 0, &key);
        assert!(matches!(len, Frame::Integer(0)));
    }

    #[test]
    fn test_xgroup_create() {
        let store = create_store();
        let key = Bytes::from("mystream");
        let group = Bytes::from("mygroup");

        // Create with MKSTREAM
        let result = xgroup_create(&store, 0, &key, &group, "$", true);
        assert!(matches!(result, Frame::Simple(_)));

        // Try to create again (should fail)
        let result = xgroup_create(&store, 0, &key, &group, "$", true);
        assert!(matches!(result, Frame::Error(_)));

        // Create another group
        let group2 = Bytes::from("mygroup2");
        let result = xgroup_create(&store, 0, &key, &group2, "0", true);
        assert!(matches!(result, Frame::Simple(_)));
    }

    #[test]
    fn test_xgroup_destroy() {
        let store = create_store();
        let key = Bytes::from("mystream");
        let group = Bytes::from("mygroup");

        // Create group with MKSTREAM
        xgroup_create(&store, 0, &key, &group, "$", true);

        // Destroy it
        let result = xgroup_destroy(&store, 0, &key, &group);
        assert!(matches!(result, Frame::Integer(1)));

        // Destroy non-existent group
        let result = xgroup_destroy(&store, 0, &key, &group);
        assert!(matches!(result, Frame::Integer(0)));
    }

    #[test]
    fn test_xreadgroup_and_xack() {
        let store = create_store();
        let key = Bytes::from("mystream");
        let group = Bytes::from("mygroup");
        let consumer = Bytes::from("myconsumer");

        // Add some entries
        let id1 = StreamEntryId::new(1000, 0);
        let id2 = StreamEntryId::new(2000, 0);
        xadd(
            &store,
            0,
            &key,
            Some(id1.clone()),
            vec![(Bytes::from("f"), Bytes::from("v1"))],
        );
        xadd(
            &store,
            0,
            &key,
            Some(id2.clone()),
            vec![(Bytes::from("f"), Bytes::from("v2"))],
        );

        // Create consumer group starting at 0
        xgroup_create(&store, 0, &key, &group, "0", false);

        // Read new entries
        let streams = vec![(key.clone(), ">".to_string())];
        let result = xreadgroup(&store, 0, &group, &consumer, None, false, &streams);
        if let Frame::Array(Some(arr)) = result {
            assert_eq!(arr.len(), 1); // One stream
            if let Frame::Array(Some(stream_arr)) = &arr[0] {
                if let Frame::Array(Some(entries)) = &stream_arr[1] {
                    assert_eq!(entries.len(), 2); // Two entries
                }
            }
        } else {
            panic!("Expected array result");
        }

        // Acknowledge one entry
        let result = xack(&store, 0, &key, &group, &[id1.clone()]);
        assert!(matches!(result, Frame::Integer(1)));

        // Check pending
        let result = xpending(&store, 0, &key, &group, None, None, None, None, None);
        if let Frame::Array(Some(arr)) = result {
            if let Frame::Integer(count) = &arr[0] {
                assert_eq!(*count, 1); // One pending
            }
        }

        // Acknowledge the other entry
        let result = xack(&store, 0, &key, &group, &[id2.clone()]);
        assert!(matches!(result, Frame::Integer(1)));
    }

    #[test]
    fn test_xclaim() {
        let store = create_store();
        let key = Bytes::from("mystream");
        let group = Bytes::from("mygroup");
        let consumer1 = Bytes::from("consumer1");
        let consumer2 = Bytes::from("consumer2");

        // Setup
        let id = StreamEntryId::new(1000, 0);
        xadd(
            &store,
            0,
            &key,
            Some(id.clone()),
            vec![(Bytes::from("f"), Bytes::from("v"))],
        );
        xgroup_create(&store, 0, &key, &group, "0", false);

        // Read with consumer1
        let streams = vec![(key.clone(), ">".to_string())];
        xreadgroup(&store, 0, &group, &consumer1, None, false, &streams);

        // Claim with consumer2 (min-idle-time 0 to claim immediately)
        let result = xclaim(
            &store,
            0,
            &key,
            &group,
            &consumer2,
            0, // min-idle-time
            &[id.clone()],
            None,
            None,
            None,
            false,
            false,
        );
        if let Frame::Array(Some(arr)) = result {
            assert_eq!(arr.len(), 1); // One claimed entry
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_xpending_extended() {
        let store = create_store();
        let key = Bytes::from("mystream");
        let group = Bytes::from("mygroup");
        let consumer = Bytes::from("myconsumer");

        // Setup
        let id1 = StreamEntryId::new(1000, 0);
        let id2 = StreamEntryId::new(2000, 0);
        xadd(
            &store,
            0,
            &key,
            Some(id1.clone()),
            vec![(Bytes::from("f"), Bytes::from("v1"))],
        );
        xadd(
            &store,
            0,
            &key,
            Some(id2.clone()),
            vec![(Bytes::from("f"), Bytes::from("v2"))],
        );
        xgroup_create(&store, 0, &key, &group, "0", false);

        // Read entries
        let streams = vec![(key.clone(), ">".to_string())];
        xreadgroup(&store, 0, &group, &consumer, None, false, &streams);

        // Get extended pending info
        let result = xpending(
            &store,
            0,
            &key,
            &group,
            Some("-"),
            Some("+"),
            Some(10),
            None,
            None,
        );
        if let Frame::Array(Some(arr)) = result {
            assert_eq!(arr.len(), 2); // Two pending entries
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_xinfo_groups() {
        let store = create_store();
        let key = Bytes::from("mystream");
        let group1 = Bytes::from("group1");
        let group2 = Bytes::from("group2");

        // Create stream and groups
        xgroup_create(&store, 0, &key, &group1, "$", true);
        xgroup_create(&store, 0, &key, &group2, "$", false);

        // Get group info
        let result = xinfo_groups(&store, 0, &key);
        if let Frame::Array(Some(arr)) = result {
            assert_eq!(arr.len(), 2); // Two groups
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_xinfo_consumers() {
        let store = create_store();
        let key = Bytes::from("mystream");
        let group = Bytes::from("mygroup");
        let consumer = Bytes::from("myconsumer");

        // Setup
        xgroup_create(&store, 0, &key, &group, "$", true);
        let id = StreamEntryId::new(1000, 0);
        xadd(
            &store,
            0,
            &key,
            Some(id),
            vec![(Bytes::from("f"), Bytes::from("v"))],
        );

        // Read to create consumer
        let streams = vec![(key.clone(), ">".to_string())];
        xreadgroup(&store, 0, &group, &consumer, None, false, &streams);

        // Get consumer info
        let result = xinfo_consumers(&store, 0, &key, &group);
        if let Frame::Array(Some(arr)) = result {
            assert_eq!(arr.len(), 1); // One consumer
        } else {
            panic!("Expected array result");
        }
    }
}
