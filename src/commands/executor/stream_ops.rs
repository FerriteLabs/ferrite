//! Stream command implementations on CommandExecutor (Redis streams and Kafka-compatible streaming).

use std::sync::OnceLock;

use bytes::Bytes;

use crate::commands::streams;
use crate::protocol::Frame;

use super::CommandExecutor;

/// Global streaming broker instance (lazily initialized).
fn streaming_broker() -> &'static ferrite_streaming::kafka::StreamingBroker {
    static BROKER: OnceLock<ferrite_streaming::kafka::StreamingBroker> = OnceLock::new();
    BROKER.get_or_init(ferrite_streaming::kafka::StreamingBroker::new)
}

impl CommandExecutor {
    // Stream commands

    #[allow(clippy::too_many_arguments)]
    pub(super) fn xadd(
        &self,
        db: u8,
        key: &Bytes,
        id: Option<&str>,
        fields: Vec<(Bytes, Bytes)>,
        maxlen: Option<usize>,
        minid: Option<&str>,
        nomkstream: bool,
    ) -> Frame {
        use crate::storage::StreamEntryId;

        // NOMKSTREAM: don't create the stream if it doesn't exist
        if nomkstream {
            match self.store.get(db, key) {
                Some(crate::storage::Value::Stream(_)) => {} // Stream exists, proceed
                Some(_) => {
                    return Frame::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                }
                None => return Frame::null(), // Stream doesn't exist, return nil
            }
        }

        // Parse the ID if provided
        let entry_id = match id {
            Some("*") | None => None, // Auto-generate
            Some(id_str) => match StreamEntryId::parse(id_str) {
                Some(parsed_id) => Some(parsed_id),
                None => {
                    return Frame::error(
                        "ERR Invalid stream ID specified as stream command argument",
                    )
                }
            },
        };

        let result = streams::xadd(&self.store, db, key, entry_id, fields);

        // Handle MAXLEN trimming after add
        if let Some(maxlen) = maxlen {
            if let Frame::Bulk(Some(_)) = &result {
                streams::xtrim(&self.store, db, key, maxlen);
            }
        }

        // Handle MINID trimming after add
        if let Some(minid_str) = minid {
            if let Frame::Bulk(Some(_)) = &result {
                streams::xtrim_by_minid(&self.store, db, key, minid_str);
            }
        }

        // Notify blocking stream manager that a new entry was added
        if let Frame::Bulk(Some(_)) = &result {
            crate::commands::blocking::notify_stream_add(&self.blocking_stream_manager, db, key);
        }

        result
    }

    pub(super) fn xlen(&self, db: u8, key: &Bytes) -> Frame {
        streams::xlen(&self.store, db, key)
    }

    #[inline]
    pub(super) fn xrange(
        &self,
        db: u8,
        key: &Bytes,
        start: &str,
        end: &str,
        count: Option<usize>,
    ) -> Frame {
        streams::xrange(&self.store, db, key, start, end, count)
    }

    #[inline]
    pub(super) fn xrevrange(
        &self,
        db: u8,
        key: &Bytes,
        end: &str,
        start: &str,
        count: Option<usize>,
    ) -> Frame {
        streams::xrevrange(&self.store, db, key, end, start, count)
    }

    #[inline]
    pub(super) fn xread(
        &self,
        db: u8,
        stream_keys: &[(Bytes, String)],
        count: Option<usize>,
    ) -> Frame {
        streams::xread(&self.store, db, stream_keys, count)
    }

    pub(super) fn xdel(&self, db: u8, key: &Bytes, ids: &[String]) -> Frame {
        use crate::storage::StreamEntryId;

        // Parse all IDs
        let mut parsed_ids = Vec::with_capacity(ids.len());
        for id_str in ids {
            match StreamEntryId::parse(id_str) {
                Some(id) => parsed_ids.push(id),
                None => {
                    return Frame::error(
                        "ERR Invalid stream ID specified as stream command argument",
                    )
                }
            }
        }

        streams::xdel(&self.store, db, key, &parsed_ids)
    }

    pub(super) fn xtrim(
        &self,
        db: u8,
        key: &Bytes,
        maxlen: Option<usize>,
        minid: Option<&str>,
    ) -> Frame {
        if let Some(maxlen) = maxlen {
            streams::xtrim(&self.store, db, key, maxlen)
        } else if let Some(minid_str) = minid {
            streams::xtrim_by_minid(&self.store, db, key, minid_str)
        } else {
            Frame::Integer(0)
        }
    }

    pub(super) fn xinfo(
        &self,
        db: u8,
        key: &Bytes,
        subcommand: &str,
        group_name: Option<&Bytes>,
    ) -> Frame {
        match subcommand.to_uppercase().as_str() {
            "STREAM" => streams::xinfo_stream(&self.store, db, key),
            "GROUPS" => streams::xinfo_groups(&self.store, db, key),
            "CONSUMERS" => match group_name {
                Some(gn) => streams::xinfo_consumers(&self.store, db, key, gn),
                None => Frame::error("ERR wrong number of arguments for 'xinfo|consumers' command"),
            },
            "HELP" => Frame::array(vec![
                Frame::bulk("XINFO <subcommand> [<arg> [value] [opt] ...]"),
                Frame::bulk("STREAM <key> [FULL [COUNT <count>]] -- Show stream info."),
                Frame::bulk("GROUPS <key> -- Show consumer groups."),
                Frame::bulk("CONSUMERS <key> <group> -- Show consumers in a group."),
            ]),
            _ => Frame::error(format!(
                "ERR Unknown subcommand or wrong number of arguments for 'xinfo|{}'",
                subcommand.to_lowercase()
            )),
        }
    }

    // ── Kafka-compatible streaming handlers ─────────────────────────────

    pub(super) async fn handle_stream_create(
        &self,
        topic: &str,
        partitions: u32,
        retention_ms: i64,
        replication: u16,
    ) -> Frame {
        let broker = streaming_broker();
        match broker.create_topic(topic.to_string(), partitions, replication, retention_ms) {
            Ok(t) => {
                let mut items = vec![
                    Frame::bulk("topic"),
                    Frame::bulk(t.name),
                    Frame::bulk("partitions"),
                    Frame::Integer(t.num_partitions as i64),
                    Frame::bulk("replication"),
                    Frame::Integer(t.replication_factor as i64),
                ];
                if retention_ms >= 0 {
                    items.push(Frame::bulk("retention_ms"));
                    items.push(Frame::Integer(retention_ms));
                }
                Frame::Array(Some(items))
            }
            Err(e) => Frame::Error(format!("ERR {e}").into()),
        }
    }

    pub(super) async fn handle_stream_delete(&self, topic: &str) -> Frame {
        let broker = streaming_broker();
        match broker.delete_topic(topic) {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::Error(format!("ERR {e}").into()),
        }
    }

    pub(super) async fn handle_stream_produce(
        &self,
        topic: &str,
        key: Option<&Bytes>,
        value: &Bytes,
        partition: Option<u32>,
    ) -> Frame {
        use ferrite_streaming::kafka::ProducerRecord;

        let broker = streaming_broker();
        let record = ProducerRecord {
            topic: topic.to_string(),
            partition,
            key: key.map(|k| k.to_vec()),
            value: value.to_vec(),
            headers: vec![],
            timestamp: None,
        };
        match broker.produce(record) {
            Ok((p, o)) => Frame::Array(Some(vec![
                Frame::bulk("partition"),
                Frame::Integer(p as i64),
                Frame::bulk("offset"),
                Frame::Integer(o),
            ])),
            Err(e) => Frame::Error(format!("ERR {e}").into()),
        }
    }

    pub(super) async fn handle_stream_fetch(
        &self,
        topic: &str,
        partition: u32,
        offset: i64,
        count: usize,
    ) -> Frame {
        let broker = streaming_broker();
        match broker.fetch(topic, partition, offset, count) {
            Ok(records) => {
                let items: Vec<Frame> = records
                    .into_iter()
                    .map(|r| {
                        Frame::Array(Some(vec![
                            Frame::bulk("offset"),
                            Frame::Integer(r.offset),
                            Frame::bulk("key"),
                            match r.key {
                                Some(k) => Frame::Bulk(Some(Bytes::from(k))),
                                None => Frame::Null,
                            },
                            Frame::bulk("value"),
                            Frame::Bulk(Some(Bytes::from(r.value))),
                            Frame::bulk("timestamp"),
                            Frame::Integer(r.timestamp),
                        ]))
                    })
                    .collect();
                Frame::Array(Some(items))
            }
            Err(e) => Frame::Error(format!("ERR {e}").into()),
        }
    }

    pub(super) async fn handle_stream_commit(
        &self,
        group: &str,
        topic: &str,
        partition: u32,
        offset: i64,
    ) -> Frame {
        let broker = streaming_broker();
        match broker.commit_offset(group, topic.to_string(), partition, offset) {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::Error(format!("ERR {e}").into()),
        }
    }

    pub(super) async fn handle_stream_topics(&self) -> Frame {
        let broker = streaming_broker();
        let topics = broker.list_topics();
        let items: Vec<Frame> = topics
            .into_iter()
            .map(|t| {
                Frame::Array(Some(vec![
                    Frame::bulk("name"),
                    Frame::bulk(t.name),
                    Frame::bulk("partitions"),
                    Frame::Integer(t.num_partitions as i64),
                    Frame::bulk("replication"),
                    Frame::Integer(t.replication_factor as i64),
                ]))
            })
            .collect();
        Frame::Array(Some(items))
    }

    pub(super) async fn handle_stream_describe(&self, topic: &str) -> Frame {
        let broker = streaming_broker();
        match broker.describe_topic(topic) {
            Some(t) => Frame::Array(Some(vec![
                Frame::bulk("name"),
                Frame::bulk(t.name),
                Frame::bulk("partitions"),
                Frame::Integer(t.num_partitions as i64),
                Frame::bulk("replication"),
                Frame::Integer(t.replication_factor as i64),
                Frame::bulk("retention_ms"),
                Frame::Integer(t.config.retention_ms),
                Frame::bulk("max_message_bytes"),
                Frame::Integer(t.config.max_message_bytes as i64),
            ])),
            None => Frame::Null,
        }
    }

    pub(super) async fn handle_stream_groups(&self, topic: Option<&str>) -> Frame {
        let broker = streaming_broker();
        let groups = broker.list_groups(topic);
        let items: Vec<Frame> = groups.into_iter().map(Frame::bulk).collect();
        Frame::Array(Some(items))
    }

    pub(super) async fn handle_stream_offsets(&self, topic: &str, partition: u32) -> Frame {
        let broker = streaming_broker();
        match broker.get_offsets(topic, partition) {
            Ok((earliest, latest)) => Frame::Array(Some(vec![
                Frame::bulk("earliest"),
                Frame::Integer(earliest),
                Frame::bulk("latest"),
                Frame::Integer(latest),
            ])),
            Err(e) => Frame::Error(format!("ERR {e}").into()),
        }
    }

    pub(super) async fn handle_stream_stats(&self) -> Frame {
        let broker = streaming_broker();
        let s = broker.stats();
        Frame::Array(Some(vec![
            Frame::bulk("total_topics"),
            Frame::Integer(s.total_topics as i64),
            Frame::bulk("total_partitions"),
            Frame::Integer(s.total_partitions as i64),
            Frame::bulk("total_messages"),
            Frame::Integer(s.total_messages as i64),
            Frame::bulk("total_consumer_groups"),
            Frame::Integer(s.total_consumer_groups as i64),
        ]))
    }
}
