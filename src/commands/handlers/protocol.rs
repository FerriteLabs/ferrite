//! PROTOCOL.* command handlers
//!
//! PROTOCOL STATUS, PROTOCOL MEMCACHED, PROTOCOL GRPC, PROTOCOL AMQP,
//! PROTOCOL HELP
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::OnceLock;

use bytes::Bytes;

use crate::protocol::Frame;
use crate::server::amqp_adapter::{AmqpAdapter, AmqpConfig};
use crate::server::grpc_service::{FerritGrpcService, GrpcServiceDefinition};
use crate::server::memcached_protocol::MemcachedStats;

use super::err_frame;

/// Global Memcached stats singleton.
static MEMCACHED_STATS: OnceLock<MemcachedStats> = OnceLock::new();

/// Global gRPC service singleton.
static GRPC_SERVICE: OnceLock<FerritGrpcService> = OnceLock::new();

/// Global AMQP adapter singleton.
static AMQP_ADAPTER: OnceLock<AmqpAdapter> = OnceLock::new();

fn get_memcached_stats() -> &'static MemcachedStats {
    MEMCACHED_STATS.get_or_init(MemcachedStats::default)
}

fn get_grpc_service() -> &'static FerritGrpcService {
    GRPC_SERVICE.get_or_init(FerritGrpcService::new)
}

fn get_amqp_adapter() -> &'static AmqpAdapter {
    AMQP_ADAPTER.get_or_init(|| AmqpAdapter::new(AmqpConfig::default()))
}

/// Dispatch a `PROTOCOL` subcommand.
pub fn protocol_command(subcommand: &str, args: &[String]) -> Frame {
    match subcommand.to_uppercase().as_str() {
        "STATUS" => protocol_status(),
        "MEMCACHED" => protocol_memcached(args),
        "GRPC" => protocol_grpc(args),
        "AMQP" => protocol_amqp(args),
        "HELP" => protocol_help(),
        _ => err_frame(&format!("unknown PROTOCOL subcommand '{}'", subcommand)),
    }
}

/// PROTOCOL STATUS
fn protocol_status() -> Frame {
    let mut map = HashMap::new();
    map.insert(
        Bytes::from("resp"),
        Frame::Bulk(Some(Bytes::from("enabled"))),
    );
    map.insert(
        Bytes::from("memcached"),
        Frame::Bulk(Some(Bytes::from("available"))),
    );
    map.insert(
        Bytes::from("grpc"),
        Frame::Bulk(Some(Bytes::from("available"))),
    );
    map.insert(
        Bytes::from("amqp"),
        Frame::Bulk(Some(if get_amqp_adapter().stats().exchanges > 0 {
            Bytes::from("active")
        } else {
            Bytes::from("available")
        })),
    );
    Frame::Map(map)
}

/// PROTOCOL MEMCACHED [STATS]
fn protocol_memcached(args: &[String]) -> Frame {
    if args.is_empty() {
        let mut map = HashMap::new();
        map.insert(
            Bytes::from("protocol"),
            Frame::Bulk(Some(Bytes::from("memcached"))),
        );
        map.insert(
            Bytes::from("text_protocol"),
            Frame::Bulk(Some(Bytes::from("supported"))),
        );
        map.insert(
            Bytes::from("binary_protocol"),
            Frame::Bulk(Some(Bytes::from("planned"))),
        );
        return Frame::Map(map);
    }

    match args[0].to_uppercase().as_str() {
        "STATS" => {
            let stats = get_memcached_stats();
            let mut map = HashMap::new();
            map.insert(
                Bytes::from("get_commands"),
                Frame::Integer(stats.get_commands as i64),
            );
            map.insert(
                Bytes::from("set_commands"),
                Frame::Integer(stats.set_commands as i64),
            );
            map.insert(
                Bytes::from("delete_commands"),
                Frame::Integer(stats.delete_commands as i64),
            );
            map.insert(Bytes::from("hits"), Frame::Integer(stats.hits as i64));
            map.insert(Bytes::from("misses"), Frame::Integer(stats.misses as i64));
            Frame::Map(map)
        }
        _ => err_frame(&format!(
            "unknown PROTOCOL MEMCACHED subcommand '{}'",
            args[0]
        )),
    }
}

/// PROTOCOL GRPC [METHODS|DESCRIPTOR]
fn protocol_grpc(args: &[String]) -> Frame {
    if args.is_empty() {
        let mut map = HashMap::new();
        map.insert(
            Bytes::from("service"),
            Frame::Bulk(Some(Bytes::from(GrpcServiceDefinition::service_name()))),
        );
        map.insert(
            Bytes::from("methods_count"),
            Frame::Integer(GrpcServiceDefinition::methods().len() as i64),
        );
        return Frame::Map(map);
    }

    match args[0].to_uppercase().as_str() {
        "METHODS" => {
            let service = get_grpc_service();
            let methods = service.list_methods();
            let frames: Vec<Frame> = methods
                .iter()
                .map(|m| {
                    let fields = vec![
                        Frame::Bulk(Some(Bytes::from("name"))),
                        Frame::Bulk(Some(Bytes::from(m.name.clone()))),
                        Frame::Bulk(Some(Bytes::from("request_type"))),
                        Frame::Bulk(Some(Bytes::from(m.request_type.clone()))),
                        Frame::Bulk(Some(Bytes::from("response_type"))),
                        Frame::Bulk(Some(Bytes::from(m.response_type.clone()))),
                        Frame::Bulk(Some(Bytes::from("streaming"))),
                        Frame::Boolean(m.is_streaming),
                    ];
                    Frame::Array(Some(fields))
                })
                .collect();
            Frame::Array(Some(frames))
        }
        "DESCRIPTOR" => {
            let service = get_grpc_service();
            Frame::Bulk(Some(Bytes::from(service.service_descriptor())))
        }
        _ => err_frame(&format!("unknown PROTOCOL GRPC subcommand '{}'", args[0])),
    }
}

/// PROTOCOL AMQP [EXCHANGES|QUEUES|STATS]
fn protocol_amqp(args: &[String]) -> Frame {
    if args.is_empty() {
        let adapter = get_amqp_adapter();
        let stats = adapter.stats();
        let mut map = HashMap::new();
        map.insert(
            Bytes::from("port"),
            Frame::Integer(AmqpConfig::default().port as i64),
        );
        map.insert(
            Bytes::from("exchanges"),
            Frame::Integer(stats.exchanges as i64),
        );
        map.insert(Bytes::from("queues"), Frame::Integer(stats.queues as i64));
        return Frame::Map(map);
    }

    match args[0].to_uppercase().as_str() {
        "EXCHANGES" => {
            let adapter = get_amqp_adapter();
            let exchanges = adapter.list_exchanges();
            let frames: Vec<Frame> = exchanges
                .iter()
                .map(|ex| {
                    let fields = vec![
                        Frame::Bulk(Some(Bytes::from("name"))),
                        Frame::Bulk(Some(Bytes::from(ex.name.clone()))),
                        Frame::Bulk(Some(Bytes::from("type"))),
                        Frame::Bulk(Some(Bytes::from(ex.exchange_type.to_string()))),
                        Frame::Bulk(Some(Bytes::from("durable"))),
                        Frame::Boolean(ex.durable),
                        Frame::Bulk(Some(Bytes::from("bindings"))),
                        Frame::Integer(ex.bindings.len() as i64),
                    ];
                    Frame::Array(Some(fields))
                })
                .collect();
            Frame::Array(Some(frames))
        }
        "QUEUES" => {
            let adapter = get_amqp_adapter();
            let queues = adapter.list_queues();
            let frames: Vec<Frame> = queues
                .iter()
                .map(|q| {
                    let fields = vec![
                        Frame::Bulk(Some(Bytes::from("name"))),
                        Frame::Bulk(Some(Bytes::from(q.name.clone()))),
                        Frame::Bulk(Some(Bytes::from("durable"))),
                        Frame::Boolean(q.durable),
                        Frame::Bulk(Some(Bytes::from("messages"))),
                        Frame::Integer(q.messages as i64),
                        Frame::Bulk(Some(Bytes::from("consumers"))),
                        Frame::Integer(q.consumers as i64),
                    ];
                    Frame::Array(Some(fields))
                })
                .collect();
            Frame::Array(Some(frames))
        }
        "STATS" => {
            let adapter = get_amqp_adapter();
            let stats = adapter.stats();
            let mut map = HashMap::new();
            map.insert(
                Bytes::from("exchanges"),
                Frame::Integer(stats.exchanges as i64),
            );
            map.insert(Bytes::from("queues"), Frame::Integer(stats.queues as i64));
            map.insert(
                Bytes::from("messages_published"),
                Frame::Integer(stats.messages_published as i64),
            );
            map.insert(
                Bytes::from("messages_delivered"),
                Frame::Integer(stats.messages_delivered as i64),
            );
            map.insert(
                Bytes::from("messages_acked"),
                Frame::Integer(stats.messages_acked as i64),
            );
            Frame::Map(map)
        }
        _ => err_frame(&format!("unknown PROTOCOL AMQP subcommand '{}'", args[0])),
    }
}

/// PROTOCOL HELP
fn protocol_help() -> Frame {
    let help = vec![
        "PROTOCOL STATUS - Show all protocol adapters status",
        "PROTOCOL MEMCACHED [STATS] - Memcached protocol info/stats",
        "PROTOCOL GRPC [METHODS|DESCRIPTOR] - gRPC service info",
        "PROTOCOL AMQP [EXCHANGES|QUEUES|STATS] - AMQP adapter info",
        "PROTOCOL HELP - This help text",
    ];

    Frame::Array(Some(
        help.into_iter()
            .map(|s| Frame::Bulk(Some(Bytes::from(s.to_string()))))
            .collect(),
    ))
}
