//! Server module for Ferrite
//!
//! This module implements the TCP server and connection handling.

/// AMQP protocol adapter for RabbitMQ-compatible message consumption
pub mod amqp_adapter;
pub mod cluster_redirect;
mod connection;
/// Native connection pooler and multiplexer
pub mod connection_pool;
/// gRPC service definition and handler
pub mod grpc_service;
mod handler;
pub mod http_gateway;
pub mod http_gateway_integration;
mod listener;
/// Memcached text/binary protocol support
pub mod memcached_protocol;
/// Transparent Redis proxy with connection pooling
pub mod proxy;
/// Zero-config smart proxy with Ferrite capability injection
pub mod smart_proxy;
/// TLS support for encrypted connections
#[cfg(feature = "tls")]
pub mod tls;
#[cfg(not(feature = "tls"))]
#[path = "tls_stub.rs"]
pub mod tls;

pub use crate::runtime::{
    ClientInfo, ClientRegistry, ConnectionId, PauseMode, SharedClientRegistry, SharedSlowLog,
    SharedSubscriptionManager, SharedWatchRegistry, SlowLog, SlowLogEntry, SubscriptionManager,
    WatchHandle, WatchRegistry,
};
pub use connection::{Connection, ProtocolVersion};
pub use listener::Server;
pub use tls::{TlsConfig, TlsError, TlsListener};
