//! Server module for Ferrite
//!
//! This module implements the TCP server and connection handling.

pub mod cluster_redirect;
mod connection;
mod handler;
mod listener;
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
