//! Shared runtime components
//!
//! These modules are shared between the server and command execution layers.

/// Client registry for tracking connected clients
pub mod clients;
/// Runtime configuration hot-reload manager.
pub mod config_manager;
/// Systematic error recovery and backpressure management.
pub mod error_recovery;
/// Redis-compatible keyspace notifications
pub mod keyspace_notify;
/// Latency event tracking for LATENCY command family
pub mod latency;
/// Graceful shutdown coordinator and crash recovery engine.
pub mod shutdown;
/// Slow query log for debugging
pub mod slowlog;
/// Pub/Sub subscription management
pub mod subscription;
/// Client-side caching via server-assisted key invalidation
pub mod tracking;
/// WATCH registry for optimistic locking
pub mod watch;

pub use clients::{ClientInfo, ClientRegistry, PauseMode, SharedClientRegistry};
pub use keyspace_notify::{KeyspaceNotifier, SharedKeyspaceNotifier};
pub use latency::LatencyTracker;
pub use slowlog::{SharedSlowLog, SlowLog, SlowLogEntry};
pub use subscription::{SharedSubscriptionManager, SubscriptionManager};
pub use tracking::{SharedTrackingTable, TrackingMode, TrackingTable};
pub use watch::{ConnectionId, SharedWatchRegistry, WatchHandle, WatchRegistry};
