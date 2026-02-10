//! Shared runtime components
//!
//! These modules are shared between the server and command execution layers.

/// Client registry for tracking connected clients
pub mod clients;
/// Slow query log for debugging
pub mod slowlog;
/// Pub/Sub subscription management
pub mod subscription;
/// WATCH registry for optimistic locking
pub mod watch;

pub use clients::{ClientInfo, ClientRegistry, PauseMode, SharedClientRegistry};
pub use slowlog::{SharedSlowLog, SlowLog, SlowLogEntry};
pub use subscription::{SharedSubscriptionManager, SubscriptionManager};
pub use watch::{ConnectionId, SharedWatchRegistry, WatchHandle, WatchRegistry};
