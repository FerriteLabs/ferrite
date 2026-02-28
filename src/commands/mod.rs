//! Command module for Ferrite
//!
//! This module implements Redis command parsing and execution.

mod bitmap;
/// Blocking list commands
pub mod blocking;
/// Extended ACL commands (LOG, CAT, GENPASS, DRYRUN)
pub mod acl_commands;
/// Extended CLIENT commands (INFO, NO-EVICT, NO-TOUCH)
pub mod client_commands;
mod executor;
mod geo;
/// Command handlers organized by category
pub mod handlers;
mod hashes;
mod hyperloglog;
mod keys;
/// LATENCY command family with LatencyTracker
pub mod latency;
mod lists;
/// Extended OBJECT commands (FREQ, IDLETIME, HELP)
pub mod object_commands;
mod parser;
/// Pub/Sub commands
pub mod pubsub;
mod scan;
/// Lua scripting commands
#[cfg(feature = "scripting")]
pub mod scripting;
#[cfg(not(feature = "scripting"))]
pub mod scripting_stub;
mod sets;
pub mod sorted_sets;
mod streams;
/// Extended Streams commands (XINFO dispatcher, XAUTOCLAIM, XPENDING extended, XCLAIM extended)
pub mod streams_extended;
/// Extended CONFIG / COMMAND sub-commands
pub mod config_commands;
mod strings;

pub use blocking::{
    notify_sorted_set_add, notify_stream_add, BlockingListManager, BlockingSortedSetManager,
    BlockingStreamManager, SharedBlockingListManager, SharedBlockingSortedSetManager,
    SharedBlockingStreamManager,
};
pub use executor::CommandExecutor;
pub use parser::Command;
#[cfg(feature = "scripting")]
pub use scripting::{ScriptCache, ScriptExecutor};
#[cfg(not(feature = "scripting"))]
pub use scripting_stub::{ScriptCache, ScriptExecutor};
