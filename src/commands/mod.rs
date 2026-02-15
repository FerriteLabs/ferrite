//! Command module for Ferrite
//!
//! This module implements Redis command parsing and execution.

mod bitmap;
/// Blocking list commands
pub mod blocking;
mod executor;
mod geo;
/// Command handlers organized by category
pub mod handlers;
mod hashes;
mod hyperloglog;
mod keys;
mod lists;
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
