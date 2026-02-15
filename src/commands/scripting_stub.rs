//! Stub scripting module when the `scripting` feature is disabled.
//!
//! Provides no-op types that return errors for all scripting commands.

use std::sync::Arc;

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::Store;

pub type ScriptHash = String;

#[derive(Default)]
pub struct ScriptCache;

impl ScriptCache {
    pub fn new() -> Self {
        Self
    }
}

pub struct ScriptExecutor;

impl ScriptExecutor {
    pub fn new(_cache: Arc<ScriptCache>, _store: Arc<Store>) -> Self {
        Self
    }

    pub fn eval(&self, _db: u8, _script: &str, _keys: &[Bytes], _args: &[Bytes]) -> Frame {
        Frame::error("ERR scripting is not available (compile with --features scripting)")
    }

    pub fn evalsha(&self, _db: u8, _sha1: &str, _keys: &[Bytes], _args: &[Bytes]) -> Frame {
        Frame::error("ERR scripting is not available (compile with --features scripting)")
    }

    pub fn script_load(&self, _script: &str) -> Frame {
        Frame::error("ERR scripting is not available (compile with --features scripting)")
    }

    pub fn script_exists(&self, _hashes: &[String]) -> Frame {
        Frame::error("ERR scripting is not available (compile with --features scripting)")
    }

    pub fn script_flush(&self) -> Frame {
        Frame::error("ERR scripting is not available (compile with --features scripting)")
    }
}
