//! Command execution engine.
//!
//! This module implements [`CommandExecutor`], which processes parsed [`Command`]
//! values and dispatches them to the appropriate handler functions.
//!
//! # Architecture
//!
//! The executor owns shared state (store, config, ACL, blocking manager, etc.)
//! and exposes two entry points:
//!
//! - [`CommandExecutor::execute`] — execute without ACL checks (internal use).
//! - [`CommandExecutor::execute_with_acl`] — execute with ACL permission checks.
//!
//! Internally, `execute_internal` contains a single large `match` over all
//! [`Command`] variants, delegating to handler functions in sibling modules
//! (e.g., `strings::incr`, `lists::lpush`, `sorted_sets::zadd`).
//!
//! # Adding a new command
//!
//! 1. Add the `Command` variant in [`super::parser`] (`parser.rs`).
//! 2. Add a match arm in `execute_internal` (search for the section comment
//!    matching the command category, e.g. `// String commands`).
//! 3. Add a match arm in `meta` for ACL metadata (same section pattern).
//! 4. Implement the handler in the appropriate sibling module.
//! 5. Add unit and integration tests.

mod advanced_ops;
mod cluster_ops;
mod key_ops;
mod meta;
mod server_ops;
mod string_ops;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use bytes::Bytes;

use crate::auth::{Permission, SharedAcl};
use crate::cluster::ClusterStateManager;
use crate::config::SharedConfig;
use crate::protocol::Frame;
use crate::runtime::{
    ClientRegistry, SharedClientRegistry, SharedSlowLog, SharedSubscriptionManager, SlowLog,
};
use crate::storage::Store;

use super::bitmap;
use super::blocking::{self, SharedBlockingListManager};
use super::hashes;
use super::keys;
use super::lists;
use super::parser::Command;
use super::pubsub;
use super::sets;
use super::sorted_sets;
use super::streams;
use super::strings;
use super::{ScriptCache, ScriptExecutor};

/// Command metadata for ACL checking
pub(crate) struct CommandMeta {
    pub(crate) name: &'static str,
    pub(crate) category: &'static str,
    pub(crate) keys: Vec<Bytes>,
    pub(crate) permission: Permission,
}

struct CommandInfo {
    name: &'static str,
    arity: i64,
    flags: &'static [&'static str],
    first_key: i64,
    last_key: i64,
    step: i64,
}

const COMMAND_INFO: &[CommandInfo] = &[
    CommandInfo {
        name: "get",
        arity: 2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandInfo {
        name: "set",
        arity: -3,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandInfo {
        name: "del",
        arity: -2,
        flags: &["write"],
        first_key: 1,
        last_key: -1,
        step: 1,
    },
    CommandInfo {
        name: "mget",
        arity: -2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: -1,
        step: 1,
    },
    CommandInfo {
        name: "mset",
        arity: -3,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: -1,
        step: 2,
    },
    CommandInfo {
        name: "incr",
        arity: 2,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandInfo {
        name: "decr",
        arity: 2,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandInfo {
        name: "lpush",
        arity: -3,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandInfo {
        name: "rpush",
        arity: -3,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandInfo {
        name: "lpop",
        arity: -2,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandInfo {
        name: "rpop",
        arity: -2,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandInfo {
        name: "lrange",
        arity: 4,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandInfo {
        name: "hset",
        arity: -4,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandInfo {
        name: "hget",
        arity: 3,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandInfo {
        name: "hdel",
        arity: -3,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandInfo {
        name: "sadd",
        arity: -3,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandInfo {
        name: "srem",
        arity: -3,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandInfo {
        name: "smembers",
        arity: 2,
        flags: &["readonly", "sort_for_script"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandInfo {
        name: "zadd",
        arity: -4,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandInfo {
        name: "zrange",
        arity: -4,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandInfo {
        name: "ping",
        arity: -1,
        flags: &["fast", "stale"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandInfo {
        name: "echo",
        arity: 2,
        flags: &["fast"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandInfo {
        name: "info",
        arity: -1,
        flags: &["stale", "fast"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandInfo {
        name: "dbsize",
        arity: 1,
        flags: &["readonly", "fast"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandInfo {
        name: "flushdb",
        arity: -1,
        flags: &["write"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandInfo {
        name: "flushall",
        arity: -1,
        flags: &["write"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandInfo {
        name: "select",
        arity: 2,
        flags: &["fast"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandInfo {
        name: "keys",
        arity: 2,
        flags: &["readonly", "sort_for_script"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandInfo {
        name: "scan",
        arity: -2,
        flags: &["readonly"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandInfo {
        name: "expire",
        arity: 3,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandInfo {
        name: "ttl",
        arity: 2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandInfo {
        name: "type",
        arity: 2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandInfo {
        name: "exists",
        arity: -2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: -1,
        step: 1,
    },
    CommandInfo {
        name: "publish",
        arity: 3,
        flags: &["pubsub", "fast"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandInfo {
        name: "subscribe",
        arity: -2,
        flags: &["pubsub", "noscript"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandInfo {
        name: "multi",
        arity: 1,
        flags: &["fast"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandInfo {
        name: "exec",
        arity: 1,
        flags: &["slow"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandInfo {
        name: "discard",
        arity: 1,
        flags: &["fast"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandInfo {
        name: "plugin",
        arity: -2,
        flags: &["admin"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
];

const COMMAND_NAMES: &[&str] = &[
    "get",
    "set",
    "del",
    "mget",
    "mset",
    "incr",
    "decr",
    "incrby",
    "decrby",
    "incrbyfloat",
    "append",
    "strlen",
    "getrange",
    "setrange",
    "setnx",
    "setex",
    "psetex",
    "getset",
    "getdel",
    "getex",
    "msetnx",
    "lpush",
    "rpush",
    "lpop",
    "rpop",
    "lrange",
    "llen",
    "lindex",
    "lset",
    "lrem",
    "linsert",
    "lpushx",
    "rpushx",
    "blpop",
    "brpop",
    "lmove",
    "blmove",
    "ltrim",
    "hset",
    "hget",
    "hmset",
    "hmget",
    "hdel",
    "hexists",
    "hgetall",
    "hkeys",
    "hvals",
    "hlen",
    "hsetnx",
    "hincrby",
    "hincrbyfloat",
    "hstrlen",
    "hrandfield",
    "sadd",
    "srem",
    "smembers",
    "sismember",
    "scard",
    "sunion",
    "sinter",
    "sdiff",
    "sunionstore",
    "sinterstore",
    "sdiffstore",
    "spop",
    "srandmember",
    "smove",
    "zadd",
    "zrem",
    "zscore",
    "zcard",
    "zrange",
    "zrank",
    "zcount",
    "zincrby",
    "zrangebyscore",
    "zrevrangebyscore",
    "zrevrange",
    "zrevrank",
    "zpopmin",
    "zpopmax",
    "bzpopmin",
    "bzpopmax",
    "zunionstore",
    "zinterstore",
    "xadd",
    "xlen",
    "xrange",
    "xrevrange",
    "xread",
    "xdel",
    "xtrim",
    "xinfo",
    "xgroup",
    "xack",
    "xpending",
    "xclaim",
    "xautoclaim",
    "xreadgroup",
    "ping",
    "echo",
    "select",
    "info",
    "dbsize",
    "flushdb",
    "flushall",
    "time",
    "command",
    "debug",
    "memory",
    "config",
    "monitor",
    "reset",
    "role",
    "module",
    "plugin",
    "bgsave",
    "save",
    "bgrewriteaof",
    "lastsave",
    "slowlog",
    "latency",
    "shutdown",
    "swapdb",
    "move",
    "migrate",
    "client",
    "quit",
    "hello",
    "replicaof",
    "slaveof",
    "replconf",
    "psync",
    "auth",
    "acl",
    "cluster",
    "eval",
    "evalsha",
    "script",
    "eval_ro",
    "evalsha_ro",
    "function",
    "fcall",
    "fcall_ro",
    "publish",
    "subscribe",
    "unsubscribe",
    "psubscribe",
    "punsubscribe",
    "multi",
    "exec",
    "discard",
    "watch",
    "unwatch",
    "expire",
    "pexpire",
    "expireat",
    "pexpireat",
    "ttl",
    "pttl",
    "persist",
    "type",
    "exists",
    "keys",
    "scan",
    "rename",
    "renamenx",
    "copy",
    "object",
    "dump",
    "restore",
    "sort",
    "touch",
    "unlink",
    "wait",
    "geoadd",
    "geopos",
    "geodist",
    "geohash",
    "georadius",
    "georadiusbymember",
    "geosearch",
    "pfadd",
    "pfcount",
    "pfmerge",
    "getbit",
    "setbit",
    "bitcount",
    "bitop",
    "bitpos",
    "bitfield",
    "bitfield_ro",
    "hscan",
    "sscan",
    "zscan",
    "lcs",
    "lpos",
    "lmpop",
    "blmpop",
    "bzmpop",
];

impl CommandInfo {
    fn to_frame(&self) -> Frame {
        Frame::array(vec![
            Frame::bulk(self.name),
            Frame::Integer(self.arity),
            Frame::array(self.flags.iter().map(|flag| Frame::bulk(*flag)).collect()),
            Frame::Integer(self.first_key),
            Frame::Integer(self.last_key),
            Frame::Integer(self.step),
        ])
    }
}

fn command_info_by_name(name: &str) -> Option<&'static CommandInfo> {
    COMMAND_INFO
        .iter()
        .find(|info| info.name.eq_ignore_ascii_case(name))
}

/// Command executor
pub struct CommandExecutor {
    /// Shared storage
    store: Arc<Store>,
    /// Shared pub/sub subscription manager
    pubsub_manager: SharedSubscriptionManager,
    /// Shared ACL manager
    acl: SharedAcl,
    /// Lua script executor
    script_executor: ScriptExecutor,
    /// Blocking list manager for BLPOP/BRPOP
    blocking_manager: SharedBlockingListManager,
    /// Blocking stream manager for XREAD BLOCK/XREADGROUP BLOCK
    blocking_stream_manager: blocking::SharedBlockingStreamManager,
    /// Blocking sorted set manager for BZPOPMIN/BZPOPMAX/BZMPOP
    blocking_zset_manager: blocking::SharedBlockingSortedSetManager,
    /// Shared configuration for hot reload
    config: Option<SharedConfig>,
    /// Slow query log
    slowlog: SharedSlowLog,
    /// Client registry for tracking connected clients
    client_registry: SharedClientRegistry,
    /// Cluster state manager for Redis Cluster protocol compatibility
    cluster_state: Option<Arc<ClusterStateManager>>,
}

impl CommandExecutor {
    /// Create a new command executor
    pub fn new(
        store: Arc<Store>,
        pubsub_manager: SharedSubscriptionManager,
        acl: SharedAcl,
        blocking_manager: SharedBlockingListManager,
        blocking_stream_manager: blocking::SharedBlockingStreamManager,
        blocking_zset_manager: blocking::SharedBlockingSortedSetManager,
    ) -> Self {
        let script_cache = Arc::new(ScriptCache::new());
        let script_executor = ScriptExecutor::new(script_cache, store.clone());
        Self {
            store,
            pubsub_manager,
            acl,
            script_executor,
            blocking_manager,
            blocking_stream_manager,
            blocking_zset_manager,
            config: None,
            slowlog: Arc::new(SlowLog::default()),
            client_registry: Arc::new(ClientRegistry::new()),
            cluster_state: None,
        }
    }

    /// Create a new command executor with shared configuration
    pub fn with_config(
        store: Arc<Store>,
        pubsub_manager: SharedSubscriptionManager,
        acl: SharedAcl,
        blocking_manager: SharedBlockingListManager,
        blocking_stream_manager: blocking::SharedBlockingStreamManager,
        blocking_zset_manager: blocking::SharedBlockingSortedSetManager,
        config: SharedConfig,
    ) -> Self {
        let script_cache = Arc::new(ScriptCache::new());
        let script_executor = ScriptExecutor::new(script_cache, store.clone());
        Self {
            store,
            pubsub_manager,
            acl,
            script_executor,
            blocking_manager,
            blocking_stream_manager,
            blocking_zset_manager,
            config: Some(config),
            slowlog: Arc::new(SlowLog::default()),
            client_registry: Arc::new(ClientRegistry::new()),
            cluster_state: None,
        }
    }

    /// Create a new command executor with shared configuration, slowlog, and client registry
    #[allow(clippy::too_many_arguments)]
    pub fn with_shared_state(
        store: Arc<Store>,
        pubsub_manager: SharedSubscriptionManager,
        acl: SharedAcl,
        blocking_manager: SharedBlockingListManager,
        blocking_stream_manager: blocking::SharedBlockingStreamManager,
        blocking_zset_manager: blocking::SharedBlockingSortedSetManager,
        config: SharedConfig,
        slowlog: SharedSlowLog,
        client_registry: SharedClientRegistry,
    ) -> Self {
        let script_cache = Arc::new(ScriptCache::new());
        let script_executor = ScriptExecutor::new(script_cache, store.clone());
        Self {
            store,
            pubsub_manager,
            acl,
            script_executor,
            blocking_manager,
            blocking_stream_manager,
            blocking_zset_manager,
            config: Some(config),
            slowlog,
            client_registry,
            cluster_state: None,
        }
    }

    /// Set the cluster state manager for Redis Cluster protocol compatibility.
    ///
    /// When set, CLUSTER subcommands will use real cluster state instead of
    /// standalone-mode defaults, and COUNTKEYSINSLOT / GETKEYSINSLOT will
    /// scan the store for keys in the requested slot.
    pub fn set_cluster_state(&mut self, cluster_state: Arc<ClusterStateManager>) {
        self.cluster_state = Some(cluster_state);
    }

    /// Get the cluster state manager, if configured.
    pub fn cluster_state_manager(&self) -> Option<&Arc<ClusterStateManager>> {
        self.cluster_state.as_ref()
    }

    /// Get the slow log
    pub fn slowlog(&self) -> &SharedSlowLog {
        &self.slowlog
    }

    /// Get the client registry
    pub fn client_registry(&self) -> &SharedClientRegistry {
        &self.client_registry
    }

    /// Get the blocking list manager
    pub fn blocking_manager(&self) -> &SharedBlockingListManager {
        &self.blocking_manager
    }

    /// Get the blocking stream manager
    pub fn blocking_stream_manager(&self) -> &blocking::SharedBlockingStreamManager {
        &self.blocking_stream_manager
    }

    /// Get the blocking sorted set manager
    pub fn blocking_zset_manager(&self) -> &blocking::SharedBlockingSortedSetManager {
        &self.blocking_zset_manager
    }

    /// Check ACL permissions for a command
    async fn check_acl(&self, username: &str, command: &Command) -> Result<(), Frame> {
        let meta = command.meta();

        // Check command permission
        if let Err(e) = self
            .acl
            .check_command(username, meta.name, Some(meta.category))
            .await
        {
            return Err(Frame::error(e.to_string()));
        }

        // Check key permissions
        for key in &meta.keys {
            let key_str = String::from_utf8_lossy(key);
            if let Err(e) = self
                .acl
                .check_key(username, &key_str, meta.permission)
                .await
            {
                return Err(Frame::error(e.to_string()));
            }
        }

        Ok(())
    }

    /// Execute a command and return the response frame
    ///
    /// This version does not perform ACL checks (for backward compatibility).
    /// Use `execute_with_acl` for ACL-enabled execution.
    pub async fn execute(&self, command: Command, db: u8) -> Frame {
        self.execute_internal(command, db).await
    }

    /// Execute a command with ACL checking
    ///
    /// This method performs ACL checks before executing the command.
    /// Returns an error frame if permission is denied.
    pub async fn execute_with_acl(&self, command: Command, db: u8, username: &str) -> Frame {
        // Check ACL permissions first
        if let Err(error_frame) = self.check_acl(username, &command).await {
            return error_frame;
        }

        // Execute the command
        self.execute_internal(command, db).await
    }

    /// Internal command execution (without ACL checks)
    async fn execute_internal(&self, command: Command, db: u8) -> Frame {
        match command {
            // String commands
            Command::Get { key } => self.get(db, &key),
            Command::Set {
                key,
                value,
                options,
            } => self.set(db, key, value, options),
            Command::Del { keys } => self.del(db, &keys),
            Command::Exists { keys } => self.exists(db, &keys),
            Command::Incr { key } => strings::incr(&self.store, db, &key),
            Command::Decr { key } => strings::decr(&self.store, db, &key),
            Command::IncrBy { key, delta } => strings::incrby(&self.store, db, &key, delta),
            Command::DecrBy { key, delta } => strings::decrby(&self.store, db, &key, delta),
            Command::MGet { keys } => self.mget(db, &keys),
            Command::MSet { pairs } => self.mset(db, pairs),
            Command::Append { key, value } => strings::append(&self.store, db, &key, &value),
            Command::StrLen { key } => strings::strlen(&self.store, db, &key),
            Command::GetRange { key, start, end } => {
                strings::getrange(&self.store, db, &key, start, end)
            }
            Command::SetRange { key, offset, value } => {
                strings::setrange(&self.store, db, &key, offset, &value)
            }
            Command::SetNx { key, value } => strings::setnx(&self.store, db, &key, &value),
            Command::SetEx {
                key,
                seconds,
                value,
            } => strings::setex(&self.store, db, &key, seconds, &value),
            Command::PSetEx {
                key,
                milliseconds,
                value,
            } => strings::psetex(&self.store, db, &key, milliseconds, &value),
            Command::GetSet { key, value } => strings::getset(&self.store, db, &key, &value),
            Command::GetDel { key } => strings::getdel(&self.store, db, &key),
            Command::IncrByFloat { key, delta } => {
                strings::incrbyfloat(&self.store, db, &key, delta)
            }
            Command::GetEx {
                key,
                ex,
                px,
                exat,
                pxat,
                persist,
            } => strings::getex(&self.store, db, &key, ex, px, exat, pxat, persist),
            Command::MSetNx { pairs } => strings::msetnx(&self.store, db, &pairs),
            Command::Lcs {
                key1,
                key2,
                len_only,
                idx,
                min_match_len,
                with_match_len,
            } => strings::lcs(
                &self.store,
                db,
                &key1,
                &key2,
                len_only,
                idx,
                min_match_len,
                with_match_len,
            ),

            // List commands
            Command::LPush { key, values } => {
                let result = lists::lpush(&self.store, db, &key, &values);
                // Notify blocking manager that elements were pushed
                if matches!(result, Frame::Integer(n) if n > 0) {
                    blocking::notify_list_push(&self.blocking_manager, db, &key);
                }
                result
            }
            Command::RPush { key, values } => {
                let result = lists::rpush(&self.store, db, &key, &values);
                // Notify blocking manager that elements were pushed
                if matches!(result, Frame::Integer(n) if n > 0) {
                    blocking::notify_list_push(&self.blocking_manager, db, &key);
                }
                result
            }
            Command::LPop { key, count } => lists::lpop(&self.store, db, &key, count),
            Command::RPop { key, count } => lists::rpop(&self.store, db, &key, count),
            Command::LRange { key, start, stop } => {
                lists::lrange(&self.store, db, &key, start, stop)
            }
            Command::LLen { key } => lists::llen(&self.store, db, &key),
            Command::LIndex { key, index } => lists::lindex(&self.store, db, &key, index),
            Command::LSet { key, index, value } => {
                lists::lset(&self.store, db, &key, index, &value)
            }
            Command::LRem { key, count, value } => {
                lists::lrem(&self.store, db, &key, count, &value)
            }
            Command::LInsert {
                key,
                before,
                pivot,
                value,
            } => lists::linsert(&self.store, db, &key, before, &pivot, &value),
            Command::LPushX { key, values } => {
                let result = lists::lpushx(&self.store, db, &key, &values);
                // Notify blocking manager if elements were pushed (result > 0 means list existed)
                if matches!(result, Frame::Integer(n) if n > 0) {
                    blocking::notify_list_push(&self.blocking_manager, db, &key);
                }
                result
            }
            Command::RPushX { key, values } => {
                let result = lists::rpushx(&self.store, db, &key, &values);
                // Notify blocking manager if elements were pushed (result > 0 means list existed)
                if matches!(result, Frame::Integer(n) if n > 0) {
                    blocking::notify_list_push(&self.blocking_manager, db, &key);
                }
                result
            }
            Command::BLPop { keys, timeout } => {
                blocking::blpop(&self.store, &self.blocking_manager, db, &keys, timeout).await
            }
            Command::BRPop { keys, timeout } => {
                blocking::brpop(&self.store, &self.blocking_manager, db, &keys, timeout).await
            }
            Command::BRPopLPush {
                source,
                destination,
                timeout,
            } => {
                blocking::brpoplpush(
                    &self.store,
                    &self.blocking_manager,
                    db,
                    &source,
                    &destination,
                    timeout,
                )
                .await
            }
            Command::BLMove {
                source,
                destination,
                wherefrom,
                whereto,
                timeout,
            } => {
                blocking::blmove(
                    &self.store,
                    &self.blocking_manager,
                    db,
                    &source,
                    &destination,
                    wherefrom,
                    whereto,
                    timeout,
                )
                .await
            }
            Command::LMove {
                source,
                destination,
                wherefrom,
                whereto,
            } => lists::lmove(&self.store, db, &source, &destination, wherefrom, whereto),
            Command::LTrim { key, start, stop } => lists::ltrim(&self.store, db, &key, start, stop),
            Command::LPos {
                key,
                element,
                rank,
                count,
                maxlen,
            } => lists::lpos(&self.store, db, &key, &element, rank, count, maxlen),
            Command::RPopLPush {
                source,
                destination,
            } => lists::rpoplpush(&self.store, db, &source, &destination),
            Command::LMPop {
                keys,
                direction,
                count,
            } => lists::lmpop(&self.store, db, &keys, direction, count),
            Command::BLMPop {
                timeout,
                keys,
                direction,
                count,
            } => {
                blocking::blmpop(
                    &self.store,
                    &self.blocking_manager,
                    db,
                    &keys,
                    direction,
                    count,
                    timeout,
                )
                .await
            }

            // Hash commands
            Command::HSet { key, pairs } => hashes::hset(&self.store, db, &key, &pairs),
            Command::HGet { key, field } => hashes::hget(&self.store, db, &key, &field),
            Command::HMSet { key, pairs } => hashes::hmset(&self.store, db, &key, &pairs),
            Command::HMGet { key, fields } => hashes::hmget(&self.store, db, &key, &fields),
            Command::HDel { key, fields } => hashes::hdel(&self.store, db, &key, &fields),
            Command::HExists { key, field } => hashes::hexists(&self.store, db, &key, &field),
            Command::HGetAll { key } => hashes::hgetall(&self.store, db, &key),
            Command::HKeys { key } => hashes::hkeys(&self.store, db, &key),
            Command::HVals { key } => hashes::hvals(&self.store, db, &key),
            Command::HLen { key } => hashes::hlen(&self.store, db, &key),
            Command::HSetNx { key, field, value } => {
                hashes::hsetnx(&self.store, db, &key, &field, &value)
            }
            Command::HIncrBy { key, field, delta } => {
                hashes::hincrby(&self.store, db, &key, &field, delta)
            }
            Command::HIncrByFloat { key, field, delta } => {
                hashes::hincrbyfloat(&self.store, db, &key, &field, delta)
            }
            Command::HStrLen { key, field } => hashes::hstrlen(&self.store, db, &key, &field),
            Command::HRandField {
                key,
                count,
                withvalues,
            } => hashes::hrandfield(&self.store, db, &key, count, withvalues),
            Command::HScan {
                key,
                cursor,
                pattern,
                count,
                novalues,
            } => hashes::hscan(
                &self.store,
                db,
                &key,
                cursor,
                pattern.as_deref(),
                count,
                novalues,
            ),

            // Set commands
            Command::SAdd { key, members } => sets::sadd(&self.store, db, &key, &members),
            Command::SRem { key, members } => sets::srem(&self.store, db, &key, &members),
            Command::SMembers { key } => sets::smembers(&self.store, db, &key),
            Command::SIsMember { key, member } => sets::sismember(&self.store, db, &key, &member),
            Command::SMIsMember { key, members } => {
                sets::smismember(&self.store, db, &key, &members)
            }
            Command::SCard { key } => sets::scard(&self.store, db, &key),
            Command::SUnion { keys } => sets::sunion(&self.store, db, &keys),
            Command::SUnionStore { destination, keys } => {
                sets::sunionstore(&self.store, db, &destination, &keys)
            }
            Command::SInter { keys } => sets::sinter(&self.store, db, &keys),
            Command::SInterStore { destination, keys } => {
                sets::sinterstore(&self.store, db, &destination, &keys)
            }
            Command::SInterCard { keys, limit } => sets::sintercard(&self.store, db, &keys, limit),
            Command::SDiff { keys } => sets::sdiff(&self.store, db, &keys),
            Command::SDiffStore { destination, keys } => {
                sets::sdiffstore(&self.store, db, &destination, &keys)
            }
            Command::SPop { key, count } => sets::spop(&self.store, db, &key, count),
            Command::SRandMember { key, count } => sets::srandmember(&self.store, db, &key, count),
            Command::SMove {
                source,
                destination,
                member,
            } => sets::smove(&self.store, db, &source, &destination, &member),

            // Sorted Set commands
            Command::ZAdd {
                key,
                options,
                pairs,
            } => {
                let result = sorted_sets::zadd(&self.store, db, &key, &options, &pairs);
                // Notify blocking sorted set manager when elements are added
                if let Frame::Integer(added) = &result {
                    if *added > 0 {
                        blocking::notify_sorted_set_add(&self.blocking_zset_manager, db, &key);
                    }
                }
                result
            }
            Command::ZRem { key, members } => sorted_sets::zrem(&self.store, db, &key, &members),
            Command::ZScore { key, member } => sorted_sets::zscore(&self.store, db, &key, &member),
            Command::ZCard { key } => sorted_sets::zcard(&self.store, db, &key),
            Command::ZCount { key, min, max } => {
                sorted_sets::zcount(&self.store, db, &key, min, max)
            }
            Command::ZRank { key, member } => sorted_sets::zrank(&self.store, db, &key, &member),
            Command::ZRevRank { key, member } => {
                sorted_sets::zrevrank(&self.store, db, &key, &member)
            }
            Command::ZRange {
                key,
                start,
                stop,
                with_scores,
            } => sorted_sets::zrange(&self.store, db, &key, start, stop, with_scores),
            Command::ZRevRange {
                key,
                start,
                stop,
                with_scores,
            } => sorted_sets::zrevrange(&self.store, db, &key, start, stop, with_scores),
            Command::ZRangeByScore {
                key,
                min,
                max,
                with_scores,
                offset,
                count,
            } => sorted_sets::zrangebyscore(
                &self.store,
                db,
                &key,
                min,
                max,
                with_scores,
                offset,
                count,
            ),
            Command::ZRevRangeByScore {
                key,
                max,
                min,
                with_scores,
                offset,
                count,
            } => sorted_sets::zrevrangebyscore(
                &self.store,
                db,
                &key,
                max,
                min,
                with_scores,
                offset,
                count,
            ),
            Command::ZIncrBy {
                key,
                increment,
                member,
            } => sorted_sets::zincrby(&self.store, db, &key, increment, &member),
            Command::ZPopMin { key, count } => sorted_sets::zpopmin(&self.store, db, &key, count),
            Command::ZPopMax { key, count } => sorted_sets::zpopmax(&self.store, db, &key, count),
            Command::ZMScore { key, members } => {
                sorted_sets::zmscore(&self.store, db, &key, &members)
            }
            Command::ZUnion {
                keys,
                weights,
                aggregate,
                withscores,
            } => sorted_sets::zunion(
                &self.store,
                db,
                &keys,
                weights.as_deref(),
                aggregate,
                withscores,
            ),
            Command::ZUnionStore {
                destination,
                keys,
                weights,
                aggregate,
            } => sorted_sets::zunionstore(
                &self.store,
                db,
                &destination,
                &keys,
                weights.as_deref(),
                aggregate,
            ),
            Command::ZInter {
                keys,
                weights,
                aggregate,
                withscores,
            } => sorted_sets::zinter(
                &self.store,
                db,
                &keys,
                weights.as_deref(),
                aggregate,
                withscores,
            ),
            Command::ZInterStore {
                destination,
                keys,
                weights,
                aggregate,
            } => sorted_sets::zinterstore(
                &self.store,
                db,
                &destination,
                &keys,
                weights.as_deref(),
                aggregate,
            ),
            Command::ZInterCard { keys, limit } => {
                sorted_sets::zintercard(&self.store, db, &keys, limit)
            }
            Command::ZDiff { keys, withscores } => {
                sorted_sets::zdiff(&self.store, db, &keys, withscores)
            }
            Command::ZDiffStore { destination, keys } => {
                sorted_sets::zdiffstore(&self.store, db, &destination, &keys)
            }
            Command::ZRangeByLex {
                key,
                min,
                max,
                offset,
                count,
            } => sorted_sets::zrangebylex(&self.store, db, &key, &min, &max, offset, count),
            Command::ZRevRangeByLex {
                key,
                max,
                min,
                offset,
                count,
            } => sorted_sets::zrevrangebylex(&self.store, db, &key, &max, &min, offset, count),
            Command::ZLexCount { key, min, max } => {
                sorted_sets::zlexcount(&self.store, db, &key, &min, &max)
            }
            Command::ZRemRangeByRank { key, start, stop } => {
                sorted_sets::zremrangebyrank(&self.store, db, &key, start, stop)
            }
            Command::ZRemRangeByScore { key, min, max } => {
                sorted_sets::zremrangebyscore(&self.store, db, &key, min, max)
            }
            Command::ZRemRangeByLex { key, min, max } => {
                sorted_sets::zremrangebylex(&self.store, db, &key, &min, &max)
            }
            Command::ZRandMember {
                key,
                count,
                withscores,
            } => sorted_sets::zrandmember(&self.store, db, &key, count, withscores),
            Command::ZMPop {
                keys,
                direction,
                count,
            } => sorted_sets::zmpop(&self.store, db, &keys, direction, count),
            Command::BZMPop {
                timeout,
                keys,
                direction,
                count,
            } => {
                let pop_min = matches!(direction, sorted_sets::ZPopDirection::Min);
                blocking::bzmpop(
                    &self.store,
                    &self.blocking_zset_manager,
                    db,
                    &keys,
                    pop_min,
                    count.unwrap_or(1),
                    timeout,
                )
                .await
            }
            Command::BZPopMin { keys, timeout } => {
                blocking::bzpopmin(&self.store, &self.blocking_zset_manager, db, &keys, timeout)
                    .await
            }
            Command::BZPopMax { keys, timeout } => {
                blocking::bzpopmax(&self.store, &self.blocking_zset_manager, db, &keys, timeout)
                    .await
            }

            // Pub/Sub commands
            Command::Subscribe { .. } => {
                // SUBSCRIBE is handled at connection level - needs async streaming
                Frame::error("ERR SUBSCRIBE command is handled at connection level")
            }
            Command::Unsubscribe { .. } => {
                // UNSUBSCRIBE is handled at connection level
                Frame::error("ERR UNSUBSCRIBE command is handled at connection level")
            }
            Command::PSubscribe { .. } => {
                // PSUBSCRIBE is handled at connection level
                Frame::error("ERR PSUBSCRIBE command is handled at connection level")
            }
            Command::PUnsubscribe { .. } => {
                // PUNSUBSCRIBE is handled at connection level
                Frame::error("ERR PUNSUBSCRIBE command is handled at connection level")
            }
            Command::Publish { channel, message } => {
                pubsub::publish(&self.pubsub_manager, &channel, &message)
            }
            Command::PubSubChannels { pattern } => {
                pubsub::pubsub_channels(&self.pubsub_manager, pattern.as_ref())
            }
            Command::PubSubNumSub { channels } => {
                pubsub::pubsub_numsub(&self.pubsub_manager, &channels)
            }
            Command::PubSubNumPat => pubsub::pubsub_numpat(&self.pubsub_manager),

            // Sharded Pub/Sub commands (Redis 7.0+)
            Command::SSubscribe { .. } => {
                // SSUBSCRIBE is handled at connection level for proper streaming
                Frame::error("ERR SSUBSCRIBE command is handled at connection level")
            }
            Command::SUnsubscribe { .. } => {
                // SUNSUBSCRIBE is handled at connection level
                Frame::error("ERR SUNSUBSCRIBE command is handled at connection level")
            }
            Command::SPublish { channel, message } => {
                // Sharded PUBLISH - uses slot-based routing
                pubsub::spublish(&self.pubsub_manager, &channel, &message)
            }
            Command::PubSubShardChannels { pattern } => {
                pubsub::pubsub_shardchannels(&self.pubsub_manager, pattern.as_ref())
            }
            Command::PubSubShardNumSub { channels } => {
                pubsub::pubsub_shardnumsub(&self.pubsub_manager, &channels)
            }

            // Transaction commands (handled at connection level)
            Command::Multi => Frame::error("ERR MULTI command is handled at connection level"),
            Command::Exec => Frame::error("ERR EXEC command is handled at connection level"),
            Command::Discard => Frame::error("ERR DISCARD command is handled at connection level"),
            Command::Watch { .. } => {
                Frame::error("ERR WATCH command is handled at connection level")
            }
            Command::Unwatch => Frame::error("ERR UNWATCH command is handled at connection level"),

            // Key commands
            Command::Expire { key, seconds } => self.expire(db, &key, seconds * 1000),
            Command::PExpire { key, milliseconds } => self.expire(db, &key, milliseconds),
            Command::Ttl { key } => self.ttl(db, &key),
            Command::PTtl { key } => self.pttl(db, &key),
            Command::Persist { key } => self.persist(db, &key),

            // Server commands
            Command::Ping { message } => self.ping(message),
            Command::Echo { message } => Frame::bulk(message),
            Command::Select { .. } => {
                // SELECT is handled by the handler directly
                Frame::simple("OK")
            }
            Command::Info { section } => self.info(section),
            Command::DbSize => self.dbsize(db),
            Command::FlushDb { .. } => self.flushdb(db),
            Command::FlushAll { .. } => self.flushall(),
            Command::Time => self.time(),
            Command::CommandCmd { subcommand, args } => self.command_cmd(subcommand, args),
            Command::Debug { subcommand, args } => self.debug(&subcommand, args),
            Command::Memory { subcommand, key } => self.memory(&subcommand, key.as_ref(), db),
            Command::Client { subcommand, args } => self.client(&subcommand, args),
            Command::Config { subcommand, args } => self.config(&subcommand, args),
            Command::Monitor => self.monitor(),
            Command::Reset => self.reset(),
            Command::Role => self.role(),
            Command::Module { subcommand, args } => self.module(&subcommand, &args),
            Command::Plugin { subcommand, args } => self.plugin(&subcommand, &args),
            Command::BgSave { schedule } => self.bgsave(schedule),
            Command::Save => self.save(),
            Command::BgRewriteAof => self.bgrewriteaof(),
            Command::LastSave => self.lastsave(),
            Command::SlowLog {
                subcommand,
                argument,
            } => self.slowlog_cmd(&subcommand, argument),
            Command::Latency { subcommand, args } => self.latency(&subcommand, &args),
            Command::Wait {
                numreplicas,
                timeout,
            } => self.wait(numreplicas, timeout),
            Command::Shutdown {
                nosave,
                save,
                now,
                force,
                abort,
            } => self.shutdown(nosave, save, now, force, abort),
            Command::SwapDb { index1, index2 } => self.swapdb(index1, index2),
            Command::Move { key, db: target_db } => self.move_key(db, &key, target_db),
            Command::Migrate {
                host,
                port,
                key,
                destination_db,
                timeout,
                copy,
                replace,
                ..
            } => self.migrate(
                &host,
                port,
                key.as_ref(),
                destination_db,
                timeout,
                copy,
                replace,
            ),

            // Connection commands
            Command::Quit => Frame::simple("OK"),
            Command::Hello {
                protocol_version,
                auth: _,
                client_name: _,
            } => self.hello(protocol_version),

            // Replication commands
            Command::ReplicaOf { host, port } | Command::SlaveOf { host, port } => {
                self.replicaof(host, port)
            }
            Command::ReplConf { options } => self.replconf(&options),
            Command::Psync {
                replication_id,
                offset,
            } => self.psync(&replication_id, offset),

            // Authentication commands
            Command::Auth { username, password } => self.auth(username.as_deref(), &password),
            Command::Acl { subcommand, args } => self.acl(&subcommand, &args),

            // Cluster commands
            Command::Cluster { subcommand, args } => self.cluster(&subcommand, &args),
            Command::Tiering {
                subcommand,
                args,
                key,
            } => self.tiering(&subcommand, &args, key.as_ref()).await,

            // CDC commands
            Command::Cdc { subcommand, args } => self.cdc(&subcommand, &args).await,

            // Temporal commands
            Command::History {
                key,
                from,
                to,
                limit,
                ascending,
                with_values,
            } => {
                self.history(db, &key, from, to, limit, ascending, with_values)
                    .await
            }
            Command::HistoryCount { key, from, to } => self.history_count(db, &key, from, to),
            Command::HistoryFirst { key } => self.history_first(db, &key),
            Command::HistoryLast { key } => self.history_last(db, &key),
            Command::Diff {
                key,
                timestamp1,
                timestamp2,
            } => self.diff(db, &key, &timestamp1, &timestamp2).await,
            Command::RestoreFrom {
                key,
                timestamp,
                target,
            } => {
                self.restore_from(db, &key, &timestamp, target.as_ref())
                    .await
            }
            Command::Temporal { subcommand, args } => self.temporal(&subcommand, &args),

            Command::Asking => {
                // ASKING tells the server the next command is from a client
                // following an ASK redirect during slot migration.
                // The server should allow the command even if the slot is being imported.
                // This sets a flag that only affects the NEXT command.
                // The actual flag handling is done at the connection level.
                Frame::simple("OK")
            }
            Command::ReadOnly => {
                // READONLY enables read-only mode for connections to replicas.
                // Allows read commands to be executed on replica nodes.
                Frame::simple("OK")
            }
            Command::ReadWrite => {
                // READWRITE disables read-only mode (default state).
                Frame::simple("OK")
            }

            // Scripting commands
            Command::Eval { script, keys, args } => {
                self.script_executor.eval(db, &script, &keys, &args)
            }
            Command::EvalSha { sha1, keys, args } => {
                self.script_executor.evalsha(db, &sha1, &keys, &args)
            }
            Command::Script { subcommand, args } => self.handle_script_command(&subcommand, &args),
            Command::EvalRo { script, keys, args } => {
                self.script_executor.eval(db, &script, &keys, &args)
            }
            Command::EvalShaRo { sha1, keys, args } => {
                self.script_executor.evalsha(db, &sha1, &keys, &args)
            }
            Command::Function { subcommand, args } => {
                // Route FaaS-specific subcommands to the async handler
                match subcommand.as_str() {
                    "DEPLOY" | "INVOKE" | "UNDEPLOY" | "FAAS.LIST" | "FAAS.INFO"
                    | "FAAS.LOGS" | "SCHEDULE" | "UNSCHEDULE" | "SCHEDULES" | "FAAS.STATS" => {
                        self.handle_faas_command(&subcommand, &args).await
                    }
                    _ => self.function(&subcommand, &args),
                }
            }
            Command::FCall {
                function,
                keys,
                args,
            } => self.fcall(db, &function, &keys, &args),
            Command::FCallRo {
                function,
                keys,
                args,
            } => {
                // FCALL_RO is read-only version
                self.fcall(db, &function, &keys, &args)
            }

            // Stream commands
            Command::XAdd {
                key,
                id,
                fields,
                maxlen,
                minid,
                nomkstream,
            } => self.xadd(
                db,
                &key,
                id.as_deref(),
                fields,
                maxlen,
                minid.as_deref(),
                nomkstream,
            ),
            Command::XLen { key } => self.xlen(db, &key),
            Command::XRange {
                key,
                start,
                end,
                count,
            } => self.xrange(db, &key, &start, &end, count),
            Command::XRevRange {
                key,
                start,
                end,
                count,
            } => self.xrevrange(db, &key, &end, &start, count),
            Command::XRead {
                streams,
                count,
                block,
            } => {
                if let Some(block_ms) = block {
                    blocking::xread_blocking(
                        &self.store,
                        &self.blocking_stream_manager,
                        db,
                        &streams,
                        count,
                        block_ms,
                    )
                    .await
                } else {
                    self.xread(db, &streams, count)
                }
            }
            Command::XDel { key, ids } => self.xdel(db, &key, &ids),
            Command::XTrim {
                key,
                maxlen,
                minid,
                approximate: _,
            } => self.xtrim(db, &key, maxlen, minid.as_deref()),
            Command::XInfo {
                key,
                subcommand,
                group_name,
            } => self.xinfo(db, &key, &subcommand, group_name.as_ref()),
            Command::XGroupCreate {
                key,
                group,
                id,
                mkstream,
            } => streams::xgroup_create(&self.store, db, &key, &group, &id, mkstream),
            Command::XGroupDestroy { key, group } => {
                streams::xgroup_destroy(&self.store, db, &key, &group)
            }
            Command::XGroupCreateConsumer {
                key,
                group,
                consumer,
            } => streams::xgroup_createconsumer(&self.store, db, &key, &group, &consumer),
            Command::XGroupDelConsumer {
                key,
                group,
                consumer,
            } => streams::xgroup_delconsumer(&self.store, db, &key, &group, &consumer),
            Command::XGroupSetId { key, group, id } => {
                streams::xgroup_setid(&self.store, db, &key, &group, &id)
            }
            Command::XAck { key, group, ids } => {
                let entry_ids: Vec<_> = ids
                    .iter()
                    .filter_map(|s| crate::storage::StreamEntryId::parse(s))
                    .collect();
                streams::xack(&self.store, db, &key, &group, &entry_ids)
            }
            Command::XPending {
                key,
                group,
                start,
                end,
                count,
                consumer,
                idle,
            } => streams::xpending(
                &self.store,
                db,
                &key,
                &group,
                start.as_deref(),
                end.as_deref(),
                count,
                consumer.as_ref(),
                idle,
            ),
            Command::XClaim {
                key,
                group,
                consumer,
                min_idle_time,
                ids,
                idle,
                time,
                retrycount,
                force,
                justid,
            } => {
                let entry_ids: Vec<_> = ids
                    .iter()
                    .filter_map(|s| crate::storage::StreamEntryId::parse(s))
                    .collect();
                streams::xclaim(
                    &self.store,
                    db,
                    &key,
                    &group,
                    &consumer,
                    min_idle_time,
                    &entry_ids,
                    idle,
                    time,
                    retrycount,
                    force,
                    justid,
                )
            }
            Command::XAutoClaim {
                key,
                group,
                consumer,
                min_idle_time,
                start,
                count,
                justid,
            } => streams::xautoclaim(
                &self.store,
                db,
                &key,
                &group,
                &consumer,
                min_idle_time,
                &start,
                count,
                justid,
            ),
            Command::XReadGroup {
                group,
                consumer,
                count,
                block,
                noack,
                streams: stream_keys,
            } => {
                if let Some(block_ms) = block {
                    blocking::xreadgroup_blocking(
                        &self.store,
                        &self.blocking_stream_manager,
                        db,
                        &group,
                        &consumer,
                        count,
                        block_ms,
                        noack,
                        &stream_keys,
                    )
                    .await
                } else {
                    streams::xreadgroup(
                        &self.store,
                        db,
                        &group,
                        &consumer,
                        count,
                        noack,
                        &stream_keys,
                    )
                }
            }

            // Geo commands
            Command::GeoAdd {
                key,
                items,
                nx,
                xx,
                ch,
            } => self.geoadd(db, &key, items, nx, xx, ch),
            Command::GeoPos { key, members } => self.geopos(db, &key, &members),
            Command::GeoDist {
                key,
                member1,
                member2,
                unit,
            } => self.geodist(db, &key, &member1, &member2, &unit),
            Command::GeoHash { key, members } => self.geohash(db, &key, &members),
            Command::GeoRadius {
                key,
                longitude,
                latitude,
                radius,
                unit,
                with_coord,
                with_dist,
                with_hash,
                count,
                asc,
            } => self.georadius(
                db, &key, longitude, latitude, radius, &unit, with_coord, with_dist, with_hash,
                count, asc,
            ),
            Command::GeoRadiusByMember {
                key,
                member,
                radius,
                unit,
                with_coord,
                with_dist,
                with_hash,
                count,
                asc,
            } => self.georadiusbymember(
                db, &key, &member, radius, &unit, with_coord, with_dist, with_hash, count, asc,
            ),
            Command::GeoSearch {
                key,
                from_member,
                from_lonlat,
                by_radius,
                by_box,
                asc,
                count,
                any,
                with_coord,
                with_dist,
                with_hash,
            } => self.geosearch(
                db,
                &key,
                from_member,
                from_lonlat,
                by_radius,
                by_box,
                asc,
                count,
                any,
                with_coord,
                with_dist,
                with_hash,
            ),

            // HyperLogLog commands
            Command::PfAdd { key, elements } => self.pfadd(db, &key, &elements),
            Command::PfCount { keys } => self.pfcount(db, &keys),
            Command::PfMerge {
                destkey,
                sourcekeys,
            } => self.pfmerge(db, &destkey, &sourcekeys),

            // Scan commands
            Command::Scan {
                cursor,
                pattern,
                count,
                type_filter,
            } => self.scan(
                db,
                cursor,
                pattern.as_deref(),
                count,
                type_filter.as_deref(),
            ),
            Command::SScan {
                key,
                cursor,
                pattern,
                count,
            } => sets::sscan(&self.store, db, &key, cursor, pattern.as_deref(), count),
            Command::ZScan {
                key,
                cursor,
                pattern,
                count,
            } => self.zscan(db, &key, cursor, pattern.as_deref(), count),

            // Bitmap commands
            Command::GetBit { key, offset } => bitmap::getbit(&self.store, db, &key, offset),
            Command::SetBit { key, offset, value } => {
                bitmap::setbit(&self.store, db, &key, offset, value)
            }
            Command::BitCount {
                key,
                start,
                end,
                bit_mode,
            } => bitmap::bitcount(&self.store, db, &key, start, end, bit_mode),
            Command::BitOp {
                operation,
                destkey,
                keys,
            } => bitmap::bitop(&self.store, db, operation, &destkey, &keys),
            Command::BitPos {
                key,
                bit,
                start,
                end,
                bit_mode,
            } => bitmap::bitpos(&self.store, db, &key, bit, start, end, bit_mode),
            Command::BitField { key, subcommands } => {
                bitmap::bitfield(&self.store, db, &key, &subcommands)
            }
            Command::BitFieldRo { key, subcommands } => {
                bitmap::bitfield_ro(&self.store, db, &key, &subcommands)
            }

            // Vector Search commands
            Command::FtCreate {
                index,
                schema,
                index_type,
                dimension,
                metric,
            } => {
                self.ft_create(
                    &index,
                    &schema,
                    index_type.as_deref(),
                    dimension,
                    metric.as_deref(),
                )
                .await
            }
            Command::FtDropIndex { index, delete_docs } => {
                self.ft_dropindex(&index, delete_docs).await
            }
            Command::FtAdd {
                index,
                key,
                vector,
                payload,
            } => self.ft_add(&index, &key, &vector, payload.as_ref()).await,
            Command::FtDel { index, key } => self.ft_del(&index, &key).await,
            Command::FtSearch {
                index,
                query,
                k,
                return_fields,
                filter,
            } => {
                self.ft_search(&index, &query, k, &return_fields, filter.as_deref())
                    .await
            }
            Command::FtInfo { index } => self.ft_info(&index).await,
            Command::FtList => self.ft_list().await,

            // CRDT commands
            Command::CrdtGCounter {
                key,
                subcommand,
                args,
            } => self.crdt_gcounter(&key, &subcommand, &args).await,
            Command::CrdtPNCounter {
                key,
                subcommand,
                args,
            } => self.crdt_pncounter(&key, &subcommand, &args).await,
            Command::CrdtLwwRegister {
                key,
                subcommand,
                args,
            } => self.crdt_lwwreg(&key, &subcommand, &args).await,
            Command::CrdtOrSet {
                key,
                subcommand,
                args,
            } => self.crdt_orset(&key, &subcommand, &args).await,
            Command::CrdtInfo { key } => self.crdt_info(key.as_ref()).await,

            // WASM commands
            Command::WasmLoad {
                name,
                module,
                replace,
                permissions,
            } => self.wasm_load(&name, &module, replace, &permissions).await,
            Command::WasmUnload { name } => self.wasm_unload(&name).await,
            Command::WasmCall { name, keys, args } => self.wasm_call(&name, &keys, &args).await,
            Command::WasmCallRo { name, keys, args } => {
                self.wasm_call_ro(&name, &keys, &args).await
            }
            Command::WasmList { with_stats } => self.wasm_list(with_stats).await,
            Command::WasmInfo { name } => self.wasm_info(&name).await,
            Command::WasmStats => self.wasm_stats().await,

            // Semantic Caching commands
            Command::SemanticSet {
                query,
                value,
                embedding,
                ttl_secs,
                threshold: _,
            } => {
                self.semantic_set(&query, &value, &embedding, ttl_secs)
                    .await
            }
            Command::SemanticGet {
                embedding,
                threshold,
                count,
            } => self.semantic_get(&embedding, threshold, count).await,
            Command::SemanticGetText {
                query,
                threshold,
                count,
            } => self.semantic_gettext(&query, threshold, count).await,
            Command::SemanticDel { id } => self.semantic_del(id).await,
            Command::SemanticClear => self.semantic_clear().await,
            Command::SemanticInfo => self.semantic_info().await,
            Command::SemanticStats => self.semantic_stats().await,
            Command::SemanticConfig {
                operation,
                param,
                value,
            } => {
                self.semantic_config(&operation, param.as_ref(), value.as_ref())
                    .await
            }

            // Trigger commands
            Command::TriggerCreate {
                name,
                event_type,
                pattern,
                actions,
                wasm_module,
                wasm_function,
                priority,
                description,
            } => {
                self.trigger_create(
                    &name,
                    &event_type,
                    &pattern,
                    &actions,
                    wasm_module.as_ref(),
                    wasm_function.as_ref(),
                    priority,
                    description.as_ref(),
                )
                .await
            }
            Command::TriggerDelete { name } => self.trigger_delete(&name).await,
            Command::TriggerGet { name } => self.trigger_get(&name).await,
            Command::TriggerList { pattern } => self.trigger_list(pattern.as_ref()).await,
            Command::TriggerEnable { name } => self.trigger_enable(&name).await,
            Command::TriggerDisable { name } => self.trigger_disable(&name).await,
            Command::TriggerFire {
                name,
                key,
                value,
                ttl,
            } => self.trigger_fire(&name, &key, value.as_ref(), ttl).await,
            Command::TriggerInfo => self.trigger_info().await,
            Command::TriggerStats => self.trigger_stats().await,
            Command::TriggerConfig {
                operation,
                param,
                value,
            } => {
                self.trigger_config(&operation, param.as_ref(), value.as_ref())
                    .await
            }

            // Key management commands
            Command::Keys { pattern } => keys::keys(&self.store, db, &pattern),
            Command::Type { key } => keys::key_type(&self.store, db, &key),
            Command::Rename { key, new_key } => keys::rename(&self.store, db, &key, &new_key),
            Command::RenameNx { key, new_key } => keys::renamenx(&self.store, db, &key, &new_key),
            Command::Copy {
                source,
                destination,
                dest_db,
                replace,
            } => keys::copy(&self.store, db, &source, &destination, dest_db, replace),
            Command::Unlink { keys: key_list } => keys::unlink(&self.store, db, &key_list),
            Command::Touch { keys: key_list } => keys::touch(&self.store, db, &key_list),
            Command::RandomKey => keys::randomkey(&self.store, db),
            Command::Object { subcommand, key } => {
                self.handle_object_command(&subcommand, key.as_ref(), db)
            }
            Command::ExpireAt { key, timestamp } => {
                keys::expireat(&self.store, db, &key, timestamp)
            }
            Command::PExpireAt { key, timestamp_ms } => {
                keys::pexpireat(&self.store, db, &key, timestamp_ms)
            }
            Command::ExpireTime { key } => keys::expiretime(&self.store, db, &key),
            Command::PExpireTime { key } => keys::pexpiretime(&self.store, db, &key),
            Command::Dump { key } => self.dump(db, &key),
            Command::Restore {
                key,
                ttl,
                data,
                replace,
            } => self.restore(db, &key, ttl, &data, replace),
            Command::Sort { key, options: _ } => {
                // Basic SORT implementation - just return the list/set/sorted set elements
                self.sort(db, &key)
            }

            // Time-Series commands
            Command::TimeSeries { subcommand, args } => {
                self.handle_timeseries_command(db, &subcommand, &args).await
            }

            // Document database commands
            Command::Document { subcommand, args } => {
                self.handle_document_command(db, &subcommand, &args).await
            }

            // Graph database commands
            Command::Graph { subcommand, args } => {
                self.handle_graph_command(db, &subcommand, &args).await
            }

            // RAG pipeline commands
            Command::Rag { subcommand, args } => {
                self.handle_rag_command(db, &subcommand, &args).await
            }

            // FerriteQL query commands
            Command::Query { subcommand, args } => {
                self.handle_query_command(db, &subcommand, &args).await
            }

            // Adaptive query optimizer
            Command::FerriteAdvisor { subcommand, args } => {
                self.ferrite_advisor(&subcommand, &args).await
            }

            // Integrated observability diagnostics
            Command::FerriteDebug { subcommand, args } => {
                self.ferrite_debug(&subcommand, &args).await
            }

            // Hybrid vector search
            Command::VectorHybridSearch {
                index,
                query_vector,
                query_text,
                top_k,
                alpha,
                strategy,
            } => {
                self.vector_hybrid_search(db, &index, &query_vector, &query_text, top_k, alpha, &strategy)
                    .await
            }
            Command::VectorRerank {
                index,
                query_text,
                doc_ids,
                top_k,
            } => {
                self.vector_rerank(db, &index, &query_text, &doc_ids, top_k)
                    .await
            }

            // Materialized view commands
            Command::ViewCreate {
                name,
                query,
                strategy,
                interval,
            } => self.handle_view_create(&name, &query, &strategy, interval).await,
            Command::ViewDrop { name } => self.handle_view_drop(&name).await,
            Command::ViewQuery { name } => self.handle_view_query(&name).await,
            Command::ViewList => self.handle_view_list().await,
            Command::ViewRefresh { name } => self.handle_view_refresh(&name).await,
            Command::ViewInfo { name } => self.handle_view_info(&name).await,

            // Live migration commands
            Command::MigrateStart {
                source_uri,
                batch_size,
                workers,
                verify,
                dry_run,
            } => {
                self.handle_migrate_start(&source_uri, batch_size, workers, verify, dry_run)
                    .await
            }
            Command::MigrateStatus => self.handle_migrate_status().await,
            Command::MigratePause => self.handle_migrate_pause().await,
            Command::MigrateResume => self.handle_migrate_resume().await,
            Command::MigrateVerify { sample_pct } => {
                self.handle_migrate_verify(sample_pct).await
            }
            Command::MigrateCutover => self.handle_migrate_cutover().await,
            Command::MigrateRollback => self.handle_migrate_rollback().await,

            // Kafka-compatible streaming commands
            Command::StreamCreate {
                topic,
                partitions,
                retention_ms,
                replication,
            } => {
                self.handle_stream_create(&topic, partitions, retention_ms, replication)
                    .await
            }
            Command::StreamDelete { topic } => self.handle_stream_delete(&topic).await,
            Command::StreamProduce {
                topic,
                key,
                value,
                partition,
            } => {
                self.handle_stream_produce(&topic, key.as_ref(), &value, partition)
                    .await
            }
            Command::StreamFetch {
                topic,
                partition,
                offset,
                count,
            } => self.handle_stream_fetch(&topic, partition, offset, count).await,
            Command::StreamCommit {
                group,
                topic,
                partition,
                offset,
            } => {
                self.handle_stream_commit(&group, &topic, partition, offset)
                    .await
            }
            Command::StreamTopics => self.handle_stream_topics().await,
            Command::StreamDescribe { topic } => self.handle_stream_describe(&topic).await,
            Command::StreamGroups { topic } => {
                self.handle_stream_groups(topic.as_deref()).await
            }
            Command::StreamOffsets { topic, partition } => {
                self.handle_stream_offsets(&topic, partition).await
            }
            Command::StreamStats => self.handle_stream_stats().await,

            // Multi-region active-active commands
            Command::RegionAdd { id, name, endpoint } => {
                self.handle_region_add(&id, &name, &endpoint).await
            }
            Command::RegionRemove { id } => self.handle_region_remove(&id).await,
            Command::RegionList => self.handle_region_list().await,
            Command::RegionStatus { id } => self.handle_region_status(id.as_deref()).await,
            Command::RegionConflicts { limit } => self.handle_region_conflicts(limit).await,
            Command::RegionStrategy { strategy } => {
                self.handle_region_strategy(strategy.as_deref()).await
            }
            Command::RegionStats => self.handle_region_stats().await,

            // Data Mesh / Federation gateway commands
            Command::FederationAdd { id, source_type, uri, name } => {
                self.handle_federation_add(&id, &source_type, &uri, name.as_deref()).await
            }
            Command::FederationRemove { id } => self.handle_federation_remove(&id).await,
            Command::FederationList => self.handle_federation_list().await,
            Command::FederationStatus { id } => self.handle_federation_status(id.as_deref()).await,
            Command::FederationNamespace { namespace, source_id } => {
                self.handle_federation_namespace(&namespace, &source_id).await
            }
            Command::FederationNamespaces => self.handle_federation_namespaces().await,
            Command::FederationQuery { query } => self.handle_federation_query(&query).await,
            Command::FederationContract { name, source_id, schema_json } => {
                self.handle_federation_contract(&name, &source_id, &schema_json).await
            }
            Command::FederationContracts => self.handle_federation_contracts().await,
            Command::FederationStats => self.handle_federation_stats().await,

            // Studio developer-experience commands
            Command::StudioSchema { db } => self.handle_studio_schema(db).await,
            Command::StudioTemplates { name } => self.handle_studio_templates(name.as_deref()).await,
            Command::StudioSetup { template } => self.handle_studio_setup(&template).await,
            Command::StudioCompat { redis_info } => {
                self.handle_studio_compat(redis_info.as_deref()).await
            }
            Command::StudioHelp { command } => self.handle_studio_help(&command).await,
            Command::StudioSuggest { context } => {
                self.handle_studio_suggest(context.as_deref()).await
            }
        }
    }
}
