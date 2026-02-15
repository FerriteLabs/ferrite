//! Command parsing — RESP frames to typed [`Command`] values.
//!
//! This module defines the [`Command`] enum (one variant per Redis command) and
//! the `from_frame` parser that converts a RESP [`Frame`] into a `Command`.
//!
//! # Layout
//!
//! - **Helper types** (`SetOptions`, `SortOptions`, `ListDirection`, …) — top of file.
//! - **`Command` enum** — all variants grouped by category (String, List, Hash, …).
//! - **`Command::from_frame`** — the main parser, one match arm per command name.
//!
//! # Adding a new command
//!
//! 1. Add a variant to the [`Command`] enum under the appropriate category comment.
//! 2. Add a match arm in `Command::from_frame` that parses the RESP arguments.
//! 3. Add the corresponding execution logic in [`super::executor`] (`executor.rs`).
//! 4. Add tests in the `#[cfg(test)]` module at the bottom of this file.

use bytes::Bytes;

use crate::error::{FerriteError, Result};
use crate::protocol::Frame;

mod parsers;

/// Direction for list operations (LEFT or RIGHT)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListDirection {
    Left,
    Right,
}

/// SET command options
#[derive(Debug, Clone, Default)]
pub struct SetOptions {
    /// Expiration in milliseconds
    pub expire_ms: Option<u64>,
    /// Only set if key doesn't exist (NX)
    pub nx: bool,
    /// Only set if key exists (XX)
    pub xx: bool,
    /// Return the old value
    pub get: bool,
}

/// SORT command options
#[derive(Debug, Clone, Default)]
pub struct SortOptions {
    /// BY pattern
    pub by: Option<String>,
    /// LIMIT offset count
    pub limit: Option<(i64, i64)>,
    /// GET patterns
    pub get: Vec<String>,
    /// Sort descending
    pub desc: bool,
    /// Sort alphabetically
    pub alpha: bool,
    /// STORE destination
    pub store: Option<Bytes>,
}

/// Parsed Redis command
#[derive(Debug, Clone)]
#[allow(missing_docs)]
pub enum Command {
    // String commands
    /// GET key
    Get { key: Bytes },
    /// SET key value [EX seconds | PX milliseconds] [NX | XX] [GET]
    Set {
        key: Bytes,
        value: Bytes,
        options: SetOptions,
    },
    /// DEL key [key ...]
    Del { keys: Vec<Bytes> },
    /// EXISTS key [key ...]
    Exists { keys: Vec<Bytes> },
    /// INCR key
    Incr { key: Bytes },
    /// DECR key
    Decr { key: Bytes },
    /// INCRBY key increment
    IncrBy { key: Bytes, delta: i64 },
    /// DECRBY key decrement
    DecrBy { key: Bytes, delta: i64 },
    /// MGET key [key ...]
    MGet { keys: Vec<Bytes> },
    /// MSET key value [key value ...]
    MSet { pairs: Vec<(Bytes, Bytes)> },
    /// APPEND key value
    Append { key: Bytes, value: Bytes },
    /// STRLEN key
    StrLen { key: Bytes },
    /// GETRANGE key start end
    GetRange { key: Bytes, start: i64, end: i64 },
    /// SETRANGE key offset value
    SetRange {
        key: Bytes,
        offset: i64,
        value: Bytes,
    },
    /// SETNX key value (SET if Not eXists)
    SetNx { key: Bytes, value: Bytes },
    /// SETEX key seconds value
    SetEx {
        key: Bytes,
        seconds: u64,
        value: Bytes,
    },
    /// PSETEX key milliseconds value
    PSetEx {
        key: Bytes,
        milliseconds: u64,
        value: Bytes,
    },
    /// GETSET key value (atomically set and return old)
    GetSet { key: Bytes, value: Bytes },
    /// GETDEL key (get and delete)
    GetDel { key: Bytes },
    /// INCRBYFLOAT key increment
    IncrByFloat { key: Bytes, delta: f64 },
    /// GETEX key [EX seconds | PX milliseconds | EXAT timestamp | PXAT timestamp | PERSIST]
    GetEx {
        key: Bytes,
        ex: Option<u64>,
        px: Option<u64>,
        exat: Option<u64>,
        pxat: Option<u64>,
        persist: bool,
    },
    /// MSETNX key value [key value ...]
    MSetNx { pairs: Vec<(Bytes, Bytes)> },
    /// LCS key1 key2 [LEN] [IDX] [MINMATCHLEN len] [WITHMATCHLEN]
    Lcs {
        key1: Bytes,
        key2: Bytes,
        len_only: bool,
        idx: bool,
        min_match_len: usize,
        with_match_len: bool,
    },

    // List commands
    /// LPUSH key element [element ...]
    LPush { key: Bytes, values: Vec<Bytes> },
    /// RPUSH key element [element ...]
    RPush { key: Bytes, values: Vec<Bytes> },
    /// LPOP key [count]
    LPop { key: Bytes, count: Option<usize> },
    /// RPOP key [count]
    RPop { key: Bytes, count: Option<usize> },
    /// LRANGE key start stop
    LRange { key: Bytes, start: i64, stop: i64 },
    /// LLEN key
    LLen { key: Bytes },
    /// LINDEX key index
    LIndex { key: Bytes, index: i64 },
    /// LSET key index element
    LSet {
        key: Bytes,
        index: i64,
        value: Bytes,
    },
    /// LREM key count element
    LRem {
        key: Bytes,
        count: i64,
        value: Bytes,
    },
    /// LINSERT key BEFORE|AFTER pivot element
    LInsert {
        key: Bytes,
        before: bool,
        pivot: Bytes,
        value: Bytes,
    },
    /// LPUSHX key element [element ...]
    LPushX { key: Bytes, values: Vec<Bytes> },
    /// RPUSHX key element [element ...]
    RPushX { key: Bytes, values: Vec<Bytes> },
    /// BLPOP key [key ...] timeout
    BLPop { keys: Vec<Bytes>, timeout: f64 },
    /// BRPOP key [key ...] timeout
    BRPop { keys: Vec<Bytes>, timeout: f64 },
    /// BRPOPLPUSH source destination timeout
    BRPopLPush {
        source: Bytes,
        destination: Bytes,
        timeout: f64,
    },
    /// BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout
    BLMove {
        source: Bytes,
        destination: Bytes,
        wherefrom: ListDirection,
        whereto: ListDirection,
        timeout: f64,
    },
    /// LMOVE source destination LEFT|RIGHT LEFT|RIGHT
    LMove {
        source: Bytes,
        destination: Bytes,
        wherefrom: ListDirection,
        whereto: ListDirection,
    },
    /// LTRIM key start stop
    LTrim { key: Bytes, start: i64, stop: i64 },
    /// LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]
    LPos {
        key: Bytes,
        element: Bytes,
        rank: Option<i64>,
        count: Option<usize>,
        maxlen: Option<usize>,
    },
    /// RPOPLPUSH source destination
    RPopLPush { source: Bytes, destination: Bytes },
    /// LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]
    LMPop {
        keys: Vec<Bytes>,
        direction: ListDirection,
        count: usize,
    },
    /// BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]
    BLMPop {
        timeout: f64,
        keys: Vec<Bytes>,
        direction: ListDirection,
        count: usize,
    },

    // Hash commands
    /// HSET key field value [field value ...]
    HSet {
        key: Bytes,
        pairs: Vec<(Bytes, Bytes)>,
    },
    /// HGET key field
    HGet { key: Bytes, field: Bytes },
    /// HMSET key field value [field value ...]
    HMSet {
        key: Bytes,
        pairs: Vec<(Bytes, Bytes)>,
    },
    /// HMGET key field [field ...]
    HMGet { key: Bytes, fields: Vec<Bytes> },
    /// HDEL key field [field ...]
    HDel { key: Bytes, fields: Vec<Bytes> },
    /// HEXISTS key field
    HExists { key: Bytes, field: Bytes },
    /// HGETALL key
    HGetAll { key: Bytes },
    /// HKEYS key
    HKeys { key: Bytes },
    /// HVALS key
    HVals { key: Bytes },
    /// HLEN key
    HLen { key: Bytes },
    /// HSETNX key field value
    HSetNx {
        key: Bytes,
        field: Bytes,
        value: Bytes,
    },
    /// HINCRBY key field increment
    HIncrBy {
        key: Bytes,
        field: Bytes,
        delta: i64,
    },
    /// HINCRBYFLOAT key field increment
    HIncrByFloat {
        key: Bytes,
        field: Bytes,
        delta: f64,
    },
    /// HSTRLEN key field
    HStrLen { key: Bytes, field: Bytes },
    /// HRANDFIELD key [count [WITHVALUES]]
    HRandField {
        key: Bytes,
        count: Option<i64>,
        withvalues: bool,
    },

    // Set commands
    /// SADD key member [member ...]
    SAdd { key: Bytes, members: Vec<Bytes> },
    /// SREM key member [member ...]
    SRem { key: Bytes, members: Vec<Bytes> },
    /// SMEMBERS key
    SMembers { key: Bytes },
    /// SISMEMBER key member
    SIsMember { key: Bytes, member: Bytes },
    /// SMISMEMBER key member [member ...]
    SMIsMember { key: Bytes, members: Vec<Bytes> },
    /// SCARD key
    SCard { key: Bytes },
    /// SUNION key [key ...]
    SUnion { keys: Vec<Bytes> },
    /// SUNIONSTORE destination key [key ...]
    SUnionStore {
        destination: Bytes,
        keys: Vec<Bytes>,
    },
    /// SINTER key [key ...]
    SInter { keys: Vec<Bytes> },
    /// SINTERSTORE destination key [key ...]
    SInterStore {
        destination: Bytes,
        keys: Vec<Bytes>,
    },
    /// SINTERCARD numkeys key [key ...] [LIMIT limit]
    SInterCard {
        keys: Vec<Bytes>,
        limit: Option<usize>,
    },
    /// SDIFF key [key ...]
    SDiff { keys: Vec<Bytes> },
    /// SDIFFSTORE destination key [key ...]
    SDiffStore {
        destination: Bytes,
        keys: Vec<Bytes>,
    },
    /// SPOP key [count]
    SPop { key: Bytes, count: Option<usize> },
    /// SRANDMEMBER key [count]
    SRandMember { key: Bytes, count: Option<i64> },
    /// SMOVE source destination member
    SMove {
        source: Bytes,
        destination: Bytes,
        member: Bytes,
    },

    // Sorted Set commands
    /// ZADD key [NX|XX] [GT|LT] [CH] score member [score member ...]
    ZAdd {
        key: Bytes,
        options: super::sorted_sets::ZAddOptions,
        pairs: Vec<(f64, Bytes)>,
    },
    /// ZREM key member [member ...]
    ZRem { key: Bytes, members: Vec<Bytes> },
    /// ZSCORE key member
    ZScore { key: Bytes, member: Bytes },
    /// ZCARD key
    ZCard { key: Bytes },
    /// ZCOUNT key min max
    ZCount { key: Bytes, min: f64, max: f64 },
    /// ZRANK key member
    ZRank { key: Bytes, member: Bytes },
    /// ZREVRANK key member
    ZRevRank { key: Bytes, member: Bytes },
    /// ZRANGE key start stop [WITHSCORES]
    ZRange {
        key: Bytes,
        start: i64,
        stop: i64,
        with_scores: bool,
    },
    /// ZREVRANGE key start stop [WITHSCORES]
    ZRevRange {
        key: Bytes,
        start: i64,
        stop: i64,
        with_scores: bool,
    },
    /// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
    ZRangeByScore {
        key: Bytes,
        min: f64,
        max: f64,
        with_scores: bool,
        offset: Option<usize>,
        count: Option<usize>,
    },
    /// ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
    ZRevRangeByScore {
        key: Bytes,
        max: f64,
        min: f64,
        with_scores: bool,
        offset: Option<usize>,
        count: Option<usize>,
    },
    /// ZINCRBY key increment member
    ZIncrBy {
        key: Bytes,
        increment: f64,
        member: Bytes,
    },
    /// ZPOPMIN key [count]
    ZPopMin { key: Bytes, count: Option<usize> },
    /// ZPOPMAX key [count]
    ZPopMax { key: Bytes, count: Option<usize> },
    /// ZMSCORE key member [member ...]
    ZMScore { key: Bytes, members: Vec<Bytes> },
    /// ZUNION numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
    ZUnion {
        keys: Vec<Bytes>,
        weights: Option<Vec<f64>>,
        aggregate: super::sorted_sets::Aggregate,
        withscores: bool,
    },
    /// ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
    ZUnionStore {
        destination: Bytes,
        keys: Vec<Bytes>,
        weights: Option<Vec<f64>>,
        aggregate: super::sorted_sets::Aggregate,
    },
    /// ZINTER numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
    ZInter {
        keys: Vec<Bytes>,
        weights: Option<Vec<f64>>,
        aggregate: super::sorted_sets::Aggregate,
        withscores: bool,
    },
    /// ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
    ZInterStore {
        destination: Bytes,
        keys: Vec<Bytes>,
        weights: Option<Vec<f64>>,
        aggregate: super::sorted_sets::Aggregate,
    },
    /// ZINTERCARD numkeys key [key ...] [LIMIT limit]
    ZInterCard {
        keys: Vec<Bytes>,
        limit: Option<usize>,
    },
    /// ZDIFF numkeys key [key ...] [WITHSCORES]
    ZDiff { keys: Vec<Bytes>, withscores: bool },
    /// ZDIFFSTORE destination numkeys key [key ...]
    ZDiffStore {
        destination: Bytes,
        keys: Vec<Bytes>,
    },
    /// ZRANGEBYLEX key min max [LIMIT offset count]
    ZRangeByLex {
        key: Bytes,
        min: super::sorted_sets::LexBound,
        max: super::sorted_sets::LexBound,
        offset: Option<usize>,
        count: Option<usize>,
    },
    /// ZREVRANGEBYLEX key max min [LIMIT offset count]
    ZRevRangeByLex {
        key: Bytes,
        max: super::sorted_sets::LexBound,
        min: super::sorted_sets::LexBound,
        offset: Option<usize>,
        count: Option<usize>,
    },
    /// ZLEXCOUNT key min max
    ZLexCount {
        key: Bytes,
        min: super::sorted_sets::LexBound,
        max: super::sorted_sets::LexBound,
    },
    /// ZREMRANGEBYRANK key start stop
    ZRemRangeByRank { key: Bytes, start: i64, stop: i64 },
    /// ZREMRANGEBYSCORE key min max
    ZRemRangeByScore { key: Bytes, min: f64, max: f64 },
    /// ZREMRANGEBYLEX key min max
    ZRemRangeByLex {
        key: Bytes,
        min: super::sorted_sets::LexBound,
        max: super::sorted_sets::LexBound,
    },
    /// ZRANDMEMBER key [count [WITHSCORES]]
    ZRandMember {
        key: Bytes,
        count: Option<i64>,
        withscores: bool,
    },
    /// ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]
    ZMPop {
        keys: Vec<Bytes>,
        direction: super::sorted_sets::ZPopDirection,
        count: Option<usize>,
    },
    /// BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
    BZMPop {
        timeout: f64,
        keys: Vec<Bytes>,
        direction: super::sorted_sets::ZPopDirection,
        count: Option<usize>,
    },
    /// BZPOPMIN key [key ...] timeout
    BZPopMin { keys: Vec<Bytes>, timeout: f64 },
    /// BZPOPMAX key [key ...] timeout
    BZPopMax { keys: Vec<Bytes>, timeout: f64 },

    // Pub/Sub commands
    /// SUBSCRIBE channel [channel ...]
    Subscribe { channels: Vec<Bytes> },
    /// UNSUBSCRIBE [channel ...]
    Unsubscribe { channels: Vec<Bytes> },
    /// PSUBSCRIBE pattern [pattern ...]
    PSubscribe { patterns: Vec<Bytes> },
    /// PUNSUBSCRIBE [pattern ...]
    PUnsubscribe { patterns: Vec<Bytes> },
    /// PUBLISH channel message
    Publish { channel: Bytes, message: Bytes },
    /// PUBSUB CHANNELS [pattern]
    PubSubChannels { pattern: Option<Bytes> },
    /// PUBSUB NUMSUB [channel ...]
    PubSubNumSub { channels: Vec<Bytes> },
    /// PUBSUB NUMPAT
    PubSubNumPat,
    /// SSUBSCRIBE shardchannel [shardchannel ...]
    SSubscribe { channels: Vec<Bytes> },
    /// SUNSUBSCRIBE [shardchannel ...]
    SUnsubscribe { channels: Vec<Bytes> },
    /// SPUBLISH shardchannel message
    SPublish { channel: Bytes, message: Bytes },
    /// PUBSUB SHARDCHANNELS [pattern]
    PubSubShardChannels { pattern: Option<Bytes> },
    /// PUBSUB SHARDNUMSUB [shardchannel ...]
    PubSubShardNumSub { channels: Vec<Bytes> },

    // Transaction commands
    /// MULTI - Start a transaction
    Multi,
    /// EXEC - Execute all queued commands
    Exec,
    /// DISCARD - Discard all queued commands
    Discard,
    /// WATCH key [key ...] - Watch keys for changes
    Watch { keys: Vec<Bytes> },
    /// UNWATCH - Unwatch all keys
    Unwatch,

    // Key commands
    /// EXPIRE key seconds
    Expire { key: Bytes, seconds: u64 },
    /// PEXPIRE key milliseconds
    PExpire { key: Bytes, milliseconds: u64 },
    /// TTL key
    Ttl { key: Bytes },
    /// PTTL key
    PTtl { key: Bytes },
    /// PERSIST key
    Persist { key: Bytes },
    /// EXPIREAT key unix-time-seconds
    ExpireAt { key: Bytes, timestamp: u64 },
    /// PEXPIREAT key unix-time-milliseconds
    PExpireAt { key: Bytes, timestamp_ms: u64 },
    /// EXPIRETIME key
    ExpireTime { key: Bytes },
    /// PEXPIRETIME key
    PExpireTime { key: Bytes },

    // Key management commands
    /// KEYS pattern
    Keys { pattern: String },
    /// TYPE key
    Type { key: Bytes },
    /// RENAME key newkey
    Rename { key: Bytes, new_key: Bytes },
    /// RENAMENX key newkey
    RenameNx { key: Bytes, new_key: Bytes },
    /// COPY source destination [DB destination-db] [REPLACE]
    Copy {
        source: Bytes,
        destination: Bytes,
        dest_db: Option<u8>,
        replace: bool,
    },
    /// UNLINK key [key ...]
    Unlink { keys: Vec<Bytes> },
    /// TOUCH key [key ...]
    Touch { keys: Vec<Bytes> },
    /// RANDOMKEY
    RandomKey,
    /// OBJECT subcommand [arguments]
    Object {
        subcommand: String,
        key: Option<Bytes>,
    },
    /// DUMP key
    Dump { key: Bytes },
    /// RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]
    Restore {
        key: Bytes,
        ttl: u64,
        data: Bytes,
        replace: bool,
    },
    /// SORT key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC|DESC] [ALPHA] [STORE destination]
    Sort { key: Bytes, options: SortOptions },

    // Server commands
    /// PING [message]
    Ping { message: Option<Bytes> },
    /// ECHO message
    Echo { message: Bytes },
    /// SELECT index
    Select { db: u8 },
    /// INFO [section]
    Info { section: Option<String> },
    /// DBSIZE
    DbSize,
    /// FLUSHDB [ASYNC|SYNC]
    FlushDb { r#async: bool },
    /// FLUSHALL [ASYNC|SYNC]
    FlushAll { r#async: bool },
    /// TIME
    Time,
    /// COMMAND [subcommand]
    CommandCmd {
        subcommand: Option<String>,
        args: Vec<Bytes>,
    },
    /// DEBUG subcommand [arg ...]
    Debug {
        subcommand: String,
        args: Vec<Bytes>,
    },
    /// MEMORY subcommand [args]
    Memory {
        subcommand: String,
        key: Option<Bytes>,
    },

    /// CONFIG subcommand [args]
    Config {
        subcommand: String,
        args: Vec<Bytes>,
    },

    /// MONITOR - stream all commands received by server
    Monitor,

    /// RESET - reset connection state
    Reset,

    /// ROLE - return replication role
    Role,

    /// MODULE subcommand [args]
    Module {
        subcommand: String,
        args: Vec<Bytes>,
    },

    // Persistence commands
    /// BGSAVE [SCHEDULE]
    BgSave { schedule: bool },
    /// SAVE
    Save,
    /// BGREWRITEAOF
    BgRewriteAof,
    /// LASTSAVE
    LastSave,
    /// SLOWLOG subcommand [argument]
    SlowLog {
        subcommand: String,
        argument: Option<i64>,
    },
    /// LATENCY subcommand [args]
    Latency {
        subcommand: String,
        args: Vec<Bytes>,
    },
    /// WAIT numreplicas timeout
    Wait { numreplicas: i64, timeout: i64 },
    /// SHUTDOWN [NOSAVE | SAVE] [NOW] [FORCE] [ABORT]
    Shutdown {
        nosave: bool,
        save: bool,
        now: bool,
        force: bool,
        abort: bool,
    },
    /// SWAPDB index1 index2
    SwapDb { index1: u8, index2: u8 },
    /// MOVE key db
    Move { key: Bytes, db: u8 },
    /// MIGRATE host port key|"" destination-db timeout [COPY] [REPLACE] [AUTH password] [AUTH2 username password] [KEYS key [key ...]]
    Migrate {
        host: String,
        port: u16,
        key: Option<Bytes>,
        destination_db: u8,
        timeout: i64,
        copy: bool,
        replace: bool,
        auth: Option<String>,
        keys: Vec<Bytes>,
    },

    // Connection commands
    /// CLIENT subcommand [args]
    Client {
        subcommand: String,
        args: Vec<Bytes>,
    },
    /// QUIT
    Quit,
    /// HELLO [protover [AUTH username password] [SETNAME clientname]]
    Hello {
        protocol_version: Option<u8>,
        auth: Option<(Bytes, Bytes)>,
        client_name: Option<Bytes>,
    },

    // Replication commands
    /// REPLICAOF host port | REPLICAOF NO ONE
    ReplicaOf {
        host: Option<String>,
        port: Option<u16>,
    },
    /// SLAVEOF host port | SLAVEOF NO ONE (alias for REPLICAOF)
    SlaveOf {
        host: Option<String>,
        port: Option<u16>,
    },
    /// REPLCONF <option> <value> [<option> <value> ...]
    ReplConf { options: Vec<(String, String)> },
    /// PSYNC replicationid offset
    Psync { replication_id: String, offset: i64 },

    // Authentication commands
    /// AUTH [username] password
    Auth {
        username: Option<String>,
        password: String,
    },
    /// ACL subcommand [args...]
    Acl {
        subcommand: String,
        args: Vec<String>,
    },

    // Cluster commands
    /// CLUSTER subcommand [args...]
    Cluster {
        subcommand: String,
        args: Vec<String>,
    },
    /// TIERING subcommand [args...] - Cost-aware tiering commands
    Tiering {
        subcommand: String,
        args: Vec<String>,
        key: Option<Bytes>,
    },
    /// CDC subcommand [args...] - Change Data Capture commands
    Cdc {
        /// Subcommand (SUBSCRIBE, READ, ACK, etc.)
        subcommand: String,
        /// Arguments
        args: Vec<String>,
    },
    /// HISTORY key [FROM ts] [TO ts] [LIMIT count] [ORDER ASC|DESC] [WITHVALUES]
    History {
        /// Key to get history for
        key: Bytes,
        /// Start timestamp (inclusive)
        from: Option<String>,
        /// End timestamp (inclusive)
        to: Option<String>,
        /// Maximum entries to return
        limit: Option<usize>,
        /// Sort order (true = ascending/oldest first)
        ascending: bool,
        /// Include values in response
        with_values: bool,
    },
    /// HISTORY.COUNT key [FROM ts] [TO ts]
    HistoryCount {
        /// Key to count history for
        key: Bytes,
        /// Start timestamp
        from: Option<String>,
        /// End timestamp
        to: Option<String>,
    },
    /// HISTORY.FIRST key
    HistoryFirst {
        /// Key to get first version for
        key: Bytes,
    },
    /// HISTORY.LAST key
    HistoryLast {
        /// Key to get last version for
        key: Bytes,
    },
    /// DIFF key timestamp1 timestamp2
    Diff {
        /// Key to compare
        key: Bytes,
        /// First timestamp
        timestamp1: String,
        /// Second timestamp
        timestamp2: String,
    },
    /// RESTORE.FROM key timestamp [NEWKEY target]
    RestoreFrom {
        /// Source key
        key: Bytes,
        /// Timestamp to restore from
        timestamp: String,
        /// Target key (optional)
        target: Option<Bytes>,
    },
    /// TEMPORAL subcommand [args...] - Temporal query commands
    Temporal {
        /// Subcommand (POLICY, INFO, CLEANUP)
        subcommand: String,
        /// Arguments
        args: Vec<String>,
    },
    /// ASKING - Used during cluster slot migration
    /// Tells the receiving node that the next command should be executed even if
    /// the slot is being imported (client got ASK redirect and is following it)
    Asking,
    /// READONLY - Enable read-only mode for connection to replica
    ReadOnly,
    /// READWRITE - Disable read-only mode (default)
    ReadWrite,

    // Scripting commands
    /// EVAL script numkeys key [key ...] arg [arg ...]
    Eval {
        script: String,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
    },
    /// EVALSHA sha1 numkeys key [key ...] arg [arg ...]
    EvalSha {
        sha1: String,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
    },
    /// SCRIPT subcommand [args...]
    Script {
        subcommand: String,
        args: Vec<String>,
    },
    /// EVAL_RO script numkeys key [key ...] arg [arg ...]
    EvalRo {
        script: String,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
    },
    /// EVALSHA_RO sha1 numkeys key [key ...] arg [arg ...]
    EvalShaRo {
        sha1: String,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
    },
    /// FUNCTION subcommand [args...]
    Function {
        subcommand: String,
        args: Vec<Bytes>,
    },
    /// FCALL function numkeys key [key ...] arg [arg ...]
    FCall {
        function: String,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
    },
    /// FCALL_RO function numkeys key [key ...] arg [arg ...]
    FCallRo {
        function: String,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
    },

    // Stream commands
    /// XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold] [LIMIT count] *|id field value [field value ...]
    XAdd {
        key: Bytes,
        id: Option<String>,
        fields: Vec<(Bytes, Bytes)>,
        maxlen: Option<usize>,
        minid: Option<String>,
        nomkstream: bool,
    },
    /// XLEN key
    XLen { key: Bytes },
    /// XRANGE key start end [COUNT count]
    XRange {
        key: Bytes,
        start: String,
        end: String,
        count: Option<usize>,
    },
    /// XREVRANGE key end start [COUNT count]
    XRevRange {
        key: Bytes,
        start: String,
        end: String,
        count: Option<usize>,
    },
    /// XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
    XRead {
        streams: Vec<(Bytes, String)>,
        count: Option<usize>,
        block: Option<u64>,
    },
    /// XDEL key id [id ...]
    XDel { key: Bytes, ids: Vec<String> },
    /// XTRIM key MAXLEN|MINID [=|~] threshold [LIMIT count]
    XTrim {
        key: Bytes,
        maxlen: Option<usize>,
        minid: Option<String>,
        approximate: bool,
    },
    /// XINFO STREAM|GROUPS|CONSUMERS key [group] [FULL [COUNT count]]
    XInfo {
        key: Bytes,
        subcommand: String,
        group_name: Option<Bytes>,
    },
    /// XGROUP CREATE key groupname id|$ [MKSTREAM] [ENTRIESREAD entries-read]
    XGroupCreate {
        key: Bytes,
        group: Bytes,
        id: String,
        mkstream: bool,
    },
    /// XGROUP DESTROY key groupname
    XGroupDestroy { key: Bytes, group: Bytes },
    /// XGROUP CREATECONSUMER key groupname consumername
    XGroupCreateConsumer {
        key: Bytes,
        group: Bytes,
        consumer: Bytes,
    },
    /// XGROUP DELCONSUMER key groupname consumername
    XGroupDelConsumer {
        key: Bytes,
        group: Bytes,
        consumer: Bytes,
    },
    /// XGROUP SETID key groupname id|$ [ENTRIESREAD entries-read]
    XGroupSetId {
        key: Bytes,
        group: Bytes,
        id: String,
    },
    /// XACK key group id [id ...]
    XAck {
        key: Bytes,
        group: Bytes,
        ids: Vec<String>,
    },
    /// XPENDING key group [[IDLE min-idle-time] start end count [consumer]]
    XPending {
        key: Bytes,
        group: Bytes,
        start: Option<String>,
        end: Option<String>,
        count: Option<usize>,
        consumer: Option<Bytes>,
        idle: Option<u64>,
    },
    /// XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [TIME ms-unix-time] [RETRYCOUNT count] [FORCE] [JUSTID]
    XClaim {
        key: Bytes,
        group: Bytes,
        consumer: Bytes,
        min_idle_time: u64,
        ids: Vec<String>,
        idle: Option<u64>,
        time: Option<u64>,
        retrycount: Option<u64>,
        force: bool,
        justid: bool,
    },
    /// XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]
    XAutoClaim {
        key: Bytes,
        group: Bytes,
        consumer: Bytes,
        min_idle_time: u64,
        start: String,
        count: Option<usize>,
        justid: bool,
    },
    /// XREADGROUP GROUP group consumer [COUNT count] [BLOCK milliseconds] [NOACK] STREAMS key [key ...] id [id ...]
    XReadGroup {
        group: Bytes,
        consumer: Bytes,
        count: Option<usize>,
        block: Option<u64>,
        noack: bool,
        streams: Vec<(Bytes, String)>,
    },

    // Geo commands
    /// GEOADD key [NX|XX] [CH] longitude latitude member [longitude latitude member ...]
    GeoAdd {
        key: Bytes,
        items: Vec<(f64, f64, Bytes)>,
        nx: bool,
        xx: bool,
        ch: bool,
    },
    /// GEOPOS key member [member ...]
    GeoPos { key: Bytes, members: Vec<Bytes> },
    /// GEODIST key member1 member2 [M|KM|FT|MI]
    GeoDist {
        key: Bytes,
        member1: Bytes,
        member2: Bytes,
        unit: String,
    },
    /// GEOHASH key member [member ...]
    GeoHash { key: Bytes, members: Vec<Bytes> },
    /// GEORADIUS key longitude latitude radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC]
    GeoRadius {
        key: Bytes,
        longitude: f64,
        latitude: f64,
        radius: f64,
        unit: String,
        with_coord: bool,
        with_dist: bool,
        with_hash: bool,
        count: Option<usize>,
        asc: bool,
    },
    /// GEORADIUSBYMEMBER key member radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC]
    GeoRadiusByMember {
        key: Bytes,
        member: Bytes,
        radius: f64,
        unit: String,
        with_coord: bool,
        with_dist: bool,
        with_hash: bool,
        count: Option<usize>,
        asc: bool,
    },
    /// GEOSEARCH key FROMMEMBER member|FROMLONLAT lon lat BYRADIUS radius M|KM|FT|MI|BYBOX width height M|KM|FT|MI [ASC|DESC] [COUNT count [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]
    GeoSearch {
        key: Bytes,
        from_member: Option<Bytes>,
        from_lonlat: Option<(f64, f64)>,
        by_radius: Option<(f64, String)>,
        by_box: Option<(f64, f64, String)>,
        asc: bool,
        count: Option<usize>,
        any: bool,
        with_coord: bool,
        with_dist: bool,
        with_hash: bool,
    },

    // HyperLogLog commands
    /// PFADD key element [element ...]
    PfAdd { key: Bytes, elements: Vec<Bytes> },
    /// PFCOUNT key [key ...]
    PfCount { keys: Vec<Bytes> },
    /// PFMERGE destkey sourcekey [sourcekey ...]
    PfMerge {
        destkey: Bytes,
        sourcekeys: Vec<Bytes>,
    },

    // Scan commands
    /// SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
    Scan {
        cursor: u64,
        pattern: Option<String>,
        count: Option<usize>,
        type_filter: Option<String>,
    },
    /// HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]
    HScan {
        key: Bytes,
        cursor: u64,
        pattern: Option<String>,
        count: Option<usize>,
        novalues: bool,
    },
    /// SSCAN key cursor [MATCH pattern] [COUNT count]
    SScan {
        key: Bytes,
        cursor: u64,
        pattern: Option<String>,
        count: Option<usize>,
    },
    /// ZSCAN key cursor [MATCH pattern] [COUNT count]
    ZScan {
        key: Bytes,
        cursor: u64,
        pattern: Option<String>,
        count: Option<usize>,
    },

    // Bitmap commands
    /// GETBIT key offset
    GetBit { key: Bytes, offset: u64 },
    /// SETBIT key offset value
    SetBit { key: Bytes, offset: u64, value: u8 },
    /// BITCOUNT key [start end [BYTE|BIT]]
    BitCount {
        key: Bytes,
        start: Option<i64>,
        end: Option<i64>,
        bit_mode: bool,
    },
    /// BITOP operation destkey key [key ...]
    BitOp {
        operation: super::bitmap::BitOperation,
        destkey: Bytes,
        keys: Vec<Bytes>,
    },
    /// BITPOS key bit [start [end [BYTE|BIT]]]
    BitPos {
        key: Bytes,
        bit: u8,
        start: Option<i64>,
        end: Option<i64>,
        bit_mode: bool,
    },
    /// BITFIELD key [GET encoding offset] [SET encoding offset value] [INCRBY encoding offset increment] [OVERFLOW WRAP|SAT|FAIL]
    BitField {
        key: Bytes,
        subcommands: Vec<super::bitmap::BitFieldSubCommand>,
    },
    /// BITFIELD_RO key GET encoding offset [GET encoding offset ...]
    BitFieldRo {
        key: Bytes,
        subcommands: Vec<super::bitmap::BitFieldSubCommand>,
    },

    // Vector Search commands (FT.* namespace like RediSearch)
    /// FT.CREATE index SCHEMA field1 type1 [field2 type2 ...]
    FtCreate {
        /// Index name
        index: Bytes,
        /// Schema definition (field name, type, options)
        schema: Vec<(String, String)>,
        /// Index type (HNSW, FLAT)
        index_type: Option<String>,
        /// Dimension for vector fields
        dimension: Option<usize>,
        /// Distance metric
        metric: Option<String>,
    },
    /// FT.DROPINDEX index [DD]
    FtDropIndex {
        /// Index name
        index: Bytes,
        /// Delete documents too
        delete_docs: bool,
    },
    /// FT.ADD index key vector [PAYLOAD data]
    FtAdd {
        /// Index name
        index: Bytes,
        /// Vector ID/key
        key: Bytes,
        /// Vector data (comma-separated floats or binary)
        vector: Vec<f32>,
        /// Optional payload
        payload: Option<Bytes>,
    },
    /// FT.DEL index key
    FtDel {
        /// Index name
        index: Bytes,
        /// Vector ID/key
        key: Bytes,
    },
    /// FT.SEARCH index query [KNN k] [RETURN fields...] [LIMIT offset count]
    FtSearch {
        /// Index name
        index: Bytes,
        /// Query vector (for KNN)
        query: Vec<f32>,
        /// Number of results
        k: usize,
        /// Fields to return
        return_fields: Vec<String>,
        /// Filter expression
        filter: Option<String>,
    },
    /// FT.INFO index
    FtInfo {
        /// Index name
        index: Bytes,
    },
    /// FT._LIST - list all indexes
    FtList,

    // CRDT commands
    /// CRDT.GCOUNTER key subcommand [args...]
    CrdtGCounter {
        /// Key name
        key: Bytes,
        /// Subcommand (INCR, GET, MERGE)
        subcommand: String,
        /// Arguments
        args: Vec<String>,
    },
    /// CRDT.PNCOUNTER key subcommand [args...]
    CrdtPNCounter {
        /// Key name
        key: Bytes,
        /// Subcommand (INCR, DECR, GET, MERGE)
        subcommand: String,
        /// Arguments
        args: Vec<String>,
    },
    /// CRDT.LWWREG key subcommand [args...]
    CrdtLwwRegister {
        /// Key name
        key: Bytes,
        /// Subcommand (SET, GET, MERGE)
        subcommand: String,
        /// Arguments
        args: Vec<String>,
    },
    /// CRDT.ORSET key subcommand [args...]
    CrdtOrSet {
        /// Key name
        key: Bytes,
        /// Subcommand (ADD, REMOVE, CONTAINS, MEMBERS, MERGE)
        subcommand: String,
        /// Arguments
        args: Vec<String>,
    },
    /// CRDT.INFO [key]
    CrdtInfo {
        /// Optional key to get specific info
        key: Option<Bytes>,
    },

    // WebAssembly function commands
    /// WASM.LOAD name module_bytes [REPLACE] [PERMISSIONS perms...]
    WasmLoad {
        /// Function name
        name: String,
        /// Module bytes (base64 encoded or raw)
        module: Bytes,
        /// Replace if exists
        replace: bool,
        /// Permissions
        permissions: Vec<String>,
    },
    /// WASM.UNLOAD name
    WasmUnload {
        /// Function name
        name: String,
    },
    /// WASM.CALL name numkeys key [key ...] arg [arg ...]
    WasmCall {
        /// Function name
        name: String,
        /// Keys
        keys: Vec<Bytes>,
        /// Arguments
        args: Vec<Bytes>,
    },
    /// WASM.CALL_RO name numkeys key [key ...] arg [arg ...] (read-only variant)
    WasmCallRo {
        /// Function name
        name: String,
        /// Keys
        keys: Vec<Bytes>,
        /// Arguments
        args: Vec<Bytes>,
    },
    /// WASM.LIST [WITHSTATS]
    WasmList {
        /// Include stats
        with_stats: bool,
    },
    /// WASM.INFO name
    WasmInfo {
        /// Function name
        name: String,
    },
    /// WASM.STATS
    WasmStats,

    // Semantic Caching commands
    /// SEMANTIC.SET query value embedding [EX seconds] [THRESHOLD threshold]
    SemanticSet {
        query: Bytes,
        value: Bytes,
        embedding: Vec<f32>,
        ttl_secs: Option<u64>,
        threshold: Option<f32>,
    },
    /// SEMANTIC.GET embedding [THRESHOLD threshold] [COUNT count]
    SemanticGet {
        embedding: Vec<f32>,
        threshold: Option<f32>,
        count: Option<usize>,
    },
    /// SEMANTIC.GETTEXT query [THRESHOLD threshold] [COUNT count] (requires auto-embed)
    SemanticGetText {
        query: Bytes,
        threshold: Option<f32>,
        count: Option<usize>,
    },
    /// SEMANTIC.DEL id
    SemanticDel { id: u64 },
    /// SEMANTIC.CLEAR
    SemanticClear,
    /// SEMANTIC.INFO
    SemanticInfo,
    /// SEMANTIC.STATS
    SemanticStats,
    /// SEMANTIC.CONFIG GET|SET [param] [value]
    SemanticConfig {
        operation: Bytes,
        param: Option<Bytes>,
        value: Option<Bytes>,
    },

    // Trigger commands
    /// TRIGGER.CREATE name ON event pattern [DO action...] [WASM module func]
    TriggerCreate {
        name: Bytes,
        event_type: Bytes,
        pattern: Bytes,
        actions: Vec<Bytes>,
        wasm_module: Option<Bytes>,
        wasm_function: Option<Bytes>,
        priority: Option<i32>,
        description: Option<Bytes>,
    },
    /// TRIGGER.DELETE name
    TriggerDelete { name: Bytes },
    /// TRIGGER.GET name
    TriggerGet { name: Bytes },
    /// TRIGGER.LIST [pattern]
    TriggerList { pattern: Option<Bytes> },
    /// TRIGGER.ENABLE name
    TriggerEnable { name: Bytes },
    /// TRIGGER.DISABLE name
    TriggerDisable { name: Bytes },
    /// TRIGGER.FIRE name key [value] [ttl]
    TriggerFire {
        name: Bytes,
        key: Bytes,
        value: Option<Bytes>,
        ttl: Option<i64>,
    },
    /// TRIGGER.INFO
    TriggerInfo,
    /// TRIGGER.STATS
    TriggerStats,
    /// TRIGGER.CONFIG GET|SET [param] [value]
    TriggerConfig {
        operation: Bytes,
        param: Option<Bytes>,
        value: Option<Bytes>,
    },

    // Time-Series commands
    /// TS.* commands - generic handler for time-series operations
    TimeSeries {
        /// Subcommand (CREATE, ADD, GET, RANGE, etc.)
        subcommand: String,
        /// Raw arguments
        args: Vec<Bytes>,
    },

    // Document database commands
    /// DOC.* commands - generic handler for document operations
    Document {
        /// Subcommand (CREATE, INSERT, FIND, etc.)
        subcommand: String,
        /// Raw arguments
        args: Vec<Bytes>,
    },

    // Graph database commands
    /// GRAPH.* commands - generic handler for graph operations
    Graph {
        /// Subcommand (CREATE, QUERY, ADDNODE, etc.)
        subcommand: String,
        /// Raw arguments
        args: Vec<Bytes>,
    },

    // RAG pipeline commands
    /// RAG.* commands - generic handler for RAG operations
    Rag {
        /// Subcommand (CREATE, INGEST, RETRIEVE, etc.)
        subcommand: String,
        /// Raw arguments
        args: Vec<Bytes>,
    },

    // FerriteQL query commands
    /// QUERY.* commands - FerriteQL SQL-like query language
    Query {
        /// Subcommand (SQL string, EXPLAIN, PREPARE, EXEC, VIEW, JSON, HELP, VERSION)
        subcommand: String,
        /// Raw arguments
        args: Vec<Bytes>,
    },
}


impl Command {
    pub fn from_frame(frame: Frame) -> Result<Command> {
        let frames = match frame {
            Frame::Array(Some(frames)) => frames,
            Frame::Array(None) => return Err(FerriteError::Protocol("empty command".to_string())),
            _ => return Err(FerriteError::Protocol("expected array".to_string())),
        };

        if frames.is_empty() {
            return Err(FerriteError::Protocol("empty command".to_string()));
        }

        let command_name = parsers::get_string(&frames[0])?.to_ascii_uppercase();
        let args = &frames[1..];

        match command_name.as_str() {
            "GET" => parsers::strings::parse_get(args),
            "SET" => parsers::strings::parse_set(args),
            "DEL" => parsers::strings::parse_del(args),
            "EXISTS" => parsers::strings::parse_exists(args),
            "INCR" => parsers::strings::parse_incr(args),
            "DECR" => parsers::strings::parse_decr(args),
            "INCRBY" => parsers::strings::parse_incrby(args),
            "DECRBY" => parsers::strings::parse_decrby(args),
            "MGET" => parsers::strings::parse_mget(args),
            "MSET" => parsers::strings::parse_mset(args),
            "APPEND" => parsers::strings::parse_append(args),
            "STRLEN" => parsers::strings::parse_strlen(args),
            "GETRANGE" => parsers::strings::parse_getrange(args),
            "SETRANGE" => parsers::strings::parse_setrange(args),
            "SETNX" => parsers::strings::parse_setnx(args),
            "SETEX" => parsers::strings::parse_setex(args),
            "PSETEX" => parsers::strings::parse_psetex(args),
            "GETSET" => parsers::strings::parse_getset(args),
            "GETDEL" => parsers::strings::parse_getdel(args),
            "INCRBYFLOAT" => parsers::strings::parse_incrbyfloat(args),
            "GETEX" => parsers::strings::parse_getex(args),
            "MSETNX" => parsers::strings::parse_msetnx(args),
            "LCS" => parsers::strings::parse_lcs(args),
            "LPUSH" => parsers::lists::parse_lpush(args),
            "RPUSH" => parsers::lists::parse_rpush(args),
            "LPOP" => parsers::lists::parse_lpop(args),
            "RPOP" => parsers::lists::parse_rpop(args),
            "BLPOP" => parsers::lists::parse_blpop(args),
            "BRPOP" => parsers::lists::parse_brpop(args),
            "BRPOPLPUSH" => parsers::lists::parse_brpoplpush(args),
            "BLMOVE" => parsers::lists::parse_blmove(args),
            "LMOVE" => parsers::lists::parse_lmove(args),
            "LTRIM" => parsers::lists::parse_ltrim(args),
            "LPOS" => parsers::lists::parse_lpos(args),
            "RPOPLPUSH" => parsers::lists::parse_rpoplpush(args),
            "LMPOP" => parsers::lists::parse_lmpop(args),
            "BLMPOP" => parsers::lists::parse_blmpop(args),
            "LRANGE" => parsers::lists::parse_lrange(args),
            "LLEN" => parsers::lists::parse_llen(args),
            "LINDEX" => parsers::lists::parse_lindex(args),
            "LSET" => parsers::lists::parse_lset(args),
            "LREM" => parsers::lists::parse_lrem(args),
            "LINSERT" => parsers::lists::parse_linsert(args),
            "LPUSHX" => parsers::lists::parse_lpushx(args),
            "RPUSHX" => parsers::lists::parse_rpushx(args),
            "HSET" => parsers::hashes::parse_hset(args),
            "HGET" => parsers::hashes::parse_hget(args),
            "HMSET" => parsers::hashes::parse_hmset(args),
            "HMGET" => parsers::hashes::parse_hmget(args),
            "HDEL" => parsers::hashes::parse_hdel(args),
            "HEXISTS" => parsers::hashes::parse_hexists(args),
            "HGETALL" => parsers::hashes::parse_hgetall(args),
            "HKEYS" => parsers::hashes::parse_hkeys(args),
            "HVALS" => parsers::hashes::parse_hvals(args),
            "HLEN" => parsers::hashes::parse_hlen(args),
            "HSETNX" => parsers::hashes::parse_hsetnx(args),
            "HINCRBY" => parsers::hashes::parse_hincrby(args),
            "HINCRBYFLOAT" => parsers::hashes::parse_hincrbyfloat(args),
            "HSTRLEN" => parsers::hashes::parse_hstrlen(args),
            "HRANDFIELD" => parsers::hashes::parse_hrandfield(args),
            "HSCAN" => parsers::scan::parse_hscan(args),
            "SADD" => parsers::sets::parse_sadd(args),
            "SREM" => parsers::sets::parse_srem(args),
            "SMEMBERS" => parsers::sets::parse_smembers(args),
            "SISMEMBER" => parsers::sets::parse_sismember(args),
            "SMISMEMBER" => parsers::sets::parse_smismember(args),
            "SCARD" => parsers::sets::parse_scard(args),
            "SUNION" => parsers::sets::parse_sunion(args),
            "SUNIONSTORE" => parsers::sets::parse_sunionstore(args),
            "SINTER" => parsers::sets::parse_sinter(args),
            "SINTERSTORE" => parsers::sets::parse_sinterstore(args),
            "SINTERCARD" => parsers::sets::parse_sintercard(args),
            "SDIFF" => parsers::sets::parse_sdiff(args),
            "SDIFFSTORE" => parsers::sets::parse_sdiffstore(args),
            "SPOP" => parsers::sets::parse_spop(args),
            "SRANDMEMBER" => parsers::sets::parse_srandmember(args),
            "SMOVE" => parsers::sets::parse_smove(args),
            "ZADD" => parsers::sorted_sets::parse_zadd(args),
            "ZREM" => parsers::sorted_sets::parse_zrem(args),
            "ZSCORE" => parsers::sorted_sets::parse_zscore(args),
            "ZCARD" => parsers::sorted_sets::parse_zcard(args),
            "ZCOUNT" => parsers::sorted_sets::parse_zcount(args),
            "ZRANK" => parsers::sorted_sets::parse_zrank(args),
            "ZREVRANK" => parsers::sorted_sets::parse_zrevrank(args),
            "ZRANGE" => parsers::sorted_sets::parse_zrange(args),
            "ZREVRANGE" => parsers::sorted_sets::parse_zrevrange(args),
            "ZRANGEBYSCORE" => parsers::sorted_sets::parse_zrangebyscore(args),
            "ZREVRANGEBYSCORE" => parsers::sorted_sets::parse_zrevrangebyscore(args),
            "ZINCRBY" => parsers::sorted_sets::parse_zincrby(args),
            "ZPOPMIN" => parsers::sorted_sets::parse_zpopmin(args),
            "ZPOPMAX" => parsers::sorted_sets::parse_zpopmax(args),
            "ZMSCORE" => parsers::sorted_sets::parse_zmscore(args),
            "ZUNION" => parsers::sorted_sets::parse_zunion(args),
            "ZUNIONSTORE" => parsers::sorted_sets::parse_zunionstore(args),
            "ZINTER" => parsers::sorted_sets::parse_zinter(args),
            "ZINTERSTORE" => parsers::sorted_sets::parse_zinterstore(args),
            "ZINTERCARD" => parsers::sorted_sets::parse_zintercard(args),
            "ZDIFF" => parsers::sorted_sets::parse_zdiff(args),
            "ZDIFFSTORE" => parsers::sorted_sets::parse_zdiffstore(args),
            "ZRANGEBYLEX" => parsers::sorted_sets::parse_zrangebylex(args),
            "ZREVRANGEBYLEX" => parsers::sorted_sets::parse_zrevrangebylex(args),
            "ZLEXCOUNT" => parsers::sorted_sets::parse_zlexcount(args),
            "ZREMRANGEBYRANK" => parsers::sorted_sets::parse_zremrangebyrank(args),
            "ZREMRANGEBYSCORE" => parsers::sorted_sets::parse_zremrangebyscore(args),
            "ZREMRANGEBYLEX" => parsers::sorted_sets::parse_zremrangebylex(args),
            "ZRANDMEMBER" => parsers::sorted_sets::parse_zrandmember(args),
            "ZMPOP" => parsers::sorted_sets::parse_zmpop(args),
            "BZMPOP" => parsers::sorted_sets::parse_bzmpop(args),
            "BZPOPMIN" => parsers::sorted_sets::parse_bzpopmin(args),
            "BZPOPMAX" => parsers::sorted_sets::parse_bzpopmax(args),
            "SUBSCRIBE" => parsers::pubsub::parse_subscribe(args),
            "UNSUBSCRIBE" => parsers::pubsub::parse_unsubscribe(args),
            "PSUBSCRIBE" => parsers::pubsub::parse_psubscribe(args),
            "PUNSUBSCRIBE" => parsers::pubsub::parse_punsubscribe(args),
            "PUBLISH" => parsers::pubsub::parse_publish(args),
            "PUBSUB" => parsers::pubsub::parse_pubsub(args),
            "SSUBSCRIBE" => parsers::pubsub::parse_ssubscribe(args),
            "SUNSUBSCRIBE" => parsers::pubsub::parse_sunsubscribe(args),
            "SPUBLISH" => parsers::pubsub::parse_spublish(args),
            "MULTI" => Ok(Command::Multi),
            "EXEC" => Ok(Command::Exec),
            "DISCARD" => Ok(Command::Discard),
            "WATCH" => parsers::transactions::parse_watch(args),
            "UNWATCH" => Ok(Command::Unwatch),
            "EXPIRE" => parsers::keys::parse_expire(args),
            "PEXPIRE" => parsers::keys::parse_pexpire(args),
            "TTL" => parsers::keys::parse_ttl(args),
            "PTTL" => parsers::keys::parse_pttl(args),
            "PERSIST" => parsers::keys::parse_persist(args),
            "EXPIREAT" => parsers::keys::parse_expireat(args),
            "PEXPIREAT" => parsers::keys::parse_pexpireat(args),
            "EXPIRETIME" => parsers::keys::parse_expiretime(args),
            "PEXPIRETIME" => parsers::keys::parse_pexpiretime(args),
            "KEYS" => parsers::keys::parse_keys(args),
            "TYPE" => parsers::keys::parse_type(args),
            "RENAME" => parsers::keys::parse_rename(args),
            "RENAMENX" => parsers::keys::parse_renamenx(args),
            "COPY" => parsers::keys::parse_copy(args),
            "UNLINK" => parsers::keys::parse_unlink(args),
            "TOUCH" => parsers::keys::parse_touch(args),
            "RANDOMKEY" => Ok(Command::RandomKey),
            "OBJECT" => parsers::keys::parse_object(args),
            "DUMP" => parsers::keys::parse_dump(args),
            "RESTORE" => parsers::keys::parse_restore(args),
            "SORT" => parsers::keys::parse_sort(args),
            "PING" => parsers::server::parse_ping(args),
            "ECHO" => parsers::server::parse_echo(args),
            "SELECT" => parsers::server::parse_select(args),
            "INFO" => parsers::server::parse_info(args),
            "DBSIZE" => Ok(Command::DbSize),
            "FLUSHDB" => parsers::server::parse_flushdb(args),
            "FLUSHALL" => parsers::server::parse_flushall(args),
            "TIME" => Ok(Command::Time),
            "COMMAND" => parsers::server::parse_command_cmd(args),
            "DEBUG" => parsers::server::parse_debug(args),
            "MEMORY" => parsers::server::parse_memory(args),
            "CONFIG" => parsers::server::parse_config(args),
            "MONITOR" => Ok(Command::Monitor),
            "RESET" => Ok(Command::Reset),
            "ROLE" => Ok(Command::Role),
            "MODULE" => parsers::server::parse_module(args),
            "BGSAVE" => parsers::server::parse_bgsave(args),
            "SAVE" => Ok(Command::Save),
            "BGREWRITEAOF" => Ok(Command::BgRewriteAof),
            "LASTSAVE" => Ok(Command::LastSave),
            "SLOWLOG" => parsers::server::parse_slowlog(args),
            "LATENCY" => parsers::server::parse_latency(args),
            "WAIT" => parsers::server::parse_wait(args),
            "SHUTDOWN" => parsers::server::parse_shutdown(args),
            "SWAPDB" => parsers::server::parse_swapdb(args),
            "MOVE" => parsers::server::parse_move(args),
            "MIGRATE" => parsers::server::parse_migrate(args),
            "CLIENT" => parsers::connection::parse_client(args),
            "QUIT" => Ok(Command::Quit),
            "HELLO" => parsers::connection::parse_hello(args),
            "REPLICAOF" => parsers::connection::parse_replicaof(args),
            "SLAVEOF" => parsers::connection::parse_slaveof(args),
            "REPLCONF" => parsers::connection::parse_replconf(args),
            "PSYNC" => parsers::connection::parse_psync(args),
            "AUTH" => parsers::connection::parse_auth(args),
            "ACL" => parsers::connection::parse_acl(args),
            "CLUSTER" => parsers::cluster::parse_cluster(args),
            "TIERING" => parsers::cluster::parse_tiering(args),
            "CDC" => parsers::cluster::parse_cdc(args),
            "HISTORY" => parsers::cluster::parse_history(args),
            "HISTORY.COUNT" => parsers::cluster::parse_history_count(args),
            "HISTORY.FIRST" => parsers::cluster::parse_history_first(args),
            "HISTORY.LAST" => parsers::cluster::parse_history_last(args),
            "DIFF" => parsers::cluster::parse_diff(args),
            "RESTORE.FROM" => parsers::cluster::parse_restore_from(args),
            "TEMPORAL" => parsers::cluster::parse_temporal(args),
            "ASKING" => Ok(Command::Asking),
            "READONLY" => Ok(Command::ReadOnly),
            "READWRITE" => Ok(Command::ReadWrite),
            "EVAL" => parsers::scripting_parsers::parse_eval(args),
            "EVALSHA" => parsers::scripting_parsers::parse_evalsha(args),
            "SCRIPT" => parsers::scripting_parsers::parse_script(args),
            "EVAL_RO" => parsers::scripting_parsers::parse_eval_ro(args),
            "EVALSHA_RO" => parsers::scripting_parsers::parse_evalsha_ro(args),
            "FUNCTION" => parsers::scripting_parsers::parse_function(args),
            "FCALL" => parsers::scripting_parsers::parse_fcall(args),
            "FCALL_RO" => parsers::scripting_parsers::parse_fcall_ro(args),
            // Stream commands
            "XADD" => parsers::streams::parse_xadd(args),
            "XLEN" => parsers::streams::parse_xlen(args),
            "XRANGE" => parsers::streams::parse_xrange(args),
            "XREVRANGE" => parsers::streams::parse_xrevrange(args),
            "XREAD" => parsers::streams::parse_xread(args),
            "XDEL" => parsers::streams::parse_xdel(args),
            "XTRIM" => parsers::streams::parse_xtrim(args),
            "XINFO" => parsers::streams::parse_xinfo(args),
            "XGROUP" => parsers::streams::parse_xgroup(args),
            "XACK" => parsers::streams::parse_xack(args),
            "XPENDING" => parsers::streams::parse_xpending(args),
            "XCLAIM" => parsers::streams::parse_xclaim(args),
            "XAUTOCLAIM" => parsers::streams::parse_xautoclaim(args),
            "XREADGROUP" => parsers::streams::parse_xreadgroup(args),

            // Geo commands
            "GEOADD" => parsers::geo::parse_geoadd(args),
            "GEOPOS" => parsers::geo::parse_geopos(args),
            "GEODIST" => parsers::geo::parse_geodist(args),
            "GEOHASH" => parsers::geo::parse_geohash(args),
            "GEORADIUS" => parsers::geo::parse_georadius(args),
            "GEORADIUSBYMEMBER" => parsers::geo::parse_georadiusbymember(args),
            "GEOSEARCH" => parsers::geo::parse_geosearch(args),

            // HyperLogLog commands
            "PFADD" => parsers::hyperloglog::parse_pfadd(args),
            "PFCOUNT" => parsers::hyperloglog::parse_pfcount(args),
            "PFMERGE" => parsers::hyperloglog::parse_pfmerge(args),

            // Scan commands
            "SCAN" => parsers::scan::parse_scan(args),
            "SSCAN" => parsers::scan::parse_sscan(args),
            "ZSCAN" => parsers::scan::parse_zscan(args),

            // Bitmap commands
            "GETBIT" => parsers::bitmap::parse_getbit(args),
            "SETBIT" => parsers::bitmap::parse_setbit(args),
            "BITCOUNT" => parsers::bitmap::parse_bitcount(args),
            "BITOP" => parsers::bitmap::parse_bitop(args),
            "BITPOS" => parsers::bitmap::parse_bitpos(args),
            "BITFIELD" => parsers::bitmap::parse_bitfield(args),
            "BITFIELD_RO" => parsers::bitmap::parse_bitfield_ro(args),

            // Vector Search commands (FT.* namespace)
            "FT.CREATE" => parsers::advanced::parse_ft_create(args),
            "FT.DROPINDEX" => parsers::advanced::parse_ft_dropindex(args),
            "FT.ADD" => parsers::advanced::parse_ft_add(args),
            "FT.DEL" => parsers::advanced::parse_ft_del(args),
            "FT.SEARCH" => parsers::advanced::parse_ft_search(args),
            "FT.INFO" => parsers::advanced::parse_ft_info(args),
            "FT._LIST" => Ok(Command::FtList),

            // CRDT commands
            "CRDT.GCOUNTER" => parsers::advanced::parse_crdt_gcounter(args),
            "CRDT.PNCOUNTER" => parsers::advanced::parse_crdt_pncounter(args),
            "CRDT.LWWREG" => parsers::advanced::parse_crdt_lwwreg(args),
            "CRDT.ORSET" => parsers::advanced::parse_crdt_orset(args),
            "CRDT.INFO" => parsers::advanced::parse_crdt_info(args),

            // WASM commands
            "WASM.LOAD" => parsers::advanced::parse_wasm_load(args),
            "WASM.UNLOAD" => parsers::advanced::parse_wasm_unload(args),
            "WASM.CALL" => parsers::advanced::parse_wasm_call(args),
            "WASM.CALL_RO" => parsers::advanced::parse_wasm_call_ro(args),
            "WASM.LIST" => parsers::advanced::parse_wasm_list(args),
            "WASM.INFO" => parsers::advanced::parse_wasm_info(args),
            "WASM.STATS" => Ok(Command::WasmStats),

            // Semantic Caching commands
            "SEMANTIC.SET" => parsers::advanced::parse_semantic_set(args),
            "SEMANTIC.GET" => parsers::advanced::parse_semantic_get(args),
            "SEMANTIC.GETTEXT" => parsers::advanced::parse_semantic_gettext(args),
            "SEMANTIC.DEL" => parsers::advanced::parse_semantic_del(args),
            "SEMANTIC.CLEAR" => Ok(Command::SemanticClear),
            "SEMANTIC.INFO" => Ok(Command::SemanticInfo),
            "SEMANTIC.STATS" => Ok(Command::SemanticStats),
            "SEMANTIC.CONFIG" => parsers::advanced::parse_semantic_config(args),

            // Trigger commands
            "TRIGGER.CREATE" => parsers::advanced::parse_trigger_create(args),
            "TRIGGER.DELETE" => parsers::advanced::parse_trigger_delete(args),
            "TRIGGER.GET" => parsers::advanced::parse_trigger_get(args),
            "TRIGGER.LIST" => parsers::advanced::parse_trigger_list(args),
            "TRIGGER.ENABLE" => parsers::advanced::parse_trigger_enable(args),
            "TRIGGER.DISABLE" => parsers::advanced::parse_trigger_disable(args),
            "TRIGGER.FIRE" => parsers::advanced::parse_trigger_fire(args),
            "TRIGGER.INFO" => Ok(Command::TriggerInfo),
            "TRIGGER.STATS" => Ok(Command::TriggerStats),
            "TRIGGER.CONFIG" => parsers::advanced::parse_trigger_config(args),

            // Time-Series commands
            cmd if cmd.starts_with("TS.") => parsers::advanced::parse_timeseries_command(cmd, args),

            // Document database commands
            cmd if cmd.starts_with("DOC.") => parsers::advanced::parse_document_command(cmd, args),

            // Graph database commands
            cmd if cmd.starts_with("GRAPH.") => parsers::advanced::parse_graph_command(cmd, args),

            // RAG pipeline commands
            cmd if cmd.starts_with("RAG.") => parsers::advanced::parse_rag_command(cmd, args),

            // FerriteQL query commands
            "QUERY" => parsers::advanced::parse_query_command("RUN", args),
            cmd if cmd.starts_with("QUERY.") => {
                let sub = cmd.strip_prefix("QUERY.").unwrap_or(cmd);
                parsers::advanced::parse_query_command(sub, args)
            }

            _ => Err(FerriteError::UnknownCommand(command_name)),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn make_command(parts: &[&str]) -> Frame {
        Frame::array(
            parts
                .iter()
                .map(|s| Frame::bulk(Bytes::from(s.to_string())))
                .collect(),
        )
    }

    #[test]
    fn test_parse_get() {
        let frame = make_command(&["GET", "mykey"]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::Get { key } => assert_eq!(key, Bytes::from("mykey")),
            _ => panic!("Expected GET command"),
        }
    }

    #[test]
    fn test_parse_set() {
        let frame = make_command(&["SET", "mykey", "myvalue"]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::Set {
                key,
                value,
                options,
            } => {
                assert_eq!(key, Bytes::from("mykey"));
                assert_eq!(value, Bytes::from("myvalue"));
                assert!(!options.nx);
                assert!(!options.xx);
            }
            _ => panic!("Expected SET command"),
        }
    }

    #[test]
    fn test_parse_set_with_options() {
        let frame = make_command(&["SET", "mykey", "myvalue", "EX", "100", "NX"]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::Set {
                key,
                value,
                options,
            } => {
                assert_eq!(key, Bytes::from("mykey"));
                assert_eq!(value, Bytes::from("myvalue"));
                assert_eq!(options.expire_ms, Some(100_000));
                assert!(options.nx);
            }
            _ => panic!("Expected SET command"),
        }
    }

    #[test]
    fn test_parse_ping() {
        let frame = make_command(&["PING"]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::Ping { message } => assert!(message.is_none()),
            _ => panic!("Expected PING command"),
        }
    }

    #[test]
    fn test_parse_ping_with_message() {
        let frame = make_command(&["PING", "hello"]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::Ping { message } => assert_eq!(message, Some(Bytes::from("hello"))),
            _ => panic!("Expected PING command"),
        }
    }

    #[test]
    fn test_case_insensitive() {
        let frame = make_command(&["get", "mykey"]);
        let cmd = Command::from_frame(frame).unwrap();
        assert!(matches!(cmd, Command::Get { .. }));

        let frame = make_command(&["GeT", "mykey"]);
        let cmd = Command::from_frame(frame).unwrap();
        assert!(matches!(cmd, Command::Get { .. }));
    }

    #[test]
    fn test_unknown_command() {
        let frame = make_command(&["UNKNOWN"]);
        let result = Command::from_frame(frame);
        assert!(matches!(result, Err(FerriteError::UnknownCommand(_))));
    }

    #[test]
    fn test_parse_getrange() {
        let frame = make_command(&["GETRANGE", "mykey", "0", "10"]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::GetRange { key, start, end } => {
                assert_eq!(key, Bytes::from("mykey"));
                assert_eq!(start, 0);
                assert_eq!(end, 10);
            }
            _ => panic!("Expected GETRANGE command"),
        }
    }

    #[test]
    fn test_parse_setrange() {
        let frame = make_command(&["SETRANGE", "mykey", "5", "Hello"]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::SetRange { key, offset, value } => {
                assert_eq!(key, Bytes::from("mykey"));
                assert_eq!(offset, 5);
                assert_eq!(value, Bytes::from("Hello"));
            }
            _ => panic!("Expected SETRANGE command"),
        }
    }

    #[test]
    fn test_parse_setnx() {
        let frame = make_command(&["SETNX", "mykey", "myvalue"]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::SetNx { key, value } => {
                assert_eq!(key, Bytes::from("mykey"));
                assert_eq!(value, Bytes::from("myvalue"));
            }
            _ => panic!("Expected SETNX command"),
        }
    }

    #[test]
    fn test_parse_setex() {
        let frame = make_command(&["SETEX", "mykey", "100", "myvalue"]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::SetEx {
                key,
                seconds,
                value,
            } => {
                assert_eq!(key, Bytes::from("mykey"));
                assert_eq!(seconds, 100);
                assert_eq!(value, Bytes::from("myvalue"));
            }
            _ => panic!("Expected SETEX command"),
        }
    }

    #[test]
    fn test_parse_psetex() {
        let frame = make_command(&["PSETEX", "mykey", "10000", "myvalue"]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::PSetEx {
                key,
                milliseconds,
                value,
            } => {
                assert_eq!(key, Bytes::from("mykey"));
                assert_eq!(milliseconds, 10000);
                assert_eq!(value, Bytes::from("myvalue"));
            }
            _ => panic!("Expected PSETEX command"),
        }
    }

    #[test]
    fn test_parse_getset() {
        let frame = make_command(&["GETSET", "mykey", "newvalue"]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::GetSet { key, value } => {
                assert_eq!(key, Bytes::from("mykey"));
                assert_eq!(value, Bytes::from("newvalue"));
            }
            _ => panic!("Expected GETSET command"),
        }
    }

    #[test]
    fn test_parse_getdel() {
        let frame = make_command(&["GETDEL", "mykey"]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::GetDel { key } => {
                assert_eq!(key, Bytes::from("mykey"));
            }
            _ => panic!("Expected GETDEL command"),
        }
    }

    // ── Parser tests for Redis compatibility commands ──────────────────

    #[test]
    fn test_parse_randomkey() {
        let frame = make_command(&["RANDOMKEY"]);
        let cmd = Command::from_frame(frame).unwrap();
        assert!(matches!(cmd, Command::RandomKey));
    }

    #[test]
    fn test_parse_dbsize() {
        let frame = make_command(&["DBSIZE"]);
        let cmd = Command::from_frame(frame).unwrap();
        assert!(matches!(cmd, Command::DbSize));
    }

    #[test]
    fn test_parse_flushdb() {
        let frame = make_command(&["FLUSHDB"]);
        let cmd = Command::from_frame(frame).unwrap();
        assert!(matches!(cmd, Command::FlushDb { r#async: false }));
    }

    #[test]
    fn test_parse_flushdb_async() {
        let frame = make_command(&["FLUSHDB", "ASYNC"]);
        let cmd = Command::from_frame(frame).unwrap();
        assert!(matches!(cmd, Command::FlushDb { r#async: true }));
    }

    #[test]
    fn test_parse_flushdb_sync() {
        let frame = make_command(&["FLUSHDB", "SYNC"]);
        let cmd = Command::from_frame(frame).unwrap();
        assert!(matches!(cmd, Command::FlushDb { r#async: false }));
    }

    #[test]
    fn test_parse_flushall() {
        let frame = make_command(&["FLUSHALL"]);
        let cmd = Command::from_frame(frame).unwrap();
        assert!(matches!(cmd, Command::FlushAll { r#async: false }));
    }

    #[test]
    fn test_parse_flushall_async() {
        let frame = make_command(&["FLUSHALL", "ASYNC"]);
        let cmd = Command::from_frame(frame).unwrap();
        assert!(matches!(cmd, Command::FlushAll { r#async: true }));
    }

    #[test]
    fn test_parse_object_encoding() {
        let frame = make_command(&["OBJECT", "ENCODING", "mykey"]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::Object { subcommand, key } => {
                assert_eq!(subcommand, "ENCODING");
                assert_eq!(key, Some(Bytes::from("mykey")));
            }
            _ => panic!("Expected OBJECT command"),
        }
    }

    #[test]
    fn test_parse_object_refcount() {
        let frame = make_command(&["OBJECT", "REFCOUNT", "mykey"]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::Object { subcommand, key } => {
                assert_eq!(subcommand, "REFCOUNT");
                assert_eq!(key, Some(Bytes::from("mykey")));
            }
            _ => panic!("Expected OBJECT command"),
        }
    }

    #[test]
    fn test_parse_object_idletime() {
        let frame = make_command(&["OBJECT", "IDLETIME", "mykey"]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::Object { subcommand, key } => {
                assert_eq!(subcommand, "IDLETIME");
                assert_eq!(key, Some(Bytes::from("mykey")));
            }
            _ => panic!("Expected OBJECT command"),
        }
    }

    #[test]
    fn test_parse_object_freq() {
        let frame = make_command(&["OBJECT", "FREQ", "mykey"]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::Object { subcommand, key } => {
                assert_eq!(subcommand, "FREQ");
                assert_eq!(key, Some(Bytes::from("mykey")));
            }
            _ => panic!("Expected OBJECT command"),
        }
    }

    #[test]
    fn test_parse_object_help() {
        let frame = make_command(&["OBJECT", "HELP"]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::Object { subcommand, key } => {
                assert_eq!(subcommand, "HELP");
                assert!(key.is_none());
            }
            _ => panic!("Expected OBJECT command"),
        }
    }

    #[test]
    fn test_parse_scan_basic() {
        let frame = make_command(&["SCAN", "0"]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::Scan { cursor, pattern, count, type_filter } => {
                assert_eq!(cursor, 0);
                assert!(pattern.is_none());
                assert!(count.is_none());
                assert!(type_filter.is_none());
            }
            _ => panic!("Expected SCAN command"),
        }
    }

    #[test]
    fn test_parse_scan_with_match_and_count() {
        let frame = make_command(&["SCAN", "0", "MATCH", "user:*", "COUNT", "100"]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::Scan { cursor, pattern, count, type_filter } => {
                assert_eq!(cursor, 0);
                assert_eq!(pattern, Some("user:*".to_string()));
                assert_eq!(count, Some(100));
                assert!(type_filter.is_none());
            }
            _ => panic!("Expected SCAN command"),
        }
    }

    #[test]
    fn test_parse_scan_with_type_filter() {
        let frame = make_command(&["SCAN", "0", "TYPE", "string"]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::Scan { cursor, type_filter, .. } => {
                assert_eq!(cursor, 0);
                assert_eq!(type_filter, Some("string".to_string()));
            }
            _ => panic!("Expected SCAN command"),
        }
    }

    #[test]
    fn test_parse_wait() {
        let frame = make_command(&["WAIT", "1", "5000"]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::Wait { numreplicas, timeout } => {
                assert_eq!(numreplicas, 1);
                assert_eq!(timeout, 5000);
            }
            _ => panic!("Expected WAIT command"),
        }
    }

    #[test]
    fn test_parse_debug_sleep() {
        let frame = make_command(&["DEBUG", "SLEEP", "1"]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::Debug { subcommand, args } => {
                assert_eq!(subcommand, "SLEEP");
                assert_eq!(args.len(), 1);
            }
            _ => panic!("Expected DEBUG command"),
        }
    }

    #[test]
    fn test_parse_command_count() {
        let frame = make_command(&["COMMAND", "COUNT"]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::CommandCmd { subcommand, .. } => {
                assert_eq!(subcommand, Some("COUNT".to_string()));
            }
            _ => panic!("Expected COMMAND command"),
        }
    }

    #[test]
    fn test_parse_command_info() {
        let frame = make_command(&["COMMAND", "INFO", "get", "set"]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::CommandCmd { subcommand, args } => {
                assert_eq!(subcommand, Some("INFO".to_string()));
                assert_eq!(args.len(), 2);
            }
            _ => panic!("Expected COMMAND command"),
        }
    }

    #[test]
    fn test_parse_command_list() {
        let frame = make_command(&["COMMAND", "LIST"]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::CommandCmd { subcommand, .. } => {
                assert_eq!(subcommand, Some("LIST".to_string()));
            }
            _ => panic!("Expected COMMAND command"),
        }
    }

    #[test]
    fn test_parse_command_docs() {
        let frame = make_command(&["COMMAND", "DOCS", "get"]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::CommandCmd { subcommand, args } => {
                assert_eq!(subcommand, Some("DOCS".to_string()));
                assert_eq!(args.len(), 1);
            }
            _ => panic!("Expected COMMAND command"),
        }
    }

    #[test]
    fn test_parse_command_getkeys() {
        let frame = make_command(&["COMMAND", "GETKEYS", "GET", "mykey"]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::CommandCmd { subcommand, args } => {
                assert_eq!(subcommand, Some("GETKEYS".to_string()));
                assert_eq!(args.len(), 2);
            }
            _ => panic!("Expected COMMAND command"),
        }
    }

    #[test]
    fn test_parse_command_no_subcommand() {
        let frame = make_command(&["COMMAND"]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::CommandCmd { subcommand, .. } => {
                assert!(subcommand.is_none());
            }
            _ => panic!("Expected COMMAND command"),
        }
    }
}
