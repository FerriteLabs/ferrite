//! ACL metadata for commands.


use crate::auth::Permission;
use crate::commands::parser::Command;

use super::CommandMeta;

impl Command {
    /// Get metadata for ACL checking
    #[inline]
    pub(crate) fn meta(&self) -> CommandMeta {
        match self {
            // String commands (read)
            Command::Get { key } => CommandMeta {
                name: "GET",
                category: "string",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::MGet { keys } => CommandMeta {
                name: "MGET",
                category: "string",
                keys: keys.clone(),
                permission: Permission::Read,
            },
            Command::StrLen { key } => CommandMeta {
                name: "STRLEN",
                category: "string",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::GetRange { key, .. } => CommandMeta {
                name: "GETRANGE",
                category: "string",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },

            // String commands (write)
            Command::Set { key, .. } => CommandMeta {
                name: "SET",
                category: "string",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::MSet { pairs } => CommandMeta {
                name: "MSET",
                category: "string",
                keys: pairs.iter().map(|(k, _)| k.clone()).collect(),
                permission: Permission::Write,
            },
            Command::Incr { key } => CommandMeta {
                name: "INCR",
                category: "string",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::Decr { key } => CommandMeta {
                name: "DECR",
                category: "string",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::IncrBy { key, .. } => CommandMeta {
                name: "INCRBY",
                category: "string",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::DecrBy { key, .. } => CommandMeta {
                name: "DECRBY",
                category: "string",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::Append { key, .. } => CommandMeta {
                name: "APPEND",
                category: "string",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::SetRange { key, .. } => CommandMeta {
                name: "SETRANGE",
                category: "string",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::SetNx { key, .. } => CommandMeta {
                name: "SETNX",
                category: "string",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::SetEx { key, .. } => CommandMeta {
                name: "SETEX",
                category: "string",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::PSetEx { key, .. } => CommandMeta {
                name: "PSETEX",
                category: "string",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::GetSet { key, .. } => CommandMeta {
                name: "GETSET",
                category: "string",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::GetDel { key } => CommandMeta {
                name: "GETDEL",
                category: "string",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::IncrByFloat { key, .. } => CommandMeta {
                name: "INCRBYFLOAT",
                category: "string",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::GetEx { key, .. } => CommandMeta {
                name: "GETEX",
                category: "string",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::MSetNx { pairs } => CommandMeta {
                name: "MSETNX",
                category: "string",
                keys: pairs.iter().map(|(k, _)| k.clone()).collect(),
                permission: Permission::Write,
            },
            Command::Lcs { key1, key2, .. } => CommandMeta {
                name: "LCS",
                category: "string",
                keys: vec![key1.clone(), key2.clone()],
                permission: Permission::Read,
            },

            // Key commands
            Command::Del { keys } => CommandMeta {
                name: "DEL",
                category: "keyspace",
                keys: keys.clone(),
                permission: Permission::Write,
            },
            Command::Exists { keys } => CommandMeta {
                name: "EXISTS",
                category: "keyspace",
                keys: keys.clone(),
                permission: Permission::Read,
            },
            Command::Expire { key, .. } => CommandMeta {
                name: "EXPIRE",
                category: "keyspace",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::PExpire { key, .. } => CommandMeta {
                name: "PEXPIRE",
                category: "keyspace",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::Ttl { key } => CommandMeta {
                name: "TTL",
                category: "keyspace",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::PTtl { key } => CommandMeta {
                name: "PTTL",
                category: "keyspace",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::Persist { key } => CommandMeta {
                name: "PERSIST",
                category: "keyspace",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },

            // List commands
            Command::LPush { key, .. } => CommandMeta {
                name: "LPUSH",
                category: "list",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::RPush { key, .. } => CommandMeta {
                name: "RPUSH",
                category: "list",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::LPop { key, .. } => CommandMeta {
                name: "LPOP",
                category: "list",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::RPop { key, .. } => CommandMeta {
                name: "RPOP",
                category: "list",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::BLPop { keys, .. } => CommandMeta {
                name: "BLPOP",
                category: "list",
                keys: keys.clone(),
                permission: Permission::Write,
            },
            Command::BRPop { keys, .. } => CommandMeta {
                name: "BRPOP",
                category: "list",
                keys: keys.clone(),
                permission: Permission::Write,
            },
            Command::BRPopLPush {
                source,
                destination,
                ..
            } => CommandMeta {
                name: "BRPOPLPUSH",
                category: "list",
                keys: vec![source.clone(), destination.clone()],
                permission: Permission::Write,
            },
            Command::BLMove {
                source,
                destination,
                ..
            } => CommandMeta {
                name: "BLMOVE",
                category: "list",
                keys: vec![source.clone(), destination.clone()],
                permission: Permission::Write,
            },
            Command::LMove {
                source,
                destination,
                ..
            } => CommandMeta {
                name: "LMOVE",
                category: "list",
                keys: vec![source.clone(), destination.clone()],
                permission: Permission::Write,
            },
            Command::LTrim { key, .. } => CommandMeta {
                name: "LTRIM",
                category: "list",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::LPos { key, .. } => CommandMeta {
                name: "LPOS",
                category: "list",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::RPopLPush {
                source,
                destination,
            } => CommandMeta {
                name: "RPOPLPUSH",
                category: "list",
                keys: vec![source.clone(), destination.clone()],
                permission: Permission::Write,
            },
            Command::LMPop { keys, .. } => CommandMeta {
                name: "LMPOP",
                category: "list",
                keys: keys.clone(),
                permission: Permission::Write,
            },
            Command::BLMPop { keys, .. } => CommandMeta {
                name: "BLMPOP",
                category: "list",
                keys: keys.clone(),
                permission: Permission::Write,
            },
            Command::LRange { key, .. } => CommandMeta {
                name: "LRANGE",
                category: "list",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::LLen { key } => CommandMeta {
                name: "LLEN",
                category: "list",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::LIndex { key, .. } => CommandMeta {
                name: "LINDEX",
                category: "list",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::LSet { key, .. } => CommandMeta {
                name: "LSET",
                category: "list",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::LRem { key, .. } => CommandMeta {
                name: "LREM",
                category: "list",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::LInsert { key, .. } => CommandMeta {
                name: "LINSERT",
                category: "list",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::LPushX { key, .. } => CommandMeta {
                name: "LPUSHX",
                category: "list",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::RPushX { key, .. } => CommandMeta {
                name: "RPUSHX",
                category: "list",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },

            // Hash commands
            Command::HSet { key, .. } => CommandMeta {
                name: "HSET",
                category: "hash",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::HGet { key, .. } => CommandMeta {
                name: "HGET",
                category: "hash",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::HMSet { key, .. } => CommandMeta {
                name: "HMSET",
                category: "hash",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::HMGet { key, .. } => CommandMeta {
                name: "HMGET",
                category: "hash",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::HDel { key, .. } => CommandMeta {
                name: "HDEL",
                category: "hash",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::HExists { key, .. } => CommandMeta {
                name: "HEXISTS",
                category: "hash",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::HGetAll { key } => CommandMeta {
                name: "HGETALL",
                category: "hash",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::HKeys { key } => CommandMeta {
                name: "HKEYS",
                category: "hash",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::HVals { key } => CommandMeta {
                name: "HVALS",
                category: "hash",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::HLen { key } => CommandMeta {
                name: "HLEN",
                category: "hash",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::HSetNx { key, .. } => CommandMeta {
                name: "HSETNX",
                category: "hash",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::HIncrBy { key, .. } => CommandMeta {
                name: "HINCRBY",
                category: "hash",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::HIncrByFloat { key, .. } => CommandMeta {
                name: "HINCRBYFLOAT",
                category: "hash",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::HStrLen { key, .. } => CommandMeta {
                name: "HSTRLEN",
                category: "hash",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::HRandField { key, .. } => CommandMeta {
                name: "HRANDFIELD",
                category: "hash",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::HScan { key, .. } => CommandMeta {
                name: "HSCAN",
                category: "hash",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },

            // Set commands
            Command::SAdd { key, .. } => CommandMeta {
                name: "SADD",
                category: "set",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::SRem { key, .. } => CommandMeta {
                name: "SREM",
                category: "set",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::SMembers { key } => CommandMeta {
                name: "SMEMBERS",
                category: "set",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::SIsMember { key, .. } => CommandMeta {
                name: "SISMEMBER",
                category: "set",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::SMIsMember { key, .. } => CommandMeta {
                name: "SMISMEMBER",
                category: "set",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::SCard { key } => CommandMeta {
                name: "SCARD",
                category: "set",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::SUnion { keys } => CommandMeta {
                name: "SUNION",
                category: "set",
                keys: keys.clone(),
                permission: Permission::Read,
            },
            Command::SUnionStore { destination, keys } => {
                let mut all_keys = vec![destination.clone()];
                all_keys.extend(keys.clone());
                CommandMeta {
                    name: "SUNIONSTORE",
                    category: "set",
                    keys: all_keys,
                    permission: Permission::Write,
                }
            }
            Command::SInter { keys } => CommandMeta {
                name: "SINTER",
                category: "set",
                keys: keys.clone(),
                permission: Permission::Read,
            },
            Command::SInterStore { destination, keys } => {
                let mut all_keys = vec![destination.clone()];
                all_keys.extend(keys.clone());
                CommandMeta {
                    name: "SINTERSTORE",
                    category: "set",
                    keys: all_keys,
                    permission: Permission::Write,
                }
            }
            Command::SInterCard { keys, .. } => CommandMeta {
                name: "SINTERCARD",
                category: "set",
                keys: keys.clone(),
                permission: Permission::Read,
            },
            Command::SDiff { keys } => CommandMeta {
                name: "SDIFF",
                category: "set",
                keys: keys.clone(),
                permission: Permission::Read,
            },
            Command::SDiffStore { destination, keys } => {
                let mut all_keys = vec![destination.clone()];
                all_keys.extend(keys.clone());
                CommandMeta {
                    name: "SDIFFSTORE",
                    category: "set",
                    keys: all_keys,
                    permission: Permission::Write,
                }
            }
            Command::SPop { key, .. } => CommandMeta {
                name: "SPOP",
                category: "set",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::SRandMember { key, .. } => CommandMeta {
                name: "SRANDMEMBER",
                category: "set",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::SMove {
                source,
                destination,
                ..
            } => CommandMeta {
                name: "SMOVE",
                category: "set",
                keys: vec![source.clone(), destination.clone()],
                permission: Permission::Write,
            },

            // Sorted set commands
            Command::ZAdd { key, .. } => CommandMeta {
                name: "ZADD",
                category: "sortedset",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::ZRem { key, .. } => CommandMeta {
                name: "ZREM",
                category: "sortedset",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::ZScore { key, .. } => CommandMeta {
                name: "ZSCORE",
                category: "sortedset",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::ZCard { key } => CommandMeta {
                name: "ZCARD",
                category: "sortedset",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::ZCount { key, .. } => CommandMeta {
                name: "ZCOUNT",
                category: "sortedset",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::ZRank { key, .. } => CommandMeta {
                name: "ZRANK",
                category: "sortedset",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::ZRevRank { key, .. } => CommandMeta {
                name: "ZREVRANK",
                category: "sortedset",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::ZRange { key, .. } => CommandMeta {
                name: "ZRANGE",
                category: "sortedset",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::ZRevRange { key, .. } => CommandMeta {
                name: "ZREVRANGE",
                category: "sortedset",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::ZRangeByScore { key, .. } => CommandMeta {
                name: "ZRANGEBYSCORE",
                category: "sortedset",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::ZRevRangeByScore { key, .. } => CommandMeta {
                name: "ZREVRANGEBYSCORE",
                category: "sortedset",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::ZIncrBy { key, .. } => CommandMeta {
                name: "ZINCRBY",
                category: "sortedset",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::ZPopMin { key, .. } => CommandMeta {
                name: "ZPOPMIN",
                category: "sortedset",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::ZPopMax { key, .. } => CommandMeta {
                name: "ZPOPMAX",
                category: "sortedset",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::ZMScore { key, .. } => CommandMeta {
                name: "ZMSCORE",
                category: "sortedset",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::ZUnion { keys, .. } => CommandMeta {
                name: "ZUNION",
                category: "sortedset",
                keys: keys.clone(),
                permission: Permission::Read,
            },
            Command::ZUnionStore {
                destination, keys, ..
            } => CommandMeta {
                name: "ZUNIONSTORE",
                category: "sortedset",
                keys: std::iter::once(destination.clone())
                    .chain(keys.clone())
                    .collect(),
                permission: Permission::Write,
            },
            Command::ZInter { keys, .. } => CommandMeta {
                name: "ZINTER",
                category: "sortedset",
                keys: keys.clone(),
                permission: Permission::Read,
            },
            Command::ZInterStore {
                destination, keys, ..
            } => CommandMeta {
                name: "ZINTERSTORE",
                category: "sortedset",
                keys: std::iter::once(destination.clone())
                    .chain(keys.clone())
                    .collect(),
                permission: Permission::Write,
            },
            Command::ZInterCard { keys, .. } => CommandMeta {
                name: "ZINTERCARD",
                category: "sortedset",
                keys: keys.clone(),
                permission: Permission::Read,
            },
            Command::ZDiff { keys, .. } => CommandMeta {
                name: "ZDIFF",
                category: "sortedset",
                keys: keys.clone(),
                permission: Permission::Read,
            },
            Command::ZDiffStore {
                destination, keys, ..
            } => CommandMeta {
                name: "ZDIFFSTORE",
                category: "sortedset",
                keys: std::iter::once(destination.clone())
                    .chain(keys.clone())
                    .collect(),
                permission: Permission::Write,
            },
            Command::ZRangeByLex { key, .. } => CommandMeta {
                name: "ZRANGEBYLEX",
                category: "sortedset",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::ZRevRangeByLex { key, .. } => CommandMeta {
                name: "ZREVRANGEBYLEX",
                category: "sortedset",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::ZLexCount { key, .. } => CommandMeta {
                name: "ZLEXCOUNT",
                category: "sortedset",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::ZRemRangeByRank { key, .. } => CommandMeta {
                name: "ZREMRANGEBYRANK",
                category: "sortedset",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::ZRemRangeByScore { key, .. } => CommandMeta {
                name: "ZREMRANGEBYSCORE",
                category: "sortedset",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::ZRemRangeByLex { key, .. } => CommandMeta {
                name: "ZREMRANGEBYLEX",
                category: "sortedset",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::ZRandMember { key, .. } => CommandMeta {
                name: "ZRANDMEMBER",
                category: "sortedset",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::ZMPop { keys, .. } => CommandMeta {
                name: "ZMPOP",
                category: "sortedset",
                keys: keys.clone(),
                permission: Permission::Write,
            },
            Command::BZMPop { keys, .. } => CommandMeta {
                name: "BZMPOP",
                category: "sortedset",
                keys: keys.clone(),
                permission: Permission::Write,
            },
            Command::BZPopMin { keys, .. } => CommandMeta {
                name: "BZPOPMIN",
                category: "sortedset",
                keys: keys.clone(),
                permission: Permission::Write,
            },
            Command::BZPopMax { keys, .. } => CommandMeta {
                name: "BZPOPMAX",
                category: "sortedset",
                keys: keys.clone(),
                permission: Permission::Write,
            },

            // Pub/Sub commands
            Command::Subscribe { .. } => CommandMeta {
                name: "SUBSCRIBE",
                category: "pubsub",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::Unsubscribe { .. } => CommandMeta {
                name: "UNSUBSCRIBE",
                category: "pubsub",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::PSubscribe { .. } => CommandMeta {
                name: "PSUBSCRIBE",
                category: "pubsub",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::PUnsubscribe { .. } => CommandMeta {
                name: "PUNSUBSCRIBE",
                category: "pubsub",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::Publish { .. } => CommandMeta {
                name: "PUBLISH",
                category: "pubsub",
                keys: vec![],
                permission: Permission::Write,
            },
            Command::PubSubChannels { .. } => CommandMeta {
                name: "PUBSUB",
                category: "pubsub",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::PubSubNumSub { .. } => CommandMeta {
                name: "PUBSUB",
                category: "pubsub",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::PubSubNumPat => CommandMeta {
                name: "PUBSUB",
                category: "pubsub",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::SSubscribe { .. } => CommandMeta {
                name: "SSUBSCRIBE",
                category: "pubsub",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::SUnsubscribe { .. } => CommandMeta {
                name: "SUNSUBSCRIBE",
                category: "pubsub",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::SPublish { channel, .. } => CommandMeta {
                name: "SPUBLISH",
                category: "pubsub",
                keys: vec![channel.clone()],
                permission: Permission::Write,
            },
            Command::PubSubShardChannels { .. } => CommandMeta {
                name: "PUBSUB",
                category: "pubsub",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::PubSubShardNumSub { .. } => CommandMeta {
                name: "PUBSUB",
                category: "pubsub",
                keys: vec![],
                permission: Permission::Read,
            },

            // Transaction commands
            Command::Multi => CommandMeta {
                name: "MULTI",
                category: "transaction",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::Exec => CommandMeta {
                name: "EXEC",
                category: "transaction",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::Discard => CommandMeta {
                name: "DISCARD",
                category: "transaction",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::Watch { keys } => CommandMeta {
                name: "WATCH",
                category: "transaction",
                keys: keys.clone(),
                permission: Permission::Read,
            },
            Command::Unwatch => CommandMeta {
                name: "UNWATCH",
                category: "transaction",
                keys: vec![],
                permission: Permission::Read,
            },

            // Server commands
            Command::Ping { .. } => CommandMeta {
                name: "PING",
                category: "connection",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::Echo { .. } => CommandMeta {
                name: "ECHO",
                category: "connection",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::Select { .. } => CommandMeta {
                name: "SELECT",
                category: "connection",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::Info { .. } => CommandMeta {
                name: "INFO",
                category: "admin",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::DbSize => CommandMeta {
                name: "DBSIZE",
                category: "keyspace",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::FlushDb { .. } => CommandMeta {
                name: "FLUSHDB",
                category: "dangerous",
                keys: vec![],
                permission: Permission::Write,
            },
            Command::FlushAll { .. } => CommandMeta {
                name: "FLUSHALL",
                category: "dangerous",
                keys: vec![],
                permission: Permission::Write,
            },
            Command::Time => CommandMeta {
                name: "TIME",
                category: "server",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::CommandCmd { .. } => CommandMeta {
                name: "COMMAND",
                category: "server",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::Debug { .. } => CommandMeta {
                name: "DEBUG",
                category: "admin",
                keys: vec![],
                permission: Permission::Write,
            },
            Command::Memory { .. } => CommandMeta {
                name: "MEMORY",
                category: "admin",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::Client { .. } => CommandMeta {
                name: "CLIENT",
                category: "connection",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::Config { .. } => CommandMeta {
                name: "CONFIG",
                category: "server",
                keys: vec![],
                permission: Permission::Write,
            },
            Command::Monitor => CommandMeta {
                name: "MONITOR",
                category: "server",
                keys: vec![],
                permission: Permission::Write,
            },
            Command::Reset => CommandMeta {
                name: "RESET",
                category: "connection",
                keys: vec![],
                permission: Permission::Write,
            },
            Command::Role => CommandMeta {
                name: "ROLE",
                category: "server",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::Module { .. } => CommandMeta {
                name: "MODULE",
                category: "server",
                keys: vec![],
                permission: Permission::Write,
            },
            Command::BgSave { .. } => CommandMeta {
                name: "BGSAVE",
                category: "server",
                keys: vec![],
                permission: Permission::Write,
            },
            Command::Save => CommandMeta {
                name: "SAVE",
                category: "server",
                keys: vec![],
                permission: Permission::Write,
            },
            Command::BgRewriteAof => CommandMeta {
                name: "BGREWRITEAOF",
                category: "server",
                keys: vec![],
                permission: Permission::Write,
            },
            Command::LastSave => CommandMeta {
                name: "LASTSAVE",
                category: "server",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::SlowLog { .. } => CommandMeta {
                name: "SLOWLOG",
                category: "server",
                keys: vec![],
                permission: Permission::Write,
            },
            Command::Latency { .. } => CommandMeta {
                name: "LATENCY",
                category: "server",
                keys: vec![],
                permission: Permission::Write,
            },
            Command::Wait { .. } => CommandMeta {
                name: "WAIT",
                category: "generic",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::Shutdown { .. } => CommandMeta {
                name: "SHUTDOWN",
                category: "server",
                keys: vec![],
                permission: Permission::Write,
            },
            Command::SwapDb { .. } => CommandMeta {
                name: "SWAPDB",
                category: "server",
                keys: vec![],
                permission: Permission::Write,
            },
            Command::Move { key, .. } => CommandMeta {
                name: "MOVE",
                category: "generic",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::Migrate { key, keys, .. } => CommandMeta {
                name: "MIGRATE",
                category: "generic",
                keys: {
                    let mut all_keys = keys.clone();
                    if let Some(k) = key {
                        all_keys.insert(0, k.clone());
                    }
                    all_keys
                },
                permission: Permission::Write,
            },
            Command::Quit => CommandMeta {
                name: "QUIT",
                category: "connection",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::Hello { .. } => CommandMeta {
                name: "HELLO",
                category: "connection",
                keys: vec![],
                permission: Permission::Read,
            },

            // Replication commands
            Command::ReplicaOf { .. } | Command::SlaveOf { .. } => CommandMeta {
                name: "REPLICAOF",
                category: "admin",
                keys: vec![],
                permission: Permission::Write,
            },
            Command::ReplConf { .. } => CommandMeta {
                name: "REPLCONF",
                category: "admin",
                keys: vec![],
                permission: Permission::Write,
            },
            Command::Psync { .. } => CommandMeta {
                name: "PSYNC",
                category: "admin",
                keys: vec![],
                permission: Permission::Read,
            },

            // Auth commands
            Command::Auth { .. } => CommandMeta {
                name: "AUTH",
                category: "connection",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::Acl { .. } => CommandMeta {
                name: "ACL",
                category: "admin",
                keys: vec![],
                permission: Permission::Read,
            },

            // Cluster commands
            Command::Cluster { .. } => CommandMeta {
                name: "CLUSTER",
                category: "admin",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::Tiering { key, .. } => CommandMeta {
                name: "TIERING",
                category: "admin",
                keys: key.as_ref().map(|k| vec![k.clone()]).unwrap_or_default(),
                permission: Permission::ReadWrite,
            },
            Command::Cdc { .. } => CommandMeta {
                name: "CDC",
                category: "admin",
                keys: vec![],
                permission: Permission::ReadWrite,
            },
            Command::History { key, .. } => CommandMeta {
                name: "HISTORY",
                category: "temporal",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::HistoryCount { key, .. } => CommandMeta {
                name: "HISTORY.COUNT",
                category: "temporal",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::HistoryFirst { key } => CommandMeta {
                name: "HISTORY.FIRST",
                category: "temporal",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::HistoryLast { key } => CommandMeta {
                name: "HISTORY.LAST",
                category: "temporal",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::Diff { key, .. } => CommandMeta {
                name: "DIFF",
                category: "temporal",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::RestoreFrom { key, target, .. } => CommandMeta {
                name: "RESTORE.FROM",
                category: "temporal",
                keys: {
                    let mut keys = vec![key.clone()];
                    if let Some(t) = target {
                        keys.push(t.clone());
                    }
                    keys
                },
                permission: Permission::ReadWrite,
            },
            Command::Temporal { .. } => CommandMeta {
                name: "TEMPORAL",
                category: "temporal",
                keys: vec![],
                permission: Permission::ReadWrite,
            },
            Command::Asking => CommandMeta {
                name: "ASKING",
                category: "cluster",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::ReadOnly => CommandMeta {
                name: "READONLY",
                category: "cluster",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::ReadWrite => CommandMeta {
                name: "READWRITE",
                category: "cluster",
                keys: vec![],
                permission: Permission::Read,
            },

            // Scripting commands
            Command::Eval { keys, .. } => CommandMeta {
                name: "EVAL",
                category: "scripting",
                keys: keys.clone(),
                permission: Permission::Write, // Scripts can modify data
            },
            Command::EvalSha { keys, .. } => CommandMeta {
                name: "EVALSHA",
                category: "scripting",
                keys: keys.clone(),
                permission: Permission::Write,
            },
            Command::Script { .. } => CommandMeta {
                name: "SCRIPT",
                category: "scripting",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::EvalRo { keys, .. } => CommandMeta {
                name: "EVAL_RO",
                category: "scripting",
                keys: keys.clone(),
                permission: Permission::Read,
            },
            Command::EvalShaRo { keys, .. } => CommandMeta {
                name: "EVALSHA_RO",
                category: "scripting",
                keys: keys.clone(),
                permission: Permission::Read,
            },
            Command::Function { .. } => CommandMeta {
                name: "FUNCTION",
                category: "scripting",
                keys: vec![],
                permission: Permission::Write,
            },
            Command::FCall { keys, .. } => CommandMeta {
                name: "FCALL",
                category: "scripting",
                keys: keys.clone(),
                permission: Permission::Write,
            },
            Command::FCallRo { keys, .. } => CommandMeta {
                name: "FCALL_RO",
                category: "scripting",
                keys: keys.clone(),
                permission: Permission::Read,
            },

            // Stream commands
            Command::XAdd { key, .. } => CommandMeta {
                name: "XADD",
                category: "stream",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::XLen { key } => CommandMeta {
                name: "XLEN",
                category: "stream",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::XRange { key, .. } => CommandMeta {
                name: "XRANGE",
                category: "stream",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::XRevRange { key, .. } => CommandMeta {
                name: "XREVRANGE",
                category: "stream",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::XRead { streams, .. } => CommandMeta {
                name: "XREAD",
                category: "stream",
                keys: streams.iter().map(|(k, _)| k.clone()).collect(),
                permission: Permission::Read,
            },
            Command::XDel { key, .. } => CommandMeta {
                name: "XDEL",
                category: "stream",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::XTrim { key, .. } => CommandMeta {
                name: "XTRIM",
                category: "stream",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::XInfo { key, .. } => CommandMeta {
                name: "XINFO",
                category: "stream",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::XGroupCreate { key, .. } => CommandMeta {
                name: "XGROUP",
                category: "stream",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::XGroupDestroy { key, .. } => CommandMeta {
                name: "XGROUP",
                category: "stream",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::XGroupCreateConsumer { key, .. } => CommandMeta {
                name: "XGROUP",
                category: "stream",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::XGroupDelConsumer { key, .. } => CommandMeta {
                name: "XGROUP",
                category: "stream",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::XGroupSetId { key, .. } => CommandMeta {
                name: "XGROUP",
                category: "stream",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::XAck { key, .. } => CommandMeta {
                name: "XACK",
                category: "stream",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::XPending { key, .. } => CommandMeta {
                name: "XPENDING",
                category: "stream",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::XClaim { key, .. } => CommandMeta {
                name: "XCLAIM",
                category: "stream",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::XAutoClaim { key, .. } => CommandMeta {
                name: "XAUTOCLAIM",
                category: "stream",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::XReadGroup { streams, .. } => CommandMeta {
                name: "XREADGROUP",
                category: "stream",
                keys: streams.iter().map(|(k, _)| k.clone()).collect(),
                permission: Permission::Write,
            },

            // Geo commands
            Command::GeoAdd { key, .. } => CommandMeta {
                name: "GEOADD",
                category: "geo",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::GeoPos { key, .. } => CommandMeta {
                name: "GEOPOS",
                category: "geo",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::GeoDist { key, .. } => CommandMeta {
                name: "GEODIST",
                category: "geo",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::GeoHash { key, .. } => CommandMeta {
                name: "GEOHASH",
                category: "geo",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::GeoRadius { key, .. } => CommandMeta {
                name: "GEORADIUS",
                category: "geo",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::GeoRadiusByMember { key, .. } => CommandMeta {
                name: "GEORADIUSBYMEMBER",
                category: "geo",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::GeoSearch { key, .. } => CommandMeta {
                name: "GEOSEARCH",
                category: "geo",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },

            // HyperLogLog commands
            Command::PfAdd { key, .. } => CommandMeta {
                name: "PFADD",
                category: "hyperloglog",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::PfCount { keys, .. } => CommandMeta {
                name: "PFCOUNT",
                category: "hyperloglog",
                keys: keys.clone(),
                permission: Permission::Read,
            },
            Command::PfMerge {
                destkey,
                sourcekeys,
                ..
            } => CommandMeta {
                name: "PFMERGE",
                category: "hyperloglog",
                keys: std::iter::once(destkey.clone())
                    .chain(sourcekeys.clone())
                    .collect(),
                permission: Permission::Write,
            },

            // Scan commands
            Command::Scan { .. } => CommandMeta {
                name: "SCAN",
                category: "keyspace",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::SScan { key, .. } => CommandMeta {
                name: "SSCAN",
                category: "set",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::ZScan { key, .. } => CommandMeta {
                name: "ZSCAN",
                category: "sortedset",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },

            // Bitmap commands
            Command::GetBit { key, .. } => CommandMeta {
                name: "GETBIT",
                category: "bitmap",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::SetBit { key, .. } => CommandMeta {
                name: "SETBIT",
                category: "bitmap",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::BitCount { key, .. } => CommandMeta {
                name: "BITCOUNT",
                category: "bitmap",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::BitOp { destkey, keys, .. } => CommandMeta {
                name: "BITOP",
                category: "bitmap",
                keys: std::iter::once(destkey.clone())
                    .chain(keys.clone())
                    .collect(),
                permission: Permission::Write,
            },
            Command::BitPos { key, .. } => CommandMeta {
                name: "BITPOS",
                category: "bitmap",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::BitField { key, .. } => CommandMeta {
                name: "BITFIELD",
                category: "bitmap",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::BitFieldRo { key, .. } => CommandMeta {
                name: "BITFIELD_RO",
                category: "bitmap",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },

            // Vector Search commands
            Command::FtCreate { index, .. } => CommandMeta {
                name: "FT.CREATE",
                category: "vector",
                keys: vec![index.clone()],
                permission: Permission::Write,
            },
            Command::FtDropIndex { index, .. } => CommandMeta {
                name: "FT.DROPINDEX",
                category: "vector",
                keys: vec![index.clone()],
                permission: Permission::Write,
            },
            Command::FtAdd { index, key, .. } => CommandMeta {
                name: "FT.ADD",
                category: "vector",
                keys: vec![index.clone(), key.clone()],
                permission: Permission::Write,
            },
            Command::FtDel { index, key } => CommandMeta {
                name: "FT.DEL",
                category: "vector",
                keys: vec![index.clone(), key.clone()],
                permission: Permission::Write,
            },
            Command::FtSearch { index, .. } => CommandMeta {
                name: "FT.SEARCH",
                category: "vector",
                keys: vec![index.clone()],
                permission: Permission::Read,
            },
            Command::FtInfo { index } => CommandMeta {
                name: "FT.INFO",
                category: "vector",
                keys: vec![index.clone()],
                permission: Permission::Read,
            },
            Command::FtList => CommandMeta {
                name: "FT._LIST",
                category: "vector",
                keys: vec![],
                permission: Permission::Read,
            },

            // CRDT commands
            Command::CrdtGCounter { key, .. } => CommandMeta {
                name: "CRDT.GCOUNTER",
                category: "crdt",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::CrdtPNCounter { key, .. } => CommandMeta {
                name: "CRDT.PNCOUNTER",
                category: "crdt",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::CrdtLwwRegister { key, .. } => CommandMeta {
                name: "CRDT.LWWREG",
                category: "crdt",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::CrdtOrSet { key, .. } => CommandMeta {
                name: "CRDT.ORSET",
                category: "crdt",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::CrdtInfo { key } => CommandMeta {
                name: "CRDT.INFO",
                category: "crdt",
                keys: key.as_ref().map(|k| vec![k.clone()]).unwrap_or_default(),
                permission: Permission::Read,
            },

            // WASM commands
            Command::WasmLoad { .. } => CommandMeta {
                name: "WASM.LOAD",
                category: "wasm",
                keys: vec![],
                permission: Permission::Write,
            },
            Command::WasmUnload { .. } => CommandMeta {
                name: "WASM.UNLOAD",
                category: "wasm",
                keys: vec![],
                permission: Permission::Write,
            },
            Command::WasmCall { keys, .. } => CommandMeta {
                name: "WASM.CALL",
                category: "wasm",
                keys: keys.clone(),
                permission: Permission::Write,
            },
            Command::WasmCallRo { keys, .. } => CommandMeta {
                name: "WASM.CALL_RO",
                category: "wasm",
                keys: keys.clone(),
                permission: Permission::Read,
            },
            Command::WasmList { .. } => CommandMeta {
                name: "WASM.LIST",
                category: "wasm",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::WasmInfo { .. } => CommandMeta {
                name: "WASM.INFO",
                category: "wasm",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::WasmStats => CommandMeta {
                name: "WASM.STATS",
                category: "wasm",
                keys: vec![],
                permission: Permission::Read,
            },

            // Semantic Caching commands
            Command::SemanticSet { query, .. } => CommandMeta {
                name: "SEMANTIC.SET",
                category: "semantic",
                keys: vec![query.clone()],
                permission: Permission::Write,
            },
            Command::SemanticGet { .. } => CommandMeta {
                name: "SEMANTIC.GET",
                category: "semantic",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::SemanticGetText { query, .. } => CommandMeta {
                name: "SEMANTIC.GETTEXT",
                category: "semantic",
                keys: vec![query.clone()],
                permission: Permission::Read,
            },
            Command::SemanticDel { .. } => CommandMeta {
                name: "SEMANTIC.DEL",
                category: "semantic",
                keys: vec![],
                permission: Permission::Write,
            },
            Command::SemanticClear => CommandMeta {
                name: "SEMANTIC.CLEAR",
                category: "semantic",
                keys: vec![],
                permission: Permission::Write,
            },
            Command::SemanticInfo => CommandMeta {
                name: "SEMANTIC.INFO",
                category: "semantic",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::SemanticStats => CommandMeta {
                name: "SEMANTIC.STATS",
                category: "semantic",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::SemanticConfig { .. } => CommandMeta {
                name: "SEMANTIC.CONFIG",
                category: "semantic",
                keys: vec![],
                permission: Permission::Write,
            },

            // Trigger commands
            Command::TriggerCreate { name, .. } => CommandMeta {
                name: "TRIGGER.CREATE",
                category: "trigger",
                keys: vec![name.clone()],
                permission: Permission::Write,
            },
            Command::TriggerDelete { name } => CommandMeta {
                name: "TRIGGER.DELETE",
                category: "trigger",
                keys: vec![name.clone()],
                permission: Permission::Write,
            },
            Command::TriggerGet { name } => CommandMeta {
                name: "TRIGGER.GET",
                category: "trigger",
                keys: vec![name.clone()],
                permission: Permission::Read,
            },
            Command::TriggerList { .. } => CommandMeta {
                name: "TRIGGER.LIST",
                category: "trigger",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::TriggerEnable { name } => CommandMeta {
                name: "TRIGGER.ENABLE",
                category: "trigger",
                keys: vec![name.clone()],
                permission: Permission::Write,
            },
            Command::TriggerDisable { name } => CommandMeta {
                name: "TRIGGER.DISABLE",
                category: "trigger",
                keys: vec![name.clone()],
                permission: Permission::Write,
            },
            Command::TriggerFire { name, key, .. } => CommandMeta {
                name: "TRIGGER.FIRE",
                category: "trigger",
                keys: vec![name.clone(), key.clone()],
                permission: Permission::Write,
            },
            Command::TriggerInfo => CommandMeta {
                name: "TRIGGER.INFO",
                category: "trigger",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::TriggerStats => CommandMeta {
                name: "TRIGGER.STATS",
                category: "trigger",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::TriggerConfig { .. } => CommandMeta {
                name: "TRIGGER.CONFIG",
                category: "trigger",
                keys: vec![],
                permission: Permission::Write,
            },

            // Time-Series commands
            Command::TimeSeries { subcommand, .. } => CommandMeta {
                name: Box::leak(format!("TS.{}", subcommand).into_boxed_str()),
                category: "timeseries",
                keys: vec![],
                permission: Permission::Write,
            },

            // Document database commands
            Command::Document { subcommand, .. } => CommandMeta {
                name: Box::leak(format!("DOC.{}", subcommand).into_boxed_str()),
                category: "document",
                keys: vec![],
                permission: Permission::Write,
            },

            // Graph database commands
            Command::Graph { subcommand, .. } => CommandMeta {
                name: Box::leak(format!("GRAPH.{}", subcommand).into_boxed_str()),
                category: "graph",
                keys: vec![],
                permission: Permission::Write,
            },

            // RAG pipeline commands
            Command::Rag { subcommand, .. } => CommandMeta {
                name: Box::leak(format!("RAG.{}", subcommand).into_boxed_str()),
                category: "rag",
                keys: vec![],
                permission: Permission::Write,
            },

            Command::Query { subcommand, .. } => CommandMeta {
                name: Box::leak(format!("QUERY.{}", subcommand).into_boxed_str()),
                category: "query",
                keys: vec![],
                permission: Permission::Read,
            },

            // Key management commands
            Command::Keys { .. } => CommandMeta {
                name: "KEYS",
                category: "keyspace",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::Type { key } => CommandMeta {
                name: "TYPE",
                category: "keyspace",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::Rename { key, new_key } => CommandMeta {
                name: "RENAME",
                category: "keyspace",
                keys: vec![key.clone(), new_key.clone()],
                permission: Permission::Write,
            },
            Command::RenameNx { key, new_key } => CommandMeta {
                name: "RENAMENX",
                category: "keyspace",
                keys: vec![key.clone(), new_key.clone()],
                permission: Permission::Write,
            },
            Command::Copy {
                source,
                destination,
                ..
            } => CommandMeta {
                name: "COPY",
                category: "keyspace",
                keys: vec![source.clone(), destination.clone()],
                permission: Permission::Write,
            },
            Command::Unlink { keys } => CommandMeta {
                name: "UNLINK",
                category: "keyspace",
                keys: keys.clone(),
                permission: Permission::Write,
            },
            Command::Touch { keys } => CommandMeta {
                name: "TOUCH",
                category: "keyspace",
                keys: keys.clone(),
                permission: Permission::Read,
            },
            Command::RandomKey => CommandMeta {
                name: "RANDOMKEY",
                category: "keyspace",
                keys: vec![],
                permission: Permission::Read,
            },
            Command::Object { key, .. } => CommandMeta {
                name: "OBJECT",
                category: "keyspace",
                keys: key.clone().into_iter().collect(),
                permission: Permission::Read,
            },
            Command::ExpireAt { key, .. } => CommandMeta {
                name: "EXPIREAT",
                category: "keyspace",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::PExpireAt { key, .. } => CommandMeta {
                name: "PEXPIREAT",
                category: "keyspace",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::ExpireTime { key } => CommandMeta {
                name: "EXPIRETIME",
                category: "keyspace",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::PExpireTime { key } => CommandMeta {
                name: "PEXPIRETIME",
                category: "keyspace",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::Dump { key } => CommandMeta {
                name: "DUMP",
                category: "keyspace",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
            Command::Restore { key, .. } => CommandMeta {
                name: "RESTORE",
                category: "keyspace",
                keys: vec![key.clone()],
                permission: Permission::Write,
            },
            Command::Sort { key, .. } => CommandMeta {
                name: "SORT",
                category: "keyspace",
                keys: vec![key.clone()],
                permission: Permission::Read,
            },
        }
    }
}
