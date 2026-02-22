//! Redis command metadata for tab completion and help

use std::collections::HashMap;

/// Information about a Redis command
// Reserved for CLI command metadata system
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct CommandInfo {
    pub name: &'static str,
    pub arity: i32, // Negative for variable, positive for fixed
    pub summary: &'static str,
}

/// Registry of all Redis commands
pub struct CommandRegistry {
    commands: HashMap<&'static str, CommandInfo>,
}

impl CommandRegistry {
    /// Create a new command registry with all known commands
    pub fn new() -> Self {
        let mut commands = HashMap::new();

        // String commands
        commands.insert(
            "GET",
            CommandInfo {
                name: "GET",
                arity: 2,
                summary: "Get the value of a key",
            },
        );
        commands.insert(
            "SET",
            CommandInfo {
                name: "SET",
                arity: -3,
                summary: "Set the value of a key",
            },
        );
        commands.insert(
            "SETNX",
            CommandInfo {
                name: "SETNX",
                arity: 3,
                summary: "Set key value only if key does not exist",
            },
        );
        commands.insert(
            "SETEX",
            CommandInfo {
                name: "SETEX",
                arity: 4,
                summary: "Set key value with expiration in seconds",
            },
        );
        commands.insert(
            "PSETEX",
            CommandInfo {
                name: "PSETEX",
                arity: 4,
                summary: "Set key value with expiration in milliseconds",
            },
        );
        commands.insert(
            "MGET",
            CommandInfo {
                name: "MGET",
                arity: -2,
                summary: "Get values of multiple keys",
            },
        );
        commands.insert(
            "MSET",
            CommandInfo {
                name: "MSET",
                arity: -3,
                summary: "Set multiple keys to multiple values",
            },
        );
        commands.insert(
            "MSETNX",
            CommandInfo {
                name: "MSETNX",
                arity: -3,
                summary: "Set multiple keys only if none exist",
            },
        );
        commands.insert(
            "INCR",
            CommandInfo {
                name: "INCR",
                arity: 2,
                summary: "Increment the integer value of a key by one",
            },
        );
        commands.insert(
            "INCRBY",
            CommandInfo {
                name: "INCRBY",
                arity: 3,
                summary: "Increment the integer value of a key by amount",
            },
        );
        commands.insert(
            "INCRBYFLOAT",
            CommandInfo {
                name: "INCRBYFLOAT",
                arity: 3,
                summary: "Increment the float value of a key",
            },
        );
        commands.insert(
            "DECR",
            CommandInfo {
                name: "DECR",
                arity: 2,
                summary: "Decrement the integer value of a key by one",
            },
        );
        commands.insert(
            "DECRBY",
            CommandInfo {
                name: "DECRBY",
                arity: 3,
                summary: "Decrement the integer value of a key",
            },
        );
        commands.insert(
            "APPEND",
            CommandInfo {
                name: "APPEND",
                arity: 3,
                summary: "Append a value to a key",
            },
        );
        commands.insert(
            "STRLEN",
            CommandInfo {
                name: "STRLEN",
                arity: 2,
                summary: "Get the length of the value stored at a key",
            },
        );
        commands.insert(
            "GETRANGE",
            CommandInfo {
                name: "GETRANGE",
                arity: 4,
                summary: "Get a substring of the string stored at a key",
            },
        );
        commands.insert(
            "SETRANGE",
            CommandInfo {
                name: "SETRANGE",
                arity: 4,
                summary: "Overwrite part of a string at key starting at offset",
            },
        );
        commands.insert(
            "GETSET",
            CommandInfo {
                name: "GETSET",
                arity: 3,
                summary: "Set the value and return the old value",
            },
        );
        commands.insert(
            "GETDEL",
            CommandInfo {
                name: "GETDEL",
                arity: 2,
                summary: "Get the value and delete the key",
            },
        );
        commands.insert(
            "GETEX",
            CommandInfo {
                name: "GETEX",
                arity: -2,
                summary: "Get the value and optionally set expiration",
            },
        );

        // Key commands
        commands.insert(
            "DEL",
            CommandInfo {
                name: "DEL",
                arity: -2,
                summary: "Delete one or more keys",
            },
        );
        commands.insert(
            "EXISTS",
            CommandInfo {
                name: "EXISTS",
                arity: -2,
                summary: "Check if key(s) exist",
            },
        );
        commands.insert(
            "EXPIRE",
            CommandInfo {
                name: "EXPIRE",
                arity: 3,
                summary: "Set a key's time to live in seconds",
            },
        );
        commands.insert(
            "EXPIREAT",
            CommandInfo {
                name: "EXPIREAT",
                arity: 3,
                summary: "Set expiration as Unix timestamp",
            },
        );
        commands.insert(
            "PEXPIRE",
            CommandInfo {
                name: "PEXPIRE",
                arity: 3,
                summary: "Set a key's time to live in milliseconds",
            },
        );
        commands.insert(
            "PEXPIREAT",
            CommandInfo {
                name: "PEXPIREAT",
                arity: 3,
                summary: "Set expiration as Unix timestamp in ms",
            },
        );
        commands.insert(
            "TTL",
            CommandInfo {
                name: "TTL",
                arity: 2,
                summary: "Get the time to live in seconds",
            },
        );
        commands.insert(
            "PTTL",
            CommandInfo {
                name: "PTTL",
                arity: 2,
                summary: "Get the time to live in milliseconds",
            },
        );
        commands.insert(
            "PERSIST",
            CommandInfo {
                name: "PERSIST",
                arity: 2,
                summary: "Remove the expiration from a key",
            },
        );
        commands.insert(
            "TYPE",
            CommandInfo {
                name: "TYPE",
                arity: 2,
                summary: "Get the type of a key",
            },
        );
        commands.insert(
            "RENAME",
            CommandInfo {
                name: "RENAME",
                arity: 3,
                summary: "Rename a key",
            },
        );
        commands.insert(
            "RENAMENX",
            CommandInfo {
                name: "RENAMENX",
                arity: 3,
                summary: "Rename a key only if new key doesn't exist",
            },
        );
        commands.insert(
            "KEYS",
            CommandInfo {
                name: "KEYS",
                arity: 2,
                summary: "Find all keys matching a pattern",
            },
        );
        commands.insert(
            "SCAN",
            CommandInfo {
                name: "SCAN",
                arity: -2,
                summary: "Incrementally iterate the keyspace",
            },
        );
        commands.insert(
            "RANDOMKEY",
            CommandInfo {
                name: "RANDOMKEY",
                arity: 1,
                summary: "Return a random key",
            },
        );
        commands.insert(
            "DUMP",
            CommandInfo {
                name: "DUMP",
                arity: 2,
                summary: "Serialize the value stored at key",
            },
        );
        commands.insert(
            "RESTORE",
            CommandInfo {
                name: "RESTORE",
                arity: -4,
                summary: "Create a key from serialized value",
            },
        );
        commands.insert(
            "TOUCH",
            CommandInfo {
                name: "TOUCH",
                arity: -2,
                summary: "Alter the last access time of keys",
            },
        );
        commands.insert(
            "UNLINK",
            CommandInfo {
                name: "UNLINK",
                arity: -2,
                summary: "Delete keys asynchronously",
            },
        );
        commands.insert(
            "COPY",
            CommandInfo {
                name: "COPY",
                arity: -3,
                summary: "Copy a key",
            },
        );
        commands.insert(
            "OBJECT",
            CommandInfo {
                name: "OBJECT",
                arity: -2,
                summary: "Inspect Redis objects",
            },
        );

        // List commands
        commands.insert(
            "LPUSH",
            CommandInfo {
                name: "LPUSH",
                arity: -3,
                summary: "Prepend values to a list",
            },
        );
        commands.insert(
            "RPUSH",
            CommandInfo {
                name: "RPUSH",
                arity: -3,
                summary: "Append values to a list",
            },
        );
        commands.insert(
            "LPOP",
            CommandInfo {
                name: "LPOP",
                arity: -2,
                summary: "Remove and get the first element",
            },
        );
        commands.insert(
            "RPOP",
            CommandInfo {
                name: "RPOP",
                arity: -2,
                summary: "Remove and get the last element",
            },
        );
        commands.insert(
            "LRANGE",
            CommandInfo {
                name: "LRANGE",
                arity: 4,
                summary: "Get a range of elements",
            },
        );
        commands.insert(
            "LLEN",
            CommandInfo {
                name: "LLEN",
                arity: 2,
                summary: "Get the length of a list",
            },
        );
        commands.insert(
            "LINDEX",
            CommandInfo {
                name: "LINDEX",
                arity: 3,
                summary: "Get an element by index",
            },
        );
        commands.insert(
            "LSET",
            CommandInfo {
                name: "LSET",
                arity: 4,
                summary: "Set element at index",
            },
        );
        commands.insert(
            "LINSERT",
            CommandInfo {
                name: "LINSERT",
                arity: 5,
                summary: "Insert element before/after pivot",
            },
        );
        commands.insert(
            "LREM",
            CommandInfo {
                name: "LREM",
                arity: 4,
                summary: "Remove elements",
            },
        );
        commands.insert(
            "LTRIM",
            CommandInfo {
                name: "LTRIM",
                arity: 4,
                summary: "Trim list to specified range",
            },
        );
        commands.insert(
            "LPUSHX",
            CommandInfo {
                name: "LPUSHX",
                arity: -3,
                summary: "Prepend value only if list exists",
            },
        );
        commands.insert(
            "RPUSHX",
            CommandInfo {
                name: "RPUSHX",
                arity: -3,
                summary: "Append value only if list exists",
            },
        );
        commands.insert(
            "BLPOP",
            CommandInfo {
                name: "BLPOP",
                arity: -3,
                summary: "Blocking left pop",
            },
        );
        commands.insert(
            "BRPOP",
            CommandInfo {
                name: "BRPOP",
                arity: -3,
                summary: "Blocking right pop",
            },
        );
        commands.insert(
            "LPOS",
            CommandInfo {
                name: "LPOS",
                arity: -3,
                summary: "Find element position",
            },
        );
        commands.insert(
            "LMOVE",
            CommandInfo {
                name: "LMOVE",
                arity: 5,
                summary: "Move element between lists",
            },
        );
        commands.insert(
            "BLMOVE",
            CommandInfo {
                name: "BLMOVE",
                arity: 6,
                summary: "Blocking move element between lists",
            },
        );
        commands.insert(
            "LMPOP",
            CommandInfo {
                name: "LMPOP",
                arity: -4,
                summary: "Pop from multiple lists",
            },
        );
        commands.insert(
            "BLMPOP",
            CommandInfo {
                name: "BLMPOP",
                arity: -5,
                summary: "Blocking pop from multiple lists",
            },
        );

        // Hash commands
        commands.insert(
            "HSET",
            CommandInfo {
                name: "HSET",
                arity: -4,
                summary: "Set field(s) in a hash",
            },
        );
        commands.insert(
            "HGET",
            CommandInfo {
                name: "HGET",
                arity: 3,
                summary: "Get a field value",
            },
        );
        commands.insert(
            "HMSET",
            CommandInfo {
                name: "HMSET",
                arity: -4,
                summary: "Set multiple fields",
            },
        );
        commands.insert(
            "HMGET",
            CommandInfo {
                name: "HMGET",
                arity: -3,
                summary: "Get multiple field values",
            },
        );
        commands.insert(
            "HDEL",
            CommandInfo {
                name: "HDEL",
                arity: -3,
                summary: "Delete field(s)",
            },
        );
        commands.insert(
            "HEXISTS",
            CommandInfo {
                name: "HEXISTS",
                arity: 3,
                summary: "Check if field exists",
            },
        );
        commands.insert(
            "HGETALL",
            CommandInfo {
                name: "HGETALL",
                arity: 2,
                summary: "Get all fields and values",
            },
        );
        commands.insert(
            "HKEYS",
            CommandInfo {
                name: "HKEYS",
                arity: 2,
                summary: "Get all fields",
            },
        );
        commands.insert(
            "HVALS",
            CommandInfo {
                name: "HVALS",
                arity: 2,
                summary: "Get all values",
            },
        );
        commands.insert(
            "HLEN",
            CommandInfo {
                name: "HLEN",
                arity: 2,
                summary: "Get number of fields",
            },
        );
        commands.insert(
            "HINCRBY",
            CommandInfo {
                name: "HINCRBY",
                arity: 4,
                summary: "Increment integer field",
            },
        );
        commands.insert(
            "HINCRBYFLOAT",
            CommandInfo {
                name: "HINCRBYFLOAT",
                arity: 4,
                summary: "Increment float field",
            },
        );
        commands.insert(
            "HSETNX",
            CommandInfo {
                name: "HSETNX",
                arity: 4,
                summary: "Set field only if it doesn't exist",
            },
        );
        commands.insert(
            "HSTRLEN",
            CommandInfo {
                name: "HSTRLEN",
                arity: 3,
                summary: "Get string length of field value",
            },
        );
        commands.insert(
            "HSCAN",
            CommandInfo {
                name: "HSCAN",
                arity: -3,
                summary: "Incrementally iterate hash fields",
            },
        );
        commands.insert(
            "HRANDFIELD",
            CommandInfo {
                name: "HRANDFIELD",
                arity: -2,
                summary: "Get random field(s)",
            },
        );

        // Set commands
        commands.insert(
            "SADD",
            CommandInfo {
                name: "SADD",
                arity: -3,
                summary: "Add member(s) to a set",
            },
        );
        commands.insert(
            "SREM",
            CommandInfo {
                name: "SREM",
                arity: -3,
                summary: "Remove member(s) from a set",
            },
        );
        commands.insert(
            "SMEMBERS",
            CommandInfo {
                name: "SMEMBERS",
                arity: 2,
                summary: "Get all members",
            },
        );
        commands.insert(
            "SISMEMBER",
            CommandInfo {
                name: "SISMEMBER",
                arity: 3,
                summary: "Check if member exists",
            },
        );
        commands.insert(
            "SMISMEMBER",
            CommandInfo {
                name: "SMISMEMBER",
                arity: -3,
                summary: "Check multiple members",
            },
        );
        commands.insert(
            "SCARD",
            CommandInfo {
                name: "SCARD",
                arity: 2,
                summary: "Get set size",
            },
        );
        commands.insert(
            "SPOP",
            CommandInfo {
                name: "SPOP",
                arity: -2,
                summary: "Remove and return random member(s)",
            },
        );
        commands.insert(
            "SRANDMEMBER",
            CommandInfo {
                name: "SRANDMEMBER",
                arity: -2,
                summary: "Get random member(s)",
            },
        );
        commands.insert(
            "SINTER",
            CommandInfo {
                name: "SINTER",
                arity: -2,
                summary: "Intersect multiple sets",
            },
        );
        commands.insert(
            "SINTERSTORE",
            CommandInfo {
                name: "SINTERSTORE",
                arity: -3,
                summary: "Intersect and store",
            },
        );
        commands.insert(
            "SUNION",
            CommandInfo {
                name: "SUNION",
                arity: -2,
                summary: "Union multiple sets",
            },
        );
        commands.insert(
            "SUNIONSTORE",
            CommandInfo {
                name: "SUNIONSTORE",
                arity: -3,
                summary: "Union and store",
            },
        );
        commands.insert(
            "SDIFF",
            CommandInfo {
                name: "SDIFF",
                arity: -2,
                summary: "Difference between sets",
            },
        );
        commands.insert(
            "SDIFFSTORE",
            CommandInfo {
                name: "SDIFFSTORE",
                arity: -3,
                summary: "Difference and store",
            },
        );
        commands.insert(
            "SMOVE",
            CommandInfo {
                name: "SMOVE",
                arity: 4,
                summary: "Move member between sets",
            },
        );
        commands.insert(
            "SSCAN",
            CommandInfo {
                name: "SSCAN",
                arity: -3,
                summary: "Incrementally iterate set members",
            },
        );
        commands.insert(
            "SINTERCARD",
            CommandInfo {
                name: "SINTERCARD",
                arity: -3,
                summary: "Cardinality of intersection",
            },
        );

        // Sorted set commands
        commands.insert(
            "ZADD",
            CommandInfo {
                name: "ZADD",
                arity: -4,
                summary: "Add member(s) with scores",
            },
        );
        commands.insert(
            "ZREM",
            CommandInfo {
                name: "ZREM",
                arity: -3,
                summary: "Remove member(s)",
            },
        );
        commands.insert(
            "ZSCORE",
            CommandInfo {
                name: "ZSCORE",
                arity: 3,
                summary: "Get member's score",
            },
        );
        commands.insert(
            "ZRANK",
            CommandInfo {
                name: "ZRANK",
                arity: 3,
                summary: "Get member's rank",
            },
        );
        commands.insert(
            "ZREVRANK",
            CommandInfo {
                name: "ZREVRANK",
                arity: 3,
                summary: "Get member's reverse rank",
            },
        );
        commands.insert(
            "ZRANGE",
            CommandInfo {
                name: "ZRANGE",
                arity: -4,
                summary: "Return range of members",
            },
        );
        commands.insert(
            "ZREVRANGE",
            CommandInfo {
                name: "ZREVRANGE",
                arity: -4,
                summary: "Return range in reverse",
            },
        );
        commands.insert(
            "ZRANGEBYSCORE",
            CommandInfo {
                name: "ZRANGEBYSCORE",
                arity: -4,
                summary: "Return members by score range",
            },
        );
        commands.insert(
            "ZREVRANGEBYSCORE",
            CommandInfo {
                name: "ZREVRANGEBYSCORE",
                arity: -4,
                summary: "Return members by score in reverse",
            },
        );
        commands.insert(
            "ZCARD",
            CommandInfo {
                name: "ZCARD",
                arity: 2,
                summary: "Get sorted set size",
            },
        );
        commands.insert(
            "ZCOUNT",
            CommandInfo {
                name: "ZCOUNT",
                arity: 4,
                summary: "Count members in score range",
            },
        );
        commands.insert(
            "ZINCRBY",
            CommandInfo {
                name: "ZINCRBY",
                arity: 4,
                summary: "Increment member's score",
            },
        );
        commands.insert(
            "ZUNIONSTORE",
            CommandInfo {
                name: "ZUNIONSTORE",
                arity: -4,
                summary: "Union and store",
            },
        );
        commands.insert(
            "ZINTERSTORE",
            CommandInfo {
                name: "ZINTERSTORE",
                arity: -4,
                summary: "Intersect and store",
            },
        );
        commands.insert(
            "ZSCAN",
            CommandInfo {
                name: "ZSCAN",
                arity: -3,
                summary: "Incrementally iterate sorted set",
            },
        );
        commands.insert(
            "ZLEXCOUNT",
            CommandInfo {
                name: "ZLEXCOUNT",
                arity: 4,
                summary: "Count members in lex range",
            },
        );
        commands.insert(
            "ZRANGEBYLEX",
            CommandInfo {
                name: "ZRANGEBYLEX",
                arity: -4,
                summary: "Return members by lex range",
            },
        );
        commands.insert(
            "ZREMRANGEBYLEX",
            CommandInfo {
                name: "ZREMRANGEBYLEX",
                arity: 4,
                summary: "Remove members by lex range",
            },
        );
        commands.insert(
            "ZREMRANGEBYRANK",
            CommandInfo {
                name: "ZREMRANGEBYRANK",
                arity: 4,
                summary: "Remove members by rank range",
            },
        );
        commands.insert(
            "ZREMRANGEBYSCORE",
            CommandInfo {
                name: "ZREMRANGEBYSCORE",
                arity: 4,
                summary: "Remove members by score range",
            },
        );
        commands.insert(
            "ZPOPMIN",
            CommandInfo {
                name: "ZPOPMIN",
                arity: -2,
                summary: "Pop member(s) with lowest score",
            },
        );
        commands.insert(
            "ZPOPMAX",
            CommandInfo {
                name: "ZPOPMAX",
                arity: -2,
                summary: "Pop member(s) with highest score",
            },
        );
        commands.insert(
            "BZPOPMIN",
            CommandInfo {
                name: "BZPOPMIN",
                arity: -3,
                summary: "Blocking pop min",
            },
        );
        commands.insert(
            "BZPOPMAX",
            CommandInfo {
                name: "BZPOPMAX",
                arity: -3,
                summary: "Blocking pop max",
            },
        );
        commands.insert(
            "ZMSCORE",
            CommandInfo {
                name: "ZMSCORE",
                arity: -3,
                summary: "Get scores of multiple members",
            },
        );
        commands.insert(
            "ZRANDMEMBER",
            CommandInfo {
                name: "ZRANDMEMBER",
                arity: -2,
                summary: "Get random member(s)",
            },
        );

        // Server commands
        commands.insert(
            "PING",
            CommandInfo {
                name: "PING",
                arity: -1,
                summary: "Ping the server",
            },
        );
        commands.insert(
            "ECHO",
            CommandInfo {
                name: "ECHO",
                arity: 2,
                summary: "Echo a message",
            },
        );
        commands.insert(
            "INFO",
            CommandInfo {
                name: "INFO",
                arity: -1,
                summary: "Get server information",
            },
        );
        commands.insert(
            "DBSIZE",
            CommandInfo {
                name: "DBSIZE",
                arity: 1,
                summary: "Return the number of keys",
            },
        );
        commands.insert(
            "FLUSHDB",
            CommandInfo {
                name: "FLUSHDB",
                arity: -1,
                summary: "Remove all keys from current database",
            },
        );
        commands.insert(
            "FLUSHALL",
            CommandInfo {
                name: "FLUSHALL",
                arity: -1,
                summary: "Remove all keys from all databases",
            },
        );
        commands.insert(
            "SELECT",
            CommandInfo {
                name: "SELECT",
                arity: 2,
                summary: "Change the selected database",
            },
        );
        commands.insert(
            "SAVE",
            CommandInfo {
                name: "SAVE",
                arity: 1,
                summary: "Synchronously save to disk",
            },
        );
        commands.insert(
            "BGSAVE",
            CommandInfo {
                name: "BGSAVE",
                arity: -1,
                summary: "Asynchronously save to disk",
            },
        );
        commands.insert(
            "BGREWRITEAOF",
            CommandInfo {
                name: "BGREWRITEAOF",
                arity: 1,
                summary: "Rewrite AOF file",
            },
        );
        commands.insert(
            "LASTSAVE",
            CommandInfo {
                name: "LASTSAVE",
                arity: 1,
                summary: "Get timestamp of last save",
            },
        );
        commands.insert(
            "TIME",
            CommandInfo {
                name: "TIME",
                arity: 1,
                summary: "Return server time",
            },
        );
        commands.insert(
            "CONFIG",
            CommandInfo {
                name: "CONFIG",
                arity: -2,
                summary: "Get/set configuration",
            },
        );
        commands.insert(
            "CLIENT",
            CommandInfo {
                name: "CLIENT",
                arity: -2,
                summary: "Client commands",
            },
        );
        commands.insert(
            "DEBUG",
            CommandInfo {
                name: "DEBUG",
                arity: -2,
                summary: "Debugging commands",
            },
        );
        commands.insert(
            "SLOWLOG",
            CommandInfo {
                name: "SLOWLOG",
                arity: -2,
                summary: "Manage slow query log",
            },
        );
        commands.insert(
            "MEMORY",
            CommandInfo {
                name: "MEMORY",
                arity: -2,
                summary: "Memory commands",
            },
        );
        commands.insert(
            "LATENCY",
            CommandInfo {
                name: "LATENCY",
                arity: -2,
                summary: "Latency commands",
            },
        );
        commands.insert(
            "COMMAND",
            CommandInfo {
                name: "COMMAND",
                arity: -1,
                summary: "Get command documentation",
            },
        );
        commands.insert(
            "SHUTDOWN",
            CommandInfo {
                name: "SHUTDOWN",
                arity: -1,
                summary: "Shut down the server",
            },
        );
        commands.insert(
            "ACL",
            CommandInfo {
                name: "ACL",
                arity: -2,
                summary: "Access control list commands",
            },
        );
        commands.insert(
            "MONITOR",
            CommandInfo {
                name: "MONITOR",
                arity: 1,
                summary: "Real-time command monitoring",
            },
        );
        commands.insert(
            "ROLE",
            CommandInfo {
                name: "ROLE",
                arity: 1,
                summary: "Return replication role",
            },
        );

        // Pub/Sub commands
        commands.insert(
            "SUBSCRIBE",
            CommandInfo {
                name: "SUBSCRIBE",
                arity: -2,
                summary: "Subscribe to channels",
            },
        );
        commands.insert(
            "UNSUBSCRIBE",
            CommandInfo {
                name: "UNSUBSCRIBE",
                arity: -1,
                summary: "Unsubscribe from channels",
            },
        );
        commands.insert(
            "PSUBSCRIBE",
            CommandInfo {
                name: "PSUBSCRIBE",
                arity: -2,
                summary: "Subscribe to patterns",
            },
        );
        commands.insert(
            "PUNSUBSCRIBE",
            CommandInfo {
                name: "PUNSUBSCRIBE",
                arity: -1,
                summary: "Unsubscribe from patterns",
            },
        );
        commands.insert(
            "PUBLISH",
            CommandInfo {
                name: "PUBLISH",
                arity: 3,
                summary: "Publish a message",
            },
        );
        commands.insert(
            "PUBSUB",
            CommandInfo {
                name: "PUBSUB",
                arity: -2,
                summary: "Pub/Sub introspection",
            },
        );

        // Transaction commands
        commands.insert(
            "MULTI",
            CommandInfo {
                name: "MULTI",
                arity: 1,
                summary: "Start a transaction",
            },
        );
        commands.insert(
            "EXEC",
            CommandInfo {
                name: "EXEC",
                arity: 1,
                summary: "Execute transaction",
            },
        );
        commands.insert(
            "DISCARD",
            CommandInfo {
                name: "DISCARD",
                arity: 1,
                summary: "Discard transaction",
            },
        );
        commands.insert(
            "WATCH",
            CommandInfo {
                name: "WATCH",
                arity: -2,
                summary: "Watch keys for changes",
            },
        );
        commands.insert(
            "UNWATCH",
            CommandInfo {
                name: "UNWATCH",
                arity: 1,
                summary: "Unwatch all keys",
            },
        );

        // Scripting commands
        commands.insert(
            "EVAL",
            CommandInfo {
                name: "EVAL",
                arity: -3,
                summary: "Execute Lua script",
            },
        );
        commands.insert(
            "EVALSHA",
            CommandInfo {
                name: "EVALSHA",
                arity: -3,
                summary: "Execute cached script",
            },
        );
        commands.insert(
            "SCRIPT",
            CommandInfo {
                name: "SCRIPT",
                arity: -2,
                summary: "Script subcommands",
            },
        );

        // Replication commands
        commands.insert(
            "REPLICAOF",
            CommandInfo {
                name: "REPLICAOF",
                arity: 3,
                summary: "Set replication master",
            },
        );
        commands.insert(
            "SLAVEOF",
            CommandInfo {
                name: "SLAVEOF",
                arity: 3,
                summary: "Set replication master (deprecated)",
            },
        );

        // Auth
        commands.insert(
            "AUTH",
            CommandInfo {
                name: "AUTH",
                arity: -2,
                summary: "Authenticate with server",
            },
        );
        commands.insert(
            "HELLO",
            CommandInfo {
                name: "HELLO",
                arity: -1,
                summary: "Handshake with server",
            },
        );

        // Misc
        commands.insert(
            "QUIT",
            CommandInfo {
                name: "QUIT",
                arity: 1,
                summary: "Close the connection",
            },
        );

        Self { commands }
    }

    /// Get all command names
    // Reserved for CLI auto-completion support
    #[allow(dead_code)]
    pub fn get_all_names(&self) -> Vec<&'static str> {
        let mut names: Vec<_> = self.commands.keys().copied().collect();
        names.sort_unstable();
        names
    }

    /// Get completions for a prefix
    pub fn get_completions(&self, prefix: &str) -> Vec<&'static str> {
        let prefix_upper = prefix.to_uppercase();
        self.commands
            .keys()
            .filter(|name| name.starts_with(&prefix_upper))
            .copied()
            .collect()
    }

    /// Get command info
    pub fn get_command(&self, name: &str) -> Option<&CommandInfo> {
        self.commands.get(name.to_uppercase().as_str())
    }
}

impl Default for CommandRegistry {
    fn default() -> Self {
        Self::new()
    }
}
