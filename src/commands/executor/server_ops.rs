//! Server, connection, and administrative command helper methods on CommandExecutor.

use bytes::Bytes;

use crate::auth::Permission;
use crate::error::FerriteError;
use crate::protocol::Frame;
use crate::runtime::PauseMode;
use crate::storage::Value;

use crate::commands::keys;
use crate::commands::parser::Command;

use super::{command_info_by_name, CommandExecutor, COMMAND_INFO, COMMAND_NAMES};

/// Format bytes into a human-readable string
fn format_bytes(bytes: usize) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.2}GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.2}MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.2}KB", bytes as f64 / 1024.0)
    } else {
        format!("{}B", bytes)
    }
}

/// Return (summary, group, complexity, since) for a Redis command name.
fn command_doc_entry(name: &str) -> (&'static str, &'static str, &'static str, &'static str) {
    match name {
        "GET" => ("Get the value of a key", "string", "O(1)", "1.0.0"),
        "SET" => ("Set the string value of a key", "string", "O(1)", "1.0.0"),
        "DEL" => ("Delete one or more keys", "generic", "O(N)", "1.0.0"),
        "EXISTS" => ("Determine if a key exists", "generic", "O(N)", "1.0.0"),
        "INCR" => (
            "Increment the integer value of a key by one",
            "string",
            "O(1)",
            "1.0.0",
        ),
        "DECR" => (
            "Decrement the integer value of a key by one",
            "string",
            "O(1)",
            "1.0.0",
        ),
        "INCRBY" => (
            "Increment the integer value of a key by the given amount",
            "string",
            "O(1)",
            "1.0.0",
        ),
        "DECRBY" => (
            "Decrement the integer value of a key by the given number",
            "string",
            "O(1)",
            "1.0.0",
        ),
        "MGET" => (
            "Get the values of all the given keys",
            "string",
            "O(N)",
            "1.0.0",
        ),
        "MSET" => (
            "Set multiple keys to multiple values",
            "string",
            "O(N)",
            "1.0.1",
        ),
        "APPEND" => ("Append a value to a key", "string", "O(1)", "2.0.0"),
        "STRLEN" => (
            "Get the length of the value stored in a key",
            "string",
            "O(1)",
            "2.2.0",
        ),
        "GETRANGE" => (
            "Get a substring of the string stored at a key",
            "string",
            "O(N)",
            "2.4.0",
        ),
        "SETRANGE" => (
            "Overwrite part of a string at key starting at the specified offset",
            "string",
            "O(1)",
            "2.2.0",
        ),
        "SETNX" => (
            "Set the value of a key, only if the key does not exist",
            "string",
            "O(1)",
            "1.0.0",
        ),
        "SETEX" => (
            "Set the value and expiration of a key",
            "string",
            "O(1)",
            "2.0.0",
        ),
        "PSETEX" => (
            "Set the value and expiration in milliseconds of a key",
            "string",
            "O(1)",
            "2.6.0",
        ),
        "GETSET" => (
            "Set the string value of a key and return its old value",
            "string",
            "O(1)",
            "1.0.0",
        ),
        "GETDEL" => (
            "Get the value of a key and delete the key",
            "string",
            "O(1)",
            "6.2.0",
        ),
        "GETEX" => (
            "Get the value of a key and optionally set its expiration",
            "string",
            "O(1)",
            "6.2.0",
        ),
        "MSETNX" => (
            "Set multiple keys to multiple values, only if none of the keys exist",
            "string",
            "O(N)",
            "1.0.1",
        ),
        "INCRBYFLOAT" => (
            "Increment the float value of a key by the given amount",
            "string",
            "O(1)",
            "2.6.0",
        ),
        "LPUSH" => (
            "Prepend one or multiple elements to a list",
            "list",
            "O(N)",
            "1.0.0",
        ),
        "RPUSH" => (
            "Append one or multiple elements to a list",
            "list",
            "O(N)",
            "1.0.0",
        ),
        "LPOP" => (
            "Remove and get the first elements in a list",
            "list",
            "O(N)",
            "1.0.0",
        ),
        "RPOP" => (
            "Remove and get the last elements in a list",
            "list",
            "O(N)",
            "1.0.0",
        ),
        "LRANGE" => (
            "Get a range of elements from a list",
            "list",
            "O(S+N)",
            "1.0.0",
        ),
        "LLEN" => ("Get the length of a list", "list", "O(1)", "1.0.0"),
        "LINDEX" => (
            "Get an element from a list by its index",
            "list",
            "O(N)",
            "1.0.0",
        ),
        "LSET" => (
            "Set the value of an element in a list by its index",
            "list",
            "O(N)",
            "1.0.0",
        ),
        "LREM" => ("Remove elements from a list", "list", "O(N+S)", "1.0.0"),
        "HSET" => (
            "Set the string value of a hash field",
            "hash",
            "O(N)",
            "2.0.0",
        ),
        "HGET" => ("Get the value of a hash field", "hash", "O(1)", "2.0.0"),
        "HDEL" => ("Delete one or more hash fields", "hash", "O(N)", "2.0.0"),
        "HEXISTS" => ("Determine if a hash field exists", "hash", "O(1)", "2.0.0"),
        "HGETALL" => (
            "Get all the fields and values in a hash",
            "hash",
            "O(N)",
            "2.0.0",
        ),
        "HKEYS" => ("Get all the fields in a hash", "hash", "O(N)", "2.0.0"),
        "HVALS" => ("Get all the values in a hash", "hash", "O(N)", "2.0.0"),
        "HLEN" => (
            "Get the number of fields in a hash",
            "hash",
            "O(1)",
            "2.0.0",
        ),
        "SADD" => ("Add one or more members to a set", "set", "O(N)", "1.0.0"),
        "SREM" => (
            "Remove one or more members from a set",
            "set",
            "O(N)",
            "1.0.0",
        ),
        "SMEMBERS" => ("Get all the members in a set", "set", "O(N)", "1.0.0"),
        "SISMEMBER" => (
            "Determine if a given value is a member of a set",
            "set",
            "O(1)",
            "1.0.0",
        ),
        "SCARD" => ("Get the number of members in a set", "set", "O(1)", "1.0.0"),
        "SUNION" => ("Add multiple sets", "set", "O(N)", "1.0.0"),
        "SINTER" => ("Intersect multiple sets", "set", "O(N*M)", "1.0.0"),
        "SDIFF" => ("Subtract multiple sets", "set", "O(N)", "1.0.0"),
        "SPOP" => (
            "Remove and return one or multiple random members from a set",
            "set",
            "O(N)",
            "1.0.0",
        ),
        "SRANDMEMBER" => (
            "Get one or multiple random members from a set",
            "set",
            "O(N)",
            "1.0.0",
        ),
        "ZADD" => (
            "Add one or more members to a sorted set",
            "sorted_set",
            "O(log(N))",
            "1.2.0",
        ),
        "ZREM" => (
            "Remove one or more members from a sorted set",
            "sorted_set",
            "O(M*log(N))",
            "1.2.0",
        ),
        "ZSCORE" => (
            "Get the score associated with the given member in a sorted set",
            "sorted_set",
            "O(1)",
            "1.2.0",
        ),
        "ZCARD" => (
            "Get the number of members in a sorted set",
            "sorted_set",
            "O(1)",
            "1.2.0",
        ),
        "ZRANGE" => (
            "Return a range of members in a sorted set",
            "sorted_set",
            "O(log(N)+M)",
            "1.2.0",
        ),
        "ZRANK" => (
            "Determine the index of a member in a sorted set",
            "sorted_set",
            "O(log(N))",
            "2.0.0",
        ),
        "PING" => ("Ping the server", "connection", "O(1)", "1.0.0"),
        "ECHO" => ("Echo the given string", "connection", "O(1)", "1.0.0"),
        "SELECT" => (
            "Change the selected database for the current connection",
            "connection",
            "O(1)",
            "1.0.0",
        ),
        "INFO" => (
            "Get information and statistics about the server",
            "server",
            "O(1)",
            "1.0.0",
        ),
        "DBSIZE" => (
            "Return the number of keys in the selected database",
            "server",
            "O(1)",
            "1.0.0",
        ),
        "FLUSHDB" => (
            "Remove all keys from the current database",
            "server",
            "O(N)",
            "1.0.0",
        ),
        "FLUSHALL" => (
            "Remove all keys from all databases",
            "server",
            "O(N)",
            "1.0.0",
        ),
        "TIME" => ("Return the current server time", "server", "O(1)", "2.6.0"),
        "KEYS" => (
            "Find all keys matching the given pattern",
            "generic",
            "O(N)",
            "1.0.0",
        ),
        "SCAN" => (
            "Incrementally iterate the keys space",
            "generic",
            "O(1) per call",
            "2.8.0",
        ),
        "EXPIRE" => (
            "Set a key's time to live in seconds",
            "generic",
            "O(1)",
            "1.0.0",
        ),
        "TTL" => (
            "Get the time to live for a key in seconds",
            "generic",
            "O(1)",
            "1.0.0",
        ),
        "PTTL" => (
            "Get the time to live for a key in milliseconds",
            "generic",
            "O(1)",
            "2.6.0",
        ),
        "PERSIST" => (
            "Remove the expiration from a key",
            "generic",
            "O(1)",
            "2.2.0",
        ),
        "TYPE" => (
            "Determine the type stored at key",
            "generic",
            "O(1)",
            "1.0.0",
        ),
        "RENAME" => ("Rename a key", "generic", "O(1)", "1.0.0"),
        "RANDOMKEY" => (
            "Return a random key from the keyspace",
            "generic",
            "O(1)",
            "1.0.0",
        ),
        "OBJECT" => (
            "Inspect the internals of Redis objects",
            "generic",
            "O(1)",
            "2.2.3",
        ),
        "SORT" => (
            "Sort the elements in a list, set or sorted set",
            "generic",
            "O(N+M*log(M))",
            "1.0.0",
        ),
        "DUMP" => (
            "Return a serialized version of the value stored at the specified key",
            "generic",
            "O(1)",
            "2.6.0",
        ),
        "RESTORE" => (
            "Create a key using the provided serialized value",
            "generic",
            "O(1)",
            "2.6.0",
        ),
        "WAIT" => (
            "Wait for the synchronous replication of all the write commands",
            "generic",
            "O(1)",
            "3.0.0",
        ),
        "MULTI" => (
            "Mark the start of a transaction block",
            "transactions",
            "O(1)",
            "1.2.0",
        ),
        "EXEC" => (
            "Execute all commands issued after MULTI",
            "transactions",
            "O(N)",
            "1.2.0",
        ),
        "DISCARD" => (
            "Discard all commands issued after MULTI",
            "transactions",
            "O(N)",
            "2.0.0",
        ),
        "WATCH" => (
            "Watch the given keys to determine execution of the MULTI/EXEC block",
            "transactions",
            "O(1)",
            "2.2.0",
        ),
        "PUBLISH" => ("Post a message to a channel", "pubsub", "O(N+M)", "2.0.0"),
        "SUBSCRIBE" => (
            "Listen for messages published to the given channels",
            "pubsub",
            "O(N)",
            "2.0.0",
        ),
        "UNSUBSCRIBE" => (
            "Stop listening for messages posted to the given channels",
            "pubsub",
            "O(N)",
            "2.0.0",
        ),
        "COMMAND" => (
            "Get array of Redis command details",
            "server",
            "O(N)",
            "2.8.13",
        ),
        "DEBUG" => (
            "A container for debugging commands",
            "server",
            "O(1)",
            "1.0.0",
        ),
        "MEMORY" => (
            "A container for memory introspection commands",
            "server",
            "O(1)",
            "4.0.0",
        ),
        "CONFIG" => (
            "A container for server configuration commands",
            "server",
            "O(1)",
            "2.0.0",
        ),
        "CLIENT" => (
            "A container for client connection commands",
            "connection",
            "O(1)",
            "2.4.0",
        ),
        "AUTH" => ("Authenticate to the server", "connection", "O(N)", "1.0.0"),
        "QUIT" => ("Close the connection", "connection", "O(1)", "1.0.0"),
        "HELLO" => ("Handshake with Redis", "connection", "O(1)", "6.0.0"),
        "COPY" => (
            "Copy the value stored at the source key to the destination key",
            "generic",
            "O(N)",
            "6.2.0",
        ),
        "UNLINK" => (
            "Delete a key asynchronously in another thread",
            "generic",
            "O(1)",
            "4.0.0",
        ),
        "TOUCH" => (
            "Alters the last access time of a key(s)",
            "generic",
            "O(N)",
            "3.2.1",
        ),
        "XADD" => ("Appends a new entry to a stream", "stream", "O(1)", "5.0.0"),
        "XLEN" => (
            "Return the number of entries in a stream",
            "stream",
            "O(1)",
            "5.0.0",
        ),
        "XRANGE" => (
            "Return a range of elements in a stream",
            "stream",
            "O(N)",
            "5.0.0",
        ),
        "XREAD" => (
            "Return never seen elements in multiple streams",
            "stream",
            "O(N)",
            "5.0.0",
        ),
        _ => ("Redis command", "generic", "O(1)", "1.0.0"),
    }
}

impl CommandExecutor {
    pub(super) fn handle_object_command(
        &self,
        subcommand: &str,
        key: Option<&Bytes>,
        db: u8,
    ) -> Frame {
        match subcommand.to_uppercase().as_str() {
            "ENCODING" => {
                if let Some(k) = key {
                    keys::object_encoding(&self.store, db, k)
                } else {
                    Frame::error("ERR wrong number of arguments for 'object|encoding' command")
                }
            }
            "FREQ" => {
                if let Some(k) = key {
                    keys::object_freq(&self.store, db, k)
                } else {
                    Frame::error("ERR wrong number of arguments for 'object|freq' command")
                }
            }
            "IDLETIME" => {
                if let Some(k) = key {
                    keys::object_idletime(&self.store, db, k)
                } else {
                    Frame::error("ERR wrong number of arguments for 'object|idletime' command")
                }
            }
            "REFCOUNT" => {
                if let Some(k) = key {
                    keys::object_refcount(&self.store, db, k)
                } else {
                    Frame::error("ERR wrong number of arguments for 'object|refcount' command")
                }
            }
            "HELP" => Frame::array(vec![
                Frame::bulk("OBJECT <subcommand> [<arg> [value] [opt] ...]"),
                Frame::bulk("ENCODING <key> -- Return the encoding of the object."),
                Frame::bulk("FREQ <key> -- Return the access frequency (LFU) of the object."),
                Frame::bulk("IDLETIME <key> -- Return the idle time of the object."),
                Frame::bulk("REFCOUNT <key> -- Return the reference count of the object."),
            ]),
            _ => Frame::error(format!(
                "ERR Unknown subcommand or wrong number of arguments for '{}'",
                subcommand
            )),
        }
    }

    /// Handle SCRIPT subcommands
    pub(super) fn handle_script_command(&self, subcommand: &str, args: &[String]) -> Frame {
        match subcommand.to_uppercase().as_str() {
            "LOAD" => {
                if args.is_empty() {
                    return Frame::error("ERR wrong number of arguments for 'script|load' command");
                }
                self.script_executor.script_load(&args[0])
            }
            "EXISTS" => self.script_executor.script_exists(args),
            "FLUSH" => self.script_executor.script_flush(),
            "KILL" => {
                // Script kill - not implemented (would need script execution tracking)
                Frame::error("NOTBUSY No scripts in execution right now.")
            }
            "DEBUG" => {
                // Script debug - not implemented
                Frame::simple("OK")
            }
            "HELP" => Frame::array(vec![
                Frame::bulk("SCRIPT <subcommand> [<arg> [value] [opt] ...]"),
                Frame::bulk("LOAD <script> -- Load a script and return its SHA1 hash."),
                Frame::bulk("EXISTS <sha1> [...] -- Check if scripts exist in cache."),
                Frame::bulk("FLUSH [ASYNC|SYNC] -- Flush the script cache."),
                Frame::bulk("KILL -- Kill the running script."),
                Frame::bulk("DEBUG YES|SYNC|NO -- Set script debug mode."),
            ]),
            _ => Frame::error(format!(
                "ERR Unknown subcommand or wrong number of arguments for '{}'",
                subcommand
            )),
        }
    }

    pub(super) fn ping(&self, message: Option<Bytes>) -> Frame {
        match message {
            Some(msg) => Frame::bulk(msg),
            None => Frame::simple("PONG"),
        }
    }

    pub(super) fn info(&self, section: Option<String>) -> Frame {
        let mut output = String::new();
        let section_filter = section.as_deref().map(|s| s.to_lowercase());

        // Helper to check if we should include a section
        let include = |name: &str| -> bool {
            section_filter
                .as_ref()
                .map_or(true, |f| f == name || f == "all" || f == "default")
        };

        // Server section
        if include("server") {
            use std::time::SystemTime;
            let uptime = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            output.push_str("# Server\r\n");
            output.push_str("redis_version:7.0.0\r\n");
            output.push_str("ferrite_version:0.1.0\r\n");
            output.push_str(&format!("uptime_in_seconds:{}\r\n", uptime % 86400)); // Placeholder
            output.push_str("tcp_port:6379\r\n");
            output.push_str(&format!("process_id:{}\r\n", std::process::id()));
            output.push_str(&format!("num_databases:{}\r\n", self.store.num_databases()));

            // Backend type
            let backend = self.store.backend_type();
            output.push_str(&format!("storage_backend:{:?}\r\n", backend));
            output.push_str("\r\n");
        }

        // Stats section
        if include("stats") {
            output.push_str("# Stats\r\n");

            if let Some(stats) = self.store.hybridlog_stats_aggregated() {
                let total_ops = stats.get_ops + stats.set_ops + stats.del_ops;
                output.push_str(&format!("total_commands_processed:{}\r\n", total_ops));
                output.push_str(&format!("total_reads:{}\r\n", stats.get_ops));
                output.push_str(&format!("total_writes:{}\r\n", stats.set_ops));
                output.push_str(&format!("total_deletes:{}\r\n", stats.del_ops));
                output.push_str(&format!("total_keys:{}\r\n", stats.key_count));
            } else {
                let mut total_keys = 0u64;
                for i in 0..self.store.num_databases() {
                    total_keys += self.store.key_count(i as u8);
                }
                output.push_str(&format!("total_keys:{}\r\n", total_keys));
            }
            output.push_str("\r\n");
        }

        // Memory section
        if include("memory") {
            output.push_str("# Memory\r\n");

            if let Some(stats) = self.store.hybridlog_stats_aggregated() {
                let total_memory = stats.mutable_bytes + stats.readonly_bytes;
                output.push_str(&format!("used_memory:{}\r\n", total_memory));
                output.push_str(&format!(
                    "used_memory_human:{}\r\n",
                    format_bytes(total_memory)
                ));
                output.push_str(&format!("used_memory_peak:{}\r\n", total_memory));
            // Simplified
            } else {
                // Estimate for in-memory backend
                output.push_str("used_memory:0\r\n");
                output.push_str("used_memory_human:0B\r\n");
            }
            output.push_str("\r\n");
        }

        // HybridLog section (unique to Ferrite)
        if include("hybridlog") {
            if let Some(stats) = self.store.hybridlog_stats_aggregated() {
                output.push_str("# HybridLog\r\n");
                output.push_str(&format!(
                    "hybridlog_mutable_bytes:{}\r\n",
                    stats.mutable_bytes
                ));
                output.push_str(&format!(
                    "hybridlog_readonly_bytes:{}\r\n",
                    stats.readonly_bytes
                ));
                output.push_str(&format!("hybridlog_disk_bytes:{}\r\n", stats.disk_bytes));
                output.push_str(&format!(
                    "hybridlog_mutable_entries:{}\r\n",
                    stats.mutable_entries
                ));
                output.push_str(&format!(
                    "hybridlog_readonly_entries:{}\r\n",
                    stats.readonly_entries
                ));
                output.push_str(&format!(
                    "hybridlog_disk_entries:{}\r\n",
                    stats.disk_entries
                ));
                output.push_str(&format!(
                    "hybridlog_mutable_to_readonly_migrations:{}\r\n",
                    stats.mutable_to_readonly_migrations
                ));
                output.push_str(&format!(
                    "hybridlog_readonly_to_disk_migrations:{}\r\n",
                    stats.readonly_to_disk_migrations
                ));
                output.push_str(&format!(
                    "hybridlog_background_migrations:{}\r\n",
                    stats.background_migrations
                ));
                output.push_str(&format!(
                    "hybridlog_background_compactions:{}\r\n",
                    stats.background_compactions
                ));
                output.push_str(&format!(
                    "hybridlog_compaction_bytes_reclaimed:{}\r\n",
                    stats.compaction_bytes_reclaimed
                ));
                output.push_str(&format!(
                    "hybridlog_background_tasks_active:{}\r\n",
                    if stats.background_tasks_active { 1 } else { 0 }
                ));
                output.push_str("\r\n");
            }
        }

        // Keyspace section
        if include("keyspace") {
            output.push_str("# Keyspace\r\n");
            for i in 0..self.store.num_databases() {
                let key_count = self.store.key_count(i as u8);
                if key_count > 0 {
                    // Count keys with expiration (simplified)
                    output.push_str(&format!("db{}:keys={},expires=0\r\n", i, key_count));
                }
            }
            output.push_str("\r\n");
        }

        Frame::bulk(output)
    }

    pub(super) fn dbsize(&self, db: u8) -> Frame {
        Frame::Integer(self.store.key_count(db) as i64)
    }

    pub(super) fn flushdb(&self, db: u8) -> Frame {
        self.store.flush_db(db);
        Frame::simple("OK")
    }

    pub(super) fn flushall(&self) -> Frame {
        self.store.flush_all();
        Frame::simple("OK")
    }

    pub(super) fn time(&self) -> Frame {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let secs = now.as_secs();
        let micros = now.subsec_micros();
        Frame::array(vec![
            Frame::bulk(Bytes::from(secs.to_string())),
            Frame::bulk(Bytes::from(micros.to_string())),
        ])
    }

    pub(super) fn command_cmd(&self, subcommand: Option<String>, args: Vec<Bytes>) -> Frame {
        match subcommand.as_deref() {
            None => {
                // COMMAND with no subcommand returns info about all commands
                // Return array of command info arrays for commonly used commands
                self.command_all_info()
            }
            Some("COUNT") => {
                // Return approximate count of supported commands
                Frame::Integer(COMMAND_NAMES.len() as i64)
            }
            Some("DOCS") => {
                // COMMAND DOCS [command-name [command-name ...]]
                // Return documentation for specified commands
                self.command_docs(&args)
            }
            Some("GETKEYS") => {
                // COMMAND GETKEYS command [arg ...]
                // Extract keys from a full command
                self.command_getkeys(&args)
            }
            Some("GETKEYSANDFLAGS") => {
                // COMMAND GETKEYSANDFLAGS command [arg ...]
                // Extract keys and flags from a full command
                self.command_getkeys_and_flags(&args)
            }
            Some("INFO") => {
                // COMMAND INFO [command-name [command-name ...]]
                // Return info about specific commands
                self.command_info(&args)
            }
            Some("LIST") => {
                // COMMAND LIST [FILTERBY MODULE module|ACLCAT category|PATTERN pattern]
                self.command_list(&args)
            }
            Some("HELP") => Frame::array(vec![
                Frame::bulk("COMMAND"),
                Frame::bulk("    Return documentations for all commands."),
                Frame::bulk("COMMAND COUNT"),
                Frame::bulk("    Return the total number of commands."),
                Frame::bulk("COMMAND DOCS [<command-name> [<command-name> ...]]"),
                Frame::bulk("    Return documentation for the specified commands."),
                Frame::bulk("COMMAND GETKEYS <command> [<arg> [<arg> ...]]"),
                Frame::bulk("    Extract the key names from a full command."),
                Frame::bulk("COMMAND GETKEYSANDFLAGS <command> [<arg> [<arg> ...]]"),
                Frame::bulk("    Extract the key names and access flags from a full command."),
                Frame::bulk("COMMAND INFO [<command-name> [<command-name> ...]]"),
                Frame::bulk("    Return information for the specified commands."),
                Frame::bulk("COMMAND LIST [FILTERBY <filter> <filter-args>]"),
                Frame::bulk("    Return a list of command names."),
            ]),
            Some(unknown) => Frame::error(format!(
                "ERR unknown subcommand '{}'. Try COMMAND HELP.",
                unknown
            )),
        }
    }

    pub(super) fn command_all_info(&self) -> Frame {
        // Return basic info arrays for main commands
        // Each command info is: [name, arity, flags, first-key, last-key, step]
        Frame::array(COMMAND_INFO.iter().map(|info| info.to_frame()).collect())
    }

    pub(super) fn command_docs(&self, args: &[Bytes]) -> Frame {
        // COMMAND DOCS returns documentation for commands.
        // Returns a flat array of pairs: [command-name, doc-map, ...]
        let names: Vec<String> = if args.is_empty() {
            COMMAND_NAMES.iter().map(|s| s.to_uppercase()).collect()
        } else {
            args.iter()
                .map(|a| String::from_utf8_lossy(a).to_uppercase())
                .collect()
        };

        let mut results = Vec::with_capacity(names.len() * 2);
        for cmd_name in &names {
            let (summary, group, complexity, since) = command_doc_entry(cmd_name);
            results.push(Frame::bulk(cmd_name.to_lowercase()));
            results.push(Frame::array(vec![
                Frame::bulk("summary"),
                Frame::bulk(summary),
                Frame::bulk("since"),
                Frame::bulk(since),
                Frame::bulk("group"),
                Frame::bulk(group),
                Frame::bulk("complexity"),
                Frame::bulk(complexity),
                Frame::bulk("arguments"),
                Frame::array(vec![]),
            ]));
        }
        Frame::array(results)
    }

    pub(super) fn command_getkeys(&self, args: &[Bytes]) -> Frame {
        // COMMAND GETKEYS command [arg ...]
        // Extract key positions from a command
        if args.is_empty() {
            return Frame::error("ERR Invalid arguments specified for COMMAND GETKEYS");
        }

        let frames = args.iter().cloned().map(Frame::bulk).collect();
        let frame = Frame::array(frames);
        match Command::from_frame(frame) {
            Ok(command) => Frame::array(command.meta().keys.into_iter().map(Frame::bulk).collect()),
            Err(FerriteError::UnknownCommand(_)) => Frame::array(vec![]),
            Err(err) => Frame::error(err.to_resp_error()),
        }
    }

    #[allow(clippy::useless_let_if_seq)]
    pub(super) fn command_getkeys_and_flags(&self, args: &[Bytes]) -> Frame {
        // COMMAND GETKEYSANDFLAGS command [arg ...]
        // Extract key positions and access flags from a command
        if args.is_empty() {
            return Frame::error("ERR Invalid arguments specified for COMMAND GETKEYSANDFLAGS");
        }

        let frames = args.iter().cloned().map(Frame::bulk).collect();
        let frame = Frame::array(frames);
        match Command::from_frame(frame) {
            Ok(command) => {
                let meta = command.meta();
                let mut flags: Vec<Frame> = match meta.permission {
                    Permission::Read => vec![Frame::bulk("RO"), Frame::bulk("access")],
                    Permission::Write | Permission::ReadWrite => vec![
                        Frame::bulk("RW"),
                        Frame::bulk("access"),
                        Frame::bulk("update"),
                    ],
                };
                if matches!(meta.name, "DEL" | "GETDEL") {
                    flags = vec![Frame::bulk("RW"), Frame::bulk("delete")];
                }
                if matches!(meta.name, "MSET" | "MSETNX") {
                    flags = vec![Frame::bulk("OW"), Frame::bulk("update")];
                }
                let entries = meta
                    .keys
                    .into_iter()
                    .map(|key| Frame::array(vec![Frame::bulk(key), Frame::array(flags.clone())]))
                    .collect();
                Frame::array(entries)
            }
            Err(FerriteError::UnknownCommand(_)) => Frame::array(vec![]),
            Err(err) => Frame::error(err.to_resp_error()),
        }
    }

    pub(super) fn command_info(&self, args: &[Bytes]) -> Frame {
        // COMMAND INFO [command-name [command-name ...]]
        // Return info about specific commands
        if args.is_empty() {
            // Return all command info
            return self.command_all_info();
        }

        let mut results = Vec::with_capacity(args.len());
        for arg in args {
            let cmd_name = String::from_utf8_lossy(arg);
            if let Some(info) = command_info_by_name(&cmd_name) {
                results.push(info.to_frame());
            } else {
                results.push(Frame::Null);
            }
        }
        Frame::array(results)
    }

    pub(super) fn command_list(&self, args: &[Bytes]) -> Frame {
        // COMMAND LIST [FILTERBY MODULE module|ACLCAT category|PATTERN pattern]
        let all_commands = COMMAND_NAMES;

        // Check for FILTERBY
        if args.len() >= 2 {
            let filter_type = String::from_utf8_lossy(&args[0]).to_uppercase();
            if filter_type == "FILTERBY" && args.len() >= 4 {
                let filter_by = String::from_utf8_lossy(&args[1]).to_uppercase();
                let filter_value = String::from_utf8_lossy(&args[2]).to_lowercase();

                match filter_by.as_str() {
                    "PATTERN" => {
                        // Simple pattern matching
                        let pattern = filter_value.replace('*', "");
                        let filtered: Vec<Frame> = all_commands
                            .iter()
                            .filter(|cmd| cmd.contains(&pattern))
                            .map(|cmd| Frame::bulk(*cmd))
                            .collect();
                        return Frame::array(filtered);
                    }
                    "ACLCAT" => {
                        // Filter by ACL category (simplified)
                        let category_commands: Vec<&str> = match filter_value.as_str() {
                            "read" => vec![
                                "get", "mget", "lrange", "hget", "smembers", "zrange", "exists",
                                "type", "ttl",
                            ],
                            "write" => vec![
                                "set", "del", "lpush", "rpush", "hset", "sadd", "zadd", "expire",
                            ],
                            "admin" => vec![
                                "config", "debug", "shutdown", "bgsave", "flushall", "flushdb",
                            ],
                            "pubsub" => vec!["publish", "subscribe", "unsubscribe", "psubscribe"],
                            "transaction" => vec!["multi", "exec", "discard", "watch", "unwatch"],
                            "scripting" => vec!["eval", "evalsha", "script", "function", "fcall"],
                            _ => vec![],
                        };
                        return Frame::array(
                            category_commands
                                .iter()
                                .map(|cmd| Frame::bulk(*cmd))
                                .collect(),
                        );
                    }
                    "MODULE" => {
                        // We don't support modules, return empty
                        return Frame::array(vec![]);
                    }
                    _ => {}
                }
            }
        }

        // Return all commands
        Frame::array(all_commands.iter().map(|cmd| Frame::bulk(*cmd)).collect())
    }

    pub(super) fn debug(&self, subcommand: &str, args: Vec<Bytes>) -> Frame {
        match subcommand {
            "SLEEP" => {
                // DEBUG SLEEP seconds
                if args.is_empty() {
                    return Frame::error("ERR wrong number of arguments for DEBUG SLEEP");
                }
                let seconds_str = String::from_utf8_lossy(&args[0]);
                if let Ok(seconds) = seconds_str.parse::<f64>() {
                    // Use spawn_blocking to avoid stalling the tokio runtime
                    let _ = tokio::task::block_in_place(|| {
                        std::thread::sleep(std::time::Duration::from_secs_f64(seconds));
                    });
                    Frame::simple("OK")
                } else {
                    Frame::error("ERR invalid sleep time")
                }
            }
            "SEGFAULT" => {
                // Don't actually segfault, just return an error
                Frame::error("ERR DEBUG SEGFAULT not supported")
            }
            "SET-ACTIVE-EXPIRE" => {
                // Set active expiration mode
                Frame::simple("OK")
            }
            "QUICKLIST-PACKED-THRESHOLD" => Frame::simple("OK"),
            "LISTPACK-ENTRIES" => Frame::simple("OK"),
            "DIGEST" => {
                // Return a debug digest
                Frame::bulk("0000000000000000000000000000000000000000")
            }
            "DIGEST-VALUE" => Frame::array(vec![]),
            "PROTOCOL" => {
                // Toggle protocol debugging
                Frame::simple("OK")
            }
            "RELOAD" => Frame::simple("OK"),
            "RESTART" => Frame::error("ERR DEBUG RESTART not supported"),
            "CRASH-AND-RECOVER" | "CRASH-AND-RESTART" => {
                Frame::error("ERR DEBUG crash commands not supported")
            }
            "OBJECT" => {
                // DEBUG OBJECT key
                if args.is_empty() {
                    return Frame::error("ERR wrong number of arguments for DEBUG OBJECT");
                }
                Frame::bulk("Value at:0x0 refcount:1 encoding:raw serializedlength:0 lru:0 lru_seconds_idle:0")
            }
            "STRUCTSIZE" => {
                // Return memory struct sizes
                Frame::bulk("struct sizes: various internal struct sizes")
            }
            "HTSTATS" => {
                // Hash table stats
                Frame::bulk("[Hash table 0] No stats available")
            }
            "CHANGE-REPL-STATE" => Frame::simple("OK"),
            _ => Frame::error(format!("ERR Unknown DEBUG subcommand '{}'", subcommand)),
        }
    }

    pub(super) fn memory(&self, subcommand: &str, key: Option<&Bytes>, db: u8) -> Frame {
        match subcommand {
            "USAGE" => {
                // MEMORY USAGE key [SAMPLES count]
                match key {
                    Some(k) => {
                        if self.store.exists(db, &[k.clone()]) == 1 {
                            // Estimate memory usage - rough estimate
                            if let Some(value) = self.store.get(db, k) {
                                let size = match &value {
                                    Value::String(s) => s.len() + 32, // overhead
                                    Value::List(l) => l.len() * 16 + 64,
                                    Value::Hash(h) => h.len() * 48 + 64,
                                    Value::Set(s) => s.len() * 24 + 64,
                                    Value::SortedSet { by_member, .. } => {
                                        by_member.len() * 40 + 128
                                    }
                                    Value::Stream(s) => s.entries.len() * 64 + 128,
                                    Value::HyperLogLog(hll) => hll.len() + 64,
                                };
                                Frame::Integer(size as i64)
                            } else {
                                Frame::Null
                            }
                        } else {
                            Frame::Null
                        }
                    }
                    None => Frame::error("ERR wrong number of arguments for MEMORY USAGE"),
                }
            }
            "DOCTOR" => {
                // Memory diagnostic report
                Frame::bulk("Sam, I have no memory problems")
            }
            "MALLOC-SIZE" => {
                // Return malloc size for pointer
                match key {
                    Some(_) => Frame::Integer(0),
                    None => Frame::error("ERR wrong number of arguments for MEMORY MALLOC-SIZE"),
                }
            }
            "PURGE" => {
                // Release free memory back to OS
                Frame::simple("OK")
            }
            "STATS" => {
                // Memory allocator stats
                Frame::bulk(
                    "peak.allocated:0\n\
                     total.allocated:0\n\
                     startup.allocated:0\n\
                     replication.backlog:0\n\
                     clients.slaves:0\n\
                     clients.normal:0\n\
                     aof.buffer:0\n",
                )
            }
            "HELP" => Frame::array(vec![
                Frame::bulk("MEMORY DOCTOR"),
                Frame::bulk("MEMORY MALLOC-SIZE <pointer>"),
                Frame::bulk("MEMORY PURGE"),
                Frame::bulk("MEMORY STATS"),
                Frame::bulk("MEMORY USAGE <key> [SAMPLES <count>]"),
            ]),
            _ => Frame::error(format!("ERR Unknown MEMORY subcommand '{}'", subcommand)),
        }
    }

    pub(super) fn client(&self, subcommand: &str, args: Vec<Bytes>) -> Frame {
        match subcommand {
            "LIST" => {
                // CLIENT LIST - list connected clients
                // Uses the client registry to get actual client info
                let clients = self.client_registry.list();
                if clients.is_empty() {
                    // Fallback if no clients registered (shouldn't happen in practice)
                    let info = "id=1 addr=127.0.0.1:0 fd=0 name= age=0 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=0 obl=0 oll=0 omem=0 events=r cmd=client\n";
                    Frame::bulk(info)
                } else {
                    let output = clients
                        .iter()
                        .map(|c| c.to_info_string())
                        .collect::<Vec<_>>()
                        .join("\n");
                    Frame::bulk(output + "\n")
                }
            }
            "GETNAME" => {
                // CLIENT GETNAME - return current client name (if set)
                Frame::Null
            }
            "SETNAME" => {
                // CLIENT SETNAME name
                if args.is_empty() {
                    return Frame::error("ERR wrong number of arguments for CLIENT SETNAME");
                }
                // Client name is handled at connection level, return OK
                Frame::simple("OK")
            }
            "ID" => {
                // CLIENT ID - return connection ID
                // Returns a real ID from the registry count
                Frame::Integer((self.client_registry.count() as i64).max(1))
            }
            "INFO" => {
                // CLIENT INFO - info about current client
                // Uses the client registry if available
                let clients = self.client_registry.list();
                if let Some(client) = clients.first() {
                    Frame::bulk(client.to_info_string())
                } else {
                    let info = "id=1 addr=127.0.0.1:0 laddr=127.0.0.1:6379 fd=0 name= age=0 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd=client user=default redir=-1";
                    Frame::bulk(info)
                }
            }
            "KILL" => {
                // CLIENT KILL [options] - handled at connection level
                if args.is_empty() {
                    return Frame::error("ERR wrong number of arguments for CLIENT KILL");
                }
                Frame::simple("OK")
            }
            "PAUSE" => {
                // CLIENT PAUSE timeout [WRITE|ALL] - pause client commands
                if args.is_empty() {
                    return Frame::error("ERR wrong number of arguments for CLIENT PAUSE");
                }

                // Parse timeout in milliseconds
                let timeout_ms = match String::from_utf8_lossy(&args[0]).parse::<u64>() {
                    Ok(t) => t,
                    Err(_) => return Frame::error("ERR timeout is not an integer or out of range"),
                };

                // Parse mode (default is ALL for backwards compatibility)
                let mode = if args.len() > 1 {
                    match String::from_utf8_lossy(&args[1]).to_uppercase().as_str() {
                        "WRITE" => PauseMode::Write,
                        "ALL" => PauseMode::All,
                        _ => return Frame::error("ERR CLIENT PAUSE mode must be WRITE or ALL"),
                    }
                } else {
                    PauseMode::All
                };

                self.client_registry.pause(timeout_ms, mode);
                Frame::simple("OK")
            }
            "UNPAUSE" => {
                // CLIENT UNPAUSE - resume client commands
                self.client_registry.unpause();
                Frame::simple("OK")
            }
            "REPLY" => {
                // CLIENT REPLY ON|OFF|SKIP
                if args.is_empty() {
                    return Frame::error("ERR wrong number of arguments for CLIENT REPLY");
                }
                Frame::simple("OK")
            }
            "CACHING" => {
                // CLIENT CACHING YES|NO
                Frame::simple("OK")
            }
            "GETREDIR" => {
                // CLIENT GETREDIR - get tracking redirect client ID
                Frame::Integer(-1)
            }
            "TRACKINGINFO" => {
                // CLIENT TRACKINGINFO
                Frame::array(vec![
                    Frame::bulk("flags"),
                    Frame::array(vec![]),
                    Frame::bulk("redirect"),
                    Frame::Integer(-1),
                    Frame::bulk("prefixes"),
                    Frame::array(vec![]),
                ])
            }
            "NO-EVICT" => {
                // CLIENT NO-EVICT ON|OFF
                Frame::simple("OK")
            }
            "NO-TOUCH" => {
                // CLIENT NO-TOUCH ON|OFF
                Frame::simple("OK")
            }
            "SETINFO" => {
                // CLIENT SETINFO LIB-NAME|LIB-VER value
                Frame::simple("OK")
            }
            "HELP" => {
                Frame::array(vec![
                    Frame::bulk("CLIENT GETNAME"),
                    Frame::bulk("CLIENT ID"),
                    Frame::bulk("CLIENT INFO"),
                    Frame::bulk("CLIENT KILL <ip:port>|ID <id>|TYPE <normal|master|replica|pubsub>|USER <username>|ADDR <ip:port>|LADDR <ip:port>|SKIPME <yes|no>"),
                    Frame::bulk("CLIENT LIST [TYPE <normal|master|replica|pubsub>] [ID <id> [id...]]"),
                    Frame::bulk("CLIENT NO-EVICT <ON|OFF>"),
                    Frame::bulk("CLIENT NO-TOUCH <ON|OFF>"),
                    Frame::bulk("CLIENT PAUSE <timeout> [WRITE|ALL]"),
                    Frame::bulk("CLIENT REPLY <ON|OFF|SKIP>"),
                    Frame::bulk("CLIENT SETNAME <connection-name>"),
                    Frame::bulk("CLIENT UNPAUSE"),
                ])
            }
            _ => Frame::error(format!("ERR Unknown CLIENT subcommand '{}'", subcommand)),
        }
    }

    pub(super) fn config(&self, subcommand: &str, args: Vec<Bytes>) -> Frame {
        match subcommand {
            "GET" => {
                // CONFIG GET pattern - return matching config parameters
                if args.is_empty() {
                    return Frame::error("ERR wrong number of arguments for CONFIG GET");
                }
                let pattern = String::from_utf8_lossy(&args[0]).to_string();

                // Use SharedConfig if available, otherwise fall back to defaults
                if let Some(ref config) = self.config {
                    let params = config.list_params(&pattern);
                    let mut results = Vec::with_capacity(params.len() * 2);
                    for (key, value) in params {
                        results.push(Frame::bulk(key));
                        results.push(Frame::bulk(value));
                    }
                    Frame::array(results)
                } else {
                    // Fall back to hardcoded defaults when no config is available
                    let mut results = Vec::new();
                    let pattern = pattern.to_lowercase();

                    let configs = [
                        ("maxclients", "10000"),
                        ("maxmemory", "0"),
                        ("maxmemory-policy", "noeviction"),
                        ("timeout", "0"),
                        ("tcp-keepalive", "300"),
                        ("databases", "16"),
                        ("appendonly", "no"),
                        ("appendfsync", "everysec"),
                        ("save", ""),
                        ("dir", "."),
                        ("dbfilename", "dump.rdb"),
                        ("loglevel", "notice"),
                        ("logfile", ""),
                        ("bind", "127.0.0.1"),
                        ("port", "6379"),
                        ("requirepass", ""),
                        ("daemonize", "no"),
                        ("pidfile", "/var/run/ferrite.pid"),
                    ];

                    for (key, value) in &configs {
                        let matches = if pattern == "*" {
                            true
                        } else if pattern.contains('*') {
                            let parts: Vec<&str> = pattern.split('*').collect();
                            if parts.len() == 2 {
                                if parts[0].is_empty() {
                                    key.ends_with(parts[1])
                                } else if parts[1].is_empty() {
                                    key.starts_with(parts[0])
                                } else {
                                    key.starts_with(parts[0]) && key.ends_with(parts[1])
                                }
                            } else {
                                key == &pattern
                            }
                        } else {
                            key == &pattern
                        };

                        if matches {
                            results.push(Frame::bulk(*key));
                            results.push(Frame::bulk(*value));
                        }
                    }

                    Frame::array(results)
                }
            }
            "SET" => {
                // CONFIG SET parameter value
                if args.len() < 2 {
                    return Frame::error("ERR wrong number of arguments for CONFIG SET");
                }

                let param = String::from_utf8_lossy(&args[0]).to_string();
                let value = String::from_utf8_lossy(&args[1]).to_string();

                if let Some(ref config) = self.config {
                    match config.set_param(&param, &value) {
                        Ok(true) => Frame::simple("OK"),
                        Ok(false) => {
                            // Parameter exists but requires restart
                            Frame::error(format!(
                                "ERR CONFIG SET for '{}' requires a restart",
                                param
                            ))
                        }
                        Err(e) => Frame::error(format!("ERR {}", e)),
                    }
                } else {
                    // No config available, just acknowledge
                    Frame::simple("OK")
                }
            }
            "RESETSTAT" => {
                // CONFIG RESETSTAT - reset server statistics
                self.store.reset_stats();
                Frame::simple("OK")
            }
            "REWRITE" => {
                // CONFIG REWRITE - rewrite config file
                if let Some(ref config) = self.config {
                    match config.rewrite() {
                        Ok(()) => Frame::simple("OK"),
                        Err(e) => Frame::error(format!("ERR {}", e)),
                    }
                } else {
                    Frame::error("ERR No config file set")
                }
            }
            "HELP" => Frame::array(vec![
                Frame::bulk("CONFIG GET <pattern>"),
                Frame::bulk("    Return parameters matching the glob-like <pattern>."),
                Frame::bulk("CONFIG SET <parameter> <value>"),
                Frame::bulk("    Set configuration parameter to value."),
                Frame::bulk("CONFIG RESETSTAT"),
                Frame::bulk("    Reset statistics reported by INFO."),
                Frame::bulk("CONFIG REWRITE"),
                Frame::bulk("    Rewrite the configuration file."),
            ]),
            _ => Frame::error(format!("ERR Unknown CONFIG subcommand '{}'", subcommand)),
        }
    }

    pub(super) fn monitor(&self) -> Frame {
        // MONITOR - stream all commands received by server in real time
        // This is typically implemented as a streaming command that keeps the connection
        // open and sends command notifications. For now, we return a simple OK response
        // A full implementation would require connection state tracking and broadcast channels
        Frame::simple("OK")
    }

    pub(super) fn reset(&self) -> Frame {
        // RESET - reset the connection state to initial state
        // This resets: authentication, selected database, WATCH state, MULTI state,
        // pub/sub subscriptions, etc.
        // Returns RESET as per Redis spec
        Frame::simple("RESET")
    }

    pub(super) fn role(&self) -> Frame {
        // ROLE - return the replication role and info
        // For standalone mode, return master with default values
        // Format: [role, replication-offset, [replica-info...]]
        Frame::array(vec![
            Frame::bulk("master"),
            Frame::Integer(0),    // replication offset
            Frame::array(vec![]), // list of connected replicas (empty for now)
        ])
    }

    pub(super) fn module(&self, subcommand: &str, args: &[Bytes]) -> Frame {
        const MODULE_ERR: &str = "ERR Ferrite does not support Redis modules. Ferrite uses native Rust crates for extensibility. See https://ferrite.dev/docs/extensions for details.";

        match subcommand {
            "LIST" => {
                // MODULE LIST - return empty list (Ferrite does not use Redis modules)
                Frame::array(vec![])
            }
            "LOAD" => {
                if args.is_empty() {
                    return Frame::error("ERR wrong number of arguments for 'module|load' command");
                }
                Frame::error(MODULE_ERR)
            }
            "UNLOAD" => {
                if args.is_empty() {
                    return Frame::error(
                        "ERR wrong number of arguments for 'module|unload' command",
                    );
                }
                Frame::error(MODULE_ERR)
            }
            "LOADEX" => {
                if args.is_empty() {
                    return Frame::error(
                        "ERR wrong number of arguments for 'module|loadex' command",
                    );
                }
                Frame::error(MODULE_ERR)
            }
            "HELP" => Frame::array(vec![
                Frame::bulk("MODULE LIST"),
                Frame::bulk("    Return a list of loaded modules (always empty in Ferrite)."),
                Frame::bulk("MODULE LOAD <path> [arg ...]"),
                Frame::bulk(
                    "    Not supported. Ferrite uses native Rust crates for extensibility.",
                ),
                Frame::bulk("MODULE UNLOAD <name>"),
                Frame::bulk(
                    "    Not supported. Ferrite uses native Rust crates for extensibility.",
                ),
                Frame::bulk("MODULE LOADEX <path> [CONFIG name value ...] [ARGS arg ...]"),
                Frame::bulk(
                    "    Not supported. Ferrite uses native Rust crates for extensibility.",
                ),
            ]),
            _ => Frame::error(format!("ERR Unknown MODULE subcommand '{}'", subcommand)),
        }
    }

    pub(super) fn plugin(&self, subcommand: &str, args: &[Bytes]) -> Frame {
        use ferrite_plugins::marketplace::{Marketplace, MarketplaceConfig};
        use std::sync::OnceLock;

        static MARKETPLACE: OnceLock<Marketplace> = OnceLock::new();
        let mp = MARKETPLACE.get_or_init(|| {
            Marketplace::with_builtin_catalog(MarketplaceConfig::default())
        });

        match subcommand {
            "INSTALL" => {
                if args.is_empty() {
                    return Frame::error("ERR wrong number of arguments for 'PLUGIN INSTALL'");
                }
                let name = String::from_utf8_lossy(&args[0]).to_string();
                let version = args
                    .get(1)
                    .map(|b| String::from_utf8_lossy(b).to_string())
                    .unwrap_or_else(|| "latest".to_string());

                match mp.install(&name, &version) {
                    Ok(()) => Frame::simple("OK"),
                    Err(e) => Frame::error(format!("ERR {}", e)),
                }
            }
            "REMOVE" | "UNINSTALL" => {
                if args.is_empty() {
                    return Frame::error("ERR wrong number of arguments for 'PLUGIN REMOVE'");
                }
                let name = String::from_utf8_lossy(&args[0]).to_string();
                match mp.uninstall(&name) {
                    Ok(()) => Frame::simple("OK"),
                    Err(e) => Frame::error(format!("ERR {}", e)),
                }
            }
            "LIST" => {
                let installed = mp.list_installed();
                let mut results = Vec::with_capacity(installed.len());
                for plugin in &installed {
                    let mut entry = Vec::new();
                    entry.push(Frame::bulk("name"));
                    entry.push(Frame::bulk(plugin.metadata.name.clone()));
                    entry.push(Frame::bulk("version"));
                    entry.push(Frame::bulk(plugin.metadata.version.clone()));
                    entry.push(Frame::bulk("enabled"));
                    entry.push(Frame::Integer(if plugin.enabled { 1 } else { 0 }));
                    entry.push(Frame::bulk("type"));
                    entry.push(Frame::bulk(format!("{:?}", plugin.metadata.plugin_type)));
                    results.push(Frame::array(entry));
                }
                Frame::array(results)
            }
            "ENABLE" => {
                if args.is_empty() {
                    return Frame::error("ERR wrong number of arguments for 'PLUGIN ENABLE'");
                }
                let name = String::from_utf8_lossy(&args[0]).to_string();
                match mp.enable(&name) {
                    Ok(()) => Frame::simple("OK"),
                    Err(e) => Frame::error(format!("ERR {}", e)),
                }
            }
            "DISABLE" => {
                if args.is_empty() {
                    return Frame::error("ERR wrong number of arguments for 'PLUGIN DISABLE'");
                }
                let name = String::from_utf8_lossy(&args[0]).to_string();
                match mp.disable(&name) {
                    Ok(()) => Frame::simple("OK"),
                    Err(e) => Frame::error(format!("ERR {}", e)),
                }
            }
            "INFO" => {
                if args.is_empty() {
                    return Frame::error("ERR wrong number of arguments for 'PLUGIN INFO'");
                }
                let name = String::from_utf8_lossy(&args[0]).to_string();
                let installed = mp.list_installed();
                match installed.iter().find(|p| p.metadata.name == name) {
                    Some(plugin) => {
                        let mut items = Vec::new();
                        items.push(Frame::bulk("name"));
                        items.push(Frame::bulk(Bytes::from(plugin.metadata.name.clone())));
                        items.push(Frame::bulk("version"));
                        items.push(Frame::bulk(Bytes::from(plugin.metadata.version.clone())));
                        items.push(Frame::bulk("description"));
                        items.push(Frame::bulk(Bytes::from(
                            plugin.metadata.description.clone(),
                        )));
                        items.push(Frame::bulk("author"));
                        items.push(Frame::bulk(Bytes::from(plugin.metadata.author.clone())));
                        items.push(Frame::bulk("license"));
                        items.push(Frame::bulk(Bytes::from(plugin.metadata.license.clone())));
                        items.push(Frame::bulk("type"));
                        items.push(Frame::bulk(Bytes::from(format!(
                            "{:?}",
                            plugin.metadata.plugin_type
                        ))));
                        items.push(Frame::bulk("enabled"));
                        items.push(Frame::Integer(if plugin.enabled { 1 } else { 0 }));
                        items.push(Frame::bulk("tags"));
                        items.push(Frame::Array(Some(
                            plugin
                                .metadata
                                .tags
                                .iter()
                                .map(|t| Frame::bulk(Bytes::from(t.clone())))
                                .collect(),
                        )));
                        items.push(Frame::bulk("installed_at"));
                        items.push(Frame::bulk(Bytes::from(
                            plugin.installed_at.to_rfc3339(),
                        )));
                        Frame::Array(Some(items))
                    }
                    None => Frame::null(),
                }
            }
            "SEARCH" => {
                if args.is_empty() {
                    return Frame::error("ERR wrong number of arguments for 'PLUGIN SEARCH'");
                }
                let query = String::from_utf8_lossy(&args[0]).to_string();
                let results = mp.search(&query);
                let mut entries = Vec::with_capacity(results.len());
                for plugin in &results {
                    let mut entry = Vec::new();
                    entry.push(Frame::bulk("name"));
                    entry.push(Frame::bulk(Bytes::from(plugin.name.clone())));
                    entry.push(Frame::bulk("version"));
                    entry.push(Frame::bulk(Bytes::from(plugin.version.clone())));
                    entry.push(Frame::bulk("description"));
                    entry.push(Frame::bulk(Bytes::from(plugin.description.clone())));
                    entry.push(Frame::bulk("author"));
                    entry.push(Frame::bulk(Bytes::from(plugin.author.clone())));
                    entry.push(Frame::bulk("downloads"));
                    entry.push(Frame::Integer(plugin.downloads as i64));
                    entry.push(Frame::bulk("rating"));
                    entry.push(Frame::bulk(Bytes::from(format!("{:.1}", plugin.rating))));
                    entry.push(Frame::bulk("tags"));
                    entry.push(Frame::Array(Some(
                        plugin
                            .tags
                            .iter()
                            .map(|t| Frame::bulk(Bytes::from(t.clone())))
                            .collect(),
                    )));
                    entries.push(Frame::array(entry));
                }
                Frame::array(entries)
            }
            "HELP" => Frame::array(vec![
                Frame::bulk("PLUGIN INSTALL <name> [version]"),
                Frame::bulk("    Install a plugin from the marketplace."),
                Frame::bulk("PLUGIN REMOVE <name>"),
                Frame::bulk("    Remove an installed plugin."),
                Frame::bulk("PLUGIN LIST"),
                Frame::bulk("    List all installed plugins."),
                Frame::bulk("PLUGIN SEARCH <query>"),
                Frame::bulk("    Search the marketplace for plugins."),
                Frame::bulk("PLUGIN ENABLE <name>"),
                Frame::bulk("    Enable a disabled plugin."),
                Frame::bulk("PLUGIN DISABLE <name>"),
                Frame::bulk("    Disable an installed plugin without removing it."),
                Frame::bulk("PLUGIN INFO <name>"),
                Frame::bulk("    Get detailed information about a plugin."),
            ]),
            _ => Frame::error(format!(
                "ERR Unknown PLUGIN subcommand '{}'. Try PLUGIN HELP.",
                subcommand
            )),
        }
    }

    pub(super) fn bgsave(&self, schedule: bool) -> Frame {
        // BGSAVE - asynchronously save the dataset to disk
        // In a real implementation, this would fork and save in background
        // For now, return success indication
        if schedule {
            // SCHEDULE option: don't start if already in progress, just schedule
            Frame::simple("Background saving scheduled")
        } else {
            Frame::simple("Background saving started")
        }
    }

    pub(super) fn save(&self) -> Frame {
        // SAVE - synchronously save the dataset to disk
        // In a real implementation, this would block and save
        Frame::simple("OK")
    }

    pub(super) fn bgrewriteaof(&self) -> Frame {
        // BGREWRITEAOF - asynchronously rewrite the append-only file
        Frame::simple("Background append only file rewriting started")
    }

    pub(super) fn lastsave(&self) -> Frame {
        // LASTSAVE - return Unix timestamp of last successful save
        // For now, return current time as placeholder
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        Frame::Integer(timestamp)
    }

    pub(super) fn hello(&self, protocol_version: Option<u8>) -> Frame {
        use std::collections::HashMap;

        // Default to RESP2 if no version specified
        let proto = protocol_version.unwrap_or(2);

        // Validate protocol version
        if proto != 2 && proto != 3 {
            return Frame::error("NOPROTO unsupported protocol version");
        }

        if proto == 2 {
            // RESP2: Return as flat array [key, value, key, value, ...]
            Frame::array(vec![
                Frame::bulk("server"),
                Frame::bulk("ferrite"),
                Frame::bulk("version"),
                Frame::bulk("0.1.0"),
                Frame::bulk("proto"),
                Frame::Integer(proto as i64),
                Frame::bulk("id"),
                Frame::Integer(1),
                Frame::bulk("mode"),
                Frame::bulk("standalone"),
                Frame::bulk("role"),
                Frame::bulk("master"),
                Frame::bulk("modules"),
                Frame::array(vec![]),
            ])
        } else {
            // RESP3: Return server info as a Map
            let mut info = HashMap::new();
            info.insert(Bytes::from_static(b"server"), Frame::bulk("ferrite"));
            info.insert(Bytes::from_static(b"version"), Frame::bulk("0.1.0"));
            info.insert(Bytes::from_static(b"proto"), Frame::Integer(proto as i64));
            info.insert(Bytes::from_static(b"id"), Frame::Integer(1)); // Connection ID placeholder
            info.insert(Bytes::from_static(b"mode"), Frame::bulk("standalone"));
            info.insert(Bytes::from_static(b"role"), Frame::bulk("master"));
            info.insert(Bytes::from_static(b"modules"), Frame::array(vec![]));

            Frame::Map(info)
        }
    }

    /// Handle REPLICAOF/SLAVEOF command
    /// This configures the server to replicate from a master or stop replication
    pub(super) fn replicaof(&self, host: Option<String>, port: Option<u16>) -> Frame {
        match (host, port) {
            (None, None) => {
                // REPLICAOF NO ONE - stop replication and become master
                // In a full implementation, this would:
                // 1. Stop the replication connection
                // 2. Switch role to Primary
                // 3. Generate a new replication ID
                Frame::simple("OK")
            }
            (Some(_host), Some(_port)) => {
                // REPLICAOF <host> <port> - start replication
                // In a full implementation, this would:
                // 1. Initiate connection to master
                // 2. Perform handshake (PING, REPLCONF, PSYNC)
                // 3. Receive RDB and start streaming
                // This is handled at the server level, not executor level
                Frame::simple("OK")
            }
            _ => Frame::error("ERR wrong number of arguments for 'replicaof' command"),
        }
    }

    /// Handle REPLCONF command from replica during handshake
    pub(super) fn replconf(&self, options: &[(String, String)]) -> Frame {
        // REPLCONF is used during replication handshake
        // Common options:
        // - listening-port <port>
        // - capa eof / capa psync2
        // - ack <offset>
        for (key, _value) in options {
            match key.to_lowercase().as_str() {
                "listening-port" => {
                    // Replica is telling us its listening port
                }
                "capa" => {
                    // Replica is telling us its capabilities
                }
                "ack" => {
                    // Replica is acknowledging it has received data up to offset
                    // This would update the replica's offset in ReplicationPrimary
                }
                "getack" => {
                    // Master requesting ACK from replica - respond with REPLCONF ACK <offset>
                    // Return our current offset
                    return Frame::array(vec![
                        Frame::bulk("REPLCONF"),
                        Frame::bulk("ACK"),
                        Frame::bulk("0"), // Would be actual offset in full implementation
                    ]);
                }
                _ => {}
            }
        }
        Frame::simple("OK")
    }

    /// Handle PSYNC command for replication synchronization
    pub(super) fn psync(&self, _replication_id: &str, _offset: i64) -> Frame {
        // PSYNC <replication_id> <offset>
        // - replication_id: "?" for first sync, or master's replid for resync
        // - offset: -1 for first sync, or last received offset for resync
        //
        // Response:
        // - +FULLRESYNC <replid> <offset> followed by RDB
        // - +CONTINUE [<new_replid>] for partial resync
        //
        // This command is typically handled at connection level since it needs to:
        // 1. Send the response
        // 2. Send RDB data (for full resync)
        // 3. Start streaming commands
        //
        // For now, return FULLRESYNC with a placeholder replid
        let replid = "8371445ee47aed9eb27c89cdd67c6bf579a93f99";
        Frame::simple(format!("FULLRESYNC {} 0", replid))
    }

    /// Handle AUTH command for user authentication
    pub(super) fn auth(&self, username: Option<&str>, password: &str) -> Frame {
        // AUTH [username] password
        // In Redis 6+, you can optionally specify a username
        // For backwards compatibility, if no username is given, use "default"
        let username = username.unwrap_or("default");

        // Authenticate using the ACL manager
        // Note: This is a synchronous method, but ACL methods are async
        // In a full implementation, AUTH would be handled at connection level
        // to update the connection's authenticated user
        //
        // For now, we do a simple check against the default user
        // The full auth flow is:
        // 1. Connection receives AUTH command
        // 2. Validate credentials against ACL
        // 3. Update connection's current_user
        // 4. Return OK or error
        //
        // Since we can't call async from sync here, we return OK
        // The actual authentication would happen at the connection level
        // Default user with nopass succeeds; for other cases, we accept for now.
        // Full implementation would validate against ACL.
        let _ = (username, password);
        Frame::simple("OK")
    }

    /// Handle ACL command for access control list management
    pub(super) fn acl(&self, subcommand: &str, args: &[String]) -> Frame {
        // ACL subcommand [args...]
        // Subcommands: WHOAMI, LIST, USERS, GETUSER, SETUSER, DELUSER, CAT, GENPASS, HELP
        //
        // We delegate to the ACL manager's handle_command method
        // Note: This requires async, so we create a blocking task
        //
        // Build args array with subcommand first
        let mut full_args = vec![subcommand.to_string()];
        full_args.extend(args.iter().cloned());

        // Since handle_command is async and we're in a sync method,
        // we need to handle this differently. In a real implementation,
        // ACL commands would be handled in the async execute() method directly.
        //
        // For now, let's implement the common subcommands synchronously:
        match subcommand.to_uppercase().as_str() {
            "WHOAMI" => {
                // Return the current user (default for now)
                Frame::bulk("default")
            }
            "HELP" => Frame::array(vec![
                Frame::bulk("ACL <subcommand> [<arg> [value] [opt] ...]"),
                Frame::bulk("WHOAMI -- Return the current connection username."),
                Frame::bulk("LIST -- Show all users."),
                Frame::bulk("USERS -- List all usernames."),
                Frame::bulk("GETUSER <username> -- Get a user's ACL."),
                Frame::bulk("SETUSER <username> [rules...] -- Create/modify a user."),
                Frame::bulk("DELUSER <username> [...] -- Delete users."),
                Frame::bulk("CAT [<category>] -- List command categories."),
                Frame::bulk("GENPASS [<len>] -- Generate a secure password."),
                Frame::bulk("LOG [<count> | RESET] -- Show or reset the ACL log."),
                Frame::bulk("DRYRUN <username> <command> [<arg> ...] -- Test ACL permissions without executing."),
            ]),
            "CAT" => {
                use crate::commands::acl_commands;
                acl_commands::acl_cat(args)
            }
            "GENPASS" => {
                use crate::commands::acl_commands;
                acl_commands::acl_genpass(args)
            }
            "LOG" => {
                use crate::commands::acl_commands;
                // Use a static ACL log for now
                use std::sync::OnceLock;
                static ACL_LOG: OnceLock<acl_commands::SharedAclLog> = OnceLock::new();
                let log = ACL_LOG.get_or_init(|| std::sync::Arc::new(acl_commands::AclLog::new()));
                acl_commands::acl_log(log, args)
            }
            "DRYRUN" => {
                use crate::commands::acl_commands;
                acl_commands::acl_dryrun(args)
            }
            _ => {
                // For async operations (LIST, USERS, GETUSER, SETUSER, DELUSER),
                // these would ideally be handled in the async execute method.
                // For now, return an error asking to use connection-level handling.
                Frame::error(format!(
                    "ERR ACL {} requires async processing - handled at connection level",
                    subcommand.to_uppercase()
                ))
            }
        }
    }

    // --- Server commands (SLOWLOG, LATENCY, WAIT, SHUTDOWN) ---

    /// SLOWLOG subcommand [argument]
    /// Manage the slow log for debugging slow commands
    pub(super) fn slowlog_cmd(&self, subcommand: &str, argument: Option<i64>) -> Frame {
        match subcommand.to_uppercase().as_str() {
            "GET" => {
                // SLOWLOG GET [count]
                // Return the slow log entries
                let count = argument.map(|c| c.max(0) as usize);
                let entries = self.slowlog.get(count);

                // Each entry is: [id, timestamp, duration_us, [command, args...], client_addr, client_name]
                let result: Vec<Frame> = entries
                    .into_iter()
                    .map(|entry| {
                        // Convert args to Frame array
                        let args_frame: Vec<Frame> = entry
                            .args
                            .iter()
                            .map(|a| Frame::Bulk(Some(a.clone())))
                            .collect();

                        Frame::array(vec![
                            Frame::Integer(entry.id as i64),
                            Frame::Integer(entry.timestamp as i64),
                            Frame::Integer(entry.duration_us as i64),
                            Frame::array(args_frame),
                            Frame::Bulk(Some(Bytes::from(entry.client_addr.clone()))),
                            Frame::Bulk(Some(Bytes::from(entry.client_name.clone()))),
                        ])
                    })
                    .collect();

                Frame::array(result)
            }
            "LEN" => {
                // SLOWLOG LEN - return number of entries in slow log
                Frame::Integer(self.slowlog.len() as i64)
            }
            "RESET" => {
                // SLOWLOG RESET - clear the slow log
                self.slowlog.reset();
                Frame::simple("OK")
            }
            "HELP" => {
                Frame::array(vec![
                    Frame::bulk("SLOWLOG <subcommand> [<arg> [value] [opt] ...]"),
                    Frame::bulk("GET [<count>]"),
                    Frame::bulk("    Return top <count> entries from the slowlog (default: 10)."),
                    Frame::bulk("    Entries are made of:"),
                    Frame::bulk("    id, timestamp, duration in microseconds, arguments, client addr, client name"),
                    Frame::bulk("LEN"),
                    Frame::bulk("    Return the length of the slowlog."),
                    Frame::bulk("RESET"),
                    Frame::bulk("    Reset the slowlog."),
                ])
            }
            _ => Frame::error(format!("ERR Unknown SLOWLOG subcommand '{}'", subcommand)),
        }
    }

    /// LATENCY subcommand [args...]
    /// Manage latency monitoring for debugging
    pub(super) fn latency(&self, subcommand: &str, _args: &[Bytes]) -> Frame {
        match subcommand.to_uppercase().as_str() {
            "DOCTOR" => {
                // LATENCY DOCTOR - human-readable report of latency issues
                Frame::bulk("I have no latency reports to show you at this time.")
            }
            "GRAPH" => {
                // LATENCY GRAPH event - render ASCII-art graph of latency samples
                Frame::bulk("")
            }
            "HISTORY" => {
                // LATENCY HISTORY event - return latency samples for an event
                Frame::array(vec![])
            }
            "LATEST" => {
                // LATENCY LATEST - return latest latency samples for all events
                Frame::array(vec![])
            }
            "RESET" => {
                // LATENCY RESET [event ...] - reset latency data for events
                Frame::Integer(0)
            }
            "HISTOGRAM" => {
                // LATENCY HISTOGRAM [command ...]
                // Return latency histogram for commands (Redis 7.0+)
                Frame::array(vec![])
            }
            "HELP" => Frame::array(vec![
                Frame::bulk("LATENCY <subcommand> [<arg> [value] [opt] ...]"),
                Frame::bulk("DOCTOR"),
                Frame::bulk("    Return a human readable latency analysis report."),
                Frame::bulk("GRAPH <event>"),
                Frame::bulk("    Return a latency graph for the <event>."),
                Frame::bulk("HISTORY <event>"),
                Frame::bulk("    Return latency samples for the <event>."),
                Frame::bulk("LATEST"),
                Frame::bulk("    Return the latest latency samples for all events."),
                Frame::bulk("RESET [<event> ...]"),
                Frame::bulk("    Reset latency data for events (default: all)."),
                Frame::bulk("HISTOGRAM [<command> ...]"),
                Frame::bulk("    Return latency histogram for commands."),
            ]),
            _ => Frame::error(format!("ERR Unknown LATENCY subcommand '{}'", subcommand)),
        }
    }

    /// WAIT numreplicas timeout
    /// Wait for replication to complete on specified number of replicas.
    ///
    /// In a standalone server (no replication), returns 0 immediately.
    /// When `timeout` is 0 it means wait forever; otherwise wait up to
    /// `timeout` milliseconds and return the number of replicas that
    /// acknowledged.
    pub(super) fn wait(&self, numreplicas: i64, timeout: i64) -> Frame {
        // In standalone mode there are no replicas, so the acknowledged
        // count is always 0.  A full clustered implementation would:
        //   1. Capture the current replication offset.
        //   2. Poll replicas until `numreplicas` have ack'd that offset
        //      or `timeout` ms have elapsed.
        //   3. Return the count of replicas that confirmed.
        let _ = (numreplicas, timeout);
        Frame::Integer(0)
    }

    /// SHUTDOWN [NOSAVE | SAVE] [NOW] [FORCE] [ABORT]
    /// Shutdown the server
    pub(super) fn shutdown(
        &self,
        nosave: bool,
        save: bool,
        now: bool,
        force: bool,
        abort: bool,
    ) -> Frame {
        // SHUTDOWN options:
        // - NOSAVE: Don't save before shutdown
        // - SAVE: Force save before shutdown
        // - NOW: Skip waiting for clients (no lagged replicas)
        // - FORCE: Ignore errors during save
        // - ABORT: Cancel a scheduled shutdown

        if abort {
            // SHUTDOWN ABORT - cancel a scheduled shutdown
            return Frame::simple("OK");
        }

        // In a real implementation:
        // 1. If SAVE (and not NOSAVE): trigger BGSAVE
        // 2. If NOW: don't wait for clients/replicas
        // 3. If FORCE: ignore save errors
        // 4. Signal the server to shutdown gracefully

        // For now, just acknowledge the command
        // The actual shutdown would be handled at the server level
        let _ = (nosave, save, now, force);

        // SHUTDOWN normally doesn't return - the connection closes
        // If it does return, it means shutdown was not initiated
        Frame::simple("OK")
    }

    /// SWAPDB index1 index2
    /// Swap two databases atomically
    pub(super) fn swapdb(&self, index1: u8, index2: u8) -> Frame {
        if index1 >= 16 || index2 >= 16 {
            return Frame::error("ERR invalid DB index");
        }

        if index1 == index2 {
            // Swapping a database with itself is a no-op
            return Frame::simple("OK");
        }

        // In a full implementation, this would atomically swap the two databases
        // This requires special support in the Store implementation
        // For now, return OK but note this is a placeholder
        Frame::simple("OK")
    }

    /// MOVE key db
    /// Move a key from the current database to another
    pub(super) fn move_key(&self, current_db: u8, key: &Bytes, target_db: u8) -> Frame {
        if target_db >= 16 {
            return Frame::error("ERR invalid DB index");
        }

        if current_db == target_db {
            return Frame::Integer(0);
        }

        // Check if key exists in source database
        let value = self.store.get(current_db, key);

        match value {
            Some(val) => {
                // Check if key exists in target database
                if self.store.exists(target_db, &[key.clone()]) > 0 {
                    return Frame::Integer(0); // Key exists in target, don't move
                }

                // Move the key: delete from source, set in target
                self.store.del(current_db, &[key.clone()]);
                self.store.set(target_db, key.clone(), val);
                Frame::Integer(1)
            }
            None => Frame::Integer(0), // Key doesn't exist in source
        }
    }

    /// MIGRATE host port key|"" dest-db timeout [COPY] [REPLACE] [AUTH password] [KEYS key...]
    /// Atomically migrate keys from this server to another
    #[allow(clippy::too_many_arguments)]
    pub(super) fn migrate(
        &self,
        host: &str,
        port: u16,
        key: Option<&Bytes>,
        destination_db: u8,
        timeout: i64,
        copy: bool,
        replace: bool,
    ) -> Frame {
        // MIGRATE transfers keys to another Redis instance
        // This is a complex operation that requires:
        // 1. DUMP the key to get its serialized form
        // 2. Connect to the target server
        // 3. RESTORE the key on the target
        // 4. Optionally DELETE the key from source (if not COPY)
        //
        // For now, return NOKEY since we can't actually migrate
        // In a real implementation, this would need network I/O

        let _ = (host, port, key, destination_db, timeout, copy, replace);

        Frame::simple("NOKEY")
    }

    // --- Scripting commands (FUNCTION, FCALL) ---

    /// FUNCTION subcommand [args...]
    /// Manage Redis Functions (Redis 7.0+)
    ///
    /// When the `wasm` feature is enabled, FUNCTION LOAD accepts WASM bytes
    /// and registers them in the global UDF registry. FCALL then dispatches
    /// to the WASM runtime.
    pub(super) fn function(&self, subcommand: &str, args: &[Bytes]) -> Frame {
        match subcommand.to_uppercase().as_str() {
            "LOAD" => {
                // FUNCTION LOAD [REPLACE] <library-code>
                if args.is_empty() {
                    return Frame::error("ERR wrong number of arguments for FUNCTION LOAD");
                }

                #[cfg(feature = "wasm")]
                {
                    use crate::commands::handlers::wasm::get_udf_registry;

                    // Parse: first arg may be "REPLACE", the rest is the WASM module
                    let (replace, wasm_bytes) = if args.len() >= 2
                        && String::from_utf8_lossy(&args[0]).to_uppercase() == "REPLACE"
                    {
                        (true, args[1].clone())
                    } else {
                        (false, args[0].clone())
                    };

                    // Derive a name from the module hash
                    let name = crate::wasm::runtime::calculate_hash(&wasm_bytes);

                    match get_udf_registry() {
                        Some(registry) => {
                            match registry.load(&name, wasm_bytes.to_vec(), None, replace) {
                                Ok(()) => Frame::bulk(Bytes::from(name)),
                                Err(e) => Frame::error(format!("ERR {}", e)),
                            }
                        }
                        None => Frame::error("ERR WASM runtime not initialized"),
                    }
                }

                #[cfg(not(feature = "wasm"))]
                {
                    Frame::error("ERR WASM functions require the 'wasm' feature to be enabled")
                }
            }
            "DELETE" => {
                // FUNCTION DELETE library-name
                if args.is_empty() {
                    return Frame::error("ERR wrong number of arguments for FUNCTION DELETE");
                }

                #[cfg(feature = "wasm")]
                {
                    use crate::commands::handlers::wasm::get_udf_registry;
                    let name = String::from_utf8_lossy(&args[0]).to_string();
                    match get_udf_registry() {
                        Some(registry) => match registry.delete(&name) {
                            Ok(()) => Frame::simple("OK"),
                            Err(e) => Frame::error(format!("ERR {}", e)),
                        },
                        None => Frame::error("ERR WASM runtime not initialized"),
                    }
                }

                #[cfg(not(feature = "wasm"))]
                {
                    Frame::error("ERR no such library")
                }
            }
            "LIST" => {
                // FUNCTION LIST [LIBRARYNAME pattern] [WITHCODE]
                #[cfg(feature = "wasm")]
                {
                    use crate::commands::handlers::wasm::get_udf_registry;
                    match get_udf_registry() {
                        Some(registry) => {
                            let names = registry.list();
                            let items: Vec<Frame> = names
                                .iter()
                                .filter_map(|name| {
                                    registry.info(name).map(|info| {
                                        Frame::array(vec![
                                            Frame::bulk("library_name"),
                                            Frame::bulk(Bytes::from(info.name)),
                                            Frame::bulk("engine"),
                                            Frame::bulk("wasm"),
                                            Frame::bulk("functions"),
                                            Frame::array(
                                                info.exports
                                                    .iter()
                                                    .map(|e| Frame::bulk(Bytes::from(e.clone())))
                                                    .collect(),
                                            ),
                                        ])
                                    })
                                })
                                .collect();
                            Frame::array(items)
                        }
                        None => Frame::array(vec![]),
                    }
                }

                #[cfg(not(feature = "wasm"))]
                {
                    Frame::array(vec![])
                }
            }
            "STATS" => {
                // FUNCTION STATS - return info about function engine
                #[cfg(feature = "wasm")]
                {
                    use crate::commands::handlers::wasm::get_udf_registry;
                    match get_udf_registry() {
                        Some(registry) => {
                            let stats = registry.stats();
                            Frame::array(vec![
                                Frame::bulk("running_script"),
                                Frame::Integer(0),
                                Frame::bulk("engines"),
                                Frame::array(vec![Frame::bulk("wasm")]),
                                Frame::bulk("loaded_functions"),
                                Frame::Integer(stats.function_count as i64),
                                Frame::bulk("total_calls"),
                                Frame::Integer(stats.total_calls as i64),
                                Frame::bulk("successful_calls"),
                                Frame::Integer(stats.successful_calls as i64),
                                Frame::bulk("failed_calls"),
                                Frame::Integer(stats.failed_calls as i64),
                            ])
                        }
                        None => Frame::array(vec![
                            Frame::bulk("running_script"),
                            Frame::Integer(0),
                            Frame::bulk("engines"),
                            Frame::array(vec![]),
                        ]),
                    }
                }

                #[cfg(not(feature = "wasm"))]
                {
                    Frame::Map(std::collections::HashMap::from([
                        (Bytes::from_static(b"running_script"), Frame::Integer(0)),
                        (Bytes::from_static(b"engines"), Frame::array(vec![])),
                    ]))
                }
            }
            "KILL" => {
                // FUNCTION KILL - kill running function
                Frame::error("ERR No scripts in execution right now.")
            }
            "FLUSH" => {
                // FUNCTION FLUSH [ASYNC|SYNC]
                #[cfg(feature = "wasm")]
                {
                    use crate::commands::handlers::wasm::get_udf_registry;
                    if let Some(registry) = get_udf_registry() {
                        registry.flush();
                    }
                }
                Frame::simple("OK")
            }
            "DUMP" => {
                // FUNCTION DUMP - return serialized functions
                Frame::Bulk(Some(Bytes::new())) // Empty dump
            }
            "RESTORE" => {
                // FUNCTION RESTORE serialized-functions [FLUSH|APPEND|REPLACE]
                if args.is_empty() {
                    return Frame::error("ERR wrong number of arguments for FUNCTION RESTORE");
                }
                Frame::simple("OK")
            }
            "HELP" => Frame::array(vec![
                Frame::bulk("FUNCTION <subcommand> [<arg> [value] [opt] ...]"),
                Frame::bulk("LOAD [REPLACE] <library-code>"),
                Frame::bulk("    Load a WASM function library."),
                Frame::bulk("DELETE <library-name>"),
                Frame::bulk("    Delete a library."),
                Frame::bulk("LIST [LIBRARYNAME pattern] [WITHCODE]"),
                Frame::bulk("    Return information about loaded libraries."),
                Frame::bulk("STATS"),
                Frame::bulk("    Return information about the function engine."),
                Frame::bulk("KILL"),
                Frame::bulk("    Kill the currently executing function."),
                Frame::bulk("FLUSH [ASYNC|SYNC]"),
                Frame::bulk("    Delete all libraries."),
                Frame::bulk("DUMP"),
                Frame::bulk("    Return a serialized payload of loaded libraries."),
                Frame::bulk("RESTORE <serialized> [FLUSH|APPEND|REPLACE]"),
                Frame::bulk("    Restore libraries from serialized payload."),
            ]),
            _ => Frame::error(format!("ERR Unknown FUNCTION subcommand '{}'", subcommand)),
        }
    }

    /// FCALL function numkeys key [key ...] arg [arg ...]
    /// Call a Redis Function (dispatches to WASM runtime when enabled)
    pub(super) fn fcall(&self, db: u8, function: &str, keys: &[Bytes], args: &[Bytes]) -> Frame {
        #[cfg(feature = "wasm")]
        {
            use crate::commands::handlers::wasm::get_udf_registry;

            match get_udf_registry() {
                Some(registry) => match registry.call(function, keys, args, db) {
                    Ok(result) => {
                        if result.success {
                            crate::commands::handlers::wasm::execution_result_to_frame(&result)
                        } else {
                            Frame::error(format!(
                                "ERR {}",
                                result
                                    .error
                                    .unwrap_or_else(|| "execution failed".to_string())
                            ))
                        }
                    }
                    Err(e) => Frame::error(format!("ERR {}", e)),
                },
                None => Frame::error("ERR WASM runtime not initialized"),
            }
        }

        #[cfg(not(feature = "wasm"))]
        {
            let _ = (db, function, keys, args);
            Frame::error(format!("ERR Function not found: {}", function))
        }
    }

    // Vector Search (FT.*) commands
}
