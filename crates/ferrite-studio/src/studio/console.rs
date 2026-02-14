//! Interactive command console for Ferrite Studio
//!
//! Provides a web-based command console with history, auto-complete,
//! syntax highlighting data, and multi-line command support.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Request to execute a command via the console.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsoleRequest {
    /// The command text to execute (may be multi-line).
    pub command: String,
    /// Session context (e.g. selected database index).
    pub db_index: Option<u32>,
}

/// Result of a console command execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsoleResponse {
    /// The original command.
    pub command: String,
    /// Formatted output (with type annotations for syntax highlighting).
    pub output: String,
    /// RESP type of the response.
    pub resp_type: RespType,
    /// Execution time in microseconds.
    pub duration_us: u64,
    /// Whether this was an error response.
    pub is_error: bool,
}

/// RESP protocol types for syntax highlighting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RespType {
    SimpleString,
    Error,
    Integer,
    BulkString,
    Array,
    Null,
}

/// A command history entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsoleHistoryEntry {
    /// The command executed.
    pub command: String,
    /// Timestamp (epoch ms).
    pub timestamp_ms: u64,
    /// Whether the command succeeded.
    pub success: bool,
}

/// Command metadata for auto-complete.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandInfo {
    /// Command name (e.g. "GET").
    pub name: String,
    /// Short description.
    pub summary: String,
    /// Argument syntax.
    pub syntax: String,
    /// Command group (e.g. "string", "list").
    pub group: String,
    /// Minimum number of arguments.
    pub min_args: usize,
    /// Whether the command is read-only.
    pub readonly: bool,
}

/// The command console engine.
pub struct Console {
    /// Command history (most recent last).
    history: parking_lot::RwLock<Vec<ConsoleHistoryEntry>>,
    /// Maximum history size.
    max_history: usize,
    /// Known commands for auto-complete.
    commands: HashMap<String, CommandInfo>,
}

impl Console {
    /// Create a new console instance.
    pub fn new() -> Self {
        Self {
            history: parking_lot::RwLock::new(Vec::new()),
            max_history: 1000,
            commands: build_command_registry(),
        }
    }

    /// Get auto-complete suggestions for a partial input.
    pub fn autocomplete(&self, input: &str) -> Vec<String> {
        let upper = input.to_uppercase();
        let parts: Vec<&str> = upper.split_whitespace().collect();

        if parts.is_empty() || (parts.len() == 1 && !input.ends_with(' ')) {
            let prefix = parts.first().copied().unwrap_or("");
            return self
                .commands
                .keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect();
        }

        Vec::new()
    }

    /// Parse multi-line input into individual commands.
    pub fn parse_multiline(input: &str) -> Vec<String> {
        input
            .lines()
            .map(|l| l.trim())
            .filter(|l| !l.is_empty() && !l.starts_with('#') && !l.starts_with("//"))
            .map(|l| l.to_string())
            .collect()
    }

    /// Record a command in history.
    pub fn record_history(&self, command: &str, success: bool) {
        let entry = ConsoleHistoryEntry {
            command: command.to_string(),
            timestamp_ms: epoch_ms(),
            success,
        };
        let mut history = self.history.write();
        history.push(entry);
        if history.len() > self.max_history {
            history.remove(0);
        }
    }

    /// Get command history.
    pub fn get_history(&self, limit: usize) -> Vec<ConsoleHistoryEntry> {
        let history = self.history.read();
        let start = history.len().saturating_sub(limit);
        history[start..].to_vec()
    }

    /// Clear command history.
    pub fn clear_history(&self) {
        self.history.write().clear();
    }

    /// Get all available command names.
    pub fn available_commands(&self) -> Vec<&CommandInfo> {
        self.commands.values().collect()
    }

    /// Get info about a specific command.
    pub fn command_info(&self, name: &str) -> Option<&CommandInfo> {
        self.commands.get(&name.to_uppercase())
    }

    /// Format a RESP value for display with syntax highlighting hints.
    pub fn format_resp_output(value: &serde_json::Value) -> (String, RespType) {
        match value {
            serde_json::Value::Null => ("(nil)".to_string(), RespType::Null),
            serde_json::Value::String(s) => {
                if s.starts_with("ERR") || s.starts_with("WRONGTYPE") {
                    (format!("(error) {}", s), RespType::Error)
                } else {
                    (format!("\"{}\"", s), RespType::BulkString)
                }
            }
            serde_json::Value::Number(n) => {
                (format!("(integer) {}", n), RespType::Integer)
            }
            serde_json::Value::Bool(b) => {
                (format!("(integer) {}", if *b { 1 } else { 0 }), RespType::Integer)
            }
            serde_json::Value::Array(arr) => {
                if arr.is_empty() {
                    return ("(empty array)".to_string(), RespType::Array);
                }
                let mut output = String::new();
                for (i, item) in arr.iter().enumerate() {
                    let (formatted, _) = Self::format_resp_output(item);
                    output.push_str(&format!("{}) {}\n", i + 1, formatted));
                }
                (output.trim_end().to_string(), RespType::Array)
            }
            serde_json::Value::Object(obj) => {
                let mut output = String::new();
                let mut i = 1;
                for (key, val) in obj {
                    let (formatted, _) = Self::format_resp_output(val);
                    output.push_str(&format!("{}) \"{}\"\n{}) {}\n", i, key, i + 1, formatted));
                    i += 2;
                }
                (output.trim_end().to_string(), RespType::Array)
            }
        }
    }

    /// History length.
    pub fn history_len(&self) -> usize {
        self.history.read().len()
    }
}

impl Default for Console {
    fn default() -> Self {
        Self::new()
    }
}

fn build_command_registry() -> HashMap<String, CommandInfo> {
    let commands = vec![
        ("GET", "Get the value of a key", "GET key", "string", 1, true),
        ("SET", "Set a key to a string value", "SET key value [EX seconds]", "string", 2, false),
        ("DEL", "Delete one or more keys", "DEL key [key ...]", "generic", 1, false),
        ("EXISTS", "Check if key exists", "EXISTS key [key ...]", "generic", 1, true),
        ("TTL", "Get time to live of a key", "TTL key", "generic", 1, true),
        ("EXPIRE", "Set a key's time to live", "EXPIRE key seconds", "generic", 2, false),
        ("PERSIST", "Remove the expiration from a key", "PERSIST key", "generic", 1, false),
        ("TYPE", "Get key type", "TYPE key", "generic", 1, true),
        ("RENAME", "Rename a key", "RENAME key newkey", "generic", 2, false),
        ("KEYS", "Find keys matching pattern", "KEYS pattern", "generic", 1, true),
        ("SCAN", "Incrementally iterate keys", "SCAN cursor [MATCH pattern] [COUNT count]", "generic", 1, true),
        ("PING", "Ping the server", "PING [message]", "connection", 0, true),
        ("INFO", "Get server information", "INFO [section]", "server", 0, true),
        ("DBSIZE", "Return the number of keys", "DBSIZE", "server", 0, true),
        ("FLUSHDB", "Remove all keys from the current db", "FLUSHDB", "server", 0, false),
        ("MGET", "Get values of multiple keys", "MGET key [key ...]", "string", 1, true),
        ("MSET", "Set multiple key-value pairs", "MSET key value [key value ...]", "string", 2, false),
        ("INCR", "Increment integer value", "INCR key", "string", 1, false),
        ("DECR", "Decrement integer value", "DECR key", "string", 1, false),
        ("APPEND", "Append to string value", "APPEND key value", "string", 2, false),
        ("STRLEN", "Get string length", "STRLEN key", "string", 1, true),
        ("LPUSH", "Push to list head", "LPUSH key element [element ...]", "list", 2, false),
        ("RPUSH", "Push to list tail", "RPUSH key element [element ...]", "list", 2, false),
        ("LPOP", "Pop from list head", "LPOP key [count]", "list", 1, false),
        ("RPOP", "Pop from list tail", "RPOP key [count]", "list", 1, false),
        ("LLEN", "Get list length", "LLEN key", "list", 1, true),
        ("LRANGE", "Get range from list", "LRANGE key start stop", "list", 3, true),
        ("HSET", "Set hash field", "HSET key field value [field value ...]", "hash", 3, false),
        ("HGET", "Get hash field", "HGET key field", "hash", 2, true),
        ("HGETALL", "Get all hash fields", "HGETALL key", "hash", 1, true),
        ("HDEL", "Delete hash field", "HDEL key field [field ...]", "hash", 2, false),
        ("HLEN", "Get hash length", "HLEN key", "hash", 1, true),
        ("SADD", "Add set member", "SADD key member [member ...]", "set", 2, false),
        ("SREM", "Remove set member", "SREM key member [member ...]", "set", 2, false),
        ("SMEMBERS", "Get all set members", "SMEMBERS key", "set", 1, true),
        ("SCARD", "Get set cardinality", "SCARD key", "set", 1, true),
        ("SISMEMBER", "Check set membership", "SISMEMBER key member", "set", 2, true),
        ("ZADD", "Add sorted set member", "ZADD key score member [score member ...]", "sorted_set", 3, false),
        ("ZRANGE", "Get range from sorted set", "ZRANGE key start stop [WITHSCORES]", "sorted_set", 3, true),
        ("ZRANK", "Get member rank", "ZRANK key member", "sorted_set", 2, true),
        ("ZSCORE", "Get member score", "ZSCORE key member", "sorted_set", 2, true),
        ("ZCARD", "Get sorted set cardinality", "ZCARD key", "sorted_set", 1, true),
        ("SELECT", "Select database", "SELECT index", "connection", 1, false),
    ];

    commands
        .into_iter()
        .map(|(name, summary, syntax, group, min_args, readonly)| {
            (
                name.to_string(),
                CommandInfo {
                    name: name.to_string(),
                    summary: summary.to_string(),
                    syntax: syntax.to_string(),
                    group: group.to_string(),
                    min_args,
                    readonly,
                },
            )
        })
        .collect()
}

fn epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_autocomplete_partial() {
        let console = Console::new();
        let suggestions = console.autocomplete("GE");
        assert!(suggestions.contains(&"GET".to_string()));
        assert!(!suggestions.contains(&"SET".to_string()));
    }

    #[test]
    fn test_autocomplete_empty() {
        let console = Console::new();
        let suggestions = console.autocomplete("");
        assert!(suggestions.len() > 10); // Should return all commands.
    }

    #[test]
    fn test_autocomplete_full_command() {
        let console = Console::new();
        let suggestions = console.autocomplete("GET ");
        // After a full command + space, no more suggestions for now.
        assert!(suggestions.is_empty());
    }

    #[test]
    fn test_parse_multiline() {
        let input = "SET foo bar\n# comment\nGET foo\n\n// another comment\nDEL foo";
        let cmds = Console::parse_multiline(input);
        assert_eq!(cmds, vec!["SET foo bar", "GET foo", "DEL foo"]);
    }

    #[test]
    fn test_history() {
        let console = Console::new();
        console.record_history("GET foo", true);
        console.record_history("SET bar baz", true);
        console.record_history("INVALID cmd", false);

        let history = console.get_history(10);
        assert_eq!(history.len(), 3);
        assert!(history[0].success);
        assert!(!history[2].success);
    }

    #[test]
    fn test_history_limit() {
        let console = Console::new();
        let history = console.get_history(5);
        assert!(history.is_empty());

        for i in 0..10 {
            console.record_history(&format!("CMD {}", i), true);
        }
        let history = console.get_history(3);
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].command, "CMD 7");
    }

    #[test]
    fn test_clear_history() {
        let console = Console::new();
        console.record_history("GET foo", true);
        console.clear_history();
        assert_eq!(console.history_len(), 0);
    }

    #[test]
    fn test_command_info() {
        let console = Console::new();
        let info = console.command_info("GET").unwrap();
        assert_eq!(info.name, "GET");
        assert!(info.readonly);
        assert_eq!(info.min_args, 1);

        let info = console.command_info("set").unwrap();
        assert_eq!(info.name, "SET");
        assert!(!info.readonly);
    }

    #[test]
    fn test_command_info_not_found() {
        let console = Console::new();
        assert!(console.command_info("NONEXISTENT").is_none());
    }

    #[test]
    fn test_available_commands() {
        let console = Console::new();
        let cmds = console.available_commands();
        assert!(cmds.len() > 30);
    }

    #[test]
    fn test_format_resp_null() {
        let (output, resp_type) = Console::format_resp_output(&serde_json::Value::Null);
        assert_eq!(output, "(nil)");
        assert_eq!(resp_type, RespType::Null);
    }

    #[test]
    fn test_format_resp_string() {
        let val = serde_json::Value::String("OK".to_string());
        let (output, resp_type) = Console::format_resp_output(&val);
        assert_eq!(output, "\"OK\"");
        assert_eq!(resp_type, RespType::BulkString);
    }

    #[test]
    fn test_format_resp_error() {
        let val = serde_json::Value::String("ERR unknown command".to_string());
        let (output, resp_type) = Console::format_resp_output(&val);
        assert!(output.contains("(error)"));
        assert_eq!(resp_type, RespType::Error);
    }

    #[test]
    fn test_format_resp_integer() {
        let val = serde_json::json!(42);
        let (output, resp_type) = Console::format_resp_output(&val);
        assert_eq!(output, "(integer) 42");
        assert_eq!(resp_type, RespType::Integer);
    }

    #[test]
    fn test_format_resp_array() {
        let val = serde_json::json!(["a", "b", "c"]);
        let (output, resp_type) = Console::format_resp_output(&val);
        assert!(output.contains("1)"));
        assert!(output.contains("2)"));
        assert!(output.contains("3)"));
        assert_eq!(resp_type, RespType::Array);
    }

    #[test]
    fn test_format_resp_empty_array() {
        let val = serde_json::json!([]);
        let (output, resp_type) = Console::format_resp_output(&val);
        assert_eq!(output, "(empty array)");
        assert_eq!(resp_type, RespType::Array);
    }

    #[test]
    fn test_format_resp_object() {
        let val = serde_json::json!({"key": "value"});
        let (output, resp_type) = Console::format_resp_output(&val);
        assert!(output.contains("key"));
        assert!(output.contains("value"));
        assert_eq!(resp_type, RespType::Array);
    }

    #[test]
    fn test_console_default() {
        let console = Console::default();
        assert!(console.available_commands().len() > 0);
    }

    #[test]
    fn test_console_request_serialization() {
        let req = ConsoleRequest {
            command: "GET foo".to_string(),
            db_index: Some(0),
        };
        let json = serde_json::to_string(&req).unwrap();
        let deserialized: ConsoleRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.command, "GET foo");
        assert_eq!(deserialized.db_index, Some(0));
    }

    #[test]
    fn test_console_response_serialization() {
        let resp = ConsoleResponse {
            command: "PING".to_string(),
            output: "PONG".to_string(),
            resp_type: RespType::SimpleString,
            duration_us: 100,
            is_error: false,
        };
        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: ConsoleResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.command, "PING");
        assert!(!deserialized.is_error);
    }

    #[test]
    fn test_history_entry_serialization() {
        let entry = ConsoleHistoryEntry {
            command: "SET a b".to_string(),
            timestamp_ms: 12345,
            success: true,
        };
        let json = serde_json::to_string(&entry).unwrap();
        let deserialized: ConsoleHistoryEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.command, "SET a b");
        assert!(deserialized.success);
    }
}
