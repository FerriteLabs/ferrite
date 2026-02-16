//! Admin command handlers
//!
//! This module contains handlers for administrative commands like:
//! - DEBUG (debugging and testing)
//! - MEMORY (memory usage and diagnostics)
//! - CLIENT (client management)
//! - CONFIG (configuration management)
//! - MODULE (module management)
//! - BGSAVE/SAVE/LASTSAVE (persistence commands)

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use crate::config::SharedConfig;
use crate::protocol::Frame;
use crate::runtime::{ClientRegistry, PauseMode};
use crate::storage::{Store, Value};

/// Handle DEBUG command
pub fn debug(subcommand: &str, args: Vec<Bytes>) -> Frame {
    match subcommand {
        "SLEEP" => {
            if args.is_empty() {
                return Frame::error("ERR wrong number of arguments for DEBUG SLEEP");
            }
            let seconds_str = String::from_utf8_lossy(&args[0]);
            if let Ok(seconds) = seconds_str.parse::<f64>() {
                std::thread::sleep(std::time::Duration::from_secs_f64(seconds));
                Frame::simple("OK")
            } else {
                Frame::error("ERR invalid sleep time")
            }
        }
        "SEGFAULT" => Frame::error("ERR DEBUG SEGFAULT not supported"),
        "SET-ACTIVE-EXPIRE" => Frame::simple("OK"),
        "QUICKLIST-PACKED-THRESHOLD" => Frame::simple("OK"),
        "LISTPACK-ENTRIES" => Frame::simple("OK"),
        "DIGEST" => Frame::bulk("0000000000000000000000000000000000000000"),
        "DIGEST-VALUE" => Frame::array(vec![]),
        "PROTOCOL" => Frame::simple("OK"),
        "RELOAD" => Frame::simple("OK"),
        "RESTART" => Frame::error("ERR DEBUG RESTART not supported"),
        "CRASH-AND-RECOVER" | "CRASH-AND-RESTART" => {
            Frame::error("ERR DEBUG crash commands not supported")
        }
        "OBJECT" => {
            if args.is_empty() {
                return Frame::error("ERR wrong number of arguments for DEBUG OBJECT");
            }
            Frame::bulk(
                "Value at:0x0 refcount:1 encoding:raw serializedlength:0 lru:0 lru_seconds_idle:0",
            )
        }
        "STRUCTSIZE" => Frame::bulk("struct sizes: various internal struct sizes"),
        "HTSTATS" => Frame::bulk("[Hash table 0] No stats available"),
        "CHANGE-REPL-STATE" => Frame::simple("OK"),
        _ => Frame::error(format!("ERR Unknown DEBUG subcommand '{}'", subcommand)),
    }
}

/// Handle MEMORY command
pub fn memory(store: &Arc<Store>, subcommand: &str, key: Option<&Bytes>, db: u8) -> Frame {
    match subcommand {
        "USAGE" => match key {
            Some(k) => {
                if store.exists(db, &[k.clone()]) == 1 {
                    if let Some(value) = store.get(db, k) {
                        let size = match &value {
                            Value::String(s) => s.len() + 32,
                            Value::List(l) => l.len() * 16 + 64,
                            Value::Hash(h) => h.len() * 48 + 64,
                            Value::Set(s) => s.len() * 24 + 64,
                            Value::SortedSet { by_member, .. } => by_member.len() * 40 + 128,
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
        },
        "DOCTOR" => Frame::bulk("Sam, I have no memory problems"),
        "MALLOC-SIZE" => match key {
            Some(_) => Frame::Integer(0),
            None => Frame::error("ERR wrong number of arguments for MEMORY MALLOC-SIZE"),
        },
        "PURGE" => Frame::simple("OK"),
        "STATS" => Frame::bulk(
            "peak.allocated:0\n\
             total.allocated:0\n\
             startup.allocated:0\n\
             replication.backlog:0\n\
             clients.slaves:0\n\
             clients.normal:0\n\
             aof.buffer:0\n",
        ),
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

/// Handle CLIENT command
pub fn client(client_registry: &ClientRegistry, subcommand: &str, args: Vec<Bytes>) -> Frame {
    match subcommand {
        "LIST" => {
            let clients = client_registry.list();
            if clients.is_empty() {
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
        "GETNAME" => Frame::Null,
        "SETNAME" => {
            if args.is_empty() {
                return Frame::error("ERR wrong number of arguments for CLIENT SETNAME");
            }
            Frame::simple("OK")
        }
        "ID" => Frame::Integer((client_registry.count() as i64).max(1)),
        "INFO" => {
            let clients = client_registry.list();
            if let Some(client) = clients.first() {
                Frame::bulk(client.to_info_string())
            } else {
                let info = "id=1 addr=127.0.0.1:0 laddr=127.0.0.1:6379 fd=0 name= age=0 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd=client user=default redir=-1";
                Frame::bulk(info)
            }
        }
        "KILL" => {
            if args.is_empty() {
                return Frame::error("ERR wrong number of arguments for CLIENT KILL");
            }
            Frame::simple("OK")
        }
        "PAUSE" => {
            if args.is_empty() {
                return Frame::error("ERR wrong number of arguments for CLIENT PAUSE");
            }
            let timeout_ms = match String::from_utf8_lossy(&args[0]).parse::<u64>() {
                Ok(t) => t,
                Err(_) => return Frame::error("ERR timeout is not an integer or out of range"),
            };
            let mode = if args.len() > 1 {
                match String::from_utf8_lossy(&args[1]).to_uppercase().as_str() {
                    "WRITE" => PauseMode::Write,
                    "ALL" => PauseMode::All,
                    _ => return Frame::error("ERR CLIENT PAUSE mode must be WRITE or ALL"),
                }
            } else {
                PauseMode::All
            };
            client_registry.pause(timeout_ms, mode);
            Frame::simple("OK")
        }
        "UNPAUSE" => {
            client_registry.unpause();
            Frame::simple("OK")
        }
        "REPLY" => {
            if args.is_empty() {
                return Frame::error("ERR wrong number of arguments for CLIENT REPLY");
            }
            Frame::simple("OK")
        }
        "CACHING" => Frame::simple("OK"),
        "GETREDIR" => Frame::Integer(-1),
        "TRACKINGINFO" => Frame::array(vec![
            Frame::bulk("flags"),
            Frame::array(vec![]),
            Frame::bulk("redirect"),
            Frame::Integer(-1),
            Frame::bulk("prefixes"),
            Frame::array(vec![]),
        ]),
        "NO-EVICT" => Frame::simple("OK"),
        "SETINFO" => Frame::simple("OK"),
        "HELP" => Frame::array(vec![
            Frame::bulk("CLIENT GETNAME"),
            Frame::bulk("CLIENT ID"),
            Frame::bulk("CLIENT INFO"),
            Frame::bulk("CLIENT KILL <ip:port>|ID <id>|TYPE <normal|master|replica|pubsub>|USER <username>|ADDR <ip:port>|LADDR <ip:port>|SKIPME <yes|no>"),
            Frame::bulk("CLIENT LIST [TYPE <normal|master|replica|pubsub>] [ID <id> [id...]]"),
            Frame::bulk("CLIENT NO-EVICT <ON|OFF>"),
            Frame::bulk("CLIENT PAUSE <timeout> [WRITE|ALL]"),
            Frame::bulk("CLIENT REPLY <ON|OFF|SKIP>"),
            Frame::bulk("CLIENT SETNAME <connection-name>"),
            Frame::bulk("CLIENT UNPAUSE"),
        ]),
        _ => Frame::error(format!("ERR Unknown CLIENT subcommand '{}'", subcommand)),
    }
}

/// Handle CONFIG command
pub fn config(
    store: &Arc<Store>,
    config: &Option<SharedConfig>,
    subcommand: &str,
    args: Vec<Bytes>,
) -> Frame {
    match subcommand {
        "GET" => {
            if args.is_empty() {
                return Frame::error("ERR wrong number of arguments for CONFIG GET");
            }
            let pattern = String::from_utf8_lossy(&args[0]).to_string();

            if let Some(ref cfg) = config {
                let params = cfg.list_params(&pattern);
                let mut results = Vec::with_capacity(params.len() * 2);
                for (key, value) in params {
                    results.push(Frame::bulk(key));
                    results.push(Frame::bulk(value));
                }
                Frame::array(results)
            } else {
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
            if args.len() < 2 {
                return Frame::error("ERR wrong number of arguments for CONFIG SET");
            }

            let param = String::from_utf8_lossy(&args[0]).to_string();
            let value = String::from_utf8_lossy(&args[1]).to_string();

            if let Some(ref cfg) = config {
                match cfg.set_param(&param, &value) {
                    Ok(true) => Frame::simple("OK"),
                    Ok(false) => {
                        Frame::error(format!("ERR CONFIG SET for '{}' requires a restart", param))
                    }
                    Err(e) => Frame::error(format!("ERR {}", e)),
                }
            } else {
                Frame::simple("OK")
            }
        }
        "RESETSTAT" => {
            store.reset_stats();
            Frame::simple("OK")
        }
        "REWRITE" => {
            if let Some(ref cfg) = config {
                match cfg.rewrite() {
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

/// Handle MODULE command
pub fn module(subcommand: &str, args: &[Bytes]) -> Frame {
    const MODULE_ERR: &str = "ERR Ferrite does not support Redis modules. Ferrite uses native Rust crates for extensibility. See https://ferrite.dev/docs/extensions for details.";

    match subcommand {
        "LIST" => Frame::array(vec![]),
        "LOAD" => {
            if args.is_empty() {
                return Frame::error("ERR wrong number of arguments for 'module|load' command");
            }
            Frame::error(MODULE_ERR)
        }
        "UNLOAD" => {
            if args.is_empty() {
                return Frame::error("ERR wrong number of arguments for 'module|unload' command");
            }
            Frame::error(MODULE_ERR)
        }
        "LOADEX" => {
            if args.is_empty() {
                return Frame::error("ERR wrong number of arguments for 'module|loadex' command");
            }
            Frame::error(MODULE_ERR)
        }
        "HELP" => Frame::array(vec![
            Frame::bulk("MODULE LIST"),
            Frame::bulk("    Return a list of loaded modules (always empty in Ferrite)."),
            Frame::bulk("MODULE LOAD <path> [arg ...]"),
            Frame::bulk("    Not supported. Ferrite uses native Rust crates for extensibility."),
            Frame::bulk("MODULE UNLOAD <name>"),
            Frame::bulk("    Not supported. Ferrite uses native Rust crates for extensibility."),
            Frame::bulk("MODULE LOADEX <path> [CONFIG name value ...] [ARGS arg ...]"),
            Frame::bulk("    Not supported. Ferrite uses native Rust crates for extensibility."),
        ]),
        _ => Frame::error(format!("ERR Unknown MODULE subcommand '{}'", subcommand)),
    }
}

/// Handle MONITOR command
pub fn monitor() -> Frame {
    Frame::simple("OK")
}

/// Handle RESET command
pub fn reset() -> Frame {
    Frame::simple("RESET")
}

/// Handle ROLE command
pub fn role() -> Frame {
    Frame::array(vec![
        Frame::bulk("master"),
        Frame::Integer(0),
        Frame::array(vec![]),
    ])
}

/// Handle BGSAVE command
pub fn bgsave(schedule: bool) -> Frame {
    if schedule {
        Frame::simple("Background saving scheduled")
    } else {
        Frame::simple("Background saving started")
    }
}

/// Handle SAVE command
pub fn save() -> Frame {
    Frame::simple("OK")
}

/// Handle BGREWRITEAOF command
pub fn bgrewriteaof() -> Frame {
    Frame::simple("Background append only file rewriting started")
}

/// Handle LASTSAVE command
pub fn lastsave() -> Frame {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    Frame::Integer(timestamp)
}

/// Handle HELLO command
pub fn hello(protocol_version: Option<u8>) -> Frame {
    let proto = protocol_version.unwrap_or(2);

    if proto != 2 && proto != 3 {
        return Frame::error("NOPROTO unsupported protocol version");
    }

    if proto == 2 {
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
        let mut info = HashMap::new();
        info.insert(Bytes::from_static(b"server"), Frame::bulk("ferrite"));
        info.insert(Bytes::from_static(b"version"), Frame::bulk("0.1.0"));
        info.insert(Bytes::from_static(b"proto"), Frame::Integer(proto as i64));
        info.insert(Bytes::from_static(b"id"), Frame::Integer(1));
        info.insert(Bytes::from_static(b"mode"), Frame::bulk("standalone"));
        info.insert(Bytes::from_static(b"role"), Frame::bulk("master"));
        info.insert(Bytes::from_static(b"modules"), Frame::array(vec![]));
        Frame::Map(info)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_debug_sleep_missing_arg() {
        let result = debug("SLEEP", vec![]);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_debug_segfault() {
        let result = debug("SEGFAULT", vec![]);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_debug_unknown() {
        let result = debug("UNKNOWN", vec![]);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_module_list() {
        let result = module("LIST", &[]);
        match result {
            Frame::Array(Some(arr)) => assert!(arr.is_empty()),
            Frame::Array(None) => {} // Also valid for empty
            _ => panic!("Expected empty array"),
        }
    }

    #[test]
    fn test_role() {
        let result = role();
        match result {
            Frame::Array(Some(arr)) => {
                assert_eq!(arr.len(), 3);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_hello_resp2() {
        let result = hello(Some(2));
        assert!(matches!(result, Frame::Array(_)));
    }

    #[test]
    fn test_hello_resp3() {
        let result = hello(Some(3));
        assert!(matches!(result, Frame::Map(_)));
    }

    #[test]
    fn test_hello_invalid_proto() {
        let result = hello(Some(4));
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_bgsave() {
        let result = bgsave(false);
        assert!(matches!(result, Frame::Simple(_)));
    }

    #[test]
    fn test_bgsave_schedule() {
        let result = bgsave(true);
        assert!(matches!(result, Frame::Simple(_)));
    }
}
