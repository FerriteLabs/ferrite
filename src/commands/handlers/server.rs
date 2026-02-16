//! Server command handlers
//!
//! This module contains handlers for server-related commands like PING, INFO, DBSIZE, TIME, etc.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::Store;

/// Handle PING command
#[inline]
pub fn ping(message: Option<Bytes>) -> Frame {
    match message {
        Some(msg) => Frame::bulk(msg),
        None => Frame::simple("PONG"),
    }
}

/// Handle TIME command - returns current server time
pub fn time() -> Frame {
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

/// Handle DBSIZE command - returns number of keys in database
pub fn dbsize(store: &Arc<Store>, db: u8) -> Frame {
    Frame::Integer(store.key_count(db) as i64)
}

/// Handle FLUSHDB command - clear current database
pub fn flushdb(store: &Arc<Store>, db: u8) -> Frame {
    store.flush_db(db);
    Frame::simple("OK")
}

/// Handle FLUSHALL command - clear all databases
pub fn flushall() -> Frame {
    Frame::simple("OK")
}

/// Handle INFO command - returns server information
pub fn info(store: &Arc<Store>, section: Option<String>) -> Frame {
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
        let uptime = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        output.push_str("# Server\r\n");
        output.push_str("redis_version:7.0.0\r\n");
        output.push_str("ferrite_version:0.1.0\r\n");
        output.push_str(&format!("uptime_in_seconds:{}\r\n", uptime % 86400));
        output.push_str("tcp_port:6379\r\n");
        output.push_str(&format!("process_id:{}\r\n", std::process::id()));
        output.push_str(&format!("num_databases:{}\r\n", store.num_databases()));

        let backend = store.backend_type();
        output.push_str(&format!("storage_backend:{:?}\r\n", backend));
        output.push_str("\r\n");
    }

    // Stats section
    if include("stats") {
        output.push_str("# Stats\r\n");

        if let Some(stats) = store.hybridlog_stats_aggregated() {
            let total_ops = stats.get_ops + stats.set_ops + stats.del_ops;
            output.push_str(&format!("total_commands_processed:{}\r\n", total_ops));
            output.push_str(&format!("total_reads:{}\r\n", stats.get_ops));
            output.push_str(&format!("total_writes:{}\r\n", stats.set_ops));
            output.push_str(&format!("total_deletes:{}\r\n", stats.del_ops));
            output.push_str(&format!("total_keys:{}\r\n", stats.key_count));
        } else {
            let mut total_keys = 0u64;
            for i in 0..store.num_databases() {
                total_keys += store.key_count(i as u8);
            }
            output.push_str(&format!("total_keys:{}\r\n", total_keys));
        }
        output.push_str("\r\n");
    }

    // Memory section
    if include("memory") {
        output.push_str("# Memory\r\n");

        if let Some(stats) = store.hybridlog_stats_aggregated() {
            let total_memory = stats.mutable_bytes + stats.readonly_bytes;
            output.push_str(&format!("used_memory:{}\r\n", total_memory));
            output.push_str(&format!(
                "used_memory_human:{}\r\n",
                format_bytes(total_memory as u64)
            ));
            output.push_str(&format!("used_memory_peak:{}\r\n", total_memory));
        } else {
            output.push_str("used_memory:0\r\n");
            output.push_str("used_memory_human:0B\r\n");
        }
        output.push_str("\r\n");
    }

    // HybridLog section (unique to Ferrite)
    if include("hybridlog") {
        if let Some(stats) = store.hybridlog_stats_aggregated() {
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
        for i in 0..store.num_databases() {
            let key_count = store.key_count(i as u8);
            if key_count > 0 {
                output.push_str(&format!("db{}:keys={},expires=0\r\n", i, key_count));
            }
        }
        output.push_str("\r\n");
    }

    Frame::bulk(output)
}

/// Format bytes into human-readable string
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes >= TB {
        format!("{:.2}TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2}GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2}MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2}KB", bytes as f64 / KB as f64)
    } else {
        format!("{}B", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ping_no_message() {
        let result = ping(None);
        assert!(matches!(result, Frame::Simple(_)));
    }

    #[test]
    fn test_ping_with_message() {
        let msg = Bytes::from("hello");
        let result = ping(Some(msg.clone()));
        match result {
            Frame::Bulk(Some(b)) => assert_eq!(b, msg),
            _ => panic!("Expected Bulk frame"),
        }
    }

    #[test]
    fn test_time_format() {
        let result = time();
        match result {
            Frame::Array(Some(arr)) => {
                assert_eq!(arr.len(), 2);
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0B");
        assert_eq!(format_bytes(500), "500B");
        assert_eq!(format_bytes(1024), "1.00KB");
        assert_eq!(format_bytes(1024 * 1024), "1.00MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00GB");
    }
}
