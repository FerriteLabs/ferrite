//! Persistent Slow Query Log
//!
//! A thread-safe, ring-buffer-based slow query log that captures commands
//! exceeding a configurable latency threshold. Oldest entries are automatically
//! evicted when the buffer reaches capacity.
//!
//! # Example
//!
//! ```ignore
//! use ferrite::observability::slow_query_log::SlowQueryLog;
//!
//! let log = SlowQueryLog::new(128, 10_000); // 128 entries, 10ms threshold
//! log.record("SET", 15_000, "127.0.0.1:6379", Some("user:*"));
//! assert_eq!(log.len(), 1);
//! ```

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

/// A single entry in the slow query log.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SlowQueryEntry {
    /// The command that was executed (e.g., "GET", "SET", "HGETALL").
    pub command: String,
    /// Execution duration in microseconds.
    pub duration_us: u64,
    /// Unix timestamp in microseconds when the command completed.
    pub timestamp: u64,
    /// Client address or identifier (e.g., "127.0.0.1:52431").
    pub client_info: String,
    /// Optional key pattern involved in the command.
    pub key_pattern: Option<String>,
}

/// A persistent, thread-safe slow query log backed by a ring buffer.
///
/// Queries whose duration exceeds the configured threshold are recorded.
/// When the log reaches its maximum capacity the oldest entries are evicted
/// first, preserving the most recent slow queries.
pub struct SlowQueryLog {
    /// Ring buffer of slow query entries.
    entries: RwLock<VecDeque<SlowQueryEntry>>,
    /// Maximum number of entries to retain.
    max_entries: usize,
    /// Duration threshold in microseconds – only queries slower than this
    /// value are recorded.
    threshold_us: u64,
}

impl SlowQueryLog {
    /// Create a new slow query log.
    ///
    /// # Arguments
    ///
    /// * `max_entries` – capacity of the ring buffer.
    /// * `threshold_us` – minimum duration in microseconds for a query to be
    ///   recorded.
    pub fn new(max_entries: usize, threshold_us: u64) -> Self {
        Self {
            entries: RwLock::new(VecDeque::with_capacity(max_entries)),
            max_entries,
            threshold_us,
        }
    }

    /// Record a slow query if its duration exceeds the threshold.
    ///
    /// Returns `true` if the entry was recorded, `false` if the duration was
    /// below the threshold.
    pub fn record(
        &self,
        command: &str,
        duration_us: u64,
        client_info: &str,
        key_pattern: Option<&str>,
    ) -> bool {
        if duration_us < self.threshold_us {
            return false;
        }

        let entry = SlowQueryEntry {
            command: command.to_string(),
            duration_us,
            timestamp: current_timestamp_us(),
            client_info: client_info.to_string(),
            key_pattern: key_pattern.map(|s| s.to_string()),
        };

        let mut entries = self.entries.write();
        if entries.len() >= self.max_entries {
            entries.pop_front();
        }
        entries.push_back(entry);
        true
    }

    /// Return a snapshot of all entries currently in the log.
    pub fn get_entries(&self) -> Vec<SlowQueryEntry> {
        let entries = self.entries.read();
        entries.iter().cloned().collect()
    }

    /// Clear all entries from the log.
    pub fn clear(&self) {
        let mut entries = self.entries.write();
        entries.clear();
    }

    /// Return the number of entries currently in the log.
    pub fn len(&self) -> usize {
        let entries = self.entries.read();
        entries.len()
    }

    /// Return `true` if the log contains no entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the configured slow-query threshold in microseconds.
    pub fn threshold(&self) -> u64 {
        self.threshold_us
    }
}

/// Get current timestamp in microseconds.
fn current_timestamp_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_above_threshold() {
        let log = SlowQueryLog::new(10, 1_000);
        assert!(log.record("GET", 5_000, "127.0.0.1:1234", Some("user:*")));
        assert_eq!(log.len(), 1);

        let entries = log.get_entries();
        assert_eq!(entries[0].command, "GET");
        assert_eq!(entries[0].duration_us, 5_000);
        assert_eq!(entries[0].client_info, "127.0.0.1:1234");
        assert_eq!(entries[0].key_pattern.as_deref(), Some("user:*"));
    }

    #[test]
    fn test_record_below_threshold() {
        let log = SlowQueryLog::new(10, 1_000);
        assert!(!log.record("GET", 500, "127.0.0.1:1234", None));
        assert_eq!(log.len(), 0);
    }

    #[test]
    fn test_ring_buffer_eviction() {
        let log = SlowQueryLog::new(3, 100);

        log.record("CMD1", 200, "c1", None);
        log.record("CMD2", 300, "c2", None);
        log.record("CMD3", 400, "c3", None);
        assert_eq!(log.len(), 3);

        // This should evict CMD1
        log.record("CMD4", 500, "c4", None);
        assert_eq!(log.len(), 3);

        let entries = log.get_entries();
        assert_eq!(entries[0].command, "CMD2");
        assert_eq!(entries[1].command, "CMD3");
        assert_eq!(entries[2].command, "CMD4");
    }

    #[test]
    fn test_clear() {
        let log = SlowQueryLog::new(10, 100);
        log.record("SET", 200, "c1", None);
        log.record("GET", 300, "c2", None);
        assert_eq!(log.len(), 2);

        log.clear();
        assert_eq!(log.len(), 0);
        assert!(log.is_empty());
    }

    #[test]
    fn test_threshold() {
        let log = SlowQueryLog::new(10, 5_000);
        assert_eq!(log.threshold(), 5_000);
    }

    #[test]
    fn test_is_empty() {
        let log = SlowQueryLog::new(10, 100);
        assert!(log.is_empty());

        log.record("SET", 200, "c1", None);
        assert!(!log.is_empty());
    }

    #[test]
    fn test_entry_timestamp_populated() {
        let log = SlowQueryLog::new(10, 100);
        log.record("HGETALL", 500, "c1", Some("hash:*"));

        let entries = log.get_entries();
        assert!(entries[0].timestamp > 0);
    }

    #[test]
    fn test_no_key_pattern() {
        let log = SlowQueryLog::new(10, 100);
        log.record("PING", 200, "c1", None);

        let entries = log.get_entries();
        assert!(entries[0].key_pattern.is_none());
    }

    #[test]
    fn test_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let log = Arc::new(SlowQueryLog::new(1000, 100));
        let mut handles = vec![];

        for i in 0..10 {
            let log = Arc::clone(&log);
            handles.push(thread::spawn(move || {
                for j in 0..100 {
                    log.record(&format!("CMD-{i}-{j}"), 200, &format!("client-{i}"), None);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(log.len(), 1000);
    }

    #[test]
    fn test_eviction_under_contention() {
        use std::sync::Arc;
        use std::thread;

        let log = Arc::new(SlowQueryLog::new(50, 100));
        let mut handles = vec![];

        for i in 0..10 {
            let log = Arc::clone(&log);
            handles.push(thread::spawn(move || {
                for j in 0..100 {
                    log.record(&format!("CMD-{i}-{j}"), 200, "c", None);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Should never exceed max_entries
        assert!(log.len() <= 50);
    }
}
