//! Slow query log for debugging slow commands
//!
//! The slow log tracks commands that exceed a configurable execution time threshold,
//! helping identify performance issues and expensive operations.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;

/// A single slow log entry
#[derive(Debug, Clone)]
pub struct SlowLogEntry {
    /// Unique ID for this entry
    pub id: u64,
    /// Unix timestamp when the command was executed
    pub timestamp: u64,
    /// Duration of the command execution in microseconds
    pub duration_us: u64,
    /// The command and its arguments
    pub args: Vec<Bytes>,
    /// Client address (if available)
    pub client_addr: String,
    /// Client name (if set via CLIENT SETNAME)
    pub client_name: String,
}

/// The slow log - a circular buffer of slow query entries
pub struct SlowLog {
    /// The log entries (newest first)
    entries: RwLock<VecDeque<SlowLogEntry>>,
    /// Maximum number of entries to keep
    max_len: usize,
    /// Threshold in microseconds - queries slower than this are logged
    threshold_us: u64,
    /// Next entry ID
    next_id: AtomicU64,
}

impl SlowLog {
    /// Create a new slow log with the given configuration
    ///
    /// # Arguments
    /// * `max_len` - Maximum number of entries to keep (default 128)
    /// * `threshold_us` - Minimum execution time in microseconds to log (default 10000 = 10ms)
    pub fn new(max_len: usize, threshold_us: u64) -> Self {
        Self {
            entries: RwLock::new(VecDeque::with_capacity(max_len)),
            max_len,
            threshold_us,
            next_id: AtomicU64::new(1),
        }
    }

    /// Create a slow log with default settings (128 entries, 10ms threshold)
    pub fn default_config() -> Self {
        Self::new(128, 10_000)
    }

    /// Record a command if it exceeded the slow log threshold
    ///
    /// # Arguments
    /// * `duration` - How long the command took to execute
    /// * `args` - The command and its arguments
    /// * `client_addr` - Client address (e.g., "192.168.1.1:12345")
    /// * `client_name` - Client name if set via CLIENT SETNAME
    ///
    /// # Returns
    /// `true` if the command was logged, `false` if it was below threshold
    pub fn record(
        &self,
        duration: Duration,
        args: Vec<Bytes>,
        client_addr: String,
        client_name: String,
    ) -> bool {
        let duration_us = duration.as_micros() as u64;

        // Check if this command exceeded the threshold
        if duration_us < self.threshold_us {
            return false;
        }

        let entry = SlowLogEntry {
            id: self.next_id.fetch_add(1, Ordering::Relaxed),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            duration_us,
            args,
            client_addr,
            client_name,
        };

        let mut entries = self.entries.write().unwrap_or_else(|e| e.into_inner());

        // Add to front (newest first)
        entries.push_front(entry);

        // Trim to max length
        while entries.len() > self.max_len {
            entries.pop_back();
        }

        true
    }

    /// Start timing a command execution
    pub fn start_timer(&self) -> Instant {
        Instant::now()
    }

    /// Get the most recent slow log entries
    ///
    /// # Arguments
    /// * `count` - Maximum number of entries to return (None for all)
    pub fn get(&self, count: Option<usize>) -> Vec<SlowLogEntry> {
        let entries = self.entries.read().unwrap_or_else(|e| e.into_inner());
        let limit = count.unwrap_or(entries.len()).min(entries.len());
        entries.iter().take(limit).cloned().collect()
    }

    /// Get the number of entries in the slow log
    pub fn len(&self) -> usize {
        self.entries.read().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Check if the slow log is empty
    pub fn is_empty(&self) -> bool {
        self.entries.read().unwrap_or_else(|e| e.into_inner()).is_empty()
    }

    /// Clear all entries from the slow log
    pub fn reset(&self) {
        self.entries.write().unwrap_or_else(|e| e.into_inner()).clear();
    }

    /// Get the current threshold in microseconds
    pub fn threshold_us(&self) -> u64 {
        self.threshold_us
    }

    /// Get the maximum length
    pub fn max_len(&self) -> usize {
        self.max_len
    }
}

impl Default for SlowLog {
    fn default() -> Self {
        Self::default_config()
    }
}

/// Shared slow log type for use across threads
pub type SharedSlowLog = std::sync::Arc<SlowLog>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slowlog_below_threshold() {
        let log = SlowLog::new(10, 10_000); // 10ms threshold

        // Record a fast command (1ms)
        let recorded = log.record(
            Duration::from_millis(1),
            vec![Bytes::from("GET"), Bytes::from("key")],
            "127.0.0.1:12345".to_string(),
            String::new(),
        );

        assert!(!recorded);
        assert_eq!(log.len(), 0);
    }

    #[test]
    fn test_slowlog_above_threshold() {
        let log = SlowLog::new(10, 10_000); // 10ms threshold

        // Record a slow command (15ms)
        let recorded = log.record(
            Duration::from_millis(15),
            vec![Bytes::from("KEYS"), Bytes::from("*")],
            "127.0.0.1:12345".to_string(),
            "test-client".to_string(),
        );

        assert!(recorded);
        assert_eq!(log.len(), 1);

        let entries = log.get(None);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].duration_us, 15_000);
        assert_eq!(entries[0].client_addr, "127.0.0.1:12345");
        assert_eq!(entries[0].client_name, "test-client");
    }

    #[test]
    fn test_slowlog_max_len() {
        let log = SlowLog::new(3, 1_000); // 3 entries max, 1ms threshold

        // Record 5 slow commands
        for i in 0..5 {
            log.record(
                Duration::from_millis(2 + i),
                vec![Bytes::from(format!("CMD{}", i))],
                "127.0.0.1:12345".to_string(),
                String::new(),
            );
        }

        // Should only have 3 entries
        assert_eq!(log.len(), 3);

        // Should have the newest 3 (CMD2, CMD3, CMD4)
        let entries = log.get(None);
        assert_eq!(entries.len(), 3);
        // Newest first
        assert_eq!(entries[0].id, 5);
        assert_eq!(entries[1].id, 4);
        assert_eq!(entries[2].id, 3);
    }

    #[test]
    fn test_slowlog_reset() {
        let log = SlowLog::new(10, 1_000);

        log.record(
            Duration::from_millis(2),
            vec![Bytes::from("GET")],
            "127.0.0.1:12345".to_string(),
            String::new(),
        );

        assert_eq!(log.len(), 1);
        log.reset();
        assert_eq!(log.len(), 0);
    }

    #[test]
    fn test_slowlog_get_with_count() {
        let log = SlowLog::new(10, 1_000);

        for _ in 0..5 {
            log.record(
                Duration::from_millis(2),
                vec![Bytes::from("CMD")],
                "127.0.0.1:12345".to_string(),
                String::new(),
            );
        }

        // Get with limit
        let entries = log.get(Some(2));
        assert_eq!(entries.len(), 2);

        // Get all
        let entries = log.get(None);
        assert_eq!(entries.len(), 5);
    }
}
