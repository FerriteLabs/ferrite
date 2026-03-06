//! Latency event tracking for the LATENCY command family
//!
//! Records the latest latency sample for each event category so that
//! `LATENCY LATEST` and `LATENCY HISTORY` return meaningful data.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

/// A single latency sample
#[derive(Debug, Clone)]
pub struct LatencySample {
    /// Unix timestamp (seconds) when the event was recorded
    pub timestamp: u64,
    /// Latency in milliseconds
    pub latency_ms: u64,
}

/// Latency event tracker — stores per-event history of latency spikes.
///
/// Only records events that exceed the configured threshold (default: 0ms,
/// meaning all events are recorded). The history is capped per event.
pub struct LatencyTracker {
    /// Per-event history: event_name → Vec<LatencySample>
    events: RwLock<HashMap<String, Vec<LatencySample>>>,
    /// Maximum history entries per event
    max_history: usize,
    /// Minimum latency (ms) to record. Events below this are ignored.
    threshold_ms: u64,
}

impl Default for LatencyTracker {
    fn default() -> Self {
        Self {
            events: RwLock::new(HashMap::new()),
            max_history: 160, // Redis default
            threshold_ms: 0,
        }
    }
}

impl LatencyTracker {
    /// Record a latency event if it exceeds the threshold.
    pub fn record(&self, event: &str, latency_ms: u64) {
        if latency_ms < self.threshold_ms {
            return;
        }

        let sample = LatencySample {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            latency_ms,
        };

        if let Ok(mut events) = self.events.write() {
            let history = events.entry(event.to_string()).or_default();
            history.push(sample);
            // Keep only the latest entries
            if history.len() > self.max_history {
                history.drain(..history.len() - self.max_history);
            }
        }
    }

    /// Get the latest sample for each event.
    /// Returns Vec<(event_name, timestamp, latest_latency_ms, max_latency_ms)>
    pub fn latest(&self) -> Vec<(String, u64, u64, u64)> {
        let events = match self.events.read() {
            Ok(e) => e,
            Err(_) => return vec![],
        };

        events
            .iter()
            .filter_map(|(name, history)| {
                let last = history.last()?;
                let max_ms = history.iter().map(|s| s.latency_ms).max().unwrap_or(0);
                Some((name.clone(), last.timestamp, last.latency_ms, max_ms))
            })
            .collect()
    }

    /// Get the history for a specific event.
    pub fn history(&self, event: &str) -> Vec<LatencySample> {
        let events = match self.events.read() {
            Ok(e) => e,
            Err(_) => return vec![],
        };

        events.get(event).cloned().unwrap_or_default()
    }

    /// Reset latency data. If events list is empty, reset all.
    /// Returns the number of events reset.
    pub fn reset(&self, event_names: &[String]) -> usize {
        if let Ok(mut events) = self.events.write() {
            if event_names.is_empty() {
                let count = events.len();
                events.clear();
                count
            } else {
                let mut count = 0;
                for name in event_names {
                    if events.remove(name).is_some() {
                        count += 1;
                    }
                }
                count
            }
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_and_latest() {
        let tracker = LatencyTracker::default();
        tracker.record("command", 5);
        tracker.record("command", 15);
        tracker.record("fork", 100);

        let latest = tracker.latest();
        assert_eq!(latest.len(), 2);

        let cmd = latest.iter().find(|(n, ..)| n == "command").unwrap();
        assert_eq!(cmd.2, 15); // latest latency
        assert_eq!(cmd.3, 15); // max latency

        let fork = latest.iter().find(|(n, ..)| n == "fork").unwrap();
        assert_eq!(fork.2, 100);
    }

    #[test]
    fn test_history() {
        let tracker = LatencyTracker::default();
        tracker.record("aof", 10);
        tracker.record("aof", 20);
        tracker.record("aof", 30);

        let history = tracker.history("aof");
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].latency_ms, 10);
        assert_eq!(history[2].latency_ms, 30);

        assert!(tracker.history("nonexistent").is_empty());
    }

    #[test]
    fn test_reset() {
        let tracker = LatencyTracker::default();
        tracker.record("command", 5);
        tracker.record("fork", 10);

        assert_eq!(tracker.reset(&["command".to_string()]), 1);
        assert_eq!(tracker.latest().len(), 1);

        assert_eq!(tracker.reset(&[]), 1); // reset all remaining
        assert!(tracker.latest().is_empty());
    }

    #[test]
    fn test_threshold_filtering() {
        let tracker = LatencyTracker {
            threshold_ms: 10,
            ..Default::default()
        };
        tracker.record("command", 5); // Below threshold — ignored
        tracker.record("command", 15); // Above threshold — recorded

        let history = tracker.history("command");
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].latency_ms, 15);
    }
}
