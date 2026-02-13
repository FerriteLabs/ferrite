//! Windowing support for streaming pipelines.

use std::collections::HashMap;
use std::time::Duration;

/// A window instance containing accumulated events.
#[derive(Debug, Clone)]
pub struct WindowInstance {
    /// Window start time (epoch millis).
    pub start_ms: u64,
    /// Window end time (epoch millis).
    pub end_ms: u64,
    /// Number of events in the window.
    pub event_count: u64,
    /// Accumulated state (key → aggregate value).
    pub state: HashMap<String, f64>,
}

impl WindowInstance {
    pub fn new(start_ms: u64, end_ms: u64) -> Self {
        Self {
            start_ms,
            end_ms,
            event_count: 0,
            state: HashMap::new(),
        }
    }

    /// Adds a value for a key to the window state.
    pub fn accumulate(&mut self, key: &str, value: f64) {
        let entry = self.state.entry(key.to_string()).or_insert(0.0);
        *entry += value;
        self.event_count += 1;
    }

    /// Returns the window duration.
    pub fn duration(&self) -> Duration {
        Duration::from_millis(self.end_ms - self.start_ms)
    }

    /// Returns whether the window has expired given a current timestamp.
    pub fn is_expired(&self, current_ms: u64) -> bool {
        current_ms >= self.end_ms
    }
}

/// Assigns events to tumbling windows.
pub fn assign_tumbling_window(timestamp_ms: u64, window_size: Duration) -> (u64, u64) {
    let size_ms = window_size.as_millis() as u64;
    let start = (timestamp_ms / size_ms) * size_ms;
    (start, start + size_ms)
}

/// Assigns events to sliding windows.
pub fn assign_sliding_windows(
    timestamp_ms: u64,
    window_size: Duration,
    slide: Duration,
) -> Vec<(u64, u64)> {
    let size_ms = window_size.as_millis() as u64;
    let slide_ms = slide.as_millis() as u64;

    if slide_ms == 0 {
        return vec![];
    }

    let mut windows = Vec::new();
    let earliest_start = timestamp_ms.saturating_sub(size_ms - slide_ms);
    let aligned_start = (earliest_start / slide_ms) * slide_ms;

    let mut start = aligned_start;
    while start <= timestamp_ms {
        let end = start + size_ms;
        if timestamp_ms >= start && timestamp_ms < end {
            windows.push((start, end));
        }
        start += slide_ms;
    }

    windows
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tumbling_window_assignment() {
        let (start, end) = assign_tumbling_window(125_000, Duration::from_secs(60));
        assert_eq!(start, 120_000);
        assert_eq!(end, 180_000);
    }

    #[test]
    fn test_sliding_window_assignment() {
        let windows =
            assign_sliding_windows(125_000, Duration::from_secs(60), Duration::from_secs(30));
        assert!(!windows.is_empty());
        // Each window should contain the event
        for (start, end) in &windows {
            assert!(125_000 >= *start && 125_000 < *end);
        }
    }

    #[test]
    fn test_window_instance() {
        let mut window = WindowInstance::new(0, 60_000);
        window.accumulate("clicks", 1.0);
        window.accumulate("clicks", 1.0);
        window.accumulate("views", 5.0);

        assert_eq!(window.event_count, 3);
        assert_eq!(window.state["clicks"], 2.0);
        assert_eq!(window.state["views"], 5.0);
        assert_eq!(window.duration(), Duration::from_secs(60));
    }

    #[test]
    fn test_window_expiry() {
        let window = WindowInstance::new(0, 60_000);
        assert!(!window.is_expired(30_000));
        assert!(window.is_expired(60_000));
        assert!(window.is_expired(90_000));
    }
}
