//! Windowing for stream processing

use std::collections::HashMap;
use std::time::Duration;

use super::StreamEvent;

/// Window types
#[derive(Debug, Clone)]
pub enum WindowType {
    /// Tumbling window (fixed, non-overlapping)
    Tumbling(Duration),
    /// Sliding window (fixed, overlapping)
    Sliding {
        /// Window size
        size: Duration,
        /// Slide interval
        slide: Duration,
    },
    /// Session window (dynamic, based on activity)
    Session {
        /// Gap between sessions
        gap: Duration,
    },
    /// Global window (all events in one window)
    Global,
    /// Count-based window
    Count(usize),
    /// Time-based window with custom boundaries
    Custom {
        /// Start time extractor
        start_fn: fn(u64) -> u64,
        /// End time extractor
        end_fn: fn(u64) -> u64,
    },
}

/// A time window
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Window {
    /// Window start time (inclusive)
    pub start: u64,
    /// Window end time (exclusive)
    pub end: u64,
}

impl Window {
    /// Create a new window
    pub fn new(start: u64, end: u64) -> Self {
        Self { start, end }
    }

    /// Check if a timestamp falls within this window
    pub fn contains(&self, timestamp: u64) -> bool {
        timestamp >= self.start && timestamp < self.end
    }

    /// Get window duration in milliseconds
    pub fn duration_ms(&self) -> u64 {
        self.end.saturating_sub(self.start)
    }

    /// Check if this window overlaps with another
    pub fn overlaps(&self, other: &Window) -> bool {
        self.start < other.end && other.start < self.end
    }

    /// Merge two overlapping windows
    pub fn merge(&self, other: &Window) -> Window {
        Window {
            start: self.start.min(other.start),
            end: self.end.max(other.end),
        }
    }
}

/// Window assigner assigns events to windows
pub trait WindowAssigner: Send + Sync {
    /// Assign an event to one or more windows
    fn assign_windows(&self, timestamp: u64) -> Vec<Window>;

    /// Get the maximum timestamp for any window that could contain this timestamp
    fn get_max_timestamp(&self, timestamp: u64) -> u64;

    /// Check if windows are mergeable (for session windows)
    fn is_mergeable(&self) -> bool {
        false
    }
}

/// Tumbling window assigner
pub struct TumblingWindowAssigner {
    size_ms: u64,
}

impl TumblingWindowAssigner {
    /// Create a new tumbling window assigner
    pub fn new(size: Duration) -> Self {
        Self {
            size_ms: size.as_millis() as u64,
        }
    }
}

impl WindowAssigner for TumblingWindowAssigner {
    fn assign_windows(&self, timestamp: u64) -> Vec<Window> {
        let start = (timestamp / self.size_ms) * self.size_ms;
        let end = start + self.size_ms;
        vec![Window::new(start, end)]
    }

    fn get_max_timestamp(&self, timestamp: u64) -> u64 {
        let start = (timestamp / self.size_ms) * self.size_ms;
        start + self.size_ms - 1
    }
}

/// Sliding window assigner
pub struct SlidingWindowAssigner {
    size_ms: u64,
    slide_ms: u64,
}

impl SlidingWindowAssigner {
    /// Create a new sliding window assigner
    pub fn new(size: Duration, slide: Duration) -> Self {
        Self {
            size_ms: size.as_millis() as u64,
            slide_ms: slide.as_millis() as u64,
        }
    }
}

impl WindowAssigner for SlidingWindowAssigner {
    fn assign_windows(&self, timestamp: u64) -> Vec<Window> {
        let mut windows = Vec::new();

        // Find all windows that contain this timestamp
        let last_start = (timestamp / self.slide_ms) * self.slide_ms;

        let mut start = if timestamp < self.size_ms {
            0
        } else {
            (last_start + self.slide_ms).saturating_sub(self.size_ms)
        };

        while start <= last_start {
            let end = start + self.size_ms;
            if timestamp >= start && timestamp < end {
                windows.push(Window::new(start, end));
            }
            start += self.slide_ms;
        }

        windows
    }

    fn get_max_timestamp(&self, timestamp: u64) -> u64 {
        let start = (timestamp / self.slide_ms) * self.slide_ms;
        start + self.size_ms - 1
    }
}

/// Session window assigner
pub struct SessionWindowAssigner {
    gap_ms: u64,
}

impl SessionWindowAssigner {
    /// Create a new session window assigner
    pub fn new(gap: Duration) -> Self {
        Self {
            gap_ms: gap.as_millis() as u64,
        }
    }
}

impl WindowAssigner for SessionWindowAssigner {
    fn assign_windows(&self, timestamp: u64) -> Vec<Window> {
        // Initial window covers just this event, extended by gap
        vec![Window::new(timestamp, timestamp + self.gap_ms)]
    }

    fn get_max_timestamp(&self, timestamp: u64) -> u64 {
        timestamp + self.gap_ms - 1
    }

    fn is_mergeable(&self) -> bool {
        true
    }
}

/// Global window assigner
pub struct GlobalWindowAssigner;

impl WindowAssigner for GlobalWindowAssigner {
    fn assign_windows(&self, _timestamp: u64) -> Vec<Window> {
        vec![Window::new(0, u64::MAX)]
    }

    fn get_max_timestamp(&self, _timestamp: u64) -> u64 {
        u64::MAX
    }
}

/// Window function for aggregating window contents
pub trait WindowFunction<T>: Send + Sync {
    /// Process all events in a window
    fn process(&self, key: &str, window: &Window, events: &[StreamEvent]) -> T;
}

/// Simple aggregating window function
pub struct AggregateWindowFunction<S, A, O>
where
    S: Default + Clone,
    A: Fn(&S, &StreamEvent) -> S,
    O: Fn(&str, &Window, &S) -> StreamEvent,
{
    aggregator: A,
    output_fn: O,
    _marker: std::marker::PhantomData<S>,
}

impl<S, A, O> AggregateWindowFunction<S, A, O>
where
    S: Default + Clone,
    A: Fn(&S, &StreamEvent) -> S,
    O: Fn(&str, &Window, &S) -> StreamEvent,
{
    /// Create a new aggregate window function
    pub fn new(aggregator: A, output_fn: O) -> Self {
        Self {
            aggregator,
            output_fn,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<S, A, O> WindowFunction<StreamEvent> for AggregateWindowFunction<S, A, O>
where
    S: Default + Clone + Send + Sync,
    A: Fn(&S, &StreamEvent) -> S + Send + Sync,
    O: Fn(&str, &Window, &S) -> StreamEvent + Send + Sync,
{
    fn process(&self, key: &str, window: &Window, events: &[StreamEvent]) -> StreamEvent {
        let mut state = S::default();
        for event in events {
            state = (self.aggregator)(&state, event);
        }
        (self.output_fn)(key, window, &state)
    }
}

/// Window state for managing window contents
pub struct WindowState {
    /// Events by window by key
    events: HashMap<String, HashMap<Window, Vec<StreamEvent>>>,
    /// Window assigner
    assigner: Box<dyn WindowAssigner>,
    /// Allowed lateness in ms
    allowed_lateness_ms: u64,
    /// Current watermark
    watermark: u64,
}

impl WindowState {
    /// Create a new window state
    pub fn new(assigner: Box<dyn WindowAssigner>) -> Self {
        Self {
            events: HashMap::new(),
            assigner,
            allowed_lateness_ms: 0,
            watermark: 0,
        }
    }

    /// Set allowed lateness
    pub fn with_allowed_lateness(mut self, lateness: Duration) -> Self {
        self.allowed_lateness_ms = lateness.as_millis() as u64;
        self
    }

    /// Add an event to windows
    pub fn add(&mut self, event: StreamEvent) -> Vec<Window> {
        let key = event.key.clone().unwrap_or_else(|| "default".to_string());
        let windows = self.assigner.assign_windows(event.timestamp);

        let key_events = self.events.entry(key).or_default();

        for window in &windows {
            // Check if event is late
            if event.timestamp < self.watermark.saturating_sub(self.allowed_lateness_ms) {
                continue; // Drop late event
            }

            key_events
                .entry(window.clone())
                .or_default()
                .push(event.clone());
        }

        // Merge windows if needed (for session windows)
        if self.assigner.is_mergeable() {
            self.merge_windows(&event.key.clone().unwrap_or_else(|| "default".to_string()));
        }

        windows
    }

    /// Merge overlapping windows (for session windows)
    fn merge_windows(&mut self, key: &str) {
        if let Some(key_events) = self.events.get_mut(key) {
            let mut windows: Vec<Window> = key_events.keys().cloned().collect();
            windows.sort_by_key(|w| w.start);

            let mut merged = Vec::new();
            let mut current: Option<Window> = None;

            for window in windows {
                if let Some(ref mut curr) = current {
                    if curr.overlaps(&window) || curr.end == window.start {
                        *curr = curr.merge(&window);
                    } else {
                        merged.push(curr.clone());
                        current = Some(window);
                    }
                } else {
                    current = Some(window);
                }
            }

            if let Some(curr) = current {
                merged.push(curr);
            }

            // Rebuild events map with merged windows
            let old_events = std::mem::take(key_events);
            for (window, events) in old_events {
                for merged_window in &merged {
                    if merged_window.contains(window.start)
                        || merged_window.contains(window.end - 1)
                    {
                        key_events
                            .entry(merged_window.clone())
                            .or_insert_with(Vec::new)
                            .extend(events.clone());
                        break;
                    }
                }
            }
        }
    }

    /// Update watermark and trigger windows
    pub fn advance_watermark(&mut self, watermark: u64) -> Vec<(String, Window, Vec<StreamEvent>)> {
        self.watermark = watermark;
        let mut triggered = Vec::new();

        for (key, key_events) in &mut self.events {
            let expired: Vec<Window> = key_events
                .keys()
                .filter(|w| w.end + self.allowed_lateness_ms <= watermark)
                .cloned()
                .collect();

            for window in expired {
                if let Some(events) = key_events.remove(&window) {
                    triggered.push((key.clone(), window, events));
                }
            }
        }

        triggered
    }

    /// Get events for a specific window
    pub fn get_window(&self, key: &str, window: &Window) -> Option<&Vec<StreamEvent>> {
        self.events.get(key).and_then(|ke| ke.get(window))
    }

    /// Clear all windows for a key
    pub fn clear_key(&mut self, key: &str) {
        self.events.remove(key);
    }

    /// Clear all windows
    pub fn clear(&mut self) {
        self.events.clear();
    }
}

/// Trigger types for windows
#[derive(Debug, Clone)]
pub enum Trigger {
    /// Trigger when watermark passes window end
    EventTime,
    /// Trigger at regular processing time intervals
    ProcessingTime(Duration),
    /// Trigger after N elements
    Count(usize),
    /// Trigger continuously on each element
    Continuous,
    /// Custom trigger
    Custom(String),
}

/// Window accumulation mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccumulationMode {
    /// Discard previous results (only emit differences)
    Discarding,
    /// Accumulate results (emit running totals)
    Accumulating,
    /// Accumulate and retract (emit retractions for previous results)
    AccumulatingAndRetracting,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_tumbling_window_assigner() {
        let assigner = TumblingWindowAssigner::new(Duration::from_secs(60));

        let windows = assigner.assign_windows(30_000); // 30 seconds
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].start, 0);
        assert_eq!(windows[0].end, 60_000);

        let windows = assigner.assign_windows(90_000); // 90 seconds
        assert_eq!(windows[0].start, 60_000);
        assert_eq!(windows[0].end, 120_000);
    }

    #[test]
    fn test_sliding_window_assigner() {
        let assigner = SlidingWindowAssigner::new(Duration::from_secs(60), Duration::from_secs(30));

        let windows = assigner.assign_windows(45_000); // 45 seconds
                                                       // Should be in windows [0-60] and [30-90]
        assert_eq!(windows.len(), 2);
    }

    #[test]
    fn test_session_window_assigner() {
        let assigner = SessionWindowAssigner::new(Duration::from_secs(30));

        let windows = assigner.assign_windows(1000);
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].start, 1000);
        assert_eq!(windows[0].end, 31_000);

        assert!(assigner.is_mergeable());
    }

    #[test]
    fn test_window_state() {
        let assigner = Box::new(TumblingWindowAssigner::new(Duration::from_secs(60)));
        let mut state = WindowState::new(assigner);

        let event1 =
            StreamEvent::with_timestamp(Some("key1".to_string()), json!({ "value": 1 }), 30_000);
        let event2 =
            StreamEvent::with_timestamp(Some("key1".to_string()), json!({ "value": 2 }), 45_000);

        state.add(event1);
        state.add(event2);

        let window = Window::new(0, 60_000);
        let events = state.get_window("key1", &window).unwrap();
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn test_window_trigger() {
        let assigner = Box::new(TumblingWindowAssigner::new(Duration::from_secs(60)));
        let mut state = WindowState::new(assigner);

        let event =
            StreamEvent::with_timestamp(Some("key1".to_string()), json!({ "value": 1 }), 30_000);
        state.add(event);

        // Advance watermark past window end
        let triggered = state.advance_watermark(60_001);
        assert_eq!(triggered.len(), 1);
        assert_eq!(triggered[0].0, "key1");
    }

    #[test]
    fn test_session_window_merge() {
        let assigner = Box::new(SessionWindowAssigner::new(Duration::from_millis(100)));
        let mut state = WindowState::new(assigner);

        // Add events close together (should merge)
        let event1 = StreamEvent::with_timestamp(Some("key1".to_string()), json!({ "v": 1 }), 0);
        let event2 = StreamEvent::with_timestamp(Some("key1".to_string()), json!({ "v": 2 }), 50);

        state.add(event1);
        state.add(event2);

        // Should have merged into one session
        let key_events = state.events.get("key1").unwrap();
        assert_eq!(key_events.len(), 1);
    }
}
