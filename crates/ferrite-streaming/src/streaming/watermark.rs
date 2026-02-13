//! Watermarks for event-time processing

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Watermark for event-time tracking
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Watermark {
    /// Watermark timestamp
    timestamp: u64,
}

impl Watermark {
    /// Create a new watermark
    pub fn new(timestamp: u64) -> Self {
        Self { timestamp }
    }

    /// Get the timestamp
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    /// Create the minimum watermark
    pub fn min() -> Self {
        Self { timestamp: 0 }
    }

    /// Create the maximum watermark
    pub fn max() -> Self {
        Self {
            timestamp: u64::MAX,
        }
    }

    /// Check if this watermark is before another
    pub fn is_before(&self, other: &Watermark) -> bool {
        self.timestamp < other.timestamp
    }

    /// Advance the watermark
    pub fn advance(&mut self, timestamp: u64) {
        if timestamp > self.timestamp {
            self.timestamp = timestamp;
        }
    }
}

impl Default for Watermark {
    fn default() -> Self {
        Self::min()
    }
}

/// Watermark strategy
#[derive(Debug, Clone)]
pub enum WatermarkStrategy {
    /// Bounded out-of-orderness
    BoundedOutOfOrderness {
        /// Maximum allowed lateness
        max_lateness: Duration,
    },
    /// Ascending timestamps (no lateness)
    AscendingTimestamps,
    /// Ingestion time (processing time as event time)
    IngestionTime,
    /// No watermarks
    NoWatermarks,
    /// Periodic watermark with fixed interval
    Periodic {
        /// Interval between watermarks
        interval: Duration,
    },
    /// Punctuated watermarks (watermarks in the data)
    Punctuated,
}

impl Default for WatermarkStrategy {
    fn default() -> Self {
        Self::BoundedOutOfOrderness {
            max_lateness: Duration::from_secs(1),
        }
    }
}

/// Watermark generator
pub struct WatermarkGenerator {
    /// Strategy for generating watermarks
    strategy: WatermarkStrategy,
    /// Current watermark
    current_watermark: Watermark,
    /// Maximum observed timestamp
    max_timestamp: u64,
    /// Last emitted watermark time
    last_emit_time: u64,
}

impl WatermarkGenerator {
    /// Create a new watermark generator
    pub fn new(strategy: WatermarkStrategy) -> Self {
        Self {
            strategy,
            current_watermark: Watermark::min(),
            max_timestamp: 0,
            last_emit_time: 0,
        }
    }

    /// Create with bounded out-of-orderness
    pub fn for_bounded_out_of_orderness(max_lateness: Duration) -> Self {
        Self::new(WatermarkStrategy::BoundedOutOfOrderness { max_lateness })
    }

    /// Create for ascending timestamps
    pub fn for_ascending_timestamps() -> Self {
        Self::new(WatermarkStrategy::AscendingTimestamps)
    }

    /// Create for ingestion time
    pub fn for_ingestion_time() -> Self {
        Self::new(WatermarkStrategy::IngestionTime)
    }

    /// Create for periodic watermarks
    pub fn for_periodic(interval: Duration) -> Self {
        Self::new(WatermarkStrategy::Periodic { interval })
    }

    /// Get current watermark
    pub fn current_watermark(&self) -> Watermark {
        self.current_watermark
    }

    /// Update with a new event timestamp
    pub fn on_event(&mut self, timestamp: u64) -> Option<Watermark> {
        // Update max timestamp
        if timestamp > self.max_timestamp {
            self.max_timestamp = timestamp;
        }

        match &self.strategy {
            WatermarkStrategy::BoundedOutOfOrderness { max_lateness } => {
                let lateness_ms = max_lateness.as_millis() as u64;
                let new_watermark = self.max_timestamp.saturating_sub(lateness_ms);

                if new_watermark > self.current_watermark.timestamp {
                    self.current_watermark = Watermark::new(new_watermark);
                    Some(self.current_watermark)
                } else {
                    None
                }
            }
            WatermarkStrategy::AscendingTimestamps => {
                if timestamp > self.current_watermark.timestamp {
                    self.current_watermark = Watermark::new(timestamp);
                    Some(self.current_watermark)
                } else {
                    None
                }
            }
            WatermarkStrategy::IngestionTime => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;

                if now > self.current_watermark.timestamp {
                    self.current_watermark = Watermark::new(now);
                    Some(self.current_watermark)
                } else {
                    None
                }
            }
            WatermarkStrategy::NoWatermarks => None,
            WatermarkStrategy::Periodic { interval } => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;

                let interval_ms = interval.as_millis() as u64;
                if now - self.last_emit_time >= interval_ms {
                    self.last_emit_time = now;
                    let new_watermark = self.max_timestamp;
                    if new_watermark > self.current_watermark.timestamp {
                        self.current_watermark = Watermark::new(new_watermark);
                        Some(self.current_watermark)
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            WatermarkStrategy::Punctuated => {
                // Punctuated watermarks are handled by on_watermark
                None
            }
        }
    }

    /// Handle explicit watermark (for punctuated strategy)
    pub fn on_watermark(&mut self, watermark: Watermark) -> Option<Watermark> {
        if watermark.timestamp > self.current_watermark.timestamp {
            self.current_watermark = watermark;
            Some(watermark)
        } else {
            None
        }
    }

    /// Force emit watermark
    pub fn emit_watermark(&mut self) -> Watermark {
        match &self.strategy {
            WatermarkStrategy::BoundedOutOfOrderness { max_lateness } => {
                let lateness_ms = max_lateness.as_millis() as u64;
                let new_watermark = self.max_timestamp.saturating_sub(lateness_ms);
                self.current_watermark = Watermark::new(new_watermark);
            }
            _ => {
                self.current_watermark = Watermark::new(self.max_timestamp);
            }
        }
        self.current_watermark
    }

    /// Check if event is late
    pub fn is_late(&self, timestamp: u64) -> bool {
        timestamp < self.current_watermark.timestamp
    }
}

/// Watermark tracker for multiple sources
pub struct WatermarkTracker {
    /// Watermarks by source
    source_watermarks: Arc<RwLock<HashMap<String, Watermark>>>,
    /// Combined watermark (minimum of all sources)
    combined_watermark: Arc<RwLock<Watermark>>,
    /// Idle timeout for sources
    idle_timeout: Duration,
    /// Last activity time by source
    last_activity: Arc<RwLock<HashMap<String, u64>>>,
}

impl WatermarkTracker {
    /// Create a new watermark tracker
    pub fn new() -> Self {
        Self {
            source_watermarks: Arc::new(RwLock::new(HashMap::new())),
            combined_watermark: Arc::new(RwLock::new(Watermark::min())),
            idle_timeout: Duration::from_secs(60),
            last_activity: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create with idle timeout
    pub fn with_idle_timeout(idle_timeout: Duration) -> Self {
        Self {
            source_watermarks: Arc::new(RwLock::new(HashMap::new())),
            combined_watermark: Arc::new(RwLock::new(Watermark::min())),
            idle_timeout,
            last_activity: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Update watermark for a source
    pub async fn update_watermark(&self, source: &str, watermark: Watermark) -> Option<Watermark> {
        let mut source_watermarks = self.source_watermarks.write().await;
        let mut last_activity = self.last_activity.write().await;

        source_watermarks.insert(source.to_string(), watermark);
        last_activity.insert(
            source.to_string(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        );

        // Calculate combined watermark (minimum of all active sources)
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let idle_timeout_ms = self.idle_timeout.as_millis() as u64;

        let mut min_watermark = Watermark::max();
        for (src, wm) in source_watermarks.iter() {
            let last = last_activity.get(src).copied().unwrap_or(0);
            if now - last < idle_timeout_ms && *wm < min_watermark {
                min_watermark = *wm;
            }
        }

        if min_watermark == Watermark::max() {
            return None;
        }

        let mut combined = self.combined_watermark.write().await;
        if min_watermark.timestamp > combined.timestamp {
            *combined = min_watermark;
            Some(min_watermark)
        } else {
            None
        }
    }

    /// Get combined watermark
    pub async fn get_watermark(&self) -> Watermark {
        *self.combined_watermark.read().await
    }

    /// Get watermark for specific source
    pub async fn get_source_watermark(&self, source: &str) -> Option<Watermark> {
        let source_watermarks = self.source_watermarks.read().await;
        source_watermarks.get(source).copied()
    }

    /// Mark source as idle
    pub async fn mark_idle(&self, source: &str) {
        let mut source_watermarks = self.source_watermarks.write().await;
        source_watermarks.remove(source);

        let mut last_activity = self.last_activity.write().await;
        last_activity.remove(source);
    }

    /// Get all active sources
    pub async fn get_active_sources(&self) -> Vec<String> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let idle_timeout_ms = self.idle_timeout.as_millis() as u64;

        let last_activity = self.last_activity.read().await;
        last_activity
            .iter()
            .filter(|(_, &last)| now - last < idle_timeout_ms)
            .map(|(src, _)| src.clone())
            .collect()
    }
}

impl Default for WatermarkTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Late data handler
#[derive(Debug, Clone)]
pub enum LateDataPolicy {
    /// Drop late events
    Drop,
    /// Allow late events up to a certain lateness
    AllowedLateness(Duration),
    /// Emit late events to side output
    SideOutput,
    /// Update windows with late events (retractions)
    UpdateAndRetract,
}

impl Default for LateDataPolicy {
    fn default() -> Self {
        Self::AllowedLateness(Duration::from_secs(0))
    }
}

/// Late data handler
pub struct LateDataHandler {
    /// Policy for handling late data
    policy: LateDataPolicy,
    /// Count of late events
    late_count: Arc<RwLock<u64>>,
    /// Side output for late events
    side_output: Arc<RwLock<Vec<LateEvent>>>,
}

/// Late event information
#[derive(Debug, Clone)]
pub struct LateEvent {
    /// Original event timestamp
    pub event_timestamp: u64,
    /// Watermark at the time
    pub watermark: u64,
    /// How late the event was
    pub lateness_ms: u64,
    /// Event data
    pub data: serde_json::Value,
}

impl LateDataHandler {
    /// Create a new late data handler
    pub fn new(policy: LateDataPolicy) -> Self {
        Self {
            policy,
            late_count: Arc::new(RwLock::new(0)),
            side_output: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Handle a potentially late event
    pub async fn handle(
        &self,
        event_timestamp: u64,
        watermark: u64,
        data: serde_json::Value,
    ) -> LateDataDecision {
        if event_timestamp >= watermark {
            return LateDataDecision::NotLate;
        }

        let lateness = watermark - event_timestamp;

        // Increment late count
        {
            let mut count = self.late_count.write().await;
            *count += 1;
        }

        match &self.policy {
            LateDataPolicy::Drop => LateDataDecision::Dropped,
            LateDataPolicy::AllowedLateness(allowed) => {
                let allowed_ms = allowed.as_millis() as u64;
                if lateness <= allowed_ms {
                    LateDataDecision::Allowed
                } else {
                    LateDataDecision::Dropped
                }
            }
            LateDataPolicy::SideOutput => {
                let late_event = LateEvent {
                    event_timestamp,
                    watermark,
                    lateness_ms: lateness,
                    data,
                };
                let mut side_output = self.side_output.write().await;
                side_output.push(late_event);
                LateDataDecision::SideOutput
            }
            LateDataPolicy::UpdateAndRetract => LateDataDecision::UpdateAndRetract,
        }
    }

    /// Get late event count
    pub async fn late_count(&self) -> u64 {
        *self.late_count.read().await
    }

    /// Get side output
    pub async fn get_side_output(&self) -> Vec<LateEvent> {
        self.side_output.read().await.clone()
    }

    /// Clear side output
    pub async fn clear_side_output(&self) {
        let mut side_output = self.side_output.write().await;
        side_output.clear();
    }
}

/// Decision for late data
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LateDataDecision {
    /// Event is not late
    NotLate,
    /// Event is late but allowed
    Allowed,
    /// Event was dropped
    Dropped,
    /// Event sent to side output
    SideOutput,
    /// Update and retract previous results
    UpdateAndRetract,
}

/// Timestamp extractor
pub trait TimestampExtractor: Send + Sync {
    /// Extract timestamp from event
    fn extract(&self, event: &serde_json::Value) -> Option<u64>;
}

/// Field-based timestamp extractor
pub struct FieldTimestampExtractor {
    /// Field name to extract timestamp from
    field: String,
    /// Whether to multiply by 1000 (seconds to milliseconds)
    multiply_by_1000: bool,
}

impl FieldTimestampExtractor {
    /// Create a new field timestamp extractor
    pub fn new(field: &str) -> Self {
        Self {
            field: field.to_string(),
            multiply_by_1000: false,
        }
    }

    /// Create for seconds (will multiply by 1000)
    pub fn for_seconds(field: &str) -> Self {
        Self {
            field: field.to_string(),
            multiply_by_1000: true,
        }
    }
}

impl TimestampExtractor for FieldTimestampExtractor {
    fn extract(&self, event: &serde_json::Value) -> Option<u64> {
        event.get(&self.field).and_then(|v| {
            let ts = v.as_u64().or_else(|| v.as_i64().map(|i| i as u64))?;
            if self.multiply_by_1000 {
                Some(ts * 1000)
            } else {
                Some(ts)
            }
        })
    }
}

/// Processing time timestamp extractor
pub struct ProcessingTimeExtractor;

impl TimestampExtractor for ProcessingTimeExtractor {
    fn extract(&self, _event: &serde_json::Value) -> Option<u64> {
        Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        )
    }
}

/// Watermark assigner
pub struct WatermarkAssigner {
    /// Timestamp extractor
    extractor: Box<dyn TimestampExtractor>,
    /// Watermark generator
    generator: WatermarkGenerator,
}

impl WatermarkAssigner {
    /// Create a new watermark assigner
    pub fn new(extractor: Box<dyn TimestampExtractor>, strategy: WatermarkStrategy) -> Self {
        Self {
            extractor,
            generator: WatermarkGenerator::new(strategy),
        }
    }

    /// Create for bounded out-of-orderness
    pub fn for_bounded_out_of_orderness(field: &str, max_lateness: Duration) -> Self {
        Self::new(
            Box::new(FieldTimestampExtractor::new(field)),
            WatermarkStrategy::BoundedOutOfOrderness { max_lateness },
        )
    }

    /// Create for ascending timestamps
    pub fn for_ascending_timestamps(field: &str) -> Self {
        Self::new(
            Box::new(FieldTimestampExtractor::new(field)),
            WatermarkStrategy::AscendingTimestamps,
        )
    }

    /// Create for ingestion time
    pub fn for_ingestion_time() -> Self {
        Self::new(
            Box::new(ProcessingTimeExtractor),
            WatermarkStrategy::IngestionTime,
        )
    }

    /// Extract timestamp from event
    pub fn extract_timestamp(&self, event: &serde_json::Value) -> Option<u64> {
        self.extractor.extract(event)
    }

    /// Process event and potentially emit watermark
    pub fn process_event(&mut self, event: &serde_json::Value) -> Option<Watermark> {
        if let Some(timestamp) = self.extractor.extract(event) {
            self.generator.on_event(timestamp)
        } else {
            None
        }
    }

    /// Get current watermark
    pub fn current_watermark(&self) -> Watermark {
        self.generator.current_watermark()
    }

    /// Check if event is late
    pub fn is_late(&self, event: &serde_json::Value) -> bool {
        if let Some(timestamp) = self.extractor.extract(event) {
            self.generator.is_late(timestamp)
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_watermark_ordering() {
        let w1 = Watermark::new(100);
        let w2 = Watermark::new(200);

        assert!(w1 < w2);
        assert!(w1.is_before(&w2));
        assert!(!w2.is_before(&w1));
    }

    #[test]
    fn test_bounded_out_of_orderness() {
        let mut generator =
            WatermarkGenerator::for_bounded_out_of_orderness(Duration::from_millis(100));

        // First event
        let wm = generator.on_event(200);
        assert_eq!(wm, Some(Watermark::new(100)));

        // Event in order
        let wm = generator.on_event(300);
        assert_eq!(wm, Some(Watermark::new(200)));

        // Out of order event (but within lateness)
        let wm = generator.on_event(250);
        assert!(wm.is_none()); // No new watermark

        // Late event
        assert!(generator.is_late(50));
        assert!(!generator.is_late(250));
    }

    #[test]
    fn test_ascending_timestamps() {
        let mut generator = WatermarkGenerator::for_ascending_timestamps();

        let wm = generator.on_event(100);
        assert_eq!(wm, Some(Watermark::new(100)));

        let wm = generator.on_event(200);
        assert_eq!(wm, Some(Watermark::new(200)));

        // Out of order event
        let wm = generator.on_event(150);
        assert!(wm.is_none());
    }

    #[tokio::test]
    async fn test_watermark_tracker() {
        let tracker = WatermarkTracker::new();

        tracker
            .update_watermark("source1", Watermark::new(100))
            .await;
        tracker
            .update_watermark("source2", Watermark::new(200))
            .await;

        // Combined watermark is minimum
        let combined = tracker.get_watermark().await;
        assert_eq!(combined.timestamp(), 100);

        // Update source1
        tracker
            .update_watermark("source1", Watermark::new(150))
            .await;
        let combined = tracker.get_watermark().await;
        assert_eq!(combined.timestamp(), 150);
    }

    #[tokio::test]
    async fn test_late_data_handler() {
        let handler =
            LateDataHandler::new(LateDataPolicy::AllowedLateness(Duration::from_millis(50)));

        // Not late
        let decision = handler.handle(100, 100, json!({})).await;
        assert_eq!(decision, LateDataDecision::NotLate);

        // Late but allowed
        let decision = handler.handle(60, 100, json!({})).await;
        assert_eq!(decision, LateDataDecision::Allowed);

        // Too late
        let decision = handler.handle(40, 100, json!({})).await;
        assert_eq!(decision, LateDataDecision::Dropped);

        assert_eq!(handler.late_count().await, 2);
    }

    #[test]
    fn test_field_timestamp_extractor() {
        let extractor = FieldTimestampExtractor::new("timestamp");
        let event = json!({"timestamp": 1234567890});

        let ts = extractor.extract(&event);
        assert_eq!(ts, Some(1234567890));
    }

    #[test]
    fn test_watermark_assigner() {
        let mut assigner =
            WatermarkAssigner::for_bounded_out_of_orderness("ts", Duration::from_millis(100));

        let event1 = json!({"ts": 200, "value": 1});
        let wm = assigner.process_event(&event1);
        assert_eq!(wm, Some(Watermark::new(100)));

        let event2 = json!({"ts": 300, "value": 2});
        let wm = assigner.process_event(&event2);
        assert_eq!(wm, Some(Watermark::new(200)));

        // Check lateness
        let late_event = json!({"ts": 50, "value": 3});
        assert!(assigner.is_late(&late_event));

        let ok_event = json!({"ts": 250, "value": 4});
        assert!(!assigner.is_late(&ok_event));
    }
}
