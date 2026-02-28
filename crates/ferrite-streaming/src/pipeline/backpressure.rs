//! Backpressure management for streaming pipelines
//!
//! Provides adaptive flow control to prevent pipeline stages from being
//! overwhelmed by upstream producers. Uses high/low watermarks to signal
//! when consumers should pause or resume reading.

#![allow(dead_code)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

/// Controls backpressure for a streaming pipeline stage.
///
/// When the number of pending items exceeds the `high_watermark`, the
/// controller signals that the upstream should pause. Once pending items
/// drop below the `low_watermark`, the upstream may resume.
pub struct BackpressureController {
    /// Pause threshold — upstream should stop when pending exceeds this.
    high_watermark: usize,
    /// Resume threshold — upstream may continue when pending drops below this.
    low_watermark: usize,
    /// Current number of pending (in-flight) items.
    pending: AtomicUsize,
}

impl BackpressureController {
    /// Create a new backpressure controller.
    ///
    /// # Arguments
    ///
    /// * `high_watermark` — pause upstream when pending items exceed this
    /// * `low_watermark` — resume upstream when pending items drop below this
    pub fn new(high_watermark: usize, low_watermark: usize) -> Self {
        Self {
            high_watermark,
            low_watermark,
            pending: AtomicUsize::new(0),
        }
    }

    /// Record that `count` items are now pending (added to the queue).
    pub fn record_pending(&self, count: usize) {
        self.pending.fetch_add(count, Ordering::Relaxed);
    }

    /// Record that `count` items have been completed (removed from the queue).
    pub fn record_completed(&self, count: usize) {
        self.pending
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(count))
            })
            .ok();
    }

    /// Returns `true` if the upstream should pause (pending > high watermark).
    pub fn should_pause(&self) -> bool {
        self.pending.load(Ordering::Relaxed) > self.high_watermark
    }

    /// Returns `true` if the upstream may resume (pending < low watermark).
    pub fn should_resume(&self) -> bool {
        self.pending.load(Ordering::Relaxed) < self.low_watermark
    }

    /// Current pressure as a ratio: 0.0 (idle) to 1.0+ (over high watermark).
    pub fn pressure_pct(&self) -> f64 {
        if self.high_watermark == 0 {
            return 0.0;
        }
        self.pending.load(Ordering::Relaxed) as f64 / self.high_watermark as f64
    }

    /// Compute an adaptive backoff duration based on current pressure.
    ///
    /// Returns `Duration::ZERO` when pressure is low, and scales up to
    /// 100 ms when pressure exceeds the high watermark.
    pub fn wait_if_pressured(&self) -> Duration {
        let pct = self.pressure_pct();
        if pct < 0.5 {
            Duration::ZERO
        } else if pct < 1.0 {
            // Linear ramp between 1 ms and 10 ms
            let ms = 1.0 + (pct - 0.5) * 18.0; // 1..10 ms
            Duration::from_millis(ms as u64)
        } else {
            // Over watermark — back off aggressively
            let ms = 10.0 + (pct - 1.0) * 90.0; // 10..100 ms
            Duration::from_millis(ms.min(100.0) as u64)
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_flow() {
        let ctrl = BackpressureController::new(100, 50);

        assert!(!ctrl.should_pause());
        assert!(ctrl.should_resume());
        assert!((ctrl.pressure_pct() - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_pause_and_resume() {
        let ctrl = BackpressureController::new(100, 50);

        ctrl.record_pending(120);
        assert!(ctrl.should_pause());
        assert!(!ctrl.should_resume());

        ctrl.record_completed(80);
        assert!(!ctrl.should_pause());
        assert!(ctrl.should_resume());
    }

    #[test]
    fn test_pressure_pct() {
        let ctrl = BackpressureController::new(100, 50);

        ctrl.record_pending(50);
        assert!((ctrl.pressure_pct() - 0.5).abs() < f64::EPSILON);

        ctrl.record_pending(50);
        assert!((ctrl.pressure_pct() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_wait_if_pressured() {
        let ctrl = BackpressureController::new(100, 50);

        assert_eq!(ctrl.wait_if_pressured(), Duration::ZERO);

        ctrl.record_pending(75);
        let wait = ctrl.wait_if_pressured();
        assert!(wait > Duration::ZERO);
        assert!(wait < Duration::from_millis(20));

        ctrl.record_pending(75); // total 150, over watermark
        let wait = ctrl.wait_if_pressured();
        assert!(wait >= Duration::from_millis(10));
    }

    #[test]
    fn test_completed_saturating() {
        let ctrl = BackpressureController::new(100, 50);
        ctrl.record_pending(10);
        ctrl.record_completed(20); // more than pending
        assert_eq!(ctrl.pressure_pct(), 0.0);
    }
}
