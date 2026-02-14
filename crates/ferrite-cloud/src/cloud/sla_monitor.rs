//! SLA Monitoring
//!
//! Tracks service-level agreement compliance for Ferrite Cloud instances,
//! records violations, and generates SLA reports with credit calculations.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, SystemTime};

use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use super::api_gateway::InstanceMetrics;

// ---------------------------------------------------------------------------
// SLA targets & violations
// ---------------------------------------------------------------------------

/// SLA targets for a managed instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlaTarget {
    /// Target uptime percentage (e.g. 99.95).
    pub uptime_target: f64,
    /// Target 99th-percentile latency in microseconds.
    pub latency_p99_target_us: u64,
    /// Window over which SLA compliance is evaluated.
    pub availability_window: Duration,
}

impl Default for SlaTarget {
    fn default() -> Self {
        Self {
            uptime_target: 99.95,
            latency_p99_target_us: 1000,
            availability_window: Duration::from_secs(30 * 24 * 3600), // 30 days
        }
    }
}

/// Classification of an SLA violation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ViolationType {
    /// Uptime dropped below target.
    UptimeBreach,
    /// Latency exceeded the target threshold.
    LatencyBreach,
    /// Error rate exceeded acceptable limits.
    ErrorRateBreach,
}

/// A recorded SLA violation event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlaViolation {
    /// Instance that experienced the violation.
    pub instance_id: String,
    /// When the violation was detected.
    pub timestamp: SystemTime,
    /// Kind of violation.
    pub violation_type: ViolationType,
    /// The measured value at the time of violation.
    pub actual_value: f64,
    /// The SLA target value.
    pub target_value: f64,
    /// Duration of the violation window.
    pub duration: Duration,
}

/// Summary SLA report for a single instance.
#[derive(Debug, Clone, Serialize)]
pub struct SlaReport {
    /// Instance identifier.
    pub instance_id: String,
    /// Reporting period.
    pub period: Duration,
    /// Observed uptime percentage.
    pub uptime_percent: f64,
    /// Target uptime percentage.
    pub target_uptime: f64,
    /// Whether the uptime target was met.
    pub uptime_met: bool,
    /// Average 99th-percentile latency in microseconds.
    pub avg_latency_p99_us: u64,
    /// Target 99th-percentile latency in microseconds.
    pub target_latency_p99_us: u64,
    /// Whether the latency target was met.
    pub latency_met: bool,
    /// Total number of violations in the period.
    pub total_violations: usize,
    /// Estimated SLA credit due (percentage of monthly bill).
    pub credits_due: f64,
}

// ---------------------------------------------------------------------------
// Uptime tracker
// ---------------------------------------------------------------------------

/// Tracks up/down observations per instance.
pub struct UptimeTracker {
    windows: DashMap<String, VecDeque<(SystemTime, bool)>>,
}

impl UptimeTracker {
    fn new() -> Self {
        Self {
            windows: DashMap::new(),
        }
    }

    /// Record an availability observation.
    pub fn record(&self, id: &str, is_up: bool) {
        self.windows
            .entry(id.to_string())
            .or_default()
            .push_back((SystemTime::now(), is_up));
    }

    /// Calculate uptime percentage for an instance (0.0–100.0).
    pub fn uptime(&self, id: &str) -> f64 {
        match self.windows.get(id) {
            Some(entries) if !entries.is_empty() => {
                let total = entries.len() as f64;
                let up = entries.iter().filter(|(_, up)| *up).count() as f64;
                (up / total) * 100.0
            }
            _ => 100.0, // no data → assume up
        }
    }
}

// ---------------------------------------------------------------------------
// SLA Monitor
// ---------------------------------------------------------------------------

/// Monitors SLA compliance across managed instances.
pub struct SlaMonitor {
    targets: RwLock<HashMap<String, SlaTarget>>,
    violations: RwLock<VecDeque<SlaViolation>>,
    uptime_tracker: UptimeTracker,
    /// Running sum of p99 latency observations per instance.
    latency_sums: RwLock<HashMap<String, (u64, u64)>>, // (sum, count)
}

impl SlaMonitor {
    /// Creates a new `SlaMonitor`.
    pub fn new() -> Self {
        Self {
            targets: RwLock::new(HashMap::new()),
            violations: RwLock::new(VecDeque::new()),
            uptime_tracker: UptimeTracker::new(),
            latency_sums: RwLock::new(HashMap::new()),
        }
    }

    /// Register (or update) the SLA target for an instance.
    pub fn add_target(&self, instance_id: &str, target: SlaTarget) {
        self.targets.write().insert(instance_id.to_string(), target);
    }

    /// Ingest a metrics check and record any SLA violations.
    pub fn record_check(&self, instance_id: &str, metrics: &InstanceMetrics) {
        let targets = self.targets.read();
        let target = match targets.get(instance_id) {
            Some(t) => t.clone(),
            None => return,
        };
        drop(targets);

        // Track uptime (consider instance "up" when connections > 0 or
        // CPU is being consumed).
        let is_up = metrics.connections > 0 || metrics.cpu_usage_percent > 0.0;
        self.uptime_tracker.record(instance_id, is_up);

        // Accumulate latency.
        {
            let mut sums = self.latency_sums.write();
            let entry = sums.entry(instance_id.to_string()).or_insert((0, 0));
            entry.0 += metrics.latency_p99_us;
            entry.1 += 1;
        }

        // Check latency target.
        if metrics.latency_p99_us > target.latency_p99_target_us {
            self.violations.write().push_back(SlaViolation {
                instance_id: instance_id.to_string(),
                timestamp: SystemTime::now(),
                violation_type: ViolationType::LatencyBreach,
                actual_value: metrics.latency_p99_us as f64,
                target_value: target.latency_p99_target_us as f64,
                duration: metrics.window,
            });
        }

        // Check uptime target.
        let uptime = self.uptime_tracker.uptime(instance_id);
        if uptime < target.uptime_target {
            self.violations.write().push_back(SlaViolation {
                instance_id: instance_id.to_string(),
                timestamp: SystemTime::now(),
                violation_type: ViolationType::UptimeBreach,
                actual_value: uptime,
                target_value: target.uptime_target,
                duration: metrics.window,
            });
        }
    }

    /// Retrieve violations for an instance, optionally filtered by time.
    pub fn get_violations(
        &self,
        instance_id: &str,
        since: Option<SystemTime>,
    ) -> Vec<SlaViolation> {
        self.violations
            .read()
            .iter()
            .filter(|v| v.instance_id == instance_id && since.map_or(true, |s| v.timestamp >= s))
            .cloned()
            .collect()
    }

    /// Get the current uptime percentage for an instance.
    pub fn get_uptime(&self, instance_id: &str) -> f64 {
        self.uptime_tracker.uptime(instance_id)
    }

    /// Generate an SLA compliance report for an instance.
    pub fn get_sla_report(&self, instance_id: &str) -> SlaReport {
        let targets = self.targets.read();
        let target = targets.get(instance_id).cloned().unwrap_or_default();
        drop(targets);

        let uptime = self.uptime_tracker.uptime(instance_id);
        let uptime_met = uptime >= target.uptime_target;

        let avg_latency = {
            let sums = self.latency_sums.read();
            match sums.get(instance_id) {
                Some(&(sum, count)) if count > 0 => sum / count,
                _ => 0,
            }
        };
        let latency_met = avg_latency <= target.latency_p99_target_us;

        let violations = self.get_violations(instance_id, None);
        let total_violations = violations.len();

        // Simple credit model: 10% credit per percentage point below target,
        // capped at 30%.
        let credits_due = if uptime_met {
            0.0
        } else {
            ((target.uptime_target - uptime) * 10.0).min(30.0)
        };

        SlaReport {
            instance_id: instance_id.to_string(),
            period: target.availability_window,
            uptime_percent: uptime,
            target_uptime: target.uptime_target,
            uptime_met,
            avg_latency_p99_us: avg_latency,
            target_latency_p99_us: target.latency_p99_target_us,
            latency_met,
            total_violations,
            credits_due,
        }
    }
}

impl Default for SlaMonitor {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_metrics(instance_id: &str, latency: u64, connections: u32) -> InstanceMetrics {
        InstanceMetrics {
            instance_id: instance_id.to_string(),
            window: Duration::from_secs(60),
            cpu_usage_percent: 45.0,
            memory_usage_bytes: 1024 * 1024 * 512,
            commands_per_sec: 10_000.0,
            connections,
            hit_rate: 0.95,
            latency_p99_us: latency,
        }
    }

    #[test]
    fn test_uptime_tracker_all_up() {
        let tracker = UptimeTracker::new();
        for _ in 0..10 {
            tracker.record("i1", true);
        }
        assert!((tracker.uptime("i1") - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_uptime_tracker_mixed() {
        let tracker = UptimeTracker::new();
        // 8 up, 2 down → 80%
        for _ in 0..8 {
            tracker.record("i1", true);
        }
        for _ in 0..2 {
            tracker.record("i1", false);
        }
        assert!((tracker.uptime("i1") - 80.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_uptime_no_data() {
        let tracker = UptimeTracker::new();
        assert!((tracker.uptime("unknown") - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_latency_violation() {
        let monitor = SlaMonitor::new();
        monitor.add_target(
            "i1",
            SlaTarget {
                latency_p99_target_us: 1000,
                ..Default::default()
            },
        );

        // Report metrics with latency exceeding the target.
        let bad_metrics = sample_metrics("i1", 2000, 10);
        monitor.record_check("i1", &bad_metrics);

        let violations = monitor.get_violations("i1", None);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].violation_type, ViolationType::LatencyBreach);
        assert_eq!(violations[0].actual_value, 2000.0);
    }

    #[test]
    fn test_no_violation_within_target() {
        let monitor = SlaMonitor::new();
        monitor.add_target("i1", SlaTarget::default());

        let ok_metrics = sample_metrics("i1", 500, 10);
        monitor.record_check("i1", &ok_metrics);

        let violations = monitor.get_violations("i1", None);
        assert!(violations.is_empty());
    }

    #[test]
    fn test_uptime_breach_violation() {
        let monitor = SlaMonitor::new();
        monitor.add_target(
            "i1",
            SlaTarget {
                uptime_target: 99.0,
                ..Default::default()
            },
        );

        // Simulate mostly-down instance (only 1 up out of 10 checks).
        for _ in 0..1 {
            monitor.uptime_tracker.record("i1", true);
        }
        for _ in 0..9 {
            monitor.uptime_tracker.record("i1", false);
        }

        // The record_check itself will add another observation; the metrics
        // indicate a healthy check but uptime is already below target.
        let metrics = sample_metrics("i1", 500, 10);
        monitor.record_check("i1", &metrics);

        let violations = monitor.get_violations("i1", None);
        assert!(violations
            .iter()
            .any(|v| v.violation_type == ViolationType::UptimeBreach));
    }

    #[test]
    fn test_sla_report_healthy() {
        let monitor = SlaMonitor::new();
        monitor.add_target("i1", SlaTarget::default());

        for _ in 0..100 {
            let m = sample_metrics("i1", 500, 10);
            monitor.record_check("i1", &m);
        }

        let report = monitor.get_sla_report("i1");
        assert_eq!(report.instance_id, "i1");
        assert!(report.uptime_met);
        assert!(report.latency_met);
        assert_eq!(report.credits_due, 0.0);
    }

    #[test]
    fn test_sla_report_credits_due() {
        let monitor = SlaMonitor::new();
        monitor.add_target(
            "i1",
            SlaTarget {
                uptime_target: 99.0,
                ..Default::default()
            },
        );

        // All checks are "down" (connections=0, cpu=0).
        for _ in 0..10 {
            let m = InstanceMetrics {
                instance_id: "i1".to_string(),
                window: Duration::from_secs(60),
                cpu_usage_percent: 0.0,
                memory_usage_bytes: 0,
                commands_per_sec: 0.0,
                connections: 0,
                hit_rate: 0.0,
                latency_p99_us: 500,
            };
            monitor.record_check("i1", &m);
        }

        let report = monitor.get_sla_report("i1");
        assert!(!report.uptime_met);
        assert!(report.credits_due > 0.0);
    }

    #[test]
    fn test_get_uptime() {
        let monitor = SlaMonitor::new();
        monitor.add_target("i1", SlaTarget::default());

        for _ in 0..5 {
            let m = sample_metrics("i1", 500, 10);
            monitor.record_check("i1", &m);
        }
        assert!((monitor.get_uptime("i1") - 100.0).abs() < f64::EPSILON);
    }
}
