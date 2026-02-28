//! Fault schedule generation for Jepsen tests
//!
//! Generates realistic fault injection schedules combining
//! network partitions, process crashes, and clock skew.

#![allow(dead_code)]
#![allow(clippy::unwrap_used)]

use std::time::Duration;

use rand::Rng;

use super::fault_injection::{FaultEvent, FaultTarget, FaultType};

// ---------------------------------------------------------------------------
// ScheduledFault
// ---------------------------------------------------------------------------

/// A fault event with a precise injection time and optional auto-heal.
#[derive(Debug, Clone)]
pub struct ScheduledFault {
    /// When to inject, measured from test start.
    pub inject_at: Duration,
    /// The fault event to inject.
    pub fault: FaultEvent,
    /// If `Some`, automatically heal the fault after this duration.
    pub heal_after: Option<Duration>,
}

// ---------------------------------------------------------------------------
// FaultSchedule
// ---------------------------------------------------------------------------

/// An ordered sequence of scheduled faults.
#[derive(Debug, Clone)]
pub struct FaultSchedule {
    /// Faults sorted by injection time.
    faults: Vec<ScheduledFault>,
}

impl FaultSchedule {
    /// Create an empty schedule.
    pub fn new() -> Self {
        Self { faults: Vec::new() }
    }

    /// Create a schedule from a pre-built list of faults.
    pub fn from_faults(mut faults: Vec<ScheduledFault>) -> Self {
        faults.sort_by_key(|f| f.inject_at);
        Self { faults }
    }

    /// Iterate over scheduled faults in chronological order.
    pub fn iter(&self) -> impl Iterator<Item = &ScheduledFault> {
        self.faults.iter()
    }

    /// Return the number of scheduled faults.
    pub fn len(&self) -> usize {
        self.faults.len()
    }

    /// Return `true` if the schedule contains no faults.
    pub fn is_empty(&self) -> bool {
        self.faults.is_empty()
    }

    /// Merge two schedules into one, maintaining chronological order.
    pub fn merge(mut self, other: FaultSchedule) -> FaultSchedule {
        self.faults.extend(other.faults);
        self.faults.sort_by_key(|f| f.inject_at);
        self
    }

    /// Produce an ASCII timeline visualization of the schedule.
    pub fn to_timeline_string(&self) -> String {
        if self.faults.is_empty() {
            return "  (no faults scheduled)".to_string();
        }

        let mut lines = Vec::new();
        lines.push("Fault Schedule Timeline".to_string());
        lines.push("─".repeat(60));

        for (i, sf) in self.faults.iter().enumerate() {
            let inject_s = sf.inject_at.as_secs_f64();
            let fault_desc = format!("{:?} -> {:?}", sf.fault.fault_type, sf.fault.target);
            let heal_desc = match sf.heal_after {
                Some(d) => format!("heals after {:.1}s", d.as_secs_f64()),
                None => "permanent".to_string(),
            };
            lines.push(format!(
                "  {:>3}. [{:>7.1}s] {} ({})",
                i + 1,
                inject_s,
                fault_desc,
                heal_desc,
            ));
        }

        lines.push("─".repeat(60));
        lines.join("\n")
    }

    /// Convert the schedule to a flat list of `FaultEvent` values
    /// suitable for use in `JepsenConfig`.
    pub fn to_fault_events(&self) -> Vec<FaultEvent> {
        self.faults.iter().map(|sf| sf.fault.clone()).collect()
    }
}

impl Default for FaultSchedule {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// ScheduleBuilder
// ---------------------------------------------------------------------------

/// Fluent builder for constructing [`FaultSchedule`]s.
pub struct ScheduleBuilder {
    /// Total test duration — used to bound random fault placement.
    test_duration: Duration,
    /// Accumulated scheduled faults.
    faults: Vec<ScheduledFault>,
}

impl ScheduleBuilder {
    /// Create a new builder for a test of the given duration.
    pub fn new(test_duration: Duration) -> Self {
        Self {
            test_duration,
            faults: Vec::new(),
        }
    }

    /// Schedule a network partition at a specific time.
    pub fn add_partition_at(
        &mut self,
        time: Duration,
        duration: Duration,
        partitions: Vec<Vec<String>>,
    ) -> &mut Self {
        self.faults.push(ScheduledFault {
            inject_at: time,
            fault: FaultEvent {
                timestamp: time,
                fault_type: FaultType::NetworkPartition { partitions },
                target: FaultTarget::AllNodes,
                duration: Some(duration),
            },
            heal_after: Some(duration),
        });
        self
    }

    /// Schedule a process crash at a specific time.
    pub fn add_crash_at(&mut self, time: Duration, node: &str) -> &mut Self {
        self.faults.push(ScheduledFault {
            inject_at: time,
            fault: FaultEvent {
                timestamp: time,
                fault_type: FaultType::ProcessCrash,
                target: FaultTarget::Node(node.to_string()),
                duration: None,
            },
            heal_after: None,
        });
        self
    }

    /// Schedule a clock skew injection at a specific time.
    pub fn add_clock_skew_at(&mut self, time: Duration, node: &str, skew_ms: i64) -> &mut Self {
        self.faults.push(ScheduledFault {
            inject_at: time,
            fault: FaultEvent {
                timestamp: time,
                fault_type: FaultType::ClockSkew { skew_ms },
                target: FaultTarget::Node(node.to_string()),
                duration: None,
            },
            heal_after: None,
        });
        self
    }

    /// Add `count` random faults spread across the test duration.
    pub fn add_random_faults(&mut self, count: usize, rng: &mut impl Rng) -> &mut Self {
        let test_secs = self.test_duration.as_secs().max(1);
        let node_names = ["n1", "n2", "n3"];

        for _ in 0..count {
            let inject_secs = rng.gen_range(0..test_secs);
            let inject_at = Duration::from_secs(inject_secs);
            let node = node_names[rng.gen_range(0..node_names.len())];

            let (fault_type, heal_after) = match rng.gen_range(0..3u8) {
                0 => {
                    // Network partition
                    let dur = Duration::from_secs(rng.gen_range(1..10));
                    let partitions = vec![
                        vec![node.to_string()],
                        node_names
                            .iter()
                            .filter(|&&n| n != node)
                            .map(|n| n.to_string())
                            .collect(),
                    ];
                    (FaultType::NetworkPartition { partitions }, Some(dur))
                }
                1 => {
                    // Process crash
                    (FaultType::ProcessCrash, None)
                }
                _ => {
                    // Clock skew
                    let skew_ms = rng.gen_range(-500..500i64);
                    (FaultType::ClockSkew { skew_ms }, None)
                }
            };

            self.faults.push(ScheduledFault {
                inject_at,
                fault: FaultEvent {
                    timestamp: inject_at,
                    fault_type,
                    target: FaultTarget::Node(node.to_string()),
                    duration: heal_after,
                },
                heal_after,
            });
        }

        self
    }

    /// Consume the builder and produce a sorted [`FaultSchedule`].
    pub fn build(self) -> FaultSchedule {
        FaultSchedule::from_faults(self.faults)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_schedule() {
        let schedule = ScheduleBuilder::new(Duration::from_secs(60)).build();
        assert!(schedule.is_empty());
        assert_eq!(schedule.len(), 0);
    }

    #[test]
    fn add_partition() {
        let schedule = {
            let mut b = ScheduleBuilder::new(Duration::from_secs(60));
            b.add_partition_at(
                Duration::from_secs(10),
                Duration::from_secs(5),
                vec![vec!["n1".into()], vec!["n2".into(), "n3".into()]],
            );
            b.build()
        };
        assert_eq!(schedule.len(), 1);
        let first = schedule.iter().next().unwrap();
        assert_eq!(first.inject_at, Duration::from_secs(10));
        assert_eq!(first.heal_after, Some(Duration::from_secs(5)));
    }

    #[test]
    fn add_crash_and_clock_skew() {
        let schedule = {
            let mut b = ScheduleBuilder::new(Duration::from_secs(120));
            b.add_crash_at(Duration::from_secs(30), "n1");
            b.add_clock_skew_at(Duration::from_secs(50), "n2", -200);
            b.build()
        };
        assert_eq!(schedule.len(), 2);
    }

    #[test]
    fn random_faults() {
        let mut rng = rand::thread_rng();
        let schedule = {
            let mut b = ScheduleBuilder::new(Duration::from_secs(60));
            b.add_random_faults(5, &mut rng);
            b.build()
        };
        assert_eq!(schedule.len(), 5);
    }

    #[test]
    fn merge_schedules() {
        let s1 = {
            let mut b = ScheduleBuilder::new(Duration::from_secs(60));
            b.add_crash_at(Duration::from_secs(10), "n1");
            b.build()
        };
        let s2 = {
            let mut b = ScheduleBuilder::new(Duration::from_secs(60));
            b.add_crash_at(Duration::from_secs(5), "n2");
            b.build()
        };
        let merged = s1.merge(s2);
        assert_eq!(merged.len(), 2);
        // First fault should be at 5s (from s2)
        let first = merged.iter().next().unwrap();
        assert_eq!(first.inject_at, Duration::from_secs(5));
    }

    #[test]
    fn timeline_string() {
        let schedule = {
            let mut b = ScheduleBuilder::new(Duration::from_secs(60));
            b.add_crash_at(Duration::from_secs(10), "n1");
            b.add_partition_at(
                Duration::from_secs(30),
                Duration::from_secs(10),
                vec![vec!["n1".into()], vec!["n2".into(), "n3".into()]],
            );
            b.build()
        };
        let timeline = schedule.to_timeline_string();
        assert!(timeline.contains("Fault Schedule Timeline"));
        assert!(timeline.contains("10.0s"));
        assert!(timeline.contains("30.0s"));
    }

    #[test]
    fn empty_timeline_string() {
        let schedule = FaultSchedule::new();
        let timeline = schedule.to_timeline_string();
        assert!(timeline.contains("no faults scheduled"));
    }
}
