//! Access pattern detection for tiering decisions
//!
//! This module analyzes key access patterns to make smarter tiering decisions.
//! It detects patterns like steady access, periodic bursts, declining access,
//! and cold data.

use super::stats::KeyAccessStats;
use serde::{Deserialize, Serialize};

/// Type of access operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessType {
    /// Read operation (GET, HGET, etc.)
    Read,
    /// Write operation (SET, HSET, etc.)
    Write,
}

/// Detected access pattern for a key
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AccessPattern {
    /// Steady stream of accesses
    Steady {
        /// Average reads per hour
        avg_reads_per_hour: f64,
        /// Average writes per hour
        avg_writes_per_hour: f64,
    },
    /// Periodic bursts (e.g., daily batch job)
    Periodic {
        /// Period between bursts in hours
        period_hours: u32,
        /// Average size of burst
        burst_size: u32,
    },
    /// Declining access (candidate for archival)
    Declining {
        /// Half-life in hours (time for access rate to halve)
        half_life_hours: f64,
    },
    /// Recently created, pattern unknown
    New,
    /// No recent access
    Cold,
    /// Write-heavy workload
    WriteHeavy {
        /// Write to read ratio
        write_read_ratio: f64,
    },
    /// Burst then idle pattern
    BurstThenIdle {
        /// Last burst size
        last_burst_size: u32,
        /// Hours since last burst
        hours_since_burst: u32,
    },
}

impl AccessPattern {
    /// Get a human-readable name for the pattern
    pub fn name(&self) -> &'static str {
        match self {
            AccessPattern::Steady { .. } => "steady",
            AccessPattern::Periodic { .. } => "periodic",
            AccessPattern::Declining { .. } => "declining",
            AccessPattern::New => "new",
            AccessPattern::Cold => "cold",
            AccessPattern::WriteHeavy { .. } => "write_heavy",
            AccessPattern::BurstThenIdle { .. } => "burst_then_idle",
        }
    }

    /// Check if this pattern suggests keeping data hot
    pub fn suggests_hot(&self) -> bool {
        matches!(
            self,
            AccessPattern::Steady {
                avg_reads_per_hour,
                ..
            } if *avg_reads_per_hour > 10.0
        ) || matches!(self, AccessPattern::WriteHeavy { .. })
            || matches!(self, AccessPattern::New)
    }

    /// Check if this pattern suggests cold storage
    pub fn suggests_cold(&self) -> bool {
        matches!(self, AccessPattern::Cold | AccessPattern::Declining { .. })
    }

    /// Get a description of the pattern
    pub fn description(&self) -> String {
        match self {
            AccessPattern::Steady {
                avg_reads_per_hour,
                avg_writes_per_hour,
            } => format!(
                "Steady: {:.1} reads/hr, {:.1} writes/hr",
                avg_reads_per_hour, avg_writes_per_hour
            ),
            AccessPattern::Periodic {
                period_hours,
                burst_size,
            } => format!(
                "Periodic: {} accesses every {} hours",
                burst_size, period_hours
            ),
            AccessPattern::Declining { half_life_hours } => {
                format!("Declining: half-life {:.1} hours", half_life_hours)
            }
            AccessPattern::New => "New: insufficient data".to_string(),
            AccessPattern::Cold => "Cold: no recent access".to_string(),
            AccessPattern::WriteHeavy { write_read_ratio } => {
                format!("Write-heavy: {:.1}x more writes", write_read_ratio)
            }
            AccessPattern::BurstThenIdle {
                last_burst_size,
                hours_since_burst,
            } => format!(
                "Burst then idle: {} ops, {} hours ago",
                last_burst_size, hours_since_burst
            ),
        }
    }
}

impl std::fmt::Display for AccessPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Access history entry for pattern detection
#[derive(Debug, Clone)]
pub struct AccessHistoryEntry {
    /// Timestamp (seconds since engine start)
    pub timestamp_secs: u64,
    /// Read count in this period
    pub reads: u32,
    /// Write count in this period
    pub writes: u32,
}

/// Analyzer for detecting access patterns
pub struct AccessPatternAnalyzer {
    /// Minimum samples needed to detect a pattern
    min_samples: usize,
    /// Threshold for considering data cold (reads per hour)
    cold_threshold: f64,
    /// Threshold for detecting declining pattern
    decline_threshold: f64,
    /// Threshold for write-heavy classification
    write_heavy_ratio: f64,
}

impl Default for AccessPatternAnalyzer {
    fn default() -> Self {
        Self {
            min_samples: 24, // 24 hours of data
            cold_threshold: 0.1,
            decline_threshold: 0.5,
            write_heavy_ratio: 3.0,
        }
    }
}

impl AccessPatternAnalyzer {
    /// Create a new analyzer
    pub fn new() -> Self {
        Self::default()
    }

    /// Create with custom thresholds
    pub fn with_thresholds(
        min_samples: usize,
        cold_threshold: f64,
        decline_threshold: f64,
    ) -> Self {
        Self {
            min_samples,
            cold_threshold,
            decline_threshold,
            write_heavy_ratio: 3.0,
        }
    }

    /// Detect access pattern from key stats
    pub fn detect_pattern(&self, stats: &KeyAccessStats) -> AccessPattern {
        let reads_1h = stats.access_counts.reads_1h as f64;
        let reads_1d = stats.access_counts.reads_1d as f64;
        let writes_1d = stats.access_counts.writes_1d as f64;

        // No recent access
        if reads_1d == 0.0 && writes_1d == 0.0 {
            return AccessPattern::Cold;
        }

        // Check for write-heavy
        if reads_1d > 0.0 && writes_1d / reads_1d > self.write_heavy_ratio {
            return AccessPattern::WriteHeavy {
                write_read_ratio: writes_1d / reads_1d,
            };
        }

        // Check for declining trend
        // If hourly rate is much lower than expected from daily rate
        let expected_hourly = reads_1d / 24.0;
        if expected_hourly > 0.0 && reads_1h < expected_hourly * self.decline_threshold {
            return AccessPattern::Declining {
                half_life_hours: self.estimate_half_life(reads_1h, expected_hourly),
            };
        }

        // Check if cold (very low access rate)
        let reads_per_hour = reads_1d / 24.0;
        if reads_per_hour < self.cold_threshold {
            return AccessPattern::Cold;
        }

        // Default to steady pattern
        AccessPattern::Steady {
            avg_reads_per_hour: reads_per_hour,
            avg_writes_per_hour: writes_1d / 24.0,
        }
    }

    /// Detect pattern from access history (more accurate)
    pub fn detect_pattern_from_history(&self, history: &[AccessHistoryEntry]) -> AccessPattern {
        if history.len() < self.min_samples {
            return AccessPattern::New;
        }

        let total_reads: u32 = history.iter().map(|e| e.reads).sum();
        let total_writes: u32 = history.iter().map(|e| e.writes).sum();

        if total_reads == 0 && total_writes == 0 {
            return AccessPattern::Cold;
        }

        // Check for periodicity
        if let Some((period, burst_size)) = self.detect_periodicity(history) {
            return AccessPattern::Periodic {
                period_hours: period,
                burst_size,
            };
        }

        // Check for decline
        if self.is_declining(history) {
            return AccessPattern::Declining {
                half_life_hours: self.calculate_half_life(history),
            };
        }

        // Check for write-heavy
        let total_reads = total_reads as f64;
        let total_writes = total_writes as f64;
        if total_reads > 0.0 && total_writes / total_reads > self.write_heavy_ratio {
            return AccessPattern::WriteHeavy {
                write_read_ratio: total_writes / total_reads,
            };
        }

        // Default to steady
        let hours = history.len() as f64;
        AccessPattern::Steady {
            avg_reads_per_hour: total_reads / hours,
            avg_writes_per_hour: total_writes / hours,
        }
    }

    /// Estimate half-life from current vs expected rate
    fn estimate_half_life(&self, current: f64, expected: f64) -> f64 {
        if current <= 0.0 || expected <= 0.0 {
            return 24.0; // Default to 24 hours
        }
        // Using exponential decay: current = expected * 0.5^(t/half_life)
        // Solving for half_life: half_life = t / log2(expected/current)
        let ratio = expected / current;
        if ratio <= 1.0 {
            return 168.0; // 1 week if not actually declining
        }
        24.0 / ratio.log2() // Assuming 24h observation period
    }

    /// Detect periodic access patterns
    fn detect_periodicity(&self, history: &[AccessHistoryEntry]) -> Option<(u32, u32)> {
        if history.len() < 48 {
            // Need at least 2 days
            return None;
        }

        // Simple periodicity detection: look for repeating peaks
        let reads: Vec<u32> = history.iter().map(|e| e.reads).collect();

        // Find peaks (local maxima significantly above mean)
        let mean = reads.iter().sum::<u32>() as f64 / reads.len() as f64;
        let threshold = mean * 2.0;

        let mut peaks: Vec<usize> = Vec::new();
        for i in 1..reads.len() - 1 {
            if reads[i] as f64 > threshold && reads[i] > reads[i - 1] && reads[i] >= reads[i + 1] {
                peaks.push(i);
            }
        }

        if peaks.len() >= 2 {
            // Calculate average period between peaks
            let mut periods: Vec<usize> = Vec::new();
            for i in 1..peaks.len() {
                periods.push(peaks[i] - peaks[i - 1]);
            }

            let avg_period = periods.iter().sum::<usize>() / periods.len();

            // Check if periods are consistent (within 20%)
            let variance: f64 = periods
                .iter()
                .map(|&p| (p as f64 - avg_period as f64).powi(2))
                .sum::<f64>()
                / periods.len() as f64;

            if variance.sqrt() < avg_period as f64 * 0.2 {
                let avg_burst: u32 =
                    peaks.iter().map(|&i| reads[i]).sum::<u32>() / peaks.len() as u32;
                return Some((avg_period as u32, avg_burst));
            }
        }

        None
    }

    /// Check if access pattern is declining
    fn is_declining(&self, history: &[AccessHistoryEntry]) -> bool {
        if history.len() < 24 {
            return false;
        }

        // Compare first half to second half
        let mid = history.len() / 2;
        let first_half_reads: u32 = history[..mid].iter().map(|e| e.reads).sum();
        let second_half_reads: u32 = history[mid..].iter().map(|e| e.reads).sum();

        // Declining if second half has less than 50% of first half
        second_half_reads as f64 <= first_half_reads as f64 * self.decline_threshold
    }

    /// Calculate half-life from history
    fn calculate_half_life(&self, history: &[AccessHistoryEntry]) -> f64 {
        if history.len() < 24 {
            return 24.0;
        }

        let mid = history.len() / 2;
        let first_half_avg =
            history[..mid].iter().map(|e| e.reads).sum::<u32>() as f64 / mid as f64;
        let second_half_avg = history[mid..].iter().map(|e| e.reads).sum::<u32>() as f64
            / (history.len() - mid) as f64;

        if second_half_avg >= first_half_avg || second_half_avg <= 0.0 {
            return 168.0; // Not declining
        }

        let ratio = first_half_avg / second_half_avg;
        (mid as f64) / ratio.log2()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tiering::stats::StorageTier;

    #[test]
    fn test_access_pattern_cold() {
        let analyzer = AccessPatternAnalyzer::new();
        let stats = KeyAccessStats::new(1024, StorageTier::Memory);

        let pattern = analyzer.detect_pattern(&stats);
        assert_eq!(pattern, AccessPattern::Cold);
    }

    #[test]
    fn test_access_pattern_steady() {
        let analyzer = AccessPatternAnalyzer::new();
        let mut stats = KeyAccessStats::new(1024, StorageTier::Memory);
        stats.access_counts.reads_1d = 240; // 10 per hour
        stats.access_counts.reads_1h = 10;

        let pattern = analyzer.detect_pattern(&stats);
        assert!(matches!(pattern, AccessPattern::Steady { .. }));
    }

    #[test]
    fn test_access_pattern_declining() {
        let analyzer = AccessPatternAnalyzer::new();
        let mut stats = KeyAccessStats::new(1024, StorageTier::Memory);
        stats.access_counts.reads_1d = 240; // Would expect 10 per hour
        stats.access_counts.reads_1h = 1; // But only 1 in last hour

        let pattern = analyzer.detect_pattern(&stats);
        assert!(matches!(pattern, AccessPattern::Declining { .. }));
    }

    #[test]
    fn test_access_pattern_write_heavy() {
        let analyzer = AccessPatternAnalyzer::new();
        let mut stats = KeyAccessStats::new(1024, StorageTier::Memory);
        stats.access_counts.reads_1d = 10;
        stats.access_counts.writes_1d = 100; // 10x more writes

        let pattern = analyzer.detect_pattern(&stats);
        assert!(matches!(pattern, AccessPattern::WriteHeavy { .. }));
    }

    #[test]
    fn test_pattern_suggests_hot() {
        let hot_pattern = AccessPattern::Steady {
            avg_reads_per_hour: 100.0,
            avg_writes_per_hour: 10.0,
        };
        assert!(hot_pattern.suggests_hot());

        let cold_pattern = AccessPattern::Cold;
        assert!(!cold_pattern.suggests_hot());
    }

    #[test]
    fn test_pattern_suggests_cold() {
        let cold_pattern = AccessPattern::Cold;
        assert!(cold_pattern.suggests_cold());

        let declining = AccessPattern::Declining {
            half_life_hours: 10.0,
        };
        assert!(declining.suggests_cold());

        let steady = AccessPattern::Steady {
            avg_reads_per_hour: 100.0,
            avg_writes_per_hour: 10.0,
        };
        assert!(!steady.suggests_cold());
    }

    #[test]
    fn test_detect_from_history_new() {
        let analyzer = AccessPatternAnalyzer::new();
        let history: Vec<AccessHistoryEntry> = (0..10)
            .map(|i| AccessHistoryEntry {
                timestamp_secs: i * 3600,
                reads: 5,
                writes: 1,
            })
            .collect();

        let pattern = analyzer.detect_pattern_from_history(&history);
        assert_eq!(pattern, AccessPattern::New);
    }

    #[test]
    fn test_detect_from_history_steady() {
        let analyzer = AccessPatternAnalyzer::with_thresholds(24, 0.1, 0.5);
        let history: Vec<AccessHistoryEntry> = (0..48)
            .map(|i| AccessHistoryEntry {
                timestamp_secs: i * 3600,
                reads: 10,
                writes: 2,
            })
            .collect();

        let pattern = analyzer.detect_pattern_from_history(&history);
        assert!(matches!(pattern, AccessPattern::Steady { .. }));
    }
}
