#![forbid(unsafe_code)]

//! FaaS function scheduler — cron-like periodic execution.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Errors from the scheduler.
#[derive(Debug, Error)]
pub enum SchedulerError {
    #[error("schedule not found: {0}")]
    NotFound(String),
    #[error("schedule already exists: {0}")]
    AlreadyExists(String),
    #[error("invalid cron expression: {0}")]
    InvalidCron(String),
}

/// A scheduled function entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleEntry {
    /// Unique schedule name.
    pub name: String,
    /// The function to invoke.
    pub function_name: String,
    /// Cron expression (e.g. `@every 5m`, `@hourly`, `@daily`, `*/5 * * * *`).
    pub cron_expr: String,
    /// Whether the schedule is active.
    pub enabled: bool,
    /// Last run timestamp (epoch secs), 0 if never.
    pub last_run: u64,
    /// Next run timestamp (epoch secs).
    pub next_run: u64,
}

/// Manages scheduled function execution.
pub struct FunctionScheduler {
    schedules: DashMap<String, ScheduleEntry>,
}

impl FunctionScheduler {
    pub fn new() -> Self {
        Self {
            schedules: DashMap::new(),
        }
    }

    /// Add or replace a schedule entry.
    pub fn schedule(&self, mut entry: ScheduleEntry) -> Result<(), SchedulerError> {
        let interval = parse_interval(&entry.cron_expr)?;
        let now = now_epoch_secs();
        entry.next_run = now + interval;
        self.schedules.insert(entry.name.clone(), entry);
        Ok(())
    }

    /// Cancel a schedule by name.
    pub fn cancel(&self, name: &str) -> Result<(), SchedulerError> {
        self.schedules
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| SchedulerError::NotFound(name.to_string()))
    }

    /// List all schedule entries.
    pub fn list(&self) -> Vec<ScheduleEntry> {
        self.schedules.iter().map(|e| e.value().clone()).collect()
    }

    /// Return the next N upcoming runs across all schedules.
    pub fn next_runs(&self, count: usize) -> Vec<(String, u64)> {
        let mut runs: Vec<(String, u64)> = self
            .schedules
            .iter()
            .filter(|e| e.enabled)
            .map(|e| (e.name.clone(), e.next_run))
            .collect();
        runs.sort_by_key(|(_, t)| *t);
        runs.truncate(count);
        runs
    }

    /// Mark a schedule as having just run and compute the next run time.
    pub fn mark_executed(&self, name: &str) -> Result<(), SchedulerError> {
        let mut entry = self
            .schedules
            .get_mut(name)
            .ok_or_else(|| SchedulerError::NotFound(name.to_string()))?;
        let interval = parse_interval(&entry.cron_expr).unwrap_or(3600);
        let now = now_epoch_secs();
        entry.last_run = now;
        entry.next_run = now + interval;
        Ok(())
    }

    /// Number of active schedules.
    pub fn len(&self) -> usize {
        self.schedules.len()
    }

    /// Whether there are no schedules.
    pub fn is_empty(&self) -> bool {
        self.schedules.is_empty()
    }
}

impl Default for FunctionScheduler {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse a simple cron/interval expression and return the interval in seconds.
///
/// Supported formats:
/// - `@every <N><unit>` where unit is `s`, `m`, `h`, `d`
/// - `@hourly` → 3600s
/// - `@daily` → 86400s
/// - `*/N * * * *` → every N minutes
fn parse_interval(expr: &str) -> Result<u64, SchedulerError> {
    let expr = expr.trim();

    if expr.eq_ignore_ascii_case("@hourly") {
        return Ok(3600);
    }
    if expr.eq_ignore_ascii_case("@daily") {
        return Ok(86400);
    }

    // @every 5m, @every 30s, @every 2h, @every 1d
    if let Some(rest) = expr
        .strip_prefix("@every ")
        .or_else(|| expr.strip_prefix("@every\t"))
    {
        let rest = rest.trim();
        if rest.is_empty() {
            return Err(SchedulerError::InvalidCron(expr.to_string()));
        }
        let (num_str, unit) = rest.split_at(rest.len() - 1);
        let num: u64 = num_str
            .parse()
            .map_err(|_| SchedulerError::InvalidCron(expr.to_string()))?;
        let multiplier = match unit {
            "s" => 1,
            "m" => 60,
            "h" => 3600,
            "d" => 86400,
            _ => return Err(SchedulerError::InvalidCron(expr.to_string())),
        };
        return Ok(num * multiplier);
    }

    // */N * * * * → every N minutes
    if expr.starts_with("*/") {
        let parts: Vec<&str> = expr.split_whitespace().collect();
        if parts.len() == 5 {
            if let Some(n_str) = parts[0].strip_prefix("*/") {
                if let Ok(n) = n_str.parse::<u64>() {
                    return Ok(n * 60);
                }
            }
        }
    }

    Err(SchedulerError::InvalidCron(expr.to_string()))
}

fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_interval_every() {
        assert_eq!(parse_interval("@every 5m").unwrap(), 300);
        assert_eq!(parse_interval("@every 30s").unwrap(), 30);
        assert_eq!(parse_interval("@every 2h").unwrap(), 7200);
        assert_eq!(parse_interval("@every 1d").unwrap(), 86400);
    }

    #[test]
    fn test_parse_interval_keywords() {
        assert_eq!(parse_interval("@hourly").unwrap(), 3600);
        assert_eq!(parse_interval("@daily").unwrap(), 86400);
    }

    #[test]
    fn test_parse_interval_cron() {
        assert_eq!(parse_interval("*/5 * * * *").unwrap(), 300);
        assert_eq!(parse_interval("*/1 * * * *").unwrap(), 60);
    }

    #[test]
    fn test_parse_interval_invalid() {
        assert!(parse_interval("bad").is_err());
        assert!(parse_interval("@every").is_err());
    }

    #[test]
    fn test_schedule_and_list() {
        let sched = FunctionScheduler::new();
        let entry = ScheduleEntry {
            name: "job1".to_string(),
            function_name: "myfn".to_string(),
            cron_expr: "@every 5m".to_string(),
            enabled: true,
            last_run: 0,
            next_run: 0,
        };
        sched.schedule(entry).unwrap();
        assert_eq!(sched.len(), 1);
        let list = sched.list();
        assert_eq!(list[0].name, "job1");
        assert!(list[0].next_run > 0);
    }

    #[test]
    fn test_cancel() {
        let sched = FunctionScheduler::new();
        let entry = ScheduleEntry {
            name: "job1".to_string(),
            function_name: "myfn".to_string(),
            cron_expr: "@hourly".to_string(),
            enabled: true,
            last_run: 0,
            next_run: 0,
        };
        sched.schedule(entry).unwrap();
        sched.cancel("job1").unwrap();
        assert!(sched.is_empty());
    }

    #[test]
    fn test_cancel_not_found() {
        let sched = FunctionScheduler::new();
        assert!(sched.cancel("nope").is_err());
    }

    #[test]
    fn test_next_runs() {
        let sched = FunctionScheduler::new();
        for i in 0..3 {
            let entry = ScheduleEntry {
                name: format!("job{}", i),
                function_name: "fn".to_string(),
                cron_expr: format!("@every {}m", i + 1),
                enabled: true,
                last_run: 0,
                next_run: 0,
            };
            sched.schedule(entry).unwrap();
        }
        let runs = sched.next_runs(2);
        assert_eq!(runs.len(), 2);
        assert!(runs[0].1 <= runs[1].1);
    }

    #[test]
    fn test_mark_executed() {
        let sched = FunctionScheduler::new();
        let entry = ScheduleEntry {
            name: "job1".to_string(),
            function_name: "fn".to_string(),
            cron_expr: "@every 10m".to_string(),
            enabled: true,
            last_run: 0,
            next_run: 0,
        };
        sched.schedule(entry).unwrap();
        sched.mark_executed("job1").unwrap();
        let list = sched.list();
        assert!(list[0].last_run > 0);
    }
}
