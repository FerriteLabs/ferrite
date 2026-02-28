#![allow(dead_code)]
//! LATENCY command family
//!
//! Implements Redis LATENCY monitoring: LATEST, HISTORY, RESET, GRAPH, HELP.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;

use crate::protocol::Frame;

/// Maximum number of latency samples per event (same as Redis).
const MAX_LATENCY_EVENTS: usize = 160;

/// A single latency sample.
#[derive(Debug, Clone)]
pub struct LatencyEvent {
    pub timestamp: u64,
    pub latency_ms: u64,
}

/// Thread-safe latency tracker.
#[derive(Debug)]
pub struct LatencyTracker {
    events: RwLock<HashMap<String, Vec<LatencyEvent>>>,
}

impl LatencyTracker {
    pub fn new() -> Self {
        Self {
            events: RwLock::new(HashMap::new()),
        }
    }

    /// Record a latency sample for the given event name.
    pub fn record(&self, event_name: &str, latency_ms: u64) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut events = self.events.write();
        let samples = events.entry(event_name.to_string()).or_default();
        samples.push(LatencyEvent {
            timestamp,
            latency_ms,
        });
        if samples.len() > MAX_LATENCY_EVENTS {
            samples.remove(0);
        }
    }

    /// LATENCY LATEST — return latest sample for each event type.
    /// Each entry: [event-name, timestamp, latency-ms, max-latency-ms]
    pub fn latest(&self) -> Frame {
        let events = self.events.read();
        if events.is_empty() {
            return Frame::array(vec![]);
        }

        let mut results: Vec<Frame> = Vec::new();
        for (name, samples) in events.iter() {
            if let Some(last) = samples.last() {
                let max_latency = samples.iter().map(|s| s.latency_ms).max().unwrap_or(0);
                results.push(Frame::array(vec![
                    Frame::bulk(name.clone()),
                    Frame::Integer(last.timestamp as i64),
                    Frame::Integer(last.latency_ms as i64),
                    Frame::Integer(max_latency as i64),
                ]));
            }
        }
        Frame::array(results)
    }

    /// LATENCY HISTORY event — return all samples for an event.
    /// Each entry: [timestamp, latency-ms]
    pub fn history(&self, event_name: &str) -> Frame {
        let events = self.events.read();
        match events.get(event_name) {
            Some(samples) => {
                let results: Vec<Frame> = samples
                    .iter()
                    .map(|s| {
                        Frame::array(vec![
                            Frame::Integer(s.timestamp as i64),
                            Frame::Integer(s.latency_ms as i64),
                        ])
                    })
                    .collect();
                Frame::array(results)
            }
            None => Frame::array(vec![]),
        }
    }

    /// LATENCY RESET [event ...] — reset events, return count of reset events.
    pub fn reset(&self, event_names: &[String]) -> Frame {
        let mut events = self.events.write();
        if event_names.is_empty() {
            let count = events.len() as i64;
            events.clear();
            Frame::Integer(count)
        } else {
            let mut count = 0i64;
            for name in event_names {
                if events.remove(name).is_some() {
                    count += 1;
                }
            }
            Frame::Integer(count)
        }
    }

    /// LATENCY GRAPH event — return ASCII art bar chart.
    pub fn graph(&self, event_name: &str) -> Frame {
        let events = self.events.read();
        match events.get(event_name) {
            Some(samples) if !samples.is_empty() => {
                let max_latency = samples
                    .iter()
                    .map(|s| s.latency_ms)
                    .max()
                    .unwrap_or(1)
                    .max(1);
                let graph_height = 10u64;

                let mut lines: Vec<String> = Vec::new();
                lines.push(format!(
                    "{} - high {max_latency} ms, low {} ms (all time high {max_latency} ms)",
                    event_name,
                    samples.iter().map(|s| s.latency_ms).min().unwrap_or(0),
                ));

                // Build columns
                let num_cols = samples.len().min(60);
                let start = if samples.len() > 60 {
                    samples.len() - 60
                } else {
                    0
                };
                let display_samples = &samples[start..];

                for row in (1..=graph_height).rev() {
                    let threshold = (row * max_latency) / graph_height;
                    let line: String = display_samples
                        .iter()
                        .take(num_cols)
                        .map(|s| if s.latency_ms >= threshold { '#' } else { ' ' })
                        .collect();
                    lines.push(line);
                }

                // Time axis
                let axis: String = (0..num_cols).map(|_| '-').collect();
                lines.push(axis);

                Frame::bulk(lines.join("\n"))
            }
            _ => Frame::bulk(""),
        }
    }

    /// LATENCY HELP — return help text.
    pub fn help(&self) -> Frame {
        Frame::array(vec![
            Frame::bulk("LATENCY <subcommand> [<arg> [value] [opt] ...]"),
            Frame::bulk("DOCTOR"),
            Frame::bulk("    Return a human readable latency analysis report."),
            Frame::bulk("GRAPH <event>"),
            Frame::bulk("    Return a latency graph for the <event>."),
            Frame::bulk("HISTORY <event>"),
            Frame::bulk("    Return latency samples for the <event>."),
            Frame::bulk("LATEST"),
            Frame::bulk("    Return the latest latency samples for all events."),
            Frame::bulk("RESET [<event> ...]"),
            Frame::bulk("    Reset latency data for events (default: all)."),
            Frame::bulk("HISTOGRAM [<command> ...]"),
            Frame::bulk("    Return latency histogram for commands."),
        ])
    }
}

impl Default for LatencyTracker {
    fn default() -> Self {
        Self::new()
    }
}

pub type SharedLatencyTracker = Arc<LatencyTracker>;
