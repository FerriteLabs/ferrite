//! Enhanced episodic memory with temporal indexing.
//!
//! Stores episodes (sequences of related events) with temporal indices for
//! efficient time-based recall and context-based retrieval.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

/// A single event within an episode.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpisodicEvent {
    /// When the event occurred.
    pub timestamp: SystemTime,
    /// Who performed the action (e.g., "user", "agent", "system").
    pub actor: String,
    /// What happened (e.g., "asked_question", "provided_answer", "tool_call").
    pub action: String,
    /// The content/payload of the event.
    pub content: String,
    /// Arbitrary metadata.
    pub metadata: HashMap<String, String>,
}

/// A coherent sequence of related events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Episode {
    /// Unique identifier.
    pub id: String,
    /// Ordered list of events in this episode.
    pub events: Vec<EpisodicEvent>,
    /// When the first event occurred.
    pub started_at: SystemTime,
    /// When the last event occurred.
    pub ended_at: SystemTime,
    /// Tags describing the context of this episode.
    pub context_tags: Vec<String>,
    /// Emotional valence from -1.0 (negative) to 1.0 (positive).
    pub emotional_valence: f64,
    /// Importance score (0.0 - 1.0).
    pub importance: f64,
    /// Number of times this episode has been recalled.
    pub access_count: u64,
    /// Auto-generated summary of the episode.
    pub summary: Option<String>,
}

/// Summary of episodes within a time period.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpisodeSummary {
    /// Start of the summarized period.
    pub period_start: SystemTime,
    /// End of the summarized period.
    pub period_end: SystemTime,
    /// Number of episodes in the period.
    pub episode_count: usize,
    /// Total number of events across all episodes.
    pub event_count: usize,
    /// Most common context tags with their counts.
    pub top_contexts: Vec<(String, usize)>,
    /// Average importance across episodes.
    pub avg_importance: f64,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn system_time_to_millis(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64
}

// ---------------------------------------------------------------------------
// EpisodicMemory
// ---------------------------------------------------------------------------

/// Enhanced episodic memory store with temporal indexing.
pub struct EpisodicMemory {
    episodes: RwLock<Vec<Episode>>,
    /// Maps timestamp_ms → episode indices for fast temporal lookup.
    temporal_index: RwLock<BTreeMap<u64, Vec<usize>>>,
    max_episodes: usize,
}

impl EpisodicMemory {
    /// Creates a new episodic memory store.
    pub fn new(max_episodes: usize) -> Self {
        Self {
            episodes: RwLock::new(Vec::new()),
            temporal_index: RwLock::new(BTreeMap::new()),
            max_episodes,
        }
    }

    /// Records an event, creating a new episode for it.
    pub fn record(&self, event: EpisodicEvent) {
        let mut episodes = self.episodes.write();
        let mut index = self.temporal_index.write();

        let ts = event.timestamp;
        let episode = Episode {
            id: uuid::Uuid::new_v4().to_string(),
            started_at: ts,
            ended_at: ts,
            context_tags: event
                .metadata
                .get("context")
                .map(|c| vec![c.clone()])
                .unwrap_or_default(),
            emotional_valence: 0.0,
            importance: 0.5,
            access_count: 0,
            summary: None,
            events: vec![event],
        };

        // Enforce capacity
        if episodes.len() >= self.max_episodes {
            // Remove the oldest episode
            if !episodes.is_empty() {
                let _oldest_ts = system_time_to_millis(episodes[0].started_at);
                episodes.remove(0);
                // Rebuild index since indices shifted
                index.clear();
                for (i, ep) in episodes.iter().enumerate() {
                    let ms = system_time_to_millis(ep.started_at);
                    index.entry(ms).or_default().push(i);
                }
            }
        }

        let idx = episodes.len();
        let ms = system_time_to_millis(ts);
        episodes.push(episode);
        index.entry(ms).or_default().push(idx);
    }

    /// Recalls the most recent episodes within `duration`, up to `limit`.
    pub fn recall_recent(&self, duration: Duration, limit: usize) -> Vec<Episode> {
        let episodes = self.episodes.read();
        let cutoff = SystemTime::now()
            .checked_sub(duration)
            .unwrap_or(UNIX_EPOCH);

        let mut results: Vec<Episode> = episodes
            .iter()
            .filter(|ep| ep.started_at >= cutoff)
            .cloned()
            .collect();

        // Most recent first
        results.sort_by(|a, b| b.started_at.cmp(&a.started_at));
        results.truncate(limit);
        results
    }

    /// Recalls episodes near a given timestamp within ±window.
    pub fn recall_around(&self, timestamp: SystemTime, window: Duration) -> Vec<Episode> {
        let episodes = self.episodes.read();
        let index = self.temporal_index.read();

        let center_ms = system_time_to_millis(timestamp);
        let window_ms = window.as_millis() as u64;
        let start_ms = center_ms.saturating_sub(window_ms);
        let end_ms = center_ms.saturating_add(window_ms);

        let mut seen = std::collections::HashSet::new();
        let mut results = Vec::new();

        for (_ts, indices) in index.range(start_ms..=end_ms) {
            for &idx in indices {
                if seen.insert(idx) {
                    if let Some(ep) = episodes.get(idx) {
                        results.push(ep.clone());
                    }
                }
            }
        }

        results.sort_by(|a, b| a.started_at.cmp(&b.started_at));
        results
    }

    /// Recalls episodes matching any of the given context tags.
    pub fn recall_by_context(&self, context_tags: &[&str], limit: usize) -> Vec<Episode> {
        let mut episodes = self.episodes.write();
        let mut results = Vec::new();

        for ep in episodes.iter_mut() {
            let matches = ep
                .context_tags
                .iter()
                .any(|tag| context_tags.contains(&tag.as_str()));
            if matches {
                ep.access_count += 1;
                results.push(ep.clone());
            }
        }

        // Most important first
        results.sort_by(|a, b| {
            b.importance
                .partial_cmp(&a.importance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(limit);
        results
    }

    /// Produces a summary of all episodes in the given time range.
    pub fn summarize_period(&self, start: SystemTime, end: SystemTime) -> EpisodeSummary {
        let episodes = self.episodes.read();

        let matching: Vec<&Episode> = episodes
            .iter()
            .filter(|ep| ep.started_at >= start && ep.ended_at <= end)
            .collect();

        let episode_count = matching.len();
        let event_count: usize = matching.iter().map(|ep| ep.events.len()).sum();
        let avg_importance = if episode_count > 0 {
            matching.iter().map(|ep| ep.importance).sum::<f64>() / episode_count as f64
        } else {
            0.0
        };

        // Count context tags
        let mut tag_counts: HashMap<String, usize> = HashMap::new();
        for ep in &matching {
            for tag in &ep.context_tags {
                *tag_counts.entry(tag.clone()).or_insert(0) += 1;
            }
        }
        let mut top_contexts: Vec<(String, usize)> = tag_counts.into_iter().collect();
        top_contexts.sort_by(|a, b| b.1.cmp(&a.1));

        EpisodeSummary {
            period_start: start,
            period_end: end,
            episode_count,
            event_count,
            top_contexts,
            avg_importance,
        }
    }

    /// Forgets (removes) all episodes that started before the given time.
    /// Returns the number of episodes removed.
    pub fn forget_before(&self, before: SystemTime) -> usize {
        let mut episodes = self.episodes.write();
        let mut index = self.temporal_index.write();

        let original_len = episodes.len();
        episodes.retain(|ep| ep.started_at >= before);
        let removed = original_len - episodes.len();

        if removed > 0 {
            // Rebuild temporal index
            index.clear();
            for (i, ep) in episodes.iter().enumerate() {
                let ms = system_time_to_millis(ep.started_at);
                index.entry(ms).or_default().push(i);
            }
        }

        removed
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_event(actor: &str, action: &str, content: &str) -> EpisodicEvent {
        EpisodicEvent {
            timestamp: SystemTime::now(),
            actor: actor.to_string(),
            action: action.to_string(),
            content: content.to_string(),
            metadata: HashMap::new(),
        }
    }

    fn make_event_with_context(content: &str, context: &str) -> EpisodicEvent {
        let mut metadata = HashMap::new();
        metadata.insert("context".to_string(), context.to_string());
        EpisodicEvent {
            timestamp: SystemTime::now(),
            actor: "agent".to_string(),
            action: "test".to_string(),
            content: content.to_string(),
            metadata,
        }
    }

    #[test]
    fn test_record_and_recall_recent() {
        let mem = EpisodicMemory::new(100);
        mem.record(make_event("user", "asked_question", "What is Rust?"));
        mem.record(make_event("agent", "provided_answer", "A systems language"));

        let recent = mem.recall_recent(Duration::from_secs(60), 10);
        assert_eq!(recent.len(), 2);
    }

    #[test]
    fn test_max_episodes_enforced() {
        let mem = EpisodicMemory::new(3);
        for i in 0..5 {
            mem.record(make_event("user", "action", &format!("event {i}")));
        }
        let recent = mem.recall_recent(Duration::from_secs(60), 100);
        assert!(recent.len() <= 3);
    }

    #[test]
    fn test_recall_by_context() {
        let mem = EpisodicMemory::new(100);
        mem.record(make_event_with_context("Rust question", "programming"));
        mem.record(make_event_with_context("Dinner plans", "personal"));
        mem.record(make_event_with_context("Code review", "programming"));

        let results = mem.recall_by_context(&["programming"], 10);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_recall_around() {
        let mem = EpisodicMemory::new(100);
        mem.record(make_event("user", "test", "event 1"));
        let center = SystemTime::now();
        mem.record(make_event("user", "test", "event 2"));

        let results = mem.recall_around(center, Duration::from_secs(5));
        assert!(!results.is_empty());
    }

    #[test]
    fn test_summarize_period() {
        let mem = EpisodicMemory::new(100);
        let start = SystemTime::now();
        mem.record(make_event_with_context("e1", "coding"));
        mem.record(make_event_with_context("e2", "coding"));
        mem.record(make_event_with_context("e3", "review"));
        let end = SystemTime::now() + Duration::from_secs(1);

        let summary = mem.summarize_period(start, end);
        assert_eq!(summary.episode_count, 3);
        assert_eq!(summary.event_count, 3);
        assert!(!summary.top_contexts.is_empty());
    }

    #[test]
    fn test_forget_before() {
        let mem = EpisodicMemory::new(100);
        mem.record(make_event("user", "test", "old event"));
        std::thread::sleep(Duration::from_millis(10));
        let cutoff = SystemTime::now();
        std::thread::sleep(Duration::from_millis(10));
        mem.record(make_event("user", "test", "new event"));

        let removed = mem.forget_before(cutoff);
        assert_eq!(removed, 1);

        let remaining = mem.recall_recent(Duration::from_secs(60), 100);
        assert_eq!(remaining.len(), 1);
    }

    #[test]
    fn test_empty_recall() {
        let mem = EpisodicMemory::new(100);
        let results = mem.recall_recent(Duration::from_secs(60), 10);
        assert!(results.is_empty());

        let summary = mem.summarize_period(SystemTime::now(), SystemTime::now());
        assert_eq!(summary.episode_count, 0);
        assert_eq!(summary.avg_importance, 0.0);
    }
}
