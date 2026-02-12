//! Working memory management with token budgeting and context windows.
//!
//! Manages the limited-capacity working memory that represents an agent's
//! current focus and active context.

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

/// Strategy for managing working memory when capacity is exceeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EvictionStrategy {
    /// Remove least recently added (FIFO).
    Fifo,
    /// Remove least recently accessed (LRU).
    Lru,
    /// Remove lowest importance first.
    LowestImportance,
    /// Summarize and compress older entries.
    Summarize,
}

impl Default for EvictionStrategy {
    fn default() -> Self {
        Self::Lru
    }
}

/// Configuration for working memory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkingMemoryConfig {
    /// Maximum number of entries.
    pub max_entries: usize,
    /// Maximum total token count.
    pub max_tokens: usize,
    /// Eviction strategy when full.
    pub eviction_strategy: EvictionStrategy,
    /// Estimated tokens per character (for token budget calculation).
    pub tokens_per_char: f64,
}

impl Default for WorkingMemoryConfig {
    fn default() -> Self {
        Self {
            max_entries: 20,
            max_tokens: 4096,
            tokens_per_char: 0.25, // ~4 chars per token
            eviction_strategy: EvictionStrategy::Lru,
        }
    }
}

/// A working memory slot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkingMemorySlot {
    /// Reference to the memory entry ID.
    pub memory_id: String,
    /// Content summary.
    pub content: String,
    /// Estimated token count.
    pub estimated_tokens: usize,
    /// Priority/importance score.
    pub priority: f64,
    /// Access timestamp (nanoseconds since epoch).
    pub last_accessed_ns: u64,
}

/// Manages the working memory context window for an agent.
pub struct WorkingMemoryManager {
    config: WorkingMemoryConfig,
    slots: VecDeque<WorkingMemorySlot>,
    total_tokens: usize,
}

impl WorkingMemoryManager {
    pub fn new(config: WorkingMemoryConfig) -> Self {
        Self {
            config,
            slots: VecDeque::new(),
            total_tokens: 0,
        }
    }

    /// Adds a slot to working memory, evicting if necessary.
    pub fn add(&mut self, slot: WorkingMemorySlot) {
        // Evict if at capacity
        while self.slots.len() >= self.config.max_entries
            || self.total_tokens + slot.estimated_tokens > self.config.max_tokens
        {
            if let Some(evicted) = self.evict_one() {
                self.total_tokens = self.total_tokens.saturating_sub(evicted.estimated_tokens);
            } else {
                break;
            }
        }

        self.total_tokens += slot.estimated_tokens;
        self.slots.push_back(slot);
    }

    /// Returns the current working memory contents.
    pub fn contents(&self) -> Vec<&WorkingMemorySlot> {
        self.slots.iter().collect()
    }

    /// Estimates token count for a given text.
    pub fn estimate_tokens(&self, text: &str) -> usize {
        (text.len() as f64 * self.config.tokens_per_char).ceil() as usize
    }

    /// Returns current token usage.
    pub fn token_usage(&self) -> usize {
        self.total_tokens
    }

    /// Returns number of occupied slots.
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// Returns whether working memory is empty.
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Clears all working memory.
    pub fn clear(&mut self) {
        self.slots.clear();
        self.total_tokens = 0;
    }

    fn evict_one(&mut self) -> Option<WorkingMemorySlot> {
        match self.config.eviction_strategy {
            EvictionStrategy::Fifo => self.slots.pop_front(),
            EvictionStrategy::Lru => {
                if self.slots.is_empty() {
                    return None;
                }
                let min_idx = self
                    .slots
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, s)| s.last_accessed_ns)
                    .map(|(i, _)| i)
                    .unwrap_or(0);
                self.slots.remove(min_idx)
            }
            EvictionStrategy::LowestImportance => {
                if self.slots.is_empty() {
                    return None;
                }
                let min_idx = self
                    .slots
                    .iter()
                    .enumerate()
                    .min_by(|(_, a), (_, b)| {
                        a.priority
                            .partial_cmp(&b.priority)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .map(|(i, _)| i)
                    .unwrap_or(0);
                self.slots.remove(min_idx)
            }
            EvictionStrategy::Summarize => {
                // Fallback to FIFO for now; full summarization requires LLM integration
                self.slots.pop_front()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_slot(id: &str, tokens: usize, priority: f64) -> WorkingMemorySlot {
        WorkingMemorySlot {
            memory_id: id.to_string(),
            content: "test content".to_string(),
            estimated_tokens: tokens,
            priority,
            last_accessed_ns: 0,
        }
    }

    #[test]
    fn test_add_and_retrieve() {
        let mut wm = WorkingMemoryManager::new(WorkingMemoryConfig::default());
        wm.add(make_slot("m1", 100, 0.5));
        wm.add(make_slot("m2", 100, 0.8));

        assert_eq!(wm.len(), 2);
        assert_eq!(wm.token_usage(), 200);
    }

    #[test]
    fn test_fifo_eviction() {
        let mut wm = WorkingMemoryManager::new(WorkingMemoryConfig {
            max_entries: 2,
            max_tokens: 10000,
            eviction_strategy: EvictionStrategy::Fifo,
            ..Default::default()
        });

        wm.add(make_slot("m1", 10, 0.5));
        wm.add(make_slot("m2", 10, 0.8));
        wm.add(make_slot("m3", 10, 0.3));

        assert_eq!(wm.len(), 2);
        let ids: Vec<_> = wm.contents().iter().map(|s| s.memory_id.as_str()).collect();
        assert_eq!(ids, vec!["m2", "m3"]); // m1 was evicted
    }

    #[test]
    fn test_lowest_importance_eviction() {
        let mut wm = WorkingMemoryManager::new(WorkingMemoryConfig {
            max_entries: 2,
            max_tokens: 10000,
            eviction_strategy: EvictionStrategy::LowestImportance,
            ..Default::default()
        });

        wm.add(make_slot("m1", 10, 0.5));
        wm.add(make_slot("m2", 10, 0.8));
        wm.add(make_slot("m3", 10, 0.3));

        assert_eq!(wm.len(), 2);
        // m3 replaced lowest (m1 at 0.5), but actually after evicting m1, we add m3
        let priorities: Vec<f64> = wm.contents().iter().map(|s| s.priority).collect();
        assert!(priorities.contains(&0.8));
    }

    #[test]
    fn test_token_budget_eviction() {
        let mut wm = WorkingMemoryManager::new(WorkingMemoryConfig {
            max_entries: 100,
            max_tokens: 250,
            eviction_strategy: EvictionStrategy::Fifo,
            ..Default::default()
        });

        wm.add(make_slot("m1", 100, 0.5));
        wm.add(make_slot("m2", 100, 0.5));
        wm.add(make_slot("m3", 100, 0.5)); // This should evict m1

        assert!(wm.token_usage() <= 250);
    }

    #[test]
    fn test_estimate_tokens() {
        let wm = WorkingMemoryManager::new(WorkingMemoryConfig::default());
        let tokens = wm.estimate_tokens("Hello world"); // 11 chars * 0.25 = 2.75 -> 3
        assert_eq!(tokens, 3);
    }

    #[test]
    fn test_clear() {
        let mut wm = WorkingMemoryManager::new(WorkingMemoryConfig::default());
        wm.add(make_slot("m1", 100, 0.5));
        wm.clear();
        assert!(wm.is_empty());
        assert_eq!(wm.token_usage(), 0);
    }
}
