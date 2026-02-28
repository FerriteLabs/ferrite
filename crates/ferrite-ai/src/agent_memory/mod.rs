//! AI Agent Memory Store
//!
//! Purpose-built memory backend for AI agents with episodic, semantic, and
//! procedural memory types. Supports automatic decay, relevance scoring,
//! context window management, and MCP (Model Context Protocol) integration.
//!
//! # Architecture
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────┐
//! │                    Agent Memory Store                       │
//! ├────────────────────────────────────────────────────────────┤
//! │  ┌──────────────┐ ┌──────────────┐ ┌────────────────────┐ │
//! │  │   Episodic   │ │   Semantic   │ │    Procedural      │ │
//! │  │   Memory     │ │   Memory     │ │    Memory          │ │
//! │  │  (events)    │ │  (knowledge) │ │  (learned skills)  │ │
//! │  └──────────────┘ └──────────────┘ └────────────────────┘ │
//! │  ┌──────────────┐ ┌──────────────┐ ┌────────────────────┐ │
//! │  │   Working    │ │   Decay      │ │   MCP Protocol     │ │
//! │  │   Memory     │ │   Engine     │ │   Server           │ │
//! │  │  (context)   │ │  (forgetting)│ │  (integration)     │ │
//! │  └──────────────┘ └──────────────┘ └────────────────────┘ │
//! └────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Memory Types
//!
//! - **Episodic**: Specific events and interactions (timestamped, decayable)
//! - **Semantic**: General knowledge and facts (persistent, searchable)
//! - **Procedural**: Learned skills and patterns (action sequences, heuristics)
//! - **Working**: Current context window (limited capacity, most relevant)
//!
//! # Example
//!
//! ```rust,ignore
//! use ferrite::agent_memory::{AgentMemoryStore, MemoryConfig, MemoryType};
//!
//! let store = AgentMemoryStore::new(MemoryConfig::default());
//!
//! // Store an episodic memory
//! store.store("agent-1", MemoryEntry {
//!     content: "User asked about Rust performance".into(),
//!     memory_type: MemoryType::Episodic,
//!     importance: 0.8,
//!     ..Default::default()
//! })?;
//!
//! // Recall relevant memories
//! let memories = store.recall("agent-1", "programming performance", 5)?;
//! ```

#![allow(dead_code)]
pub mod decay;
pub mod episodic;
pub mod feature_store;
pub mod mcp;
pub mod procedural;
pub mod semantic_memory;
pub mod sharing;
pub mod store;
pub mod working;

pub use episodic::{Episode, EpisodeSummary, EpisodicEvent, EpisodicMemory};
pub use procedural::{ActionRecord, ActionStep, ProceduralMemory, Skill, SkillFeedback};
pub use semantic_memory::{
    cosine_similarity, SemanticEntry, SemanticMemoryStore, SemanticSearchResult,
};
pub use sharing::{MemorySharingHub, SharedMemory, SharingError, SharingPermission, Visibility};
pub use store::{
    AgentMemoryConfig, AgentMemoryStats, MemoryError, PersistentAgentMemoryStore,
    StoreMemoryEntry, StoreMemoryType,
};

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the agent memory store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryConfig {
    /// Maximum number of memories per agent.
    pub max_memories_per_agent: usize,
    /// Maximum working memory slots.
    pub working_memory_capacity: usize,
    /// Default decay rate (0.0 = no decay, 1.0 = instant decay).
    pub default_decay_rate: f64,
    /// Minimum importance threshold for retention during compaction.
    pub importance_threshold: f64,
    /// How often to run decay/compaction (seconds).
    pub compaction_interval_secs: u64,
    /// Enable automatic summarization of old episodic memories.
    pub auto_summarize: bool,
    /// Maximum token budget for working memory context.
    pub max_context_tokens: usize,
    /// Enable MCP server for external agent framework integration.
    pub mcp_enabled: bool,
    /// MCP server listen address.
    pub mcp_listen_addr: String,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            max_memories_per_agent: 10_000,
            working_memory_capacity: 20,
            default_decay_rate: 0.01,
            importance_threshold: 0.1,
            compaction_interval_secs: 300,
            auto_summarize: true,
            max_context_tokens: 4096,
            mcp_enabled: false,
            mcp_listen_addr: "127.0.0.1:3100".to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// Memory types
// ---------------------------------------------------------------------------

/// Classification of memory types following cognitive science models.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MemoryType {
    /// Specific events and interactions with timestamps.
    Episodic,
    /// General knowledge and facts.
    Semantic,
    /// Learned skills, action sequences, and heuristics.
    Procedural,
    /// Current active context (limited capacity).
    Working,
}

impl Default for MemoryType {
    fn default() -> Self {
        Self::Episodic
    }
}

/// A single memory entry stored for an agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryEntry {
    /// Unique identifier.
    pub id: String,
    /// The memory content (text).
    pub content: String,
    /// Type of memory.
    pub memory_type: MemoryType,
    /// Importance score (0.0 - 1.0).
    pub importance: f64,
    /// When this memory was created.
    pub created_at: SystemTime,
    /// When this memory was last accessed.
    pub last_accessed: SystemTime,
    /// Number of times this memory was recalled.
    pub access_count: u64,
    /// Current strength after decay (0.0 - 1.0).
    pub strength: f64,
    /// Optional embedding vector for semantic search.
    pub embedding: Option<Vec<f32>>,
    /// Arbitrary metadata tags.
    pub metadata: HashMap<String, String>,
}

impl Default for MemoryEntry {
    fn default() -> Self {
        let now = SystemTime::now();
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            content: String::new(),
            memory_type: MemoryType::Episodic,
            importance: 0.5,
            created_at: now,
            last_accessed: now,
            access_count: 0,
            strength: 1.0,
            embedding: None,
            metadata: HashMap::new(),
        }
    }
}

/// Result of a memory recall operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecallResult {
    /// The memory entry.
    pub entry: MemoryEntry,
    /// Relevance score for this recall (0.0 - 1.0).
    pub relevance: f64,
}

// ---------------------------------------------------------------------------
// Agent Memory Store
// ---------------------------------------------------------------------------

/// The main agent memory store managing memories for multiple agents.
pub struct AgentMemoryStore {
    config: MemoryConfig,
    agents: RwLock<HashMap<String, AgentMemory>>,
    stats: MemoryStats,
}

/// Per-agent memory collection.
struct AgentMemory {
    memories: Vec<MemoryEntry>,
    working_memory: Vec<String>, // IDs of entries in working memory
}

impl AgentMemory {
    fn new() -> Self {
        Self {
            memories: Vec::new(),
            working_memory: Vec::new(),
        }
    }
}

/// Global memory store statistics.
#[derive(Debug, Default)]
pub struct MemoryStats {
    pub total_memories: AtomicU64,
    pub total_recalls: AtomicU64,
    pub total_stores: AtomicU64,
    pub total_compactions: AtomicU64,
    pub total_decayed: AtomicU64,
}

impl AgentMemoryStore {
    /// Creates a new agent memory store with the given configuration.
    pub fn new(config: MemoryConfig) -> Self {
        Self {
            config,
            agents: RwLock::new(HashMap::new()),
            stats: MemoryStats::default(),
        }
    }

    /// Stores a new memory for an agent.
    pub fn store(&self, agent_id: &str, entry: MemoryEntry) -> Result<String, AgentMemoryError> {
        let mut agents = self.agents.write();
        let agent = agents
            .entry(agent_id.to_string())
            .or_insert_with(AgentMemory::new);

        if agent.memories.len() >= self.config.max_memories_per_agent {
            // Compact: remove weakest memories
            agent.memories.sort_by(|a, b| {
                a.strength
                    .partial_cmp(&b.strength)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            agent
                .memories
                .truncate(self.config.max_memories_per_agent - 1);
        }

        let id = entry.id.clone();
        agent.memories.push(entry);
        self.stats.total_stores.fetch_add(1, Ordering::Relaxed);
        self.stats.total_memories.fetch_add(1, Ordering::Relaxed);

        Ok(id)
    }

    /// Stores an episodic memory with a simple API.
    pub fn store_episodic(
        &self,
        agent_id: &str,
        content: &str,
        importance: f64,
    ) -> Result<String, AgentMemoryError> {
        let entry = MemoryEntry {
            content: content.to_string(),
            memory_type: MemoryType::Episodic,
            importance: importance.clamp(0.0, 1.0),
            ..Default::default()
        };
        self.store(agent_id, entry)
    }

    /// Stores a semantic (knowledge) memory.
    pub fn store_semantic(
        &self,
        agent_id: &str,
        content: &str,
        importance: f64,
    ) -> Result<String, AgentMemoryError> {
        let entry = MemoryEntry {
            content: content.to_string(),
            memory_type: MemoryType::Semantic,
            importance: importance.clamp(0.0, 1.0),
            strength: 1.0, // Semantic memories don't decay by default
            ..Default::default()
        };
        self.store(agent_id, entry)
    }

    /// Stores a procedural (skill) memory.
    pub fn store_procedural(
        &self,
        agent_id: &str,
        content: &str,
        importance: f64,
    ) -> Result<String, AgentMemoryError> {
        let entry = MemoryEntry {
            content: content.to_string(),
            memory_type: MemoryType::Procedural,
            importance: importance.clamp(0.0, 1.0),
            ..Default::default()
        };
        self.store(agent_id, entry)
    }

    /// Recalls the most relevant memories for a given query.
    pub fn recall(
        &self,
        agent_id: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<RecallResult>, AgentMemoryError> {
        let agents = self.agents.read();
        let agent = agents
            .get(agent_id)
            .ok_or_else(|| AgentMemoryError::AgentNotFound(agent_id.to_string()))?;

        self.stats.total_recalls.fetch_add(1, Ordering::Relaxed);

        let query_lower = query.to_lowercase();
        let query_words: Vec<&str> = query_lower.split_whitespace().collect();

        let mut results: Vec<RecallResult> = agent
            .memories
            .iter()
            .filter(|m| m.strength > self.config.importance_threshold)
            .map(|entry| {
                let relevance = compute_text_relevance(&query_words, &entry.content);
                let recency_boost = compute_recency_boost(entry.last_accessed);
                let strength_factor = entry.strength;
                let importance_factor = entry.importance;

                let final_score = relevance * 0.4
                    + recency_boost * 0.2
                    + strength_factor * 0.2
                    + importance_factor * 0.2;

                RecallResult {
                    entry: entry.clone(),
                    relevance: final_score.clamp(0.0, 1.0),
                }
            })
            .collect();

        results.sort_by(|a, b| {
            b.relevance
                .partial_cmp(&a.relevance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(limit);

        Ok(results)
    }

    /// Recalls memories filtered by type.
    pub fn recall_by_type(
        &self,
        agent_id: &str,
        memory_type: MemoryType,
        limit: usize,
    ) -> Result<Vec<MemoryEntry>, AgentMemoryError> {
        let agents = self.agents.read();
        let agent = agents
            .get(agent_id)
            .ok_or_else(|| AgentMemoryError::AgentNotFound(agent_id.to_string()))?;

        let mut entries: Vec<_> = agent
            .memories
            .iter()
            .filter(|m| {
                m.memory_type == memory_type && m.strength > self.config.importance_threshold
            })
            .cloned()
            .collect();

        entries.sort_by(|a, b| {
            b.last_accessed
                .partial_cmp(&a.last_accessed)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        entries.truncate(limit);

        Ok(entries)
    }

    /// Gets the current working memory context for an agent.
    pub fn working_memory(&self, agent_id: &str) -> Result<Vec<MemoryEntry>, AgentMemoryError> {
        let agents = self.agents.read();
        let agent = agents
            .get(agent_id)
            .ok_or_else(|| AgentMemoryError::AgentNotFound(agent_id.to_string()))?;

        let entries: Vec<_> = agent
            .memories
            .iter()
            .filter(|m| agent.working_memory.contains(&m.id))
            .cloned()
            .collect();

        Ok(entries)
    }

    /// Promotes a memory entry to working memory.
    pub fn promote_to_working(
        &self,
        agent_id: &str,
        memory_id: &str,
    ) -> Result<(), AgentMemoryError> {
        let mut agents = self.agents.write();
        let agent = agents
            .get_mut(agent_id)
            .ok_or_else(|| AgentMemoryError::AgentNotFound(agent_id.to_string()))?;

        if !agent.memories.iter().any(|m| m.id == memory_id) {
            return Err(AgentMemoryError::MemoryNotFound(memory_id.to_string()));
        }

        if !agent.working_memory.contains(&memory_id.to_string()) {
            if agent.working_memory.len() >= self.config.working_memory_capacity {
                agent.working_memory.remove(0); // FIFO eviction
            }
            agent.working_memory.push(memory_id.to_string());
        }

        Ok(())
    }

    /// Removes all memories for an agent.
    pub fn clear_agent(&self, agent_id: &str) -> Result<u64, AgentMemoryError> {
        let mut agents = self.agents.write();
        if let Some(agent) = agents.remove(agent_id) {
            let count = agent.memories.len() as u64;
            Ok(count)
        } else {
            Err(AgentMemoryError::AgentNotFound(agent_id.to_string()))
        }
    }

    /// Returns the number of memories for an agent.
    pub fn memory_count(&self, agent_id: &str) -> usize {
        self.agents
            .read()
            .get(agent_id)
            .map(|a| a.memories.len())
            .unwrap_or(0)
    }

    /// Lists all known agent IDs.
    pub fn list_agents(&self) -> Vec<String> {
        self.agents.read().keys().cloned().collect()
    }

    /// Applies decay to all episodic memories across all agents.
    pub fn apply_decay(&self) {
        let mut agents = self.agents.write();
        let mut total_decayed = 0u64;

        for agent in agents.values_mut() {
            for memory in &mut agent.memories {
                if memory.memory_type == MemoryType::Episodic {
                    memory.strength *= 1.0 - self.config.default_decay_rate;
                    if memory.strength < self.config.importance_threshold {
                        total_decayed += 1;
                    }
                }
            }
            // Remove fully decayed memories
            let threshold = self.config.importance_threshold;
            agent
                .memories
                .retain(|m| m.memory_type != MemoryType::Episodic || m.strength >= threshold);
        }

        self.stats
            .total_decayed
            .fetch_add(total_decayed, Ordering::Relaxed);
        self.stats.total_compactions.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns store statistics.
    pub fn stats_snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            total_memories: self.stats.total_memories.load(Ordering::Relaxed),
            total_recalls: self.stats.total_recalls.load(Ordering::Relaxed),
            total_stores: self.stats.total_stores.load(Ordering::Relaxed),
            total_compactions: self.stats.total_compactions.load(Ordering::Relaxed),
            total_decayed: self.stats.total_decayed.load(Ordering::Relaxed),
            agent_count: self.agents.read().len() as u64,
        }
    }
}

/// Snapshot of memory store statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsSnapshot {
    pub total_memories: u64,
    pub total_recalls: u64,
    pub total_stores: u64,
    pub total_compactions: u64,
    pub total_decayed: u64,
    pub agent_count: u64,
}

// ---------------------------------------------------------------------------
// Relevance computation helpers
// ---------------------------------------------------------------------------

fn compute_text_relevance(query_words: &[&str], content: &str) -> f64 {
    if query_words.is_empty() {
        return 0.0;
    }
    let content_lower = content.to_lowercase();
    let matches = query_words
        .iter()
        .filter(|w| content_lower.contains(**w))
        .count();
    matches as f64 / query_words.len() as f64
}

fn compute_recency_boost(last_accessed: SystemTime) -> f64 {
    let elapsed = SystemTime::now()
        .duration_since(last_accessed)
        .unwrap_or(Duration::from_secs(86400));

    let hours = elapsed.as_secs_f64() / 3600.0;
    // Exponential decay: boost of 1.0 for recent, approaches 0 for old
    (-hours / 24.0).exp()
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors from the agent memory store.
#[derive(Debug, Clone, thiserror::Error)]
pub enum AgentMemoryError {
    #[error("agent not found: {0}")]
    AgentNotFound(String),

    #[error("memory not found: {0}")]
    MemoryNotFound(String),

    #[error("working memory full: capacity {0}")]
    WorkingMemoryFull(usize),

    #[error("memory store limit exceeded for agent: {0}")]
    StoreLimitExceeded(String),

    #[error("invalid importance score: {0} (must be 0.0-1.0)")]
    InvalidImportance(f64),

    #[error("embedding dimension mismatch: expected {expected}, got {got}")]
    EmbeddingDimensionMismatch { expected: usize, got: usize },

    #[error("MCP protocol error: {0}")]
    McpError(String),
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = MemoryConfig::default();
        assert_eq!(config.max_memories_per_agent, 10_000);
        assert_eq!(config.working_memory_capacity, 20);
        assert!(!config.mcp_enabled);
    }

    #[test]
    fn test_store_and_recall() {
        let store = AgentMemoryStore::new(MemoryConfig::default());

        store
            .store_episodic("agent-1", "User asked about Rust performance", 0.9)
            .unwrap();
        store
            .store_episodic("agent-1", "User prefers Python for scripting", 0.7)
            .unwrap();
        store
            .store_episodic("agent-1", "Discussion about database design", 0.8)
            .unwrap();

        let results = store.recall("agent-1", "Rust performance", 2).unwrap();
        assert!(!results.is_empty());
        assert!(results.len() <= 2);
        // Most relevant should be about Rust performance
        assert!(results[0].entry.content.contains("Rust"));
    }

    #[test]
    fn test_store_different_types() {
        let store = AgentMemoryStore::new(MemoryConfig::default());

        store.store_episodic("a1", "meeting happened", 0.5).unwrap();
        store
            .store_semantic("a1", "Rust is a systems language", 0.8)
            .unwrap();
        store
            .store_procedural("a1", "always check return types", 0.7)
            .unwrap();

        let episodic = store
            .recall_by_type("a1", MemoryType::Episodic, 10)
            .unwrap();
        assert_eq!(episodic.len(), 1);

        let semantic = store
            .recall_by_type("a1", MemoryType::Semantic, 10)
            .unwrap();
        assert_eq!(semantic.len(), 1);
        assert!(semantic[0].content.contains("Rust"));

        let procedural = store
            .recall_by_type("a1", MemoryType::Procedural, 10)
            .unwrap();
        assert_eq!(procedural.len(), 1);
    }

    #[test]
    fn test_agent_not_found() {
        let store = AgentMemoryStore::new(MemoryConfig::default());
        let result = store.recall("nonexistent", "query", 5);
        assert!(matches!(result, Err(AgentMemoryError::AgentNotFound(_))));
    }

    #[test]
    fn test_working_memory() {
        let store = AgentMemoryStore::new(MemoryConfig {
            working_memory_capacity: 2,
            ..Default::default()
        });

        let id1 = store.store_episodic("a1", "memory 1", 0.5).unwrap();
        let id2 = store.store_episodic("a1", "memory 2", 0.5).unwrap();
        let id3 = store.store_episodic("a1", "memory 3", 0.5).unwrap();

        store.promote_to_working("a1", &id1).unwrap();
        store.promote_to_working("a1", &id2).unwrap();

        let wm = store.working_memory("a1").unwrap();
        assert_eq!(wm.len(), 2);

        // Adding third should evict first (FIFO)
        store.promote_to_working("a1", &id3).unwrap();
        let wm = store.working_memory("a1").unwrap();
        assert_eq!(wm.len(), 2);
    }

    #[test]
    fn test_decay() {
        let store = AgentMemoryStore::new(MemoryConfig {
            default_decay_rate: 0.99, // Very aggressive decay
            importance_threshold: 0.5,
            ..Default::default()
        });

        store.store_episodic("a1", "will decay", 0.5).unwrap();
        store.store_semantic("a1", "won't decay", 0.8).unwrap();

        assert_eq!(store.memory_count("a1"), 2);

        // Apply decay — episodic should drop below threshold
        store.apply_decay();
        assert_eq!(store.memory_count("a1"), 1); // Only semantic remains
    }

    #[test]
    fn test_clear_agent() {
        let store = AgentMemoryStore::new(MemoryConfig::default());
        store.store_episodic("a1", "memory", 0.5).unwrap();
        store.store_episodic("a1", "memory 2", 0.5).unwrap();

        let cleared = store.clear_agent("a1").unwrap();
        assert_eq!(cleared, 2);
        assert_eq!(store.memory_count("a1"), 0);
    }

    #[test]
    fn test_list_agents() {
        let store = AgentMemoryStore::new(MemoryConfig::default());
        store.store_episodic("agent-1", "m1", 0.5).unwrap();
        store.store_episodic("agent-2", "m2", 0.5).unwrap();

        let mut agents = store.list_agents();
        agents.sort();
        assert_eq!(agents, vec!["agent-1", "agent-2"]);
    }

    #[test]
    fn test_stats() {
        let store = AgentMemoryStore::new(MemoryConfig::default());
        store.store_episodic("a1", "m1", 0.5).unwrap();
        store.store_episodic("a1", "m2", 0.5).unwrap();
        let _ = store.recall("a1", "query", 5);

        let stats = store.stats_snapshot();
        assert_eq!(stats.total_stores, 2);
        assert_eq!(stats.total_recalls, 1);
        assert_eq!(stats.agent_count, 1);
    }

    #[test]
    fn test_memory_entry_default() {
        let entry = MemoryEntry::default();
        assert_eq!(entry.memory_type, MemoryType::Episodic);
        assert_eq!(entry.importance, 0.5);
        assert_eq!(entry.strength, 1.0);
        assert!(!entry.id.is_empty());
    }

    #[test]
    fn test_text_relevance() {
        let words: Vec<&str> = vec!["rust", "performance"];
        assert!(compute_text_relevance(&words, "Rust has great performance") > 0.5);
        assert!(compute_text_relevance(&words, "Python is dynamic") < 0.1);
        assert_eq!(compute_text_relevance(&[], "anything"), 0.0);
    }

    #[test]
    fn test_importance_clamping() {
        let store = AgentMemoryStore::new(MemoryConfig::default());
        store.store_episodic("a1", "test", 1.5).unwrap(); // Should clamp to 1.0

        let memories = store.recall_by_type("a1", MemoryType::Episodic, 1).unwrap();
        assert!((memories[0].importance - 1.0).abs() < f64::EPSILON);
    }
}
