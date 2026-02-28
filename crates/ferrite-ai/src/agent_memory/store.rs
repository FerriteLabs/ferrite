//! AI Agent Memory Store
//!
//! Purpose-built persistent memory for AI agents with vector-indexed
//! recall, conversation history, and tool call caching.
//!
//! Designed for LangChain, LlamaIndex, AutoGen, and similar frameworks.
#![allow(dead_code)]

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the purpose-built agent memory store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentMemoryConfig {
    /// Maximum number of memories across all agents.
    pub max_memories: usize,
    /// Maximum number of concurrent conversations/agents tracked.
    pub max_conversations: usize,
    /// Embedding vector dimension (default 384 for MiniLM).
    pub embedding_dimension: usize,
    /// Minimum cosine similarity for semantic recall results.
    pub similarity_threshold: f32,
    /// Optional TTL for memories (None = persist forever).
    pub ttl: Option<Duration>,
    /// Enable semantic search (term-based cosine similarity).
    pub enable_semantic_search: bool,
}

impl Default for AgentMemoryConfig {
    fn default() -> Self {
        Self {
            max_memories: 10_000,
            max_conversations: 100,
            embedding_dimension: 384,
            similarity_threshold: 0.75,
            ttl: None,
            enable_semantic_search: true,
        }
    }
}

// ---------------------------------------------------------------------------
// Memory types
// ---------------------------------------------------------------------------

/// Classification of agent memory entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StoreMemoryType {
    /// Conversation history entries.
    Conversation,
    /// Factual knowledge.
    Fact,
    /// Cached tool/function call results.
    ToolResult,
    /// Agent observations about the environment.
    Observation,
    /// Self-reflections and meta-reasoning.
    Reflection,
    /// Planning and goal-tracking entries.
    Plan,
    /// State checkpoints for agent resumption.
    Checkpoint,
}

impl StoreMemoryType {
    /// Parse from a string (case-insensitive).
    pub fn from_str_loose(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "conversation" => Some(Self::Conversation),
            "fact" => Some(Self::Fact),
            "tool" | "toolresult" | "tool_result" => Some(Self::ToolResult),
            "observation" => Some(Self::Observation),
            "reflection" => Some(Self::Reflection),
            "plan" => Some(Self::Plan),
            "checkpoint" => Some(Self::Checkpoint),
            _ => None,
        }
    }
}

impl std::fmt::Display for StoreMemoryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Conversation => write!(f, "conversation"),
            Self::Fact => write!(f, "fact"),
            Self::ToolResult => write!(f, "tool_result"),
            Self::Observation => write!(f, "observation"),
            Self::Reflection => write!(f, "reflection"),
            Self::Plan => write!(f, "plan"),
            Self::Checkpoint => write!(f, "checkpoint"),
        }
    }
}

// ---------------------------------------------------------------------------
// Memory entry
// ---------------------------------------------------------------------------

/// A single memory entry stored for an AI agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreMemoryEntry {
    /// Unique identifier.
    pub id: String,
    /// The agent that owns this memory.
    pub agent_id: String,
    /// The memory content (text).
    pub content: String,
    /// Classification of this memory.
    pub memory_type: StoreMemoryType,
    /// Optional embedding vector for semantic search.
    pub embedding: Option<Vec<f32>>,
    /// Arbitrary metadata.
    pub metadata: HashMap<String, serde_json::Value>,
    /// Importance score (0.0–1.0).
    pub importance: f32,
    /// Number of times this memory was accessed.
    pub access_count: u64,
    /// Creation timestamp (epoch milliseconds).
    pub created_at: u64,
    /// Last access timestamp (epoch milliseconds).
    pub last_accessed: u64,
    /// Optional expiration timestamp (epoch milliseconds).
    pub expires_at: Option<u64>,
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

/// Statistics for a single agent's memories.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentMemoryStats {
    pub total_memories: u64,
    pub by_type: HashMap<StoreMemoryType, u64>,
    pub total_bytes: u64,
    pub oldest_memory: Option<u64>,
    pub newest_memory: Option<u64>,
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors from the agent memory store.
#[derive(Debug, Clone, thiserror::Error)]
pub enum MemoryError {
    #[error("store full: capacity {0}")]
    StoreFull(usize),

    #[error("memory not found: {0}")]
    NotFound(String),

    #[error("invalid agent id: {0}")]
    InvalidAgent(String),

    #[error("serialization failed: {0}")]
    SerializationFailed(String),
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn generate_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Simple term-frequency cosine similarity between query and content.
fn term_similarity(query: &str, content: &str) -> f32 {
    let q_lower = query.to_lowercase();
    let q_words: Vec<&str> = q_lower.split_whitespace().collect();
    let c_lower = content.to_lowercase();
    if q_words.is_empty() {
        return 0.0;
    }
    let matches = q_words.iter().filter(|w| c_lower.contains(**w)).count();
    matches as f32 / q_words.len() as f32
}

// ---------------------------------------------------------------------------
// Agent Memory Store
// ---------------------------------------------------------------------------

/// Purpose-built persistent memory store for AI agents.
pub struct PersistentAgentMemoryStore {
    config: AgentMemoryConfig,
    /// All memories keyed by memory ID.
    memories: RwLock<HashMap<String, StoreMemoryEntry>>,
    /// Checkpoints keyed by checkpoint ID → serialized state.
    checkpoints: RwLock<HashMap<String, serde_json::Value>>,
}

impl PersistentAgentMemoryStore {
    /// Creates a new agent memory store with the given configuration.
    pub fn new(config: AgentMemoryConfig) -> Self {
        Self {
            config,
            memories: RwLock::new(HashMap::new()),
            checkpoints: RwLock::new(HashMap::new()),
        }
    }

    /// Store a new memory, returning its ID.
    pub fn remember(
        &self,
        agent_id: &str,
        content: &str,
        memory_type: StoreMemoryType,
        metadata: HashMap<String, serde_json::Value>,
    ) -> Result<String, MemoryError> {
        if agent_id.is_empty() {
            return Err(MemoryError::InvalidAgent("empty agent id".into()));
        }

        let mut memories = self.memories.write();
        if memories.len() >= self.config.max_memories {
            return Err(MemoryError::StoreFull(self.config.max_memories));
        }

        let now = now_millis();
        let id = generate_id();
        let expires_at = self.config.ttl.map(|ttl| now + ttl.as_millis() as u64);

        let entry = StoreMemoryEntry {
            id: id.clone(),
            agent_id: agent_id.to_string(),
            content: content.to_string(),
            memory_type,
            embedding: None,
            metadata,
            importance: 0.5,
            access_count: 0,
            created_at: now,
            last_accessed: now,
            expires_at,
        };

        memories.insert(id.clone(), entry);
        Ok(id)
    }

    /// Semantic search by content similarity using simple term matching.
    pub fn recall(&self, agent_id: &str, query: &str, top_k: usize) -> Vec<StoreMemoryEntry> {
        let now = now_millis();
        let memories = self.memories.read();

        let mut scored: Vec<(f32, StoreMemoryEntry)> = memories
            .values()
            .filter(|m| {
                m.agent_id == agent_id && !is_expired(m, now)
            })
            .map(|m| {
                let sim = if self.config.enable_semantic_search {
                    term_similarity(query, &m.content)
                } else {
                    0.0
                };
                (sim, m.clone())
            })
            .filter(|(sim, _)| *sim >= self.config.similarity_threshold || !self.config.enable_semantic_search)
            .collect();

        scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(top_k);

        // Update access counts (drop read lock, acquire write)
        drop(memories);
        let mut memories = self.memories.write();
        for (_, entry) in &scored {
            if let Some(m) = memories.get_mut(&entry.id) {
                m.access_count += 1;
                m.last_accessed = now;
            }
        }

        scored.into_iter().map(|(_, e)| e).collect()
    }

    /// Return most recent memories for an agent.
    pub fn recall_recent(&self, agent_id: &str, limit: usize) -> Vec<StoreMemoryEntry> {
        let now = now_millis();
        let memories = self.memories.read();

        let mut entries: Vec<StoreMemoryEntry> = memories
            .values()
            .filter(|m| m.agent_id == agent_id && !is_expired(m, now))
            .cloned()
            .collect();

        entries.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        entries.truncate(limit);
        entries
    }

    /// Return memories filtered by type.
    pub fn recall_by_type(
        &self,
        agent_id: &str,
        memory_type: StoreMemoryType,
        limit: usize,
    ) -> Vec<StoreMemoryEntry> {
        let now = now_millis();
        let memories = self.memories.read();

        let mut entries: Vec<StoreMemoryEntry> = memories
            .values()
            .filter(|m| {
                m.agent_id == agent_id
                    && m.memory_type == memory_type
                    && !is_expired(m, now)
            })
            .cloned()
            .collect();

        entries.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        entries.truncate(limit);
        entries
    }

    /// Delete a specific memory by ID.
    pub fn forget(&self, memory_id: &str) -> Result<(), MemoryError> {
        let mut memories = self.memories.write();
        if memories.remove(memory_id).is_some() {
            Ok(())
        } else {
            Err(MemoryError::NotFound(memory_id.to_string()))
        }
    }

    /// Delete all memories for an agent, returning the count deleted.
    pub fn forget_agent(&self, agent_id: &str) -> usize {
        let mut memories = self.memories.write();
        let before = memories.len();
        memories.retain(|_, m| m.agent_id != agent_id);
        before - memories.len()
    }

    /// Forget memories older than a given timestamp (epoch ms).
    pub fn forget_before(&self, agent_id: &str, before: u64) -> usize {
        let mut memories = self.memories.write();
        let count_before = memories.len();
        memories.retain(|_, m| !(m.agent_id == agent_id && m.created_at < before));
        count_before - memories.len()
    }

    /// Update the importance score for a memory.
    pub fn update_importance(&self, memory_id: &str, importance: f32) -> Result<(), MemoryError> {
        let mut memories = self.memories.write();
        if let Some(m) = memories.get_mut(memory_id) {
            m.importance = importance.clamp(0.0, 1.0);
            Ok(())
        } else {
            Err(MemoryError::NotFound(memory_id.to_string()))
        }
    }

    /// Save a state checkpoint for an agent.
    pub fn checkpoint(
        &self,
        agent_id: &str,
        state: serde_json::Value,
    ) -> Result<String, MemoryError> {
        if agent_id.is_empty() {
            return Err(MemoryError::InvalidAgent("empty agent id".into()));
        }

        let id = generate_id();
        let mut checkpoints = self.checkpoints.write();
        let mut wrapper = serde_json::Map::new();
        wrapper.insert("agent_id".to_string(), serde_json::Value::String(agent_id.to_string()));
        wrapper.insert("state".to_string(), state);
        wrapper.insert(
            "created_at".to_string(),
            serde_json::Value::Number(serde_json::Number::from(now_millis())),
        );
        checkpoints.insert(id.clone(), serde_json::Value::Object(wrapper));
        Ok(id)
    }

    /// Restore a previously saved checkpoint.
    pub fn restore_checkpoint(&self, checkpoint_id: &str) -> Option<serde_json::Value> {
        let checkpoints = self.checkpoints.read();
        checkpoints.get(checkpoint_id).cloned()
    }

    /// Return statistics for a specific agent.
    pub fn stats(&self, agent_id: &str) -> AgentMemoryStats {
        let memories = self.memories.read();
        let now = now_millis();

        let mut total_memories: u64 = 0;
        let mut by_type: HashMap<StoreMemoryType, u64> = HashMap::new();
        let mut total_bytes: u64 = 0;
        let mut oldest: Option<u64> = None;
        let mut newest: Option<u64> = None;

        for m in memories.values() {
            if m.agent_id != agent_id || is_expired(m, now) {
                continue;
            }
            total_memories += 1;
            *by_type.entry(m.memory_type).or_insert(0) += 1;
            total_bytes += m.content.len() as u64;

            match oldest {
                None => oldest = Some(m.created_at),
                Some(o) if m.created_at < o => oldest = Some(m.created_at),
                _ => {}
            }
            match newest {
                None => newest = Some(m.created_at),
                Some(n) if m.created_at > n => newest = Some(m.created_at),
                _ => {}
            }
        }

        AgentMemoryStats {
            total_memories,
            by_type,
            total_bytes,
            oldest_memory: oldest,
            newest_memory: newest,
        }
    }
}

fn is_expired(entry: &StoreMemoryEntry, now: u64) -> bool {
    entry.expires_at.map_or(false, |exp| now > exp)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn default_store() -> PersistentAgentMemoryStore {
        PersistentAgentMemoryStore::new(AgentMemoryConfig::default())
    }

    fn store_with_threshold(threshold: f32) -> PersistentAgentMemoryStore {
        PersistentAgentMemoryStore::new(AgentMemoryConfig {
            similarity_threshold: threshold,
            ..Default::default()
        })
    }

    #[test]
    fn test_remember_and_recall_recent() {
        let store = default_store();
        let id = store
            .remember("a1", "hello world", StoreMemoryType::Conversation, HashMap::new())
            .unwrap();
        assert!(!id.is_empty());

        let recent = store.recall_recent("a1", 10);
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].content, "hello world");
    }

    #[test]
    fn test_recall_semantic() {
        let store = store_with_threshold(0.3);
        store
            .remember("a1", "Rust is a fast systems language", StoreMemoryType::Fact, HashMap::new())
            .unwrap();
        store
            .remember("a1", "Python is great for scripting", StoreMemoryType::Fact, HashMap::new())
            .unwrap();
        store
            .remember("a1", "Databases store data on disk", StoreMemoryType::Fact, HashMap::new())
            .unwrap();

        let results = store.recall("a1", "Rust fast", 2);
        assert!(!results.is_empty());
        assert!(results[0].content.contains("Rust"));
    }

    #[test]
    fn test_recall_by_type() {
        let store = default_store();
        store.remember("a1", "conv1", StoreMemoryType::Conversation, HashMap::new()).unwrap();
        store.remember("a1", "fact1", StoreMemoryType::Fact, HashMap::new()).unwrap();
        store.remember("a1", "obs1", StoreMemoryType::Observation, HashMap::new()).unwrap();

        let facts = store.recall_by_type("a1", StoreMemoryType::Fact, 10);
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].content, "fact1");
    }

    #[test]
    fn test_forget_single() {
        let store = default_store();
        let id = store.remember("a1", "to forget", StoreMemoryType::Fact, HashMap::new()).unwrap();
        assert!(store.forget(&id).is_ok());
        assert!(store.forget(&id).is_err());
        assert_eq!(store.recall_recent("a1", 10).len(), 0);
    }

    #[test]
    fn test_forget_agent() {
        let store = default_store();
        store.remember("a1", "m1", StoreMemoryType::Fact, HashMap::new()).unwrap();
        store.remember("a1", "m2", StoreMemoryType::Fact, HashMap::new()).unwrap();
        store.remember("a2", "m3", StoreMemoryType::Fact, HashMap::new()).unwrap();

        let deleted = store.forget_agent("a1");
        assert_eq!(deleted, 2);
        assert_eq!(store.recall_recent("a1", 10).len(), 0);
        assert_eq!(store.recall_recent("a2", 10).len(), 1);
    }

    #[test]
    fn test_forget_before() {
        let store = default_store();
        store.remember("a1", "old", StoreMemoryType::Fact, HashMap::new()).unwrap();
        // All memories created at ~now, so forget_before(now+1000) should delete them
        let deleted = store.forget_before("a1", now_millis() + 1000);
        assert_eq!(deleted, 1);
    }

    #[test]
    fn test_update_importance() {
        let store = default_store();
        let id = store.remember("a1", "test", StoreMemoryType::Fact, HashMap::new()).unwrap();
        store.update_importance(&id, 0.9).unwrap();

        let recent = store.recall_recent("a1", 1);
        assert!((recent[0].importance - 0.9).abs() < f32::EPSILON);
    }

    #[test]
    fn test_update_importance_clamping() {
        let store = default_store();
        let id = store.remember("a1", "test", StoreMemoryType::Fact, HashMap::new()).unwrap();
        store.update_importance(&id, 2.0).unwrap();

        let recent = store.recall_recent("a1", 1);
        assert!((recent[0].importance - 1.0).abs() < f32::EPSILON);
    }

    #[test]
    fn test_checkpoint_and_restore() {
        let store = default_store();
        let state = serde_json::json!({"step": 5, "goal": "finish task"});
        let cp_id = store.checkpoint("a1", state.clone()).unwrap();

        let restored = store.restore_checkpoint(&cp_id).unwrap();
        assert_eq!(restored["state"], state);
        assert_eq!(restored["agent_id"], "a1");
    }

    #[test]
    fn test_stats() {
        let store = default_store();
        store.remember("a1", "conv", StoreMemoryType::Conversation, HashMap::new()).unwrap();
        store.remember("a1", "fact1", StoreMemoryType::Fact, HashMap::new()).unwrap();
        store.remember("a1", "fact2", StoreMemoryType::Fact, HashMap::new()).unwrap();

        let stats = store.stats("a1");
        assert_eq!(stats.total_memories, 3);
        assert_eq!(*stats.by_type.get(&StoreMemoryType::Fact).unwrap_or(&0), 2);
        assert_eq!(*stats.by_type.get(&StoreMemoryType::Conversation).unwrap_or(&0), 1);
        assert!(stats.total_bytes > 0);
    }

    #[test]
    fn test_store_full() {
        let store = PersistentAgentMemoryStore::new(AgentMemoryConfig {
            max_memories: 2,
            ..Default::default()
        });
        store.remember("a1", "m1", StoreMemoryType::Fact, HashMap::new()).unwrap();
        store.remember("a1", "m2", StoreMemoryType::Fact, HashMap::new()).unwrap();
        let result = store.remember("a1", "m3", StoreMemoryType::Fact, HashMap::new());
        assert!(matches!(result, Err(MemoryError::StoreFull(_))));
    }

    #[test]
    fn test_invalid_agent() {
        let store = default_store();
        let result = store.remember("", "test", StoreMemoryType::Fact, HashMap::new());
        assert!(matches!(result, Err(MemoryError::InvalidAgent(_))));
    }

    #[test]
    fn test_memory_type_parse() {
        assert_eq!(StoreMemoryType::from_str_loose("conversation"), Some(StoreMemoryType::Conversation));
        assert_eq!(StoreMemoryType::from_str_loose("FACT"), Some(StoreMemoryType::Fact));
        assert_eq!(StoreMemoryType::from_str_loose("tool"), Some(StoreMemoryType::ToolResult));
        assert_eq!(StoreMemoryType::from_str_loose("unknown"), None);
    }

    #[test]
    fn test_restore_nonexistent_checkpoint() {
        let store = default_store();
        assert!(store.restore_checkpoint("nonexistent").is_none());
    }
}
