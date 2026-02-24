#![forbid(unsafe_code)]
//! Architecture templates — pre-built patterns for common use cases.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Registry of built-in architecture templates.
#[derive(Debug, Clone)]
pub struct TemplateRegistry {
    templates: HashMap<String, ArchitectureTemplate>,
}

/// A reusable architecture template with setup instructions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchitectureTemplate {
    pub name: String,
    pub description: String,
    pub category: TemplateCategory,
    pub setup_commands: Vec<String>,
    pub sample_data: Vec<(String, String)>,
    pub documentation: String,
}

/// Category of an architecture template.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TemplateCategory {
    RagPipeline,
    RealtimeLeaderboard,
    SessionStore,
    IoTIngestion,
    EventDriven,
    Caching,
}

impl std::fmt::Display for TemplateCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RagPipeline => write!(f, "RAG Pipeline"),
            Self::RealtimeLeaderboard => write!(f, "Realtime Leaderboard"),
            Self::SessionStore => write!(f, "Session Store"),
            Self::IoTIngestion => write!(f, "IoT Ingestion"),
            Self::EventDriven => write!(f, "Event Driven"),
            Self::Caching => write!(f, "Caching"),
        }
    }
}

impl TemplateRegistry {
    /// Create a new registry pre-populated with built-in templates.
    #[allow(dead_code)]
    pub fn new() -> Self {
        let mut templates = HashMap::new();

        templates.insert("rag-pipeline".to_string(), ArchitectureTemplate {
            name: "rag-pipeline".to_string(),
            description: "Vector index + document ingestion + semantic search for RAG applications".to_string(),
            category: TemplateCategory::RagPipeline,
            setup_commands: vec![
                "FT.CREATE docs_idx ON HASH PREFIX 1 doc: SCHEMA title TEXT content TEXT embedding VECTOR HNSW 6 TYPE FLOAT32 DIM 384 DISTANCE_METRIC COSINE".to_string(),
                "HSET doc:1 title \"Getting Started\" content \"Welcome to Ferrite, a high-performance key-value store.\" embedding \"<vector>\"".to_string(),
                "HSET doc:2 title \"Configuration\" content \"Ferrite supports multiple configuration options.\" embedding \"<vector>\"".to_string(),
                "HSET doc:3 title \"Clustering\" content \"Ferrite supports distributed clustering for horizontal scaling.\" embedding \"<vector>\"".to_string(),
            ],
            sample_data: vec![
                ("FT.SEARCH docs_idx \"@content:ferrite\"".to_string(), "Full-text search over documents".to_string()),
                ("VECTOR.SEARCH docs_idx <query_vector> KNN 5".to_string(), "Semantic search using vector similarity".to_string()),
            ],
            documentation: "RAG Pipeline Template\n\nThis template sets up a Retrieval-Augmented Generation pipeline with:\n- A search index for hybrid text + vector search\n- Sample documents with embeddings\n- Example queries for both full-text and semantic search\n\nReplace <vector> with actual embedding vectors from your model.".to_string(),
        });

        templates.insert("leaderboard".to_string(), ArchitectureTemplate {
            name: "leaderboard".to_string(),
            description: "Sorted sets with real-time score updates for gaming and ranking".to_string(),
            category: TemplateCategory::RealtimeLeaderboard,
            setup_commands: vec![
                "ZADD leaderboard:global 1500 player:alice".to_string(),
                "ZADD leaderboard:global 1450 player:bob".to_string(),
                "ZADD leaderboard:global 1600 player:charlie".to_string(),
                "ZADD leaderboard:global 1380 player:diana".to_string(),
                "ZADD leaderboard:weekly 500 player:alice".to_string(),
                "ZADD leaderboard:weekly 620 player:bob".to_string(),
            ],
            sample_data: vec![
                ("ZREVRANGE leaderboard:global 0 9 WITHSCORES".to_string(), "Top 10 players globally".to_string()),
                ("ZRANK leaderboard:global player:alice".to_string(), "Get player rank".to_string()),
                ("ZINCRBY leaderboard:global 50 player:alice".to_string(), "Update score after a win".to_string()),
            ],
            documentation: "Leaderboard Template\n\nReal-time ranking system using sorted sets:\n- Global all-time leaderboard\n- Weekly rotating leaderboard\n- Atomic score updates with ZINCRBY\n- Efficient top-N queries with ZREVRANGE".to_string(),
        });

        templates.insert("session-store".to_string(), ArchitectureTemplate {
            name: "session-store".to_string(),
            description: "Hash-based session management with TTL for web applications".to_string(),
            category: TemplateCategory::SessionStore,
            setup_commands: vec![
                "HSET session:abc123 user_id 42 username alice role admin created_at 1700000000 last_active 1700003600".to_string(),
                "EXPIRE session:abc123 3600".to_string(),
                "HSET session:def456 user_id 17 username bob role user created_at 1700001000 last_active 1700003500".to_string(),
                "EXPIRE session:def456 3600".to_string(),
                "SET session:index:42 abc123".to_string(),
                "SET session:index:17 def456".to_string(),
            ],
            sample_data: vec![
                ("HGETALL session:abc123".to_string(), "Retrieve full session data".to_string()),
                ("HSET session:abc123 last_active 1700007200".to_string(), "Update last active timestamp".to_string()),
                ("TTL session:abc123".to_string(), "Check remaining session TTL".to_string()),
                ("EXPIRE session:abc123 3600".to_string(), "Refresh session expiry on activity".to_string()),
            ],
            documentation: "Session Store Template\n\nHash-based session management:\n- Each session is a hash with user metadata\n- TTL-based automatic expiry (1 hour default)\n- Secondary index for user_id -> session_id lookup\n- Touch-to-refresh pattern for active sessions".to_string(),
        });

        templates.insert("iot-dashboard".to_string(), ArchitectureTemplate {
            name: "iot-dashboard".to_string(),
            description: "Time-series ingestion with aggregation for IoT sensor data".to_string(),
            category: TemplateCategory::IoTIngestion,
            setup_commands: vec![
                "TS.CREATE sensor:temp:office RETENTION 86400000 LABELS location office type temperature unit celsius".to_string(),
                "TS.CREATE sensor:humidity:office RETENTION 86400000 LABELS location office type humidity unit percent".to_string(),
                "TS.ADD sensor:temp:office * 22.5".to_string(),
                "TS.ADD sensor:temp:office * 23.1".to_string(),
                "TS.ADD sensor:humidity:office * 45.0".to_string(),
                "TS.ADD sensor:humidity:office * 47.2".to_string(),
            ],
            sample_data: vec![
                ("TS.RANGE sensor:temp:office - + AGGREGATION avg 3600000".to_string(), "Hourly average temperature".to_string()),
                ("TS.MRANGE - + FILTER location=office".to_string(), "All office sensor readings".to_string()),
                ("TS.GET sensor:temp:office".to_string(), "Latest temperature reading".to_string()),
            ],
            documentation: "IoT Dashboard Template\n\nTime-series sensor data ingestion:\n- Auto-timestamped data points with TS.ADD\n- 24-hour retention with automatic cleanup\n- Label-based filtering across sensors\n- Aggregation queries (avg, min, max) for dashboards".to_string(),
        });

        Self { templates }
    }

    /// Get a template by name.
    #[allow(dead_code)]
    pub fn get(&self, name: &str) -> Option<&ArchitectureTemplate> {
        self.templates.get(name)
    }

    /// List all templates as (name, description) pairs.
    #[allow(dead_code)]
    pub fn list(&self) -> Vec<(&str, &str)> {
        let mut items: Vec<(&str, &str)> = self
            .templates
            .values()
            .map(|t| (t.name.as_str(), t.description.as_str()))
            .collect();
        items.sort_by_key(|(name, _)| *name);
        items
    }

    /// Get the setup commands for a template.
    #[allow(dead_code)]
    pub fn setup_commands(&self, name: &str) -> Option<Vec<String>> {
        self.templates.get(name).map(|t| t.setup_commands.clone())
    }
}

impl Default for TemplateRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_new_has_templates() {
        let reg = TemplateRegistry::new();
        let list = reg.list();
        assert!(list.len() >= 4);
    }

    #[test]
    fn test_get_rag_pipeline() {
        let reg = TemplateRegistry::new();
        let tpl = reg.get("rag-pipeline").unwrap();
        assert_eq!(tpl.category, TemplateCategory::RagPipeline);
        assert!(!tpl.setup_commands.is_empty());
        assert!(!tpl.documentation.is_empty());
    }

    #[test]
    fn test_get_leaderboard() {
        let reg = TemplateRegistry::new();
        let tpl = reg.get("leaderboard").unwrap();
        assert_eq!(tpl.category, TemplateCategory::RealtimeLeaderboard);
    }

    #[test]
    fn test_get_session_store() {
        let reg = TemplateRegistry::new();
        let tpl = reg.get("session-store").unwrap();
        assert_eq!(tpl.category, TemplateCategory::SessionStore);
    }

    #[test]
    fn test_get_iot_dashboard() {
        let reg = TemplateRegistry::new();
        let tpl = reg.get("iot-dashboard").unwrap();
        assert_eq!(tpl.category, TemplateCategory::IoTIngestion);
    }

    #[test]
    fn test_get_missing_template() {
        let reg = TemplateRegistry::new();
        assert!(reg.get("nonexistent").is_none());
    }

    #[test]
    fn test_setup_commands() {
        let reg = TemplateRegistry::new();
        let cmds = reg.setup_commands("leaderboard").unwrap();
        assert!(!cmds.is_empty());
        assert!(cmds[0].starts_with("ZADD"));
    }

    #[test]
    fn test_list_sorted() {
        let reg = TemplateRegistry::new();
        let list = reg.list();
        let names: Vec<&str> = list.iter().map(|(n, _)| *n).collect();
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted);
    }

    #[test]
    fn test_category_display() {
        assert_eq!(TemplateCategory::RagPipeline.to_string(), "RAG Pipeline");
        assert_eq!(TemplateCategory::SessionStore.to_string(), "Session Store");
    }
}
