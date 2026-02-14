//! Snippet Management
//!
//! Manages shareable code snippets for the playground.

use super::PlaygroundError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::RwLock;

/// Unique snippet identifier
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SnippetId(String);

impl SnippetId {
    /// Create a new unique snippet ID
    pub fn new() -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        Self(format!("snip-{:x}", timestamp))
    }

    /// Create from string
    pub fn from_string(s: String) -> Self {
        Self(s)
    }

    /// Get as string reference
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for SnippetId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for SnippetId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A shareable code snippet
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Snippet {
    /// Unique identifier
    pub id: SnippetId,
    /// Snippet name
    pub name: String,
    /// Description
    pub description: String,
    /// Commands in the snippet
    pub commands: Vec<String>,
    /// Creation timestamp
    pub created_at: u64,
    /// Author (optional)
    pub author: Option<String>,
    /// Tags for categorization
    pub tags: Vec<String>,
    /// View count
    pub views: u64,
    /// Like count
    pub likes: u64,
}

impl Snippet {
    /// Create a new snippet
    pub fn new(name: &str, description: &str, commands: Vec<String>) -> Self {
        Self {
            id: SnippetId::new(),
            name: name.to_string(),
            description: description.to_string(),
            commands,
            created_at: current_timestamp_ms(),
            author: None,
            tags: vec![],
            views: 0,
            likes: 0,
        }
    }

    /// Create a builder
    pub fn builder() -> SnippetBuilder {
        SnippetBuilder::new()
    }

    /// Get shareable URL
    pub fn share_url(&self, base_url: &str) -> String {
        format!("{}/playground/snippet/{}", base_url, self.id)
    }

    /// Increment view count
    pub fn increment_views(&mut self) {
        self.views += 1;
    }

    /// Increment like count
    pub fn increment_likes(&mut self) {
        self.likes += 1;
    }
}

/// Builder for snippets
pub struct SnippetBuilder {
    name: String,
    description: String,
    commands: Vec<String>,
    author: Option<String>,
    tags: Vec<String>,
}

impl SnippetBuilder {
    /// Create new builder
    pub fn new() -> Self {
        Self {
            name: String::new(),
            description: String::new(),
            commands: Vec::new(),
            author: None,
            tags: Vec::new(),
        }
    }

    /// Set name
    pub fn name(mut self, name: &str) -> Self {
        self.name = name.to_string();
        self
    }

    /// Set description
    pub fn description(mut self, desc: &str) -> Self {
        self.description = desc.to_string();
        self
    }

    /// Add a command
    pub fn command(mut self, cmd: &str) -> Self {
        self.commands.push(cmd.to_string());
        self
    }

    /// Add multiple commands
    pub fn commands(mut self, cmds: Vec<String>) -> Self {
        self.commands = cmds;
        self
    }

    /// Set author
    pub fn author(mut self, author: &str) -> Self {
        self.author = Some(author.to_string());
        self
    }

    /// Add tag
    pub fn tag(mut self, tag: &str) -> Self {
        self.tags.push(tag.to_string());
        self
    }

    /// Add multiple tags
    pub fn tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }

    /// Build the snippet
    pub fn build(self) -> Snippet {
        Snippet {
            id: SnippetId::new(),
            name: self.name,
            description: self.description,
            commands: self.commands,
            created_at: current_timestamp_ms(),
            author: self.author,
            tags: self.tags,
            views: 0,
            likes: 0,
        }
    }
}

impl Default for SnippetBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Manages snippet storage and retrieval
pub struct SnippetManager {
    /// In-memory snippet storage
    snippets: RwLock<HashMap<SnippetId, Snippet>>,
    /// Featured snippets
    featured: RwLock<Vec<SnippetId>>,
}

impl SnippetManager {
    /// Create a new snippet manager
    pub fn new() -> Self {
        let manager = Self {
            snippets: RwLock::new(HashMap::new()),
            featured: RwLock::new(Vec::new()),
        };
        manager.load_example_snippets();
        manager
    }

    /// Save a snippet
    pub fn save(&self, snippet: Snippet) -> Result<(), PlaygroundError> {
        let mut snippets = self
            .snippets
            .write()
            .map_err(|_| PlaygroundError::Runtime("lock error".to_string()))?;
        snippets.insert(snippet.id.clone(), snippet);
        Ok(())
    }

    /// Get a snippet by ID
    pub fn get(&self, id: &SnippetId) -> Result<Snippet, PlaygroundError> {
        let mut snippets = self
            .snippets
            .write()
            .map_err(|_| PlaygroundError::Runtime("lock error".to_string()))?;

        match snippets.get_mut(id) {
            Some(snippet) => {
                snippet.increment_views();
                Ok(snippet.clone())
            }
            None => Err(PlaygroundError::SnippetNotFound(id.to_string())),
        }
    }

    /// Delete a snippet
    pub fn delete(&self, id: &SnippetId) -> Result<(), PlaygroundError> {
        let mut snippets = self
            .snippets
            .write()
            .map_err(|_| PlaygroundError::Runtime("lock error".to_string()))?;

        if snippets.remove(id).is_some() {
            Ok(())
        } else {
            Err(PlaygroundError::SnippetNotFound(id.to_string()))
        }
    }

    /// List all snippets
    pub fn list(&self) -> Vec<SnippetSummary> {
        let snippets = self.snippets.read().expect("snippets lock");
        snippets
            .values()
            .map(|s| SnippetSummary {
                id: s.id.clone(),
                name: s.name.clone(),
                description: s.description.clone(),
                command_count: s.commands.len(),
                tags: s.tags.clone(),
                views: s.views,
                likes: s.likes,
                created_at: s.created_at,
            })
            .collect()
    }

    /// Search snippets by query
    pub fn search(&self, query: &str) -> Vec<SnippetSummary> {
        let query_lower = query.to_lowercase();
        let snippets = self.snippets.read().expect("snippets lock");

        snippets
            .values()
            .filter(|s| {
                s.name.to_lowercase().contains(&query_lower)
                    || s.description.to_lowercase().contains(&query_lower)
                    || s.tags
                        .iter()
                        .any(|t| t.to_lowercase().contains(&query_lower))
            })
            .map(|s| SnippetSummary {
                id: s.id.clone(),
                name: s.name.clone(),
                description: s.description.clone(),
                command_count: s.commands.len(),
                tags: s.tags.clone(),
                views: s.views,
                likes: s.likes,
                created_at: s.created_at,
            })
            .collect()
    }

    /// Get snippets by tag
    pub fn by_tag(&self, tag: &str) -> Vec<SnippetSummary> {
        let tag_lower = tag.to_lowercase();
        let snippets = self.snippets.read().expect("snippets lock");

        snippets
            .values()
            .filter(|s| s.tags.iter().any(|t| t.to_lowercase() == tag_lower))
            .map(|s| SnippetSummary {
                id: s.id.clone(),
                name: s.name.clone(),
                description: s.description.clone(),
                command_count: s.commands.len(),
                tags: s.tags.clone(),
                views: s.views,
                likes: s.likes,
                created_at: s.created_at,
            })
            .collect()
    }

    /// Get featured snippets
    pub fn featured(&self) -> Vec<Snippet> {
        let featured = self.featured.read().expect("featured lock");
        let snippets = self.snippets.read().expect("snippets lock");

        featured
            .iter()
            .filter_map(|id| snippets.get(id).cloned())
            .collect()
    }

    /// Get popular snippets (by views)
    pub fn popular(&self, limit: usize) -> Vec<SnippetSummary> {
        let snippets = self.snippets.read().expect("snippets lock");
        let mut sorted: Vec<_> = snippets.values().collect();
        sorted.sort_by(|a, b| b.views.cmp(&a.views));

        sorted
            .into_iter()
            .take(limit)
            .map(|s| SnippetSummary {
                id: s.id.clone(),
                name: s.name.clone(),
                description: s.description.clone(),
                command_count: s.commands.len(),
                tags: s.tags.clone(),
                views: s.views,
                likes: s.likes,
                created_at: s.created_at,
            })
            .collect()
    }

    /// Load example snippets
    fn load_example_snippets(&self) {
        let examples = vec![
            Snippet::builder()
                .name("Hello World")
                .description("Basic SET and GET operations")
                .command("SET greeting \"Hello, Ferrite!\"")
                .command("GET greeting")
                .tag("beginner")
                .tag("strings")
                .build(),
            Snippet::builder()
                .name("Counter Example")
                .description("Using INCR for atomic counters")
                .command("SET visitors 0")
                .command("INCR visitors")
                .command("INCR visitors")
                .command("INCR visitors")
                .command("GET visitors")
                .tag("beginner")
                .tag("counters")
                .build(),
            Snippet::builder()
                .name("User Session")
                .description("Storing user session data in a hash")
                .command("HSET session:123 user_id 456")
                .command("HSET session:123 username john_doe")
                .command("HSET session:123 logged_in true")
                .command("HGETALL session:123")
                .tag("intermediate")
                .tag("hashes")
                .tag("sessions")
                .build(),
            Snippet::builder()
                .name("Task Queue")
                .description("Simple task queue with lists")
                .command("RPUSH tasks \"task1:process email\"")
                .command("RPUSH tasks \"task2:send notification\"")
                .command("RPUSH tasks \"task3:generate report\"")
                .command("LLEN tasks")
                .command("LPOP tasks")
                .command("LRANGE tasks 0 -1")
                .tag("intermediate")
                .tag("lists")
                .tag("queues")
                .build(),
            Snippet::builder()
                .name("Leaderboard")
                .description("Game leaderboard with sorted sets")
                .command("ZADD leaderboard 100 player1")
                .command("ZADD leaderboard 250 player2")
                .command("ZADD leaderboard 175 player3")
                .command("ZADD leaderboard 300 player4")
                .command("ZRANGE leaderboard 0 -1 WITHSCORES")
                .command("ZRANK leaderboard player2")
                .tag("intermediate")
                .tag("sorted-sets")
                .tag("games")
                .build(),
            Snippet::builder()
                .name("Unique Visitors")
                .description("Track unique visitors with sets")
                .command("SADD visitors:today user1")
                .command("SADD visitors:today user2")
                .command("SADD visitors:today user1")
                .command("SCARD visitors:today")
                .command("SMEMBERS visitors:today")
                .tag("beginner")
                .tag("sets")
                .tag("analytics")
                .build(),
        ];

        let mut snippets = self.snippets.write().expect("snippets lock");
        let mut featured = self.featured.write().expect("featured lock");

        for snippet in examples {
            featured.push(snippet.id.clone());
            snippets.insert(snippet.id.clone(), snippet);
        }
    }
}

impl Default for SnippetManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Snippet summary (for listing)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnippetSummary {
    /// Snippet ID
    pub id: SnippetId,
    /// Name
    pub name: String,
    /// Description
    pub description: String,
    /// Number of commands
    pub command_count: usize,
    /// Tags
    pub tags: Vec<String>,
    /// View count
    pub views: u64,
    /// Like count
    pub likes: u64,
    /// Creation timestamp
    pub created_at: u64,
}

/// Get current timestamp in milliseconds
fn current_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snippet_creation() {
        let snippet = Snippet::new("Test Snippet", "A test", vec!["SET foo bar".to_string()]);
        assert_eq!(snippet.name, "Test Snippet");
        assert_eq!(snippet.commands.len(), 1);
    }

    #[test]
    fn test_snippet_builder() {
        let snippet = Snippet::builder()
            .name("Builder Test")
            .description("Testing builder")
            .command("GET foo")
            .command("SET bar baz")
            .tag("test")
            .build();

        assert_eq!(snippet.name, "Builder Test");
        assert_eq!(snippet.commands.len(), 2);
        assert_eq!(snippet.tags, vec!["test"]);
    }

    #[test]
    fn test_snippet_manager() {
        let manager = SnippetManager::new();

        // Should have example snippets loaded
        let all = manager.list();
        assert!(!all.is_empty());

        // Test search
        let results = manager.search("hello");
        assert!(!results.is_empty());

        // Test by tag
        let tagged = manager.by_tag("beginner");
        assert!(!tagged.is_empty());
    }

    #[test]
    fn test_snippet_save_get() {
        let manager = SnippetManager::new();

        let snippet = Snippet::new("Custom", "Custom snippet", vec!["PING".to_string()]);
        let id = snippet.id.clone();

        manager.save(snippet).unwrap();

        let retrieved = manager.get(&id).unwrap();
        assert_eq!(retrieved.name, "Custom");
        assert_eq!(retrieved.views, 1); // View incremented on get
    }

    #[test]
    fn test_snippet_delete() {
        let manager = SnippetManager::new();
        let snippet = Snippet::new("ToDelete", "Will be deleted", vec!["PING".to_string()]);
        let id = snippet.id.clone();
        manager.save(snippet).unwrap();
        manager.delete(&id).unwrap();
        assert!(manager.get(&id).is_err());
    }

    #[test]
    fn test_snippet_delete_not_found() {
        let manager = SnippetManager::new();
        let id = SnippetId::from_string("nonexistent".to_string());
        assert!(manager.delete(&id).is_err());
    }

    #[test]
    fn test_snippet_get_not_found() {
        let manager = SnippetManager::new();
        let id = SnippetId::from_string("nonexistent".to_string());
        assert!(manager.get(&id).is_err());
    }

    #[test]
    fn test_snippet_featured() {
        let manager = SnippetManager::new();
        let featured = manager.featured();
        assert!(!featured.is_empty());
    }

    #[test]
    fn test_snippet_popular() {
        let manager = SnippetManager::new();
        let popular = manager.popular(3);
        assert!(!popular.is_empty());
        assert!(popular.len() <= 3);
    }

    #[test]
    fn test_snippet_id() {
        let id = SnippetId::new();
        assert!(id.as_str().starts_with("snip-"));
        assert!(!id.to_string().is_empty());
        let id2 = SnippetId::from_string("custom-id".to_string());
        assert_eq!(id2.as_str(), "custom-id");
    }

    #[test]
    fn test_snippet_id_default() {
        let id = SnippetId::default();
        assert!(id.as_str().starts_with("snip-"));
    }

    #[test]
    fn test_snippet_share_url() {
        let snippet = Snippet::new("Test", "Test", vec![]);
        let url = snippet.share_url("https://example.com");
        assert!(url.starts_with("https://example.com/playground/snippet/"));
    }

    #[test]
    fn test_snippet_views_likes() {
        let mut snippet = Snippet::new("Test", "Test", vec![]);
        assert_eq!(snippet.views, 0);
        assert_eq!(snippet.likes, 0);
        snippet.increment_views();
        snippet.increment_likes();
        assert_eq!(snippet.views, 1);
        assert_eq!(snippet.likes, 1);
    }

    #[test]
    fn test_snippet_builder_with_author_and_tags() {
        let snippet = Snippet::builder()
            .name("Auth Test")
            .description("Test with author")
            .command("PING")
            .author("test_user")
            .tags(vec!["tag1".to_string(), "tag2".to_string()])
            .build();
        assert_eq!(snippet.author, Some("test_user".to_string()));
        assert_eq!(snippet.tags.len(), 2);
    }

    #[test]
    fn test_snippet_builder_with_commands() {
        let snippet = Snippet::builder()
            .name("Multi")
            .description("Multiple commands")
            .commands(vec!["SET a 1".to_string(), "GET a".to_string()])
            .build();
        assert_eq!(snippet.commands.len(), 2);
    }

    #[test]
    fn test_snippet_builder_default() {
        let builder = SnippetBuilder::default();
        let snippet = builder.name("Default").description("Default builder").build();
        assert_eq!(snippet.name, "Default");
    }

    #[test]
    fn test_snippet_manager_default() {
        let manager = SnippetManager::default();
        assert!(!manager.list().is_empty());
    }

    #[test]
    fn test_snippet_search_by_tag() {
        let manager = SnippetManager::new();
        let results = manager.by_tag("strings");
        assert!(!results.is_empty());
    }

    #[test]
    fn test_snippet_search_no_results() {
        let manager = SnippetManager::new();
        let results = manager.search("zzzznonexistentzzz");
        assert!(results.is_empty());
    }

    #[test]
    fn test_snippet_update_via_save() {
        let manager = SnippetManager::new();
        let mut snippet = Snippet::new("Updatable", "Will be updated", vec!["PING".to_string()]);
        let id = snippet.id.clone();
        manager.save(snippet.clone()).unwrap();

        // Update the snippet
        snippet.description = "Updated description".to_string();
        snippet.commands.push("GET foo".to_string());
        manager.save(snippet).unwrap();

        let retrieved = manager.get(&id).unwrap();
        assert_eq!(retrieved.description, "Updated description");
        assert_eq!(retrieved.commands.len(), 2);
    }

    #[test]
    fn test_snippet_search_by_description() {
        let manager = SnippetManager::new();
        let snippet = Snippet::builder()
            .name("Custom Search Test")
            .description("This snippet demonstrates unique_search_term functionality")
            .command("SET foo bar")
            .build();
        manager.save(snippet).unwrap();

        let results = manager.search("unique_search_term");
        assert!(!results.is_empty());
    }

    #[test]
    fn test_snippet_crud_full_lifecycle() {
        let manager = SnippetManager::new();

        // Create
        let snippet = Snippet::new("CRUD Test", "Test CRUD", vec!["SET k v".to_string()]);
        let id = snippet.id.clone();

        // Save (Create)
        manager.save(snippet).unwrap();

        // Read
        let retrieved = manager.get(&id).unwrap();
        assert_eq!(retrieved.name, "CRUD Test");
        assert_eq!(retrieved.views, 1); // View incremented

        // Read again - views should increase
        let retrieved = manager.get(&id).unwrap();
        assert_eq!(retrieved.views, 2);

        // List
        let list = manager.list();
        assert!(list.iter().any(|s| s.id == id));

        // Delete
        manager.delete(&id).unwrap();
        assert!(manager.get(&id).is_err());
    }

    #[test]
    fn test_snippet_summary_fields() {
        let manager = SnippetManager::new();
        let snippet = Snippet::builder()
            .name("Summary Test")
            .description("Test summary fields")
            .command("SET a 1")
            .command("GET a")
            .tag("test-tag")
            .build();
        let id = snippet.id.clone();
        manager.save(snippet).unwrap();

        let list = manager.list();
        let summary = list.iter().find(|s| s.id == id).unwrap();
        assert_eq!(summary.name, "Summary Test");
        assert_eq!(summary.command_count, 2);
        assert_eq!(summary.tags, vec!["test-tag"]);
        assert_eq!(summary.views, 0);
        assert_eq!(summary.likes, 0);
    }
}
