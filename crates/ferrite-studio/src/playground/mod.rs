//! Interactive Playground
//!
//! WebAssembly-based playground that runs Ferrite in the browser.
//!
//! # Features
//!
//! - Full Ferrite compiled to WASM
//! - Interactive tutorials with step-by-step guidance
//! - Shareable snippets with unique URLs
//! - Embedded in documentation
//!
//! # Example
//!
//! ```ignore
//! use ferrite::playground::{Playground, PlaygroundConfig};
//!
//! let playground = Playground::new(PlaygroundConfig::default());
//! let result = playground.execute("SET foo bar").await?;
//! ```

mod runtime;
mod snippets;
mod tutorials;
pub mod web_server;

pub use runtime::{DataValue, ExecutionResult, PlaygroundRuntime, ResultType, RuntimeConfig};
pub use snippets::{Snippet, SnippetBuilder, SnippetId, SnippetManager, SnippetSummary};
pub use tutorials::{Tutorial, TutorialBuilder, TutorialManager, TutorialStep};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Playground configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlaygroundConfig {
    /// Maximum memory for WASM runtime (bytes)
    pub max_memory_bytes: usize,
    /// Maximum execution time per command (ms)
    pub max_execution_time_ms: u64,
    /// Maximum number of keys allowed
    pub max_keys: usize,
    /// Enable network access (for demos)
    pub network_enabled: bool,
    /// Enable persistence (save/load state)
    pub persistence_enabled: bool,
    /// Sandbox mode (restrict dangerous operations)
    pub sandbox_mode: bool,
    /// Enable tutorial mode
    pub tutorial_mode: bool,
}

impl Default for PlaygroundConfig {
    fn default() -> Self {
        Self {
            max_memory_bytes: 64 * 1024 * 1024, // 64MB
            max_execution_time_ms: 5000,
            max_keys: 10_000,
            network_enabled: false,
            persistence_enabled: true,
            sandbox_mode: true,
            tutorial_mode: true,
        }
    }
}

impl PlaygroundConfig {
    /// Create a minimal configuration for embedded use
    pub fn minimal() -> Self {
        Self {
            max_memory_bytes: 16 * 1024 * 1024, // 16MB
            max_execution_time_ms: 1000,
            max_keys: 1_000,
            network_enabled: false,
            persistence_enabled: false,
            sandbox_mode: true,
            tutorial_mode: false,
        }
    }

    /// Create a full-featured configuration
    pub fn full() -> Self {
        Self {
            max_memory_bytes: 256 * 1024 * 1024, // 256MB
            max_execution_time_ms: 30000,
            max_keys: 100_000,
            network_enabled: true,
            persistence_enabled: true,
            sandbox_mode: false,
            tutorial_mode: true,
        }
    }
}

/// Main playground instance
pub struct Playground {
    /// Configuration
    config: PlaygroundConfig,
    /// Runtime instance
    runtime: Arc<RwLock<PlaygroundRuntime>>,
    /// Snippet manager
    snippets: Arc<SnippetManager>,
    /// Tutorial manager
    tutorials: Arc<TutorialManager>,
    /// Session history
    history: Arc<RwLock<Vec<HistoryEntry>>>,
    /// Current session ID
    session_id: String,
}

impl Playground {
    /// Create a new playground instance
    pub fn new(config: PlaygroundConfig) -> Self {
        let runtime_config = RuntimeConfig {
            max_memory_bytes: config.max_memory_bytes,
            max_execution_time_ms: config.max_execution_time_ms,
            max_keys: config.max_keys,
            sandbox_mode: config.sandbox_mode,
        };

        Self {
            config: config.clone(),
            runtime: Arc::new(RwLock::new(PlaygroundRuntime::new(runtime_config))),
            snippets: Arc::new(SnippetManager::new()),
            tutorials: Arc::new(TutorialManager::new()),
            history: Arc::new(RwLock::new(Vec::new())),
            session_id: generate_session_id(),
        }
    }

    /// Execute a command in the playground
    pub async fn execute(&self, command: &str) -> Result<ExecutionResult, PlaygroundError> {
        let start = std::time::Instant::now();

        // Parse and validate command
        let parsed = self.parse_command(command)?;

        // Check sandbox restrictions
        if self.config.sandbox_mode {
            self.check_sandbox_restrictions(&parsed)?;
        }

        // Execute in runtime
        let mut runtime = self.runtime.write().await;
        let result = runtime.execute(&parsed).await?;

        // Record in history
        let entry = HistoryEntry {
            command: command.to_string(),
            result: result.clone(),
            timestamp: current_timestamp_ms(),
            duration_ms: start.elapsed().as_millis() as u64,
        };
        self.history.write().await.push(entry);

        Ok(result)
    }

    /// Execute multiple commands
    pub async fn execute_batch(
        &self,
        commands: &[&str],
    ) -> Vec<Result<ExecutionResult, PlaygroundError>> {
        let mut results = Vec::with_capacity(commands.len());
        for cmd in commands {
            results.push(self.execute(cmd).await);
        }
        results
    }

    /// Get command history
    pub async fn get_history(&self) -> Vec<HistoryEntry> {
        self.history.read().await.clone()
    }

    /// Clear history
    pub async fn clear_history(&self) {
        self.history.write().await.clear();
    }

    /// Reset the playground state
    pub async fn reset(&self) -> Result<(), PlaygroundError> {
        let mut runtime = self.runtime.write().await;
        runtime.reset()?;
        self.history.write().await.clear();
        Ok(())
    }

    /// Create a shareable snippet from current state
    pub async fn create_snippet(
        &self,
        name: &str,
        description: &str,
    ) -> Result<SnippetId, PlaygroundError> {
        let history = self.history.read().await;
        let commands: Vec<String> = history.iter().map(|h| h.command.clone()).collect();

        let snippet = Snippet {
            id: SnippetId::new(),
            name: name.to_string(),
            description: description.to_string(),
            commands,
            created_at: current_timestamp_ms(),
            author: None,
            tags: vec![],
            views: 0,
            likes: 0,
        };

        self.snippets.save(snippet.clone())?;
        Ok(snippet.id)
    }

    /// Load and execute a snippet
    pub async fn load_snippet(
        &self,
        id: &SnippetId,
    ) -> Result<Vec<ExecutionResult>, PlaygroundError> {
        let snippet = self.snippets.get(id)?;
        let mut results = Vec::new();

        for cmd in &snippet.commands {
            results.push(self.execute(cmd).await?);
        }

        Ok(results)
    }

    /// Start a tutorial
    pub async fn start_tutorial(
        &self,
        tutorial_id: &str,
    ) -> Result<TutorialSession, PlaygroundError> {
        let tutorial = self.tutorials.get(tutorial_id)?;
        Ok(TutorialSession {
            tutorial,
            current_step: 0,
            completed_steps: vec![],
            started_at: current_timestamp_ms(),
        })
    }

    /// Get available tutorials
    pub fn list_tutorials(&self) -> Vec<TutorialInfo> {
        self.tutorials.list()
    }

    /// Get current session ID
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Export current state
    pub async fn export_state(&self) -> Result<PlaygroundState, PlaygroundError> {
        let runtime = self.runtime.read().await;
        let history = self.history.read().await;

        Ok(PlaygroundState {
            session_id: self.session_id.clone(),
            data: runtime.export_data()?,
            history: history.clone(),
            timestamp: current_timestamp_ms(),
        })
    }

    /// Import state
    pub async fn import_state(&self, state: PlaygroundState) -> Result<(), PlaygroundError> {
        let mut runtime = self.runtime.write().await;
        runtime.import_data(&state.data)?;
        *self.history.write().await = state.history;
        Ok(())
    }

    /// Parse a command string
    fn parse_command(&self, command: &str) -> Result<ParsedCommand, PlaygroundError> {
        let parts: Vec<&str> = command.split_whitespace().collect();
        if parts.is_empty() {
            return Err(PlaygroundError::EmptyCommand);
        }

        Ok(ParsedCommand {
            name: parts[0].to_uppercase(),
            args: parts[1..].iter().map(|s| s.to_string()).collect(),
        })
    }

    /// Check sandbox restrictions
    fn check_sandbox_restrictions(&self, cmd: &ParsedCommand) -> Result<(), PlaygroundError> {
        // Restricted commands in sandbox mode
        const RESTRICTED: &[&str] = &[
            "CONFIG",
            "DEBUG",
            "SHUTDOWN",
            "SLAVEOF",
            "REPLICAOF",
            "CLUSTER",
            "MIGRATE",
            "DUMP",
            "RESTORE",
            "MODULE",
            "ACL",
            "BGSAVE",
            "BGREWRITEAOF",
            "SAVE",
        ];

        if RESTRICTED.contains(&cmd.name.as_str()) {
            return Err(PlaygroundError::RestrictedCommand(cmd.name.clone()));
        }

        Ok(())
    }
}

/// Parsed command
#[derive(Clone, Debug)]
pub struct ParsedCommand {
    /// Command name
    pub name: String,
    /// Command arguments
    pub args: Vec<String>,
}

/// History entry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistoryEntry {
    /// Command executed
    pub command: String,
    /// Execution result
    pub result: ExecutionResult,
    /// Timestamp
    pub timestamp: u64,
    /// Duration in milliseconds
    pub duration_ms: u64,
}

/// Tutorial session
#[derive(Clone, Debug)]
pub struct TutorialSession {
    /// Tutorial being run
    pub tutorial: Tutorial,
    /// Current step index
    pub current_step: usize,
    /// Completed step indices
    pub completed_steps: Vec<usize>,
    /// Start time
    pub started_at: u64,
}

impl TutorialSession {
    /// Get current step
    pub fn current(&self) -> Option<&TutorialStep> {
        self.tutorial.steps.get(self.current_step)
    }

    /// Advance to next step
    pub fn advance(&mut self) -> Option<&TutorialStep> {
        if self.current_step < self.tutorial.steps.len() {
            self.completed_steps.push(self.current_step);
            self.current_step += 1;
        }
        self.current()
    }

    /// Check if completed
    pub fn is_completed(&self) -> bool {
        self.current_step >= self.tutorial.steps.len()
    }

    /// Get progress percentage
    pub fn progress(&self) -> f64 {
        if self.tutorial.steps.is_empty() {
            return 100.0;
        }
        (self.completed_steps.len() as f64 / self.tutorial.steps.len() as f64) * 100.0
    }
}

/// Tutorial info (summary)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TutorialInfo {
    /// Tutorial ID
    pub id: String,
    /// Tutorial title
    pub title: String,
    /// Description
    pub description: String,
    /// Difficulty level
    pub difficulty: Difficulty,
    /// Estimated time in minutes
    pub estimated_minutes: u32,
    /// Number of steps
    pub step_count: usize,
    /// Tags
    pub tags: Vec<String>,
}

/// Difficulty level
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Difficulty {
    /// Beginner level
    Beginner,
    /// Intermediate level
    Intermediate,
    /// Advanced level
    Advanced,
}

/// Playground state for export/import
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlaygroundState {
    /// Session ID
    pub session_id: String,
    /// Data snapshot
    pub data: HashMap<String, Vec<u8>>,
    /// Command history
    pub history: Vec<HistoryEntry>,
    /// Export timestamp
    pub timestamp: u64,
}

/// Playground errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum PlaygroundError {
    /// Empty command
    #[error("empty command")]
    EmptyCommand,

    /// Command not found
    #[error("unknown command: {0}")]
    UnknownCommand(String),

    /// Command restricted in sandbox mode
    #[error("command '{0}' is restricted in sandbox mode")]
    RestrictedCommand(String),

    /// Execution timeout
    #[error("execution timeout after {0}ms")]
    Timeout(u64),

    /// Memory limit exceeded
    #[error("memory limit exceeded: {used} > {limit}")]
    MemoryLimitExceeded { used: usize, limit: usize },

    /// Key limit exceeded
    #[error("key limit exceeded: {count} > {limit}")]
    KeyLimitExceeded { count: usize, limit: usize },

    /// Runtime error
    #[error("runtime error: {0}")]
    Runtime(String),

    /// Snippet not found
    #[error("snippet not found: {0}")]
    SnippetNotFound(String),

    /// Tutorial not found
    #[error("tutorial not found: {0}")]
    TutorialNotFound(String),

    /// Serialization error
    #[error("serialization error: {0}")]
    Serialization(String),
}

/// Generate a unique session ID
fn generate_session_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("pg-{:x}", timestamp)
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
    fn test_playground_config_default() {
        let config = PlaygroundConfig::default();
        assert!(config.sandbox_mode);
        assert!(config.tutorial_mode);
        assert_eq!(config.max_keys, 10_000);
    }

    #[test]
    fn test_playground_config_minimal() {
        let config = PlaygroundConfig::minimal();
        assert!(config.sandbox_mode);
        assert!(!config.tutorial_mode);
        assert_eq!(config.max_keys, 1_000);
    }

    #[test]
    fn test_parse_command() {
        let playground = Playground::new(PlaygroundConfig::default());

        let cmd = playground.parse_command("SET foo bar").unwrap();
        assert_eq!(cmd.name, "SET");
        assert_eq!(cmd.args, vec!["foo", "bar"]);
    }

    #[test]
    fn test_sandbox_restrictions() {
        let playground = Playground::new(PlaygroundConfig::default());

        let cmd = ParsedCommand {
            name: "CONFIG".to_string(),
            args: vec!["GET".to_string(), "maxmemory".to_string()],
        };

        assert!(playground.check_sandbox_restrictions(&cmd).is_err());
    }

    #[test]
    fn test_tutorial_session_progress() {
        let tutorial = Tutorial {
            id: "test".to_string(),
            title: "Test Tutorial".to_string(),
            description: "A test".to_string(),
            difficulty: Difficulty::Beginner,
            estimated_minutes: 5,
            steps: vec![
                TutorialStep {
                    title: "Step 1".to_string(),
                    description: "First step".to_string(),
                    command: Some("SET foo bar".to_string()),
                    expected_result: None,
                    hints: vec![],
                },
                TutorialStep {
                    title: "Step 2".to_string(),
                    description: "Second step".to_string(),
                    command: Some("GET foo".to_string()),
                    expected_result: Some("bar".to_string()),
                    hints: vec![],
                },
            ],
            tags: vec![],
        };

        let mut session = TutorialSession {
            tutorial,
            current_step: 0,
            completed_steps: vec![],
            started_at: 0,
        };

        assert_eq!(session.progress(), 0.0);
        session.advance();
        assert_eq!(session.progress(), 50.0);
        session.advance();
        assert!(session.is_completed());
    }

    #[tokio::test]
    async fn test_playground_execute() {
        let playground = Playground::new(PlaygroundConfig::default());
        let result = playground.execute("SET mykey myvalue").await.unwrap();
        assert!(!result.is_error);
        let result = playground.execute("GET mykey").await.unwrap();
        assert_eq!(result.value, Some("myvalue".to_string()));
    }

    #[tokio::test]
    async fn test_playground_execute_batch() {
        let playground = Playground::new(PlaygroundConfig::default());
        let results = playground
            .execute_batch(&["SET a 1", "SET b 2", "GET a"])
            .await;
        assert_eq!(results.len(), 3);
        assert!(!results[0].as_ref().unwrap().is_error);
        assert_eq!(results[2].as_ref().unwrap().value, Some("1".to_string()));
    }

    #[tokio::test]
    async fn test_playground_history() {
        let playground = Playground::new(PlaygroundConfig::default());
        playground.execute("SET foo bar").await.unwrap();
        playground.execute("GET foo").await.unwrap();
        let history = playground.get_history().await;
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].command, "SET foo bar");
        playground.clear_history().await;
        assert!(playground.get_history().await.is_empty());
    }

    #[tokio::test]
    async fn test_playground_reset() {
        let playground = Playground::new(PlaygroundConfig::default());
        playground.execute("SET key val").await.unwrap();
        playground.reset().await.unwrap();
        let result = playground.execute("GET key").await.unwrap();
        assert_eq!(result.result_type, ResultType::Null);
    }

    #[tokio::test]
    async fn test_playground_sandbox_blocks_restricted() {
        let playground = Playground::new(PlaygroundConfig::default());
        let result = playground.execute("CONFIG GET maxmemory").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_playground_empty_command() {
        let playground = Playground::new(PlaygroundConfig::default());
        let result = playground.execute("").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_playground_session_id() {
        let playground = Playground::new(PlaygroundConfig::default());
        assert!(playground.session_id().starts_with("pg-"));
    }

    #[tokio::test]
    async fn test_playground_export_import_state() {
        let playground = Playground::new(PlaygroundConfig::default());
        playground.execute("SET k1 v1").await.unwrap();
        playground.execute("SET k2 v2").await.unwrap();
        let state = playground.export_state().await.unwrap();
        assert!(!state.data.is_empty());
        assert_eq!(state.history.len(), 2);

        let playground2 = Playground::new(PlaygroundConfig::default());
        playground2.import_state(state).await.unwrap();
        let result = playground2.execute("GET k1").await.unwrap();
        assert_eq!(result.value, Some("v1".to_string()));
    }

    #[tokio::test]
    async fn test_playground_create_snippet() {
        let playground = Playground::new(PlaygroundConfig::default());
        playground.execute("SET foo bar").await.unwrap();
        let id = playground
            .create_snippet("Test Snippet", "A test snippet")
            .await
            .unwrap();
        assert!(!id.as_str().is_empty());
    }

    #[test]
    fn test_playground_list_tutorials() {
        let playground = Playground::new(PlaygroundConfig::default());
        let tutorials = playground.list_tutorials();
        assert!(!tutorials.is_empty());
    }

    #[tokio::test]
    async fn test_playground_start_tutorial() {
        let playground = Playground::new(PlaygroundConfig::default());
        let session = playground.start_tutorial("getting-started").await.unwrap();
        assert!(!session.tutorial.steps.is_empty());
        assert_eq!(session.current_step, 0);
        assert!(!session.is_completed());
    }

    #[test]
    fn test_playground_config_full() {
        let config = PlaygroundConfig::full();
        assert!(!config.sandbox_mode);
        assert!(config.network_enabled);
        assert_eq!(config.max_keys, 100_000);
    }

    #[test]
    fn test_tutorial_session_current() {
        let tutorial = Tutorial::new("test", "Test", "Test tutorial");
        let session = TutorialSession {
            tutorial: tutorial.clone(),
            current_step: 0,
            completed_steps: vec![],
            started_at: 0,
        };
        // Empty tutorial, so current returns None
        assert!(session.current().is_none());
    }

    #[test]
    fn test_tutorial_session_empty_progress() {
        let tutorial = Tutorial::new("test", "Test", "Test tutorial");
        let session = TutorialSession {
            tutorial,
            current_step: 0,
            completed_steps: vec![],
            started_at: 0,
        };
        assert_eq!(session.progress(), 100.0);
    }

    #[test]
    fn test_difficulty_enum() {
        assert_ne!(Difficulty::Beginner, Difficulty::Intermediate);
        assert_ne!(Difficulty::Intermediate, Difficulty::Advanced);
    }

    #[test]
    fn test_playground_error_display() {
        let err = PlaygroundError::EmptyCommand;
        assert_eq!(err.to_string(), "empty command");
        let err = PlaygroundError::UnknownCommand("FOO".to_string());
        assert_eq!(err.to_string(), "unknown command: FOO");
        let err = PlaygroundError::RestrictedCommand("CONFIG".to_string());
        assert!(err.to_string().contains("CONFIG"));
        let err = PlaygroundError::Timeout(5000);
        assert!(err.to_string().contains("5000"));
        let err = PlaygroundError::MemoryLimitExceeded {
            used: 1024,
            limit: 512,
        };
        assert!(err.to_string().contains("1024"));
        let err = PlaygroundError::KeyLimitExceeded {
            count: 100,
            limit: 50,
        };
        assert!(err.to_string().contains("100"));
    }

    #[tokio::test]
    async fn test_playground_sandbox_blocks_all_restricted() {
        let playground = Playground::new(PlaygroundConfig::default());
        let restricted = vec![
            "CONFIG GET maxmemory",
            "DEBUG SLEEP 1",
            "SHUTDOWN NOSAVE",
            "SLAVEOF NO ONE",
            "CLUSTER INFO",
            "ACL LIST",
        ];
        for cmd in restricted {
            let result = playground.execute(cmd).await;
            assert!(result.is_err(), "Command '{}' should be restricted", cmd);
        }
    }

    #[tokio::test]
    async fn test_playground_no_sandbox_allows_all() {
        let config = PlaygroundConfig {
            sandbox_mode: false,
            ..PlaygroundConfig::default()
        };
        let playground = Playground::new(config);
        // CONFIG is not actually implemented so it returns UnknownCommand, not RestrictedCommand
        let result = playground.execute("CONFIG GET maxmemory").await;
        match result {
            Err(PlaygroundError::RestrictedCommand(_)) => {
                panic!("Should not be restricted when sandbox is off")
            }
            _ => {} // Any other result is fine
        }
    }

    #[tokio::test]
    async fn test_playground_load_snippet() {
        let playground = Playground::new(PlaygroundConfig::default());
        playground.execute("SET foo bar").await.unwrap();
        playground.execute("SET baz qux").await.unwrap();
        let id = playground
            .create_snippet("Load Test", "Testing snippet loading")
            .await
            .unwrap();

        // Create a fresh playground and load the snippet
        let playground2 = Playground::new(PlaygroundConfig::default());
        // The snippet manager is per-playground, so use the same one
        let results = playground.load_snippet(&id).await.unwrap();
        assert_eq!(results.len(), 2);
        // Verify commands were executed
        let _ = playground2;
    }

    #[tokio::test]
    async fn test_playground_tutorial_session_advance() {
        let playground = Playground::new(PlaygroundConfig::default());
        let mut session = playground.start_tutorial("getting-started").await.unwrap();
        assert_eq!(session.progress(), 0.0);
        assert!(!session.is_completed());

        // Advance through all steps
        let total_steps = session.tutorial.steps.len();
        for _ in 0..total_steps {
            session.advance();
        }
        assert!(session.is_completed());
        assert_eq!(session.progress(), 100.0);
    }

    #[tokio::test]
    async fn test_playground_multiple_data_types() {
        let playground = Playground::new(PlaygroundConfig::default());
        // String
        playground.execute("SET str_key hello").await.unwrap();
        // List
        playground.execute("RPUSH list_key a b c").await.unwrap();
        // Hash
        playground.execute("HSET hash_key f1 v1").await.unwrap();
        // Set
        playground.execute("SADD set_key m1 m2").await.unwrap();
        // Sorted set
        playground.execute("ZADD zset_key 1 m1").await.unwrap();

        // Export state should contain all keys
        let state = playground.export_state().await.unwrap();
        assert_eq!(state.data.len(), 5);
    }

    #[test]
    fn test_generate_session_id_unique() {
        let id1 = generate_session_id();
        // Sleep briefly to ensure different timestamps
        std::thread::sleep(std::time::Duration::from_millis(1));
        let id2 = generate_session_id();
        assert_ne!(id1, id2);
        assert!(id1.starts_with("pg-"));
        assert!(id2.starts_with("pg-"));
    }

    #[test]
    fn test_playground_state_serialization() {
        let state = PlaygroundState {
            session_id: "pg-test".to_string(),
            data: HashMap::new(),
            history: vec![],
            timestamp: 12345,
        };
        let json = serde_json::to_string(&state).unwrap();
        let deserialized: PlaygroundState = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.session_id, "pg-test");
        assert_eq!(deserialized.timestamp, 12345);
    }
}
