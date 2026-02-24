//! Web Playground Server
//!
//! HTTP-based server for the interactive Ferrite playground, providing session
//! management, command execution, tutorial delivery, and snippet sharing.
//!
//! # Features
//!
//! - Multi-session playground with automatic expiry
//! - Redis command and FerriteQL query execution
//! - Built-in interactive tutorials for all skill levels
//! - Shareable snippets with embeddable HTML widgets
//! - Real-time statistics and monitoring
//!
//! # Example
//!
//! ```ignore
//! use ferrite_studio::playground::web_server::{PlaygroundServer, PlaygroundServerConfig};
//!
//! let server = PlaygroundServer::new(PlaygroundServerConfig::default());
//! let session = server.create_session().unwrap();
//! let result = server.execute_command(&session, "SET foo bar").unwrap();
//! assert!(result.success);
//! ```

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can occur in the web playground server
#[derive(Debug, Clone, thiserror::Error)]
pub enum PlaygroundError {
    /// Session not found
    #[error("session not found: {0}")]
    SessionNotFound(String),

    /// Session has expired
    #[error("session expired: {0}")]
    SessionExpired(String),

    /// Maximum number of concurrent sessions reached
    #[error("max sessions reached: {limit}")]
    MaxSessionsReached { limit: usize },

    /// Command exceeds maximum allowed length
    #[error("command too long: {len} > {limit}")]
    CommandTooLong { len: usize, limit: usize },

    /// Result exceeds maximum allowed size
    #[error("result too large: {size} > {limit}")]
    ResultTooLarge { size: usize, limit: usize },

    /// Snippet not found
    #[error("snippet not found: {0}")]
    SnippetNotFound(String),

    /// Internal error
    #[error("internal error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the web playground server
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlaygroundServerConfig {
    /// Maximum number of concurrent sessions
    pub max_sessions: usize,
    /// Session timeout in seconds
    pub session_timeout_secs: u64,
    /// Maximum command length in bytes
    pub max_command_length: usize,
    /// Maximum result size in bytes
    pub max_result_size: usize,
    /// Enable snippet sharing
    pub enable_sharing: bool,
    /// Enable built-in tutorials
    pub enable_tutorials: bool,
    /// WASM memory limit in megabytes
    pub wasm_memory_limit_mb: u32,
}

impl Default for PlaygroundServerConfig {
    fn default() -> Self {
        Self {
            max_sessions: 1000,
            session_timeout_secs: 3600,
            max_command_length: 10_000,
            max_result_size: 1_048_576, // 1MB
            enable_sharing: true,
            enable_tutorials: true,
            wasm_memory_limit_mb: 64,
        }
    }
}

// ---------------------------------------------------------------------------
// Session types
// ---------------------------------------------------------------------------

/// Unique session identifier (UUID wrapper)
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionId(String);

impl SessionId {
    /// Create a new unique session ID
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// Create from an existing string
    pub fn from_string(s: String) -> Self {
        Self(s)
    }

    /// Get as string reference
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Information about an active playground session
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionInfo {
    /// Session identifier
    pub id: SessionId,
    /// When the session was created
    pub created_at: DateTime<Utc>,
    /// When the session was last used
    pub last_activity: DateTime<Utc>,
    /// Total commands executed in this session
    pub commands_executed: u64,
    /// Number of keys currently stored
    pub keys_count: u64,
    /// Estimated memory usage in bytes
    pub memory_used_bytes: u64,
}

/// Internal session state
struct Session {
    /// Session metadata
    info: SessionInfo,
    /// In-memory key-value store for this session
    data: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// Execution types
// ---------------------------------------------------------------------------

/// Result of executing a command or query
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionResult {
    /// Whether the command succeeded
    pub success: bool,
    /// Output string
    pub output: String,
    /// Execution time in microseconds
    pub execution_time_us: u64,
    /// Type of the result value
    pub result_type: ResultType,
    /// Error message, if any
    pub error: Option<String>,
}

/// Type of a command result
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResultType {
    /// Simple string (e.g. "OK", "PONG")
    SimpleString,
    /// Integer value
    Integer,
    /// Bulk string value
    BulkString,
    /// Array of values
    Array,
    /// Null / nil
    Null,
    /// Error response
    Error,
    /// Tabular result (FerriteQL)
    Table,
    /// JSON result (FerriteQL)
    Json,
}

// ---------------------------------------------------------------------------
// Tutorial types
// ---------------------------------------------------------------------------

/// Difficulty level for tutorials
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Difficulty {
    /// Beginner level
    Beginner,
    /// Intermediate level
    Intermediate,
    /// Advanced level
    Advanced,
}

/// A complete interactive tutorial
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Tutorial {
    /// Unique identifier
    pub id: String,
    /// Display title
    pub title: String,
    /// Brief description
    pub description: String,
    /// Difficulty level
    pub difficulty: Difficulty,
    /// Ordered list of steps
    pub steps: Vec<TutorialStep>,
    /// Estimated completion time in minutes
    pub estimated_minutes: u32,
    /// Categorization tags
    pub tags: Vec<String>,
}

/// A single step within a tutorial
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TutorialStep {
    /// Step title
    pub title: String,
    /// Instructional description
    pub description: String,
    /// Command to execute
    pub command: String,
    /// Expected output for validation
    pub expected_output: Option<String>,
    /// Hint to show if the user is stuck
    pub hint: Option<String>,
}

/// Summary view of a tutorial for listing
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TutorialSummary {
    /// Tutorial identifier
    pub id: String,
    /// Display title
    pub title: String,
    /// Difficulty level
    pub difficulty: Difficulty,
    /// Estimated completion time in minutes
    pub estimated_minutes: u32,
    /// Categorization tags
    pub tags: Vec<String>,
}

// ---------------------------------------------------------------------------
// Snippet types
// ---------------------------------------------------------------------------

/// Data required to create a new shared snippet
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnippetCreate {
    /// Snippet title
    pub title: String,
    /// Optional description
    pub description: Option<String>,
    /// Commands in the snippet
    pub commands: Vec<String>,
    /// Categorization tags
    pub tags: Vec<String>,
    /// Optional author name
    pub author: Option<String>,
}

/// A shared snippet with metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SharedSnippet {
    /// Unique identifier
    pub id: String,
    /// Snippet title
    pub title: String,
    /// Optional description
    pub description: Option<String>,
    /// Commands in the snippet
    pub commands: Vec<String>,
    /// Categorization tags
    pub tags: Vec<String>,
    /// Optional author name
    pub author: Option<String>,
    /// When the snippet was created
    pub created_at: DateTime<Utc>,
    /// Number of times the snippet has been viewed
    pub views: u64,
    /// Shareable URL
    pub url: String,
}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

/// Aggregate statistics for the playground server
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PlaygroundStats {
    /// Number of currently active sessions
    pub active_sessions: u64,
    /// Total sessions created since server start
    pub total_sessions: u64,
    /// Total commands executed across all sessions
    pub total_commands: u64,
    /// Total snippets created
    pub total_snippets: u64,
    /// Total tutorial completions
    pub total_tutorial_completions: u64,
    /// Average session duration in seconds
    pub avg_session_duration_secs: f64,
}

// ---------------------------------------------------------------------------
// Default tutorials
// ---------------------------------------------------------------------------

/// Build the set of default built-in tutorials
fn default_tutorials() -> Vec<Tutorial> {
    vec![
        Tutorial {
            id: "getting-started".to_string(),
            title: "Getting Started".to_string(),
            description: "Learn the basics of Ferrite with simple key-value operations".to_string(),
            difficulty: Difficulty::Beginner,
            estimated_minutes: 5,
            tags: vec!["basics".to_string(), "beginner".to_string()],
            steps: vec![
                TutorialStep {
                    title: "Ping the Server".to_string(),
                    description: "Use PING to check if Ferrite is responding.".to_string(),
                    command: "PING".to_string(),
                    expected_output: Some("PONG".to_string()),
                    hint: Some("Just type PING and press enter.".to_string()),
                },
                TutorialStep {
                    title: "Store a Value".to_string(),
                    description: "Use SET to store a key-value pair.".to_string(),
                    command: "SET greeting \"Hello, World!\"".to_string(),
                    expected_output: Some("OK".to_string()),
                    hint: Some("Syntax: SET key value".to_string()),
                },
                TutorialStep {
                    title: "Retrieve a Value".to_string(),
                    description: "Use GET to retrieve the value you just stored.".to_string(),
                    command: "GET greeting".to_string(),
                    expected_output: Some("Hello, World!".to_string()),
                    hint: Some("Syntax: GET key".to_string()),
                },
                TutorialStep {
                    title: "Delete a Key".to_string(),
                    description: "Use DEL to remove a key and its value.".to_string(),
                    command: "DEL greeting".to_string(),
                    expected_output: Some("1".to_string()),
                    hint: Some("DEL returns the number of keys removed.".to_string()),
                },
                TutorialStep {
                    title: "Check Existence".to_string(),
                    description: "Use EXISTS to check whether a key exists.".to_string(),
                    command: "EXISTS greeting".to_string(),
                    expected_output: Some("0".to_string()),
                    hint: Some("EXISTS returns 1 if the key exists, 0 otherwise.".to_string()),
                },
            ],
        },
        Tutorial {
            id: "data-types".to_string(),
            title: "Data Types".to_string(),
            description: "Explore Ferrite's rich data structures: lists, hashes, sets, and sorted sets".to_string(),
            difficulty: Difficulty::Beginner,
            estimated_minutes: 10,
            tags: vec!["data-types".to_string(), "beginner".to_string()],
            steps: vec![
                TutorialStep {
                    title: "Lists".to_string(),
                    description: "Push items to a list with RPUSH and retrieve with LRANGE.".to_string(),
                    command: "RPUSH fruits apple banana cherry".to_string(),
                    expected_output: Some("3".to_string()),
                    hint: Some("RPUSH appends elements to the right side of a list.".to_string()),
                },
                TutorialStep {
                    title: "Hashes".to_string(),
                    description: "Store structured data with HSET and retrieve with HGETALL.".to_string(),
                    command: "HSET user:1 name Alice age 30".to_string(),
                    expected_output: Some("2".to_string()),
                    hint: Some("HSET sets field-value pairs in a hash.".to_string()),
                },
                TutorialStep {
                    title: "Sets".to_string(),
                    description: "Add unique members to a set with SADD.".to_string(),
                    command: "SADD tags rust performance database".to_string(),
                    expected_output: Some("3".to_string()),
                    hint: Some("Sets only store unique members.".to_string()),
                },
                TutorialStep {
                    title: "Sorted Sets".to_string(),
                    description: "Add scored members with ZADD and retrieve ranked results.".to_string(),
                    command: "ZADD leaderboard 100 alice 200 bob 150 charlie".to_string(),
                    expected_output: Some("3".to_string()),
                    hint: Some("ZADD adds members with a numeric score for ordering.".to_string()),
                },
            ],
        },
        Tutorial {
            id: "ferriteql-basics".to_string(),
            title: "FerriteQL Basics".to_string(),
            description: "Learn to query Ferrite using SQL-like FerriteQL syntax".to_string(),
            difficulty: Difficulty::Intermediate,
            estimated_minutes: 15,
            tags: vec!["ferriteql".to_string(), "query".to_string()],
            steps: vec![
                TutorialStep {
                    title: "SELECT Query".to_string(),
                    description: "Use SELECT to retrieve keys matching a pattern.".to_string(),
                    command: "SELECT * FROM keys WHERE pattern = 'user:*'".to_string(),
                    expected_output: None,
                    hint: Some("FerriteQL uses SQL-like syntax to query Ferrite data.".to_string()),
                },
                TutorialStep {
                    title: "WHERE Clause".to_string(),
                    description: "Filter results with WHERE conditions.".to_string(),
                    command: "SELECT * FROM hash WHERE key = 'user:1' AND field = 'name'".to_string(),
                    expected_output: None,
                    hint: Some("WHERE clauses filter by key, field, or value.".to_string()),
                },
                TutorialStep {
                    title: "JOIN Operations".to_string(),
                    description: "Join data across multiple keys.".to_string(),
                    command: "SELECT a.name, b.score FROM hash a JOIN zset b ON a.key = b.member WHERE a.key = 'user:*'".to_string(),
                    expected_output: None,
                    hint: Some("JOINs correlate data across different data structures.".to_string()),
                },
                TutorialStep {
                    title: "GROUP BY".to_string(),
                    description: "Aggregate results with GROUP BY.".to_string(),
                    command: "SELECT type, COUNT(*) FROM keys GROUP BY type".to_string(),
                    expected_output: None,
                    hint: Some("GROUP BY works with COUNT, SUM, AVG, MIN, MAX.".to_string()),
                },
            ],
        },
        Tutorial {
            id: "vector-search".to_string(),
            title: "Vector Search".to_string(),
            description: "Build and query vector indexes for similarity search".to_string(),
            difficulty: Difficulty::Advanced,
            estimated_minutes: 20,
            tags: vec!["vectors".to_string(), "ai".to_string(), "search".to_string()],
            steps: vec![
                TutorialStep {
                    title: "Create Index".to_string(),
                    description: "Create a vector index with VECTOR.CREATE.".to_string(),
                    command: "VECTOR.CREATE idx DIM 128 DISTANCE COSINE".to_string(),
                    expected_output: Some("OK".to_string()),
                    hint: Some("DIM specifies the vector dimension; DISTANCE sets the metric.".to_string()),
                },
                TutorialStep {
                    title: "Add Vectors".to_string(),
                    description: "Add vectors to the index with VECTOR.ADD.".to_string(),
                    command: "VECTOR.ADD idx doc1 VALUES 0.1 0.2 0.3".to_string(),
                    expected_output: Some("OK".to_string()),
                    hint: Some("Provide the index name, a key, and the vector values.".to_string()),
                },
                TutorialStep {
                    title: "Search Vectors".to_string(),
                    description: "Find similar vectors with VECTOR.SEARCH.".to_string(),
                    command: "VECTOR.SEARCH idx VALUES 0.1 0.2 0.3 LIMIT 5".to_string(),
                    expected_output: None,
                    hint: Some("LIMIT controls how many nearest neighbors are returned.".to_string()),
                },
            ],
        },
        Tutorial {
            id: "pubsub-patterns".to_string(),
            title: "Pub/Sub Patterns".to_string(),
            description: "Learn real-time messaging with publish/subscribe patterns".to_string(),
            difficulty: Difficulty::Intermediate,
            estimated_minutes: 10,
            tags: vec!["pubsub".to_string(), "messaging".to_string()],
            steps: vec![
                TutorialStep {
                    title: "Subscribe to a Channel".to_string(),
                    description: "Use SUBSCRIBE to listen on a channel.".to_string(),
                    command: "SUBSCRIBE notifications".to_string(),
                    expected_output: None,
                    hint: Some("SUBSCRIBE blocks and waits for messages on the channel.".to_string()),
                },
                TutorialStep {
                    title: "Publish a Message".to_string(),
                    description: "Use PUBLISH to send a message to subscribers.".to_string(),
                    command: "PUBLISH notifications \"Hello, subscribers!\"".to_string(),
                    expected_output: None,
                    hint: Some("PUBLISH returns the number of subscribers that received the message.".to_string()),
                },
                TutorialStep {
                    title: "Pattern Subscribe".to_string(),
                    description: "Use PSUBSCRIBE to listen on channels matching a pattern.".to_string(),
                    command: "PSUBSCRIBE notifications.*".to_string(),
                    expected_output: None,
                    hint: Some("Glob patterns let you subscribe to multiple channels at once.".to_string()),
                },
            ],
        },
    ]
}

// ---------------------------------------------------------------------------
// PlaygroundServer
// ---------------------------------------------------------------------------

/// Web playground server that manages sessions, tutorials, and snippet sharing
///
/// Provides the backend logic for the interactive Ferrite web playground.
/// Each session gets an isolated in-memory key-value store for safe
/// experimentation. Sessions are automatically expired after a configurable
/// timeout.
pub struct PlaygroundServer {
    /// Server configuration
    config: PlaygroundServerConfig,
    /// Active sessions keyed by session ID
    sessions: RwLock<HashMap<SessionId, Session>>,
    /// Shared snippets keyed by snippet ID
    snippets: RwLock<HashMap<String, SharedSnippet>>,
    /// Built-in tutorials keyed by tutorial ID
    tutorials: HashMap<String, Tutorial>,
    /// Total sessions ever created
    total_sessions: AtomicU64,
    /// Total commands executed across all sessions
    total_commands: AtomicU64,
    /// Total tutorial completions
    total_tutorial_completions: AtomicU64,
    /// Sum of session durations for average calculation
    session_duration_sum_secs: AtomicU64,
    /// Number of closed sessions (for average calculation)
    closed_session_count: AtomicU64,
}

impl PlaygroundServer {
    /// Create a new playground server with the given configuration
    pub fn new(config: PlaygroundServerConfig) -> Self {
        let tutorials: HashMap<String, Tutorial> = if config.enable_tutorials {
            default_tutorials()
                .into_iter()
                .map(|t| (t.id.clone(), t))
                .collect()
        } else {
            HashMap::new()
        };

        Self {
            config,
            sessions: RwLock::new(HashMap::new()),
            snippets: RwLock::new(HashMap::new()),
            tutorials,
            total_sessions: AtomicU64::new(0),
            total_commands: AtomicU64::new(0),
            total_tutorial_completions: AtomicU64::new(0),
            session_duration_sum_secs: AtomicU64::new(0),
            closed_session_count: AtomicU64::new(0),
        }
    }

    /// Create a new playground session
    ///
    /// Returns an error if the maximum number of concurrent sessions has been
    /// reached.
    pub fn create_session(&self) -> Result<SessionId, PlaygroundError> {
        let mut sessions = self
            .sessions
            .write()
            .map_err(|e| PlaygroundError::Internal(e.to_string()))?;

        if sessions.len() >= self.config.max_sessions {
            return Err(PlaygroundError::MaxSessionsReached {
                limit: self.config.max_sessions,
            });
        }

        let now = Utc::now();
        let id = SessionId::new();
        let session = Session {
            info: SessionInfo {
                id: id.clone(),
                created_at: now,
                last_activity: now,
                commands_executed: 0,
                keys_count: 0,
                memory_used_bytes: 0,
            },
            data: HashMap::new(),
        };

        sessions.insert(id.clone(), session);
        self.total_sessions.fetch_add(1, Ordering::Relaxed);

        Ok(id)
    }

    /// Execute a Redis-compatible command in a session
    pub fn execute_command(
        &self,
        session_id: &SessionId,
        command: &str,
    ) -> Result<ExecutionResult, PlaygroundError> {
        // Validate command length
        if command.len() > self.config.max_command_length {
            return Err(PlaygroundError::CommandTooLong {
                len: command.len(),
                limit: self.config.max_command_length,
            });
        }

        let start = std::time::Instant::now();

        let mut sessions = self
            .sessions
            .write()
            .map_err(|e| PlaygroundError::Internal(e.to_string()))?;

        let session = sessions
            .get_mut(session_id)
            .ok_or_else(|| PlaygroundError::SessionNotFound(session_id.to_string()))?;

        // Check expiry
        let elapsed_secs = (Utc::now() - session.info.last_activity).num_seconds() as u64;
        if elapsed_secs >= self.config.session_timeout_secs {
            return Err(PlaygroundError::SessionExpired(session_id.to_string()));
        }

        // Parse command
        let parts: Vec<&str> = command.split_whitespace().collect();
        if parts.is_empty() {
            return Ok(ExecutionResult {
                success: false,
                output: String::new(),
                execution_time_us: start.elapsed().as_micros() as u64,
                result_type: ResultType::Error,
                error: Some("empty command".to_string()),
            });
        }

        let cmd_name = parts[0].to_uppercase();
        let result = match cmd_name.as_str() {
            "PING" => ExecutionResult {
                success: true,
                output: "PONG".to_string(),
                execution_time_us: 0,
                result_type: ResultType::SimpleString,
                error: None,
            },
            "SET" => {
                if parts.len() < 3 {
                    ExecutionResult {
                        success: false,
                        output: String::new(),
                        execution_time_us: 0,
                        result_type: ResultType::Error,
                        error: Some("ERR wrong number of arguments for 'SET' command".to_string()),
                    }
                } else {
                    let key = parts[1].to_string();
                    let value = parts[2..].join(" ").trim_matches('"').to_string();
                    session.data.insert(key, value);
                    ExecutionResult {
                        success: true,
                        output: "OK".to_string(),
                        execution_time_us: 0,
                        result_type: ResultType::SimpleString,
                        error: None,
                    }
                }
            }
            "GET" => {
                if parts.len() < 2 {
                    ExecutionResult {
                        success: false,
                        output: String::new(),
                        execution_time_us: 0,
                        result_type: ResultType::Error,
                        error: Some("ERR wrong number of arguments for 'GET' command".to_string()),
                    }
                } else {
                    match session.data.get(parts[1]) {
                        Some(val) => ExecutionResult {
                            success: true,
                            output: val.clone(),
                            execution_time_us: 0,
                            result_type: ResultType::BulkString,
                            error: None,
                        },
                        None => ExecutionResult {
                            success: true,
                            output: "(nil)".to_string(),
                            execution_time_us: 0,
                            result_type: ResultType::Null,
                            error: None,
                        },
                    }
                }
            }
            "DEL" => {
                if parts.len() < 2 {
                    ExecutionResult {
                        success: false,
                        output: String::new(),
                        execution_time_us: 0,
                        result_type: ResultType::Error,
                        error: Some("ERR wrong number of arguments for 'DEL' command".to_string()),
                    }
                } else {
                    let mut count = 0i64;
                    for key in &parts[1..] {
                        if session.data.remove(*key).is_some() {
                            count += 1;
                        }
                    }
                    ExecutionResult {
                        success: true,
                        output: count.to_string(),
                        execution_time_us: 0,
                        result_type: ResultType::Integer,
                        error: None,
                    }
                }
            }
            "EXISTS" => {
                if parts.len() < 2 {
                    ExecutionResult {
                        success: false,
                        output: String::new(),
                        execution_time_us: 0,
                        result_type: ResultType::Error,
                        error: Some(
                            "ERR wrong number of arguments for 'EXISTS' command".to_string(),
                        ),
                    }
                } else {
                    let count: i64 = parts[1..]
                        .iter()
                        .filter(|k| session.data.contains_key(**k))
                        .count() as i64;
                    ExecutionResult {
                        success: true,
                        output: count.to_string(),
                        execution_time_us: 0,
                        result_type: ResultType::Integer,
                        error: None,
                    }
                }
            }
            "KEYS" => {
                let pattern = if parts.len() > 1 { parts[1] } else { "*" };
                let keys: Vec<String> = session
                    .data
                    .keys()
                    .filter(|k| simple_pattern_match(pattern, k))
                    .cloned()
                    .collect();
                ExecutionResult {
                    success: true,
                    output: format!("{:?}", keys),
                    execution_time_us: 0,
                    result_type: ResultType::Array,
                    error: None,
                }
            }
            "DBSIZE" => ExecutionResult {
                success: true,
                output: session.data.len().to_string(),
                execution_time_us: 0,
                result_type: ResultType::Integer,
                error: None,
            },
            "FLUSHDB" => {
                session.data.clear();
                ExecutionResult {
                    success: true,
                    output: "OK".to_string(),
                    execution_time_us: 0,
                    result_type: ResultType::SimpleString,
                    error: None,
                }
            }
            _ => ExecutionResult {
                success: false,
                output: String::new(),
                execution_time_us: 0,
                result_type: ResultType::Error,
                error: Some(format!("ERR unknown command '{cmd_name}'")),
            },
        };

        let elapsed_us = start.elapsed().as_micros() as u64;
        let result = ExecutionResult {
            execution_time_us: elapsed_us,
            ..result
        };

        // Check result size
        if result.output.len() > self.config.max_result_size {
            return Err(PlaygroundError::ResultTooLarge {
                size: result.output.len(),
                limit: self.config.max_result_size,
            });
        }

        // Update session metadata
        session.info.last_activity = Utc::now();
        session.info.commands_executed += 1;
        session.info.keys_count = session.data.len() as u64;
        session.info.memory_used_bytes = session
            .data
            .iter()
            .map(|(k, v)| (k.len() + v.len()) as u64)
            .sum();

        self.total_commands.fetch_add(1, Ordering::Relaxed);

        Ok(result)
    }

    /// Execute a FerriteQL query in a session
    pub fn execute_ferriteql(
        &self,
        session_id: &SessionId,
        query: &str,
    ) -> Result<ExecutionResult, PlaygroundError> {
        // Validate query length
        if query.len() > self.config.max_command_length {
            return Err(PlaygroundError::CommandTooLong {
                len: query.len(),
                limit: self.config.max_command_length,
            });
        }

        let start = std::time::Instant::now();

        let sessions = self
            .sessions
            .read()
            .map_err(|e| PlaygroundError::Internal(e.to_string()))?;

        let session = sessions
            .get(session_id)
            .ok_or_else(|| PlaygroundError::SessionNotFound(session_id.to_string()))?;

        // Check expiry
        let elapsed_secs = (Utc::now() - session.info.last_activity).num_seconds() as u64;
        if elapsed_secs >= self.config.session_timeout_secs {
            return Err(PlaygroundError::SessionExpired(session_id.to_string()));
        }

        let query_upper = query.trim().to_uppercase();

        let result_type = if query_upper.starts_with("SELECT") {
            ResultType::Table
        } else {
            ResultType::Json
        };

        // Simulated FerriteQL execution
        let output = format!(
            "{{\"query\": \"{}\", \"rows\": [], \"affected\": 0}}",
            query.replace('"', "\\\"")
        );

        let elapsed_us = start.elapsed().as_micros() as u64;

        // Update command count
        self.total_commands.fetch_add(1, Ordering::Relaxed);

        Ok(ExecutionResult {
            success: true,
            output,
            execution_time_us: elapsed_us,
            result_type,
            error: None,
        })
    }

    /// Get information about an active session
    pub fn get_session(&self, session_id: &SessionId) -> Option<SessionInfo> {
        let sessions = self.sessions.read().ok()?;
        sessions.get(session_id).map(|s| s.info.clone())
    }

    /// List all available tutorials
    pub fn list_tutorials(&self) -> Vec<TutorialSummary> {
        self.tutorials
            .values()
            .map(|t| TutorialSummary {
                id: t.id.clone(),
                title: t.title.clone(),
                difficulty: t.difficulty.clone(),
                estimated_minutes: t.estimated_minutes,
                tags: t.tags.clone(),
            })
            .collect()
    }

    /// Get a tutorial by its ID
    pub fn get_tutorial(&self, id: &str) -> Option<Tutorial> {
        self.tutorials.get(id).cloned()
    }

    /// Create a new shared snippet
    ///
    /// Returns an error if sharing is disabled.
    pub fn create_snippet(&self, snippet: SnippetCreate) -> Result<SharedSnippet, PlaygroundError> {
        if !self.config.enable_sharing {
            return Err(PlaygroundError::Internal(
                "snippet sharing is disabled".to_string(),
            ));
        }

        let id = Uuid::new_v4().to_string();
        let shared = SharedSnippet {
            url: format!("/playground/snippet/{}", id),
            id: id.clone(),
            title: snippet.title,
            description: snippet.description,
            commands: snippet.commands,
            tags: snippet.tags,
            author: snippet.author,
            created_at: Utc::now(),
            views: 0,
        };

        let mut snippets = self
            .snippets
            .write()
            .map_err(|e| PlaygroundError::Internal(e.to_string()))?;
        snippets.insert(id, shared.clone());

        Ok(shared)
    }

    /// Get a shared snippet by its ID, incrementing the view count
    pub fn get_snippet(&self, id: &str) -> Option<SharedSnippet> {
        let mut snippets = self.snippets.write().ok()?;
        let snippet = snippets.get_mut(id)?;
        snippet.views += 1;
        Some(snippet.clone())
    }

    /// Generate embeddable HTML for a snippet
    pub fn get_embed_html(&self, snippet_id: &str) -> String {
        let snippet = self.get_snippet(snippet_id);
        match snippet {
            Some(s) => {
                let commands_html: String = s
                    .commands
                    .iter()
                    .map(|c| format!("<code>{}</code>", html_escape(c)))
                    .collect::<Vec<_>>()
                    .join("\n");

                format!(
                    r#"<div class="ferrite-playground-embed" data-snippet-id="{}">
  <h3>{}</h3>
  <pre>{}</pre>
  <a href="{}" target="_blank">Open in Playground</a>
</div>"#,
                    html_escape(&s.id),
                    html_escape(&s.title),
                    commands_html,
                    html_escape(&s.url),
                )
            }
            None => format!(
                r#"<div class="ferrite-playground-embed error">Snippet not found: {}</div>"#,
                html_escape(snippet_id),
            ),
        }
    }

    /// Remove all sessions that have exceeded the timeout
    ///
    /// Returns the number of sessions removed.
    pub fn cleanup_expired_sessions(&self) -> usize {
        let mut sessions = match self.sessions.write() {
            Ok(s) => s,
            Err(_) => return 0,
        };

        let now = Utc::now();
        let timeout = chrono::Duration::seconds(self.config.session_timeout_secs as i64);

        let expired_ids: Vec<SessionId> = sessions
            .iter()
            .filter(|(_, s)| now - s.info.last_activity >= timeout)
            .map(|(id, _)| id.clone())
            .collect();

        let count = expired_ids.len();

        for id in &expired_ids {
            if let Some(session) = sessions.remove(id) {
                let duration_secs =
                    (session.info.last_activity - session.info.created_at).num_seconds() as u64;
                self.session_duration_sum_secs
                    .fetch_add(duration_secs, Ordering::Relaxed);
                self.closed_session_count.fetch_add(1, Ordering::Relaxed);
            }
        }

        count
    }

    /// Get aggregate server statistics
    pub fn get_stats(&self) -> PlaygroundStats {
        let active = self.sessions.read().map(|s| s.len() as u64).unwrap_or(0);

        let total_snippets = self.snippets.read().map(|s| s.len() as u64).unwrap_or(0);

        let closed = self.closed_session_count.load(Ordering::Relaxed);
        let duration_sum = self.session_duration_sum_secs.load(Ordering::Relaxed);
        let avg_duration = if closed > 0 {
            duration_sum as f64 / closed as f64
        } else {
            0.0
        };

        PlaygroundStats {
            active_sessions: active,
            total_sessions: self.total_sessions.load(Ordering::Relaxed),
            total_commands: self.total_commands.load(Ordering::Relaxed),
            total_snippets,
            total_tutorial_completions: self.total_tutorial_completions.load(Ordering::Relaxed),
            avg_session_duration_secs: avg_duration,
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Simple glob-style pattern matching (supports `*` and `?`)
fn simple_pattern_match(pattern: &str, text: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    let mut p = pattern.chars().peekable();
    let mut t = text.chars().peekable();

    while let Some(pc) = p.next() {
        match pc {
            '*' => {
                if p.peek().is_none() {
                    return true;
                }
                let rest: String = p.collect();
                let remaining: String = t.collect();
                for i in 0..=remaining.len() {
                    if simple_pattern_match(&rest, &remaining[i..]) {
                        return true;
                    }
                }
                return false;
            }
            '?' => {
                if t.next().is_none() {
                    return false;
                }
            }
            c => {
                if t.next() != Some(c) {
                    return false;
                }
            }
        }
    }

    t.peek().is_none()
}

/// Basic HTML escaping for embed output
fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> PlaygroundServerConfig {
        PlaygroundServerConfig {
            max_sessions: 10,
            session_timeout_secs: 3600,
            max_command_length: 1000,
            max_result_size: 1_048_576,
            enable_sharing: true,
            enable_tutorials: true,
            wasm_memory_limit_mb: 64,
        }
    }

    // -- Config tests -------------------------------------------------------

    #[test]
    fn test_config_defaults() {
        let config = PlaygroundServerConfig::default();
        assert_eq!(config.max_sessions, 1000);
        assert_eq!(config.session_timeout_secs, 3600);
        assert_eq!(config.max_command_length, 10_000);
        assert_eq!(config.max_result_size, 1_048_576);
        assert!(config.enable_sharing);
        assert!(config.enable_tutorials);
        assert_eq!(config.wasm_memory_limit_mb, 64);
    }

    #[test]
    fn test_config_custom() {
        let config = PlaygroundServerConfig {
            max_sessions: 5,
            session_timeout_secs: 60,
            max_command_length: 100,
            max_result_size: 512,
            enable_sharing: false,
            enable_tutorials: false,
            wasm_memory_limit_mb: 32,
        };
        assert_eq!(config.max_sessions, 5);
        assert!(!config.enable_sharing);
        assert!(!config.enable_tutorials);
    }

    // -- Session lifecycle tests --------------------------------------------

    #[test]
    fn test_create_session() {
        let server = PlaygroundServer::new(test_config());
        let id = server.create_session().unwrap();
        assert!(!id.as_str().is_empty());
    }

    #[test]
    fn test_get_session() {
        let server = PlaygroundServer::new(test_config());
        let id = server.create_session().unwrap();
        let info = server.get_session(&id).unwrap();
        assert_eq!(info.id, id);
        assert_eq!(info.commands_executed, 0);
        assert_eq!(info.keys_count, 0);
    }

    #[test]
    fn test_get_nonexistent_session() {
        let server = PlaygroundServer::new(test_config());
        let fake = SessionId::from_string("nonexistent".to_string());
        assert!(server.get_session(&fake).is_none());
    }

    #[test]
    fn test_max_sessions_reached() {
        let config = PlaygroundServerConfig {
            max_sessions: 2,
            ..test_config()
        };
        let server = PlaygroundServer::new(config);
        server.create_session().unwrap();
        server.create_session().unwrap();
        let result = server.create_session();
        assert!(result.is_err());
        match result.unwrap_err() {
            PlaygroundError::MaxSessionsReached { limit } => assert_eq!(limit, 2),
            other => panic!("unexpected error: {other}"),
        }
    }

    // -- Command execution tests --------------------------------------------

    #[test]
    fn test_execute_ping() {
        let server = PlaygroundServer::new(test_config());
        let id = server.create_session().unwrap();
        let result = server.execute_command(&id, "PING").unwrap();
        assert!(result.success);
        assert_eq!(result.output, "PONG");
        assert_eq!(result.result_type, ResultType::SimpleString);
        assert!(result.error.is_none());
    }

    #[test]
    fn test_execute_set_get() {
        let server = PlaygroundServer::new(test_config());
        let id = server.create_session().unwrap();

        let set_result = server.execute_command(&id, "SET mykey myvalue").unwrap();
        assert!(set_result.success);
        assert_eq!(set_result.output, "OK");

        let get_result = server.execute_command(&id, "GET mykey").unwrap();
        assert!(get_result.success);
        assert_eq!(get_result.output, "myvalue");
        assert_eq!(get_result.result_type, ResultType::BulkString);
    }

    #[test]
    fn test_execute_get_missing_key() {
        let server = PlaygroundServer::new(test_config());
        let id = server.create_session().unwrap();
        let result = server.execute_command(&id, "GET nonexistent").unwrap();
        assert!(result.success);
        assert_eq!(result.result_type, ResultType::Null);
    }

    #[test]
    fn test_execute_del() {
        let server = PlaygroundServer::new(test_config());
        let id = server.create_session().unwrap();
        server.execute_command(&id, "SET k1 v1").unwrap();
        let result = server.execute_command(&id, "DEL k1").unwrap();
        assert!(result.success);
        assert_eq!(result.output, "1");
        assert_eq!(result.result_type, ResultType::Integer);
    }

    #[test]
    fn test_execute_exists() {
        let server = PlaygroundServer::new(test_config());
        let id = server.create_session().unwrap();
        server.execute_command(&id, "SET k1 v1").unwrap();
        let result = server.execute_command(&id, "EXISTS k1").unwrap();
        assert_eq!(result.output, "1");
        let result = server.execute_command(&id, "EXISTS missing").unwrap();
        assert_eq!(result.output, "0");
    }

    #[test]
    fn test_execute_dbsize() {
        let server = PlaygroundServer::new(test_config());
        let id = server.create_session().unwrap();
        server.execute_command(&id, "SET a 1").unwrap();
        server.execute_command(&id, "SET b 2").unwrap();
        let result = server.execute_command(&id, "DBSIZE").unwrap();
        assert_eq!(result.output, "2");
    }

    #[test]
    fn test_execute_flushdb() {
        let server = PlaygroundServer::new(test_config());
        let id = server.create_session().unwrap();
        server.execute_command(&id, "SET a 1").unwrap();
        let result = server.execute_command(&id, "FLUSHDB").unwrap();
        assert!(result.success);
        let result = server.execute_command(&id, "DBSIZE").unwrap();
        assert_eq!(result.output, "0");
    }

    #[test]
    fn test_execute_unknown_command() {
        let server = PlaygroundServer::new(test_config());
        let id = server.create_session().unwrap();
        let result = server.execute_command(&id, "FAKECMD arg1").unwrap();
        assert!(!result.success);
        assert_eq!(result.result_type, ResultType::Error);
        assert!(result.error.is_some());
    }

    #[test]
    fn test_execute_empty_command() {
        let server = PlaygroundServer::new(test_config());
        let id = server.create_session().unwrap();
        let result = server.execute_command(&id, "   ").unwrap();
        assert!(!result.success);
        assert_eq!(result.result_type, ResultType::Error);
    }

    #[test]
    fn test_command_too_long() {
        let config = PlaygroundServerConfig {
            max_command_length: 10,
            ..test_config()
        };
        let server = PlaygroundServer::new(config);
        let id = server.create_session().unwrap();
        let result = server.execute_command(&id, "SET longkey longvalue_that_exceeds");
        assert!(result.is_err());
        match result.unwrap_err() {
            PlaygroundError::CommandTooLong { len, limit } => {
                assert!(len > limit);
                assert_eq!(limit, 10);
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn test_execute_on_nonexistent_session() {
        let server = PlaygroundServer::new(test_config());
        let fake = SessionId::from_string("no-such-session".to_string());
        let result = server.execute_command(&fake, "PING");
        assert!(result.is_err());
        match result.unwrap_err() {
            PlaygroundError::SessionNotFound(_) => {}
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn test_session_metadata_updated() {
        let server = PlaygroundServer::new(test_config());
        let id = server.create_session().unwrap();

        server.execute_command(&id, "SET a 1").unwrap();
        server.execute_command(&id, "SET b 2").unwrap();

        let info = server.get_session(&id).unwrap();
        assert_eq!(info.commands_executed, 2);
        assert_eq!(info.keys_count, 2);
        assert!(info.memory_used_bytes > 0);
    }

    // -- FerriteQL tests ----------------------------------------------------

    #[test]
    fn test_ferriteql_select() {
        let server = PlaygroundServer::new(test_config());
        let id = server.create_session().unwrap();
        let result = server.execute_ferriteql(&id, "SELECT * FROM keys").unwrap();
        assert!(result.success);
        assert_eq!(result.result_type, ResultType::Table);
        assert!(result.output.contains("query"));
    }

    #[test]
    fn test_ferriteql_non_select() {
        let server = PlaygroundServer::new(test_config());
        let id = server.create_session().unwrap();
        let result = server
            .execute_ferriteql(&id, "INSERT INTO keys VALUES ('k', 'v')")
            .unwrap();
        assert!(result.success);
        assert_eq!(result.result_type, ResultType::Json);
    }

    #[test]
    fn test_ferriteql_nonexistent_session() {
        let server = PlaygroundServer::new(test_config());
        let fake = SessionId::from_string("nope".to_string());
        let result = server.execute_ferriteql(&fake, "SELECT 1");
        assert!(result.is_err());
    }

    // -- Tutorial tests -----------------------------------------------------

    #[test]
    fn test_list_tutorials() {
        let server = PlaygroundServer::new(test_config());
        let tutorials = server.list_tutorials();
        assert!(!tutorials.is_empty());
        assert!(tutorials.len() >= 5);
    }

    #[test]
    fn test_default_tutorials_loaded() {
        let server = PlaygroundServer::new(test_config());
        let names: Vec<String> = server
            .list_tutorials()
            .iter()
            .map(|t| t.id.clone())
            .collect();
        assert!(names.contains(&"getting-started".to_string()));
        assert!(names.contains(&"data-types".to_string()));
        assert!(names.contains(&"ferriteql-basics".to_string()));
        assert!(names.contains(&"vector-search".to_string()));
        assert!(names.contains(&"pubsub-patterns".to_string()));
    }

    #[test]
    fn test_get_tutorial() {
        let server = PlaygroundServer::new(test_config());
        let tutorial = server.get_tutorial("getting-started").unwrap();
        assert_eq!(tutorial.title, "Getting Started");
        assert_eq!(tutorial.difficulty, Difficulty::Beginner);
        assert_eq!(tutorial.estimated_minutes, 5);
        assert!(!tutorial.steps.is_empty());
    }

    #[test]
    fn test_get_tutorial_not_found() {
        let server = PlaygroundServer::new(test_config());
        assert!(server.get_tutorial("nonexistent-tutorial").is_none());
    }

    #[test]
    fn test_tutorials_disabled() {
        let config = PlaygroundServerConfig {
            enable_tutorials: false,
            ..test_config()
        };
        let server = PlaygroundServer::new(config);
        assert!(server.list_tutorials().is_empty());
    }

    #[test]
    fn test_tutorial_difficulties() {
        let server = PlaygroundServer::new(test_config());
        let gs = server.get_tutorial("getting-started").unwrap();
        assert_eq!(gs.difficulty, Difficulty::Beginner);
        let fql = server.get_tutorial("ferriteql-basics").unwrap();
        assert_eq!(fql.difficulty, Difficulty::Intermediate);
        let vs = server.get_tutorial("vector-search").unwrap();
        assert_eq!(vs.difficulty, Difficulty::Advanced);
    }

    // -- Snippet tests ------------------------------------------------------

    #[test]
    fn test_create_snippet() {
        let server = PlaygroundServer::new(test_config());
        let snippet = server
            .create_snippet(SnippetCreate {
                title: "Test Snippet".to_string(),
                description: Some("A test".to_string()),
                commands: vec!["SET foo bar".to_string(), "GET foo".to_string()],
                tags: vec!["test".to_string()],
                author: Some("tester".to_string()),
            })
            .unwrap();

        assert!(!snippet.id.is_empty());
        assert_eq!(snippet.title, "Test Snippet");
        assert_eq!(snippet.commands.len(), 2);
        assert_eq!(snippet.views, 0);
        assert!(snippet.url.contains(&snippet.id));
    }

    #[test]
    fn test_get_snippet() {
        let server = PlaygroundServer::new(test_config());
        let created = server
            .create_snippet(SnippetCreate {
                title: "My Snippet".to_string(),
                description: None,
                commands: vec!["PING".to_string()],
                tags: vec![],
                author: None,
            })
            .unwrap();

        let fetched = server.get_snippet(&created.id).unwrap();
        assert_eq!(fetched.title, "My Snippet");
        assert_eq!(fetched.views, 1); // view count incremented
    }

    #[test]
    fn test_get_snippet_not_found() {
        let server = PlaygroundServer::new(test_config());
        assert!(server.get_snippet("nonexistent-id").is_none());
    }

    #[test]
    fn test_snippet_view_count_increments() {
        let server = PlaygroundServer::new(test_config());
        let created = server
            .create_snippet(SnippetCreate {
                title: "Views Test".to_string(),
                description: None,
                commands: vec!["PING".to_string()],
                tags: vec![],
                author: None,
            })
            .unwrap();

        server.get_snippet(&created.id);
        server.get_snippet(&created.id);
        let fetched = server.get_snippet(&created.id).unwrap();
        assert_eq!(fetched.views, 3);
    }

    #[test]
    fn test_snippet_sharing_disabled() {
        let config = PlaygroundServerConfig {
            enable_sharing: false,
            ..test_config()
        };
        let server = PlaygroundServer::new(config);
        let result = server.create_snippet(SnippetCreate {
            title: "Nope".to_string(),
            description: None,
            commands: vec![],
            tags: vec![],
            author: None,
        });
        assert!(result.is_err());
    }

    // -- Embed HTML tests ---------------------------------------------------

    #[test]
    fn test_embed_html_with_snippet() {
        let server = PlaygroundServer::new(test_config());
        let created = server
            .create_snippet(SnippetCreate {
                title: "Embed Test".to_string(),
                description: None,
                commands: vec!["SET x 1".to_string(), "GET x".to_string()],
                tags: vec![],
                author: None,
            })
            .unwrap();

        let html = server.get_embed_html(&created.id);
        assert!(html.contains("ferrite-playground-embed"));
        assert!(html.contains("Embed Test"));
        assert!(html.contains("SET x 1"));
        assert!(html.contains("GET x"));
        assert!(html.contains("Open in Playground"));
    }

    #[test]
    fn test_embed_html_not_found() {
        let server = PlaygroundServer::new(test_config());
        let html = server.get_embed_html("no-such-snippet");
        assert!(html.contains("Snippet not found"));
    }

    // -- Session timeout / expiry tests -------------------------------------

    #[test]
    fn test_cleanup_no_expired() {
        let server = PlaygroundServer::new(test_config());
        server.create_session().unwrap();
        let removed = server.cleanup_expired_sessions();
        assert_eq!(removed, 0);
    }

    #[test]
    fn test_cleanup_expired_sessions() {
        let config = PlaygroundServerConfig {
            session_timeout_secs: 1,
            ..test_config()
        };
        let server = PlaygroundServer::new(config);
        server.create_session().unwrap();
        server.create_session().unwrap();

        std::thread::sleep(std::time::Duration::from_secs(2));

        let removed = server.cleanup_expired_sessions();
        assert_eq!(removed, 2);

        let stats = server.get_stats();
        assert_eq!(stats.active_sessions, 0);
    }

    #[test]
    fn test_session_expired_error_on_execute() {
        let config = PlaygroundServerConfig {
            session_timeout_secs: 1,
            ..test_config()
        };
        let server = PlaygroundServer::new(config);
        let id = server.create_session().unwrap();

        std::thread::sleep(std::time::Duration::from_secs(2));

        let result = server.execute_command(&id, "PING");
        assert!(result.is_err());
        match result.unwrap_err() {
            PlaygroundError::SessionExpired(_) => {}
            other => panic!("unexpected error: {other}"),
        }
    }

    // -- Stats tests --------------------------------------------------------

    #[test]
    fn test_stats_initial() {
        let server = PlaygroundServer::new(test_config());
        let stats = server.get_stats();
        assert_eq!(stats.active_sessions, 0);
        assert_eq!(stats.total_sessions, 0);
        assert_eq!(stats.total_commands, 0);
        assert_eq!(stats.total_snippets, 0);
        assert_eq!(stats.avg_session_duration_secs, 0.0);
    }

    #[test]
    fn test_stats_after_activity() {
        let server = PlaygroundServer::new(test_config());

        let id1 = server.create_session().unwrap();
        let id2 = server.create_session().unwrap();

        server.execute_command(&id1, "SET a 1").unwrap();
        server.execute_command(&id1, "GET a").unwrap();
        server.execute_command(&id2, "PING").unwrap();

        server
            .create_snippet(SnippetCreate {
                title: "S".to_string(),
                description: None,
                commands: vec!["PING".to_string()],
                tags: vec![],
                author: None,
            })
            .unwrap();

        let stats = server.get_stats();
        assert_eq!(stats.active_sessions, 2);
        assert_eq!(stats.total_sessions, 2);
        assert_eq!(stats.total_commands, 3);
        assert_eq!(stats.total_snippets, 1);
    }

    // -- SessionId tests ----------------------------------------------------

    #[test]
    fn test_session_id_display() {
        let id = SessionId::from_string("test-id-123".to_string());
        assert_eq!(format!("{id}"), "test-id-123");
        assert_eq!(id.as_str(), "test-id-123");
    }

    #[test]
    fn test_session_id_default() {
        let id = SessionId::default();
        assert!(!id.as_str().is_empty());
    }

    #[test]
    fn test_session_id_equality() {
        let a = SessionId::from_string("same".to_string());
        let b = SessionId::from_string("same".to_string());
        assert_eq!(a, b);
    }

    // -- Result type tests --------------------------------------------------

    #[test]
    fn test_result_types_coverage() {
        let server = PlaygroundServer::new(test_config());
        let id = server.create_session().unwrap();

        let r = server.execute_command(&id, "PING").unwrap();
        assert_eq!(r.result_type, ResultType::SimpleString);

        server.execute_command(&id, "SET k v").unwrap();
        let r = server.execute_command(&id, "GET k").unwrap();
        assert_eq!(r.result_type, ResultType::BulkString);

        let r = server.execute_command(&id, "DEL k").unwrap();
        assert_eq!(r.result_type, ResultType::Integer);

        let r = server.execute_command(&id, "GET missing").unwrap();
        assert_eq!(r.result_type, ResultType::Null);

        let r = server.execute_command(&id, "KEYS *").unwrap();
        assert_eq!(r.result_type, ResultType::Array);

        let r = server.execute_command(&id, "BADCMD").unwrap();
        assert_eq!(r.result_type, ResultType::Error);
    }

    #[test]
    fn test_ferriteql_result_types() {
        let server = PlaygroundServer::new(test_config());
        let id = server.create_session().unwrap();

        let r = server.execute_ferriteql(&id, "SELECT * FROM t").unwrap();
        assert_eq!(r.result_type, ResultType::Table);

        let r = server
            .execute_ferriteql(&id, "INSERT INTO t VALUES (1)")
            .unwrap();
        assert_eq!(r.result_type, ResultType::Json);
    }

    // -- Pattern matching helper tests --------------------------------------

    #[test]
    fn test_simple_pattern_match() {
        assert!(simple_pattern_match("*", "anything"));
        assert!(simple_pattern_match("foo", "foo"));
        assert!(!simple_pattern_match("foo", "bar"));
        assert!(simple_pattern_match("user:*", "user:123"));
        assert!(!simple_pattern_match("user:*", "admin:1"));
        assert!(simple_pattern_match("h?llo", "hello"));
        assert!(simple_pattern_match("h?llo", "hallo"));
        assert!(!simple_pattern_match("h?llo", "hllo"));
    }

    // -- HTML escape helper tests -------------------------------------------

    #[test]
    fn test_html_escape() {
        assert_eq!(html_escape("<script>"), "&lt;script&gt;");
        assert_eq!(html_escape("a&b"), "a&amp;b");
        assert_eq!(html_escape("\"quoted\""), "&quot;quoted&quot;");
        assert_eq!(html_escape("plain"), "plain");
    }
}
