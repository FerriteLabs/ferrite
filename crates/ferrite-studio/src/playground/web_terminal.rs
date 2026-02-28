//! WebSocket Terminal Adapter for Playground
//!
//! Provides a WebSocket-friendly terminal adapter that connects browser terminals
//! to playground sessions managed by [`SessionManager`].
//!
//! # Example
//!
//! ```ignore
//! use ferrite_studio::playground::session_manager::SessionManager;
//! use ferrite_studio::playground::web_terminal::{WebTerminalAdapter, WebTerminalConfig};
//! use std::sync::Arc;
//!
//! let session_mgr = Arc::new(SessionManager::new(Default::default()));
//! let adapter = WebTerminalAdapter::new(session_mgr, WebTerminalConfig::default());
//! let handle = adapter.create_terminal(Default::default()).unwrap();
//! let output = adapter.send_command(&handle.terminal_id, "PING").unwrap();
//! assert_eq!(output.output, "PONG");
//! ```

#![forbid(unsafe_code)]

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use super::session_manager::{SessionManager, SessionOptions};

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Errors returned by [`WebTerminalAdapter`] operations.
#[derive(Debug, Clone, thiserror::Error)]
pub enum TerminalError {
    /// Terminal with the given ID was not found
    #[error("terminal not found: {0}")]
    NotFound(String),
    /// The underlying session has expired
    #[error("session expired: {0}")]
    SessionExpired(String),
    /// Maximum number of terminal connections reached
    #[error("maximum connections reached")]
    MaxConnectionsReached,
    /// A command failed to execute
    #[error("command failed: {0}")]
    CommandFailed(String),
    /// Requested tutorial was not found
    #[error("tutorial not found: {0}")]
    TutorialNotFound(String),
    /// The request was invalid
    #[error("invalid request: {0}")]
    InvalidRequest(String),
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the [`WebTerminalAdapter`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WebTerminalConfig {
    /// Maximum number of concurrent terminal connections (default: 200)
    pub max_connections: usize,
    /// Seconds of inactivity before a terminal is considered stale (default: 600)
    pub idle_timeout_secs: u64,
    /// Maximum size of the per-terminal output buffer in bytes (default: 65536)
    pub max_output_buffer: usize,
    /// Whether to attach syntax classification hints to output (default: true)
    pub enable_syntax_highlighting: bool,
    /// Welcome message displayed when a terminal is created
    pub welcome_message: String,
    /// Prompt string returned with every command output
    pub prompt: String,
}

impl Default for WebTerminalConfig {
    fn default() -> Self {
        Self {
            max_connections: 200,
            idle_timeout_secs: 600,
            max_output_buffer: 64 * 1024,
            enable_syntax_highlighting: true,
            welcome_message: "Welcome to Ferrite Playground! Type HELP for commands.".to_string(),
            prompt: "ferrite> ".to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// Connection state
// ---------------------------------------------------------------------------

/// State of a terminal connection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TerminalConnectionState {
    /// Terminal is actively being used
    Active,
    /// Terminal is idle (no recent activity)
    Idle,
    /// Terminal has been closed
    Closed,
}

// ---------------------------------------------------------------------------
// Terminal connection (internal)
// ---------------------------------------------------------------------------

/// Internal representation of a single terminal connection.
pub struct TerminalConnection {
    /// Unique terminal identifier
    pub id: String,
    /// Associated session identifier in the [`SessionManager`]
    pub session_id: String,
    /// Unix timestamp (seconds) when the connection was created
    pub created_at: u64,
    /// Unix timestamp (seconds) of the last activity
    pub last_activity: u64,
    /// Command history for this terminal
    pub history: Vec<TerminalHistoryEntry>,
    /// Buffered output lines
    pub output_buffer: Vec<String>,
    /// Optional tutorial runner attached to this terminal
    pub tutorial: Option<TutorialRunner>,
    /// Current connection state
    pub state: TerminalConnectionState,
}

// ---------------------------------------------------------------------------
// Public data types
// ---------------------------------------------------------------------------

/// Options for creating a new terminal.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TerminalOptions {
    /// Start with a specific tutorial (e.g. `"getting-started"`)
    pub tutorial: Option<String>,
    /// Preload commands or dataset identifier
    pub preload: Option<String>,
    /// User-visible label for the terminal tab
    pub label: Option<String>,
}

/// Handle returned after successfully creating a terminal.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TerminalHandle {
    /// Unique terminal identifier
    pub terminal_id: String,
    /// Associated session identifier
    pub session_id: String,
    /// Welcome message to display
    pub welcome_message: String,
    /// Prompt string
    pub prompt: String,
    /// Tutorial info if a tutorial was requested
    pub tutorial_info: Option<WebTutorialInfo>,
}

/// Output from executing a command in a terminal.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TerminalOutput {
    /// Formatted command result
    pub output: String,
    /// Whether the output represents an error
    pub is_error: bool,
    /// Execution time in microseconds
    pub execution_time_us: u64,
    /// Prompt to display after this output
    pub prompt: String,
    /// Hint for the current tutorial step, if applicable
    pub tutorial_hint: Option<String>,
    /// Syntax classification for highlighting (e.g. `"string"`, `"error"`, `"integer"`, `"array"`)
    pub syntax_class: Option<String>,
}

/// A single entry in the terminal command history.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TerminalHistoryEntry {
    /// The command that was entered
    pub input: String,
    /// The result that was returned
    pub output: String,
    /// Unix timestamp (seconds) of the command
    pub timestamp: u64,
    /// Execution duration in microseconds
    pub duration_us: u64,
    /// Whether the result was an error
    pub is_error: bool,
}

/// Read-only snapshot of a terminal's state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TerminalState {
    /// Terminal identifier
    pub terminal_id: String,
    /// Associated session identifier
    pub session_id: String,
    /// Unix timestamp (seconds) of creation
    pub created_at: u64,
    /// Unix timestamp (seconds) of last activity
    pub last_activity: u64,
    /// Total number of commands executed
    pub command_count: usize,
    /// Current connection state
    pub state: TerminalConnectionState,
    /// Tutorial info if running a tutorial
    pub tutorial: Option<WebTutorialInfo>,
}

// ---------------------------------------------------------------------------
// Tutorial types
// ---------------------------------------------------------------------------

/// Manages a step-by-step tutorial within a terminal.
pub struct TutorialRunner {
    /// Tutorial identifier
    pub tutorial_id: String,
    /// Ordered list of tutorial steps
    pub steps: Vec<WebTutorialStep>,
    /// Index of the current step (0-based)
    pub current_step: usize,
    /// Whether the tutorial has been completed
    pub completed: bool,
}

/// A single step in a terminal tutorial.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WebTutorialStep {
    /// 1-based step number
    pub step_number: usize,
    /// Total number of steps in the tutorial
    pub total_steps: usize,
    /// Step title
    pub title: String,
    /// Instructional description
    pub description: String,
    /// The command the user is expected to type
    pub expected_command: Option<String>,
    /// Hint text to guide the user
    pub hint: String,
    /// Whether this step has been completed
    pub completed: bool,
}

/// Summary information about a tutorial attached to a terminal.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WebTutorialInfo {
    /// Tutorial identifier
    pub id: String,
    /// Tutorial title
    pub title: String,
    /// Current step index (0-based)
    pub current_step: usize,
    /// Total number of steps
    pub total_steps: usize,
    /// Whether the tutorial is complete
    pub completed: bool,
}

// ---------------------------------------------------------------------------
// HTTP response
// ---------------------------------------------------------------------------

/// Simple HTTP response returned by [`WebTerminalAdapter::handle_http_request`].
pub struct HttpResponse {
    /// HTTP status code
    pub status: u16,
    /// Content-Type header value
    pub content_type: String,
    /// Response body
    pub body: String,
}

impl HttpResponse {
    fn json(status: u16, body: &str) -> Self {
        Self {
            status,
            content_type: "application/json".to_string(),
            body: body.to_string(),
        }
    }

    fn ok<T: Serialize>(value: &T) -> Self {
        let body = serde_json::to_string(value).unwrap_or_else(|e| {
            format!(r#"{{"error":"serialization error: {e}"}}"#)
        });
        Self::json(200, &body)
    }

    fn created<T: Serialize>(value: &T) -> Self {
        let body = serde_json::to_string(value).unwrap_or_else(|e| {
            format!(r#"{{"error":"serialization error: {e}"}}"#)
        });
        Self::json(201, &body)
    }

    fn error(status: u16, message: &str) -> Self {
        let body = serde_json::to_string(&serde_json::json!({"error": message}))
            .unwrap_or_else(|_| format!(r#"{{"error":"{message}"}}"#));
        Self::json(status, &body)
    }

    fn not_found(message: &str) -> Self {
        Self::error(404, message)
    }
}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

/// Atomic counters for terminal adapter statistics.
struct TerminalStats {
    terminals_created: AtomicU64,
    terminals_closed: AtomicU64,
    commands_executed: AtomicU64,
    commands_failed: AtomicU64,
}

impl TerminalStats {
    fn new() -> Self {
        Self {
            terminals_created: AtomicU64::new(0),
            terminals_closed: AtomicU64::new(0),
            commands_executed: AtomicU64::new(0),
            commands_failed: AtomicU64::new(0),
        }
    }
}

/// Point-in-time snapshot of terminal adapter statistics.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TerminalStatsSnapshot {
    /// Total terminals created
    pub terminals_created: u64,
    /// Total terminals closed
    pub terminals_closed: u64,
    /// Total commands executed
    pub commands_executed: u64,
    /// Total commands that failed
    pub commands_failed: u64,
    /// Number of currently active connections
    pub active_connections: usize,
}

// ---------------------------------------------------------------------------
// WebTerminalAdapter
// ---------------------------------------------------------------------------

/// Bridges browser terminals to playground sessions.
///
/// Each terminal maps 1-to-1 to a [`SessionManager`] session and adds
/// output buffering, syntax classification, tutorial guidance, and REST
/// endpoint routing on top.
pub struct WebTerminalAdapter {
    /// Shared reference to the underlying session manager
    session_manager: Arc<SessionManager>,
    /// Adapter configuration
    config: WebTerminalConfig,
    /// Active terminal connections keyed by terminal ID
    connections: RwLock<HashMap<String, TerminalConnection>>,
    /// Aggregate statistics
    stats: TerminalStats,
    /// Counter for generating unique terminal IDs
    id_counter: AtomicU64,
    /// Built-in tutorial steps for "Getting Started"
    builtin_tutorial_steps: Vec<WebTutorialStep>,
}

impl WebTerminalAdapter {
    /// Create a new adapter with the given session manager and configuration.
    ///
    /// A built-in "Getting Started" tutorial with five steps is automatically
    /// registered.
    pub fn new(session_manager: Arc<SessionManager>, config: WebTerminalConfig) -> Self {
        let steps = build_getting_started_steps();
        Self {
            session_manager,
            config,
            connections: RwLock::new(HashMap::new()),
            stats: TerminalStats::new(),
            id_counter: AtomicU64::new(0),
            builtin_tutorial_steps: steps,
        }
    }

    /// Create a new terminal connection.
    ///
    /// A backing session is created in the [`SessionManager`] automatically.
    pub fn create_terminal(
        &self,
        options: TerminalOptions,
    ) -> Result<TerminalHandle, TerminalError> {
        let conns = self.connections.read();
        if conns.len() >= self.config.max_connections {
            return Err(TerminalError::MaxConnectionsReached);
        }
        drop(conns);

        // Create a backing session
        let session_opts = SessionOptions {
            tutorial: options.tutorial.clone(),
            preload_dataset: options.preload.clone(),
            label: options.label.clone(),
        };
        let session_handle = self
            .session_manager
            .create_session(session_opts)
            .map_err(|e| TerminalError::CommandFailed(e.to_string()))?;

        let now = current_timestamp_secs();
        let terminal_id = self.generate_terminal_id(now);

        // Build tutorial runner if requested
        let tutorial_runner = options
            .tutorial
            .as_ref()
            .and_then(|tid| self.build_tutorial_runner(tid));

        let tutorial_info = tutorial_runner.as_ref().map(|r| runner_to_info(r));

        let connection = TerminalConnection {
            id: terminal_id.clone(),
            session_id: session_handle.id.clone(),
            created_at: now,
            last_activity: now,
            history: Vec::new(),
            output_buffer: Vec::new(),
            tutorial: tutorial_runner,
            state: TerminalConnectionState::Active,
        };

        self.connections
            .write()
            .insert(terminal_id.clone(), connection);
        self.stats.terminals_created.fetch_add(1, Ordering::Relaxed);

        Ok(TerminalHandle {
            terminal_id,
            session_id: session_handle.id,
            welcome_message: self.config.welcome_message.clone(),
            prompt: self.config.prompt.clone(),
            tutorial_info,
        })
    }

    /// Send a command to the terminal and return the formatted output.
    pub fn send_command(
        &self,
        terminal_id: &str,
        input: &str,
    ) -> Result<TerminalOutput, TerminalError> {
        let session_id = {
            let conns = self.connections.read();
            let conn = conns
                .get(terminal_id)
                .ok_or_else(|| TerminalError::NotFound(terminal_id.to_string()))?;
            if conn.state == TerminalConnectionState::Closed {
                return Err(TerminalError::NotFound(terminal_id.to_string()));
            }
            conn.session_id.clone()
        };

        // Execute via session manager
        let cmd_output = self
            .session_manager
            .execute_command(&session_id, input)
            .map_err(|e| {
                self.stats.commands_failed.fetch_add(1, Ordering::Relaxed);
                match e {
                    super::session_manager::SessionError::SessionExpired(id) => {
                        TerminalError::SessionExpired(id)
                    }
                    super::session_manager::SessionError::SessionNotFound(id) => {
                        TerminalError::SessionExpired(id)
                    }
                    other => TerminalError::CommandFailed(other.to_string()),
                }
            })?;

        self.stats.commands_executed.fetch_add(1, Ordering::Relaxed);

        let now = current_timestamp_secs();
        let syntax_class = if self.config.enable_syntax_highlighting {
            Some(classify_output(&cmd_output.result))
        } else {
            None
        };

        // Update connection state and get tutorial hint
        let tutorial_hint = {
            let mut conns = self.connections.write();
            if let Some(conn) = conns.get_mut(terminal_id) {
                conn.last_activity = now;
                conn.state = TerminalConnectionState::Active;

                // Record history
                let entry = TerminalHistoryEntry {
                    input: input.to_string(),
                    output: cmd_output.result.clone(),
                    timestamp: now,
                    duration_us: cmd_output.execution_time_us,
                    is_error: cmd_output.is_error,
                };
                conn.history.push(entry);

                // Buffer output (trim if over limit)
                conn.output_buffer.push(cmd_output.result.clone());
                let total_size: usize = conn.output_buffer.iter().map(|s| s.len()).sum();
                while total_size > self.config.max_output_buffer && conn.output_buffer.len() > 1 {
                    conn.output_buffer.remove(0);
                }

                // Get tutorial hint for current step
                conn.tutorial.as_ref().and_then(|runner| {
                    if runner.completed {
                        None
                    } else {
                        runner
                            .steps
                            .get(runner.current_step)
                            .map(|step| step.hint.clone())
                    }
                })
            } else {
                None
            }
        };

        Ok(TerminalOutput {
            output: cmd_output.result,
            is_error: cmd_output.is_error,
            execution_time_us: cmd_output.execution_time_us,
            prompt: self.config.prompt.clone(),
            tutorial_hint,
            syntax_class,
        })
    }

    /// Retrieve the command history for a terminal.
    pub fn get_history(
        &self,
        terminal_id: &str,
    ) -> Result<Vec<TerminalHistoryEntry>, TerminalError> {
        let conns = self.connections.read();
        let conn = conns
            .get(terminal_id)
            .ok_or_else(|| TerminalError::NotFound(terminal_id.to_string()))?;
        Ok(conn.history.clone())
    }

    /// Close a terminal and destroy its backing session.
    pub fn close_terminal(&self, terminal_id: &str) -> Result<(), TerminalError> {
        let mut conns = self.connections.write();
        let conn = conns
            .get_mut(terminal_id)
            .ok_or_else(|| TerminalError::NotFound(terminal_id.to_string()))?;

        let session_id = conn.session_id.clone();
        conn.state = TerminalConnectionState::Closed;
        conns.remove(terminal_id);
        drop(conns);

        // Best-effort destroy session
        let _ = self.session_manager.destroy_session(&session_id);
        self.stats.terminals_closed.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Return the number of active (non-closed) terminal connections.
    pub fn active_terminals(&self) -> usize {
        let conns = self.connections.read();
        conns
            .values()
            .filter(|c| c.state != TerminalConnectionState::Closed)
            .count()
    }

    /// Get a read-only snapshot of a terminal's state.
    pub fn get_terminal_state(&self, terminal_id: &str) -> Option<TerminalState> {
        let conns = self.connections.read();
        conns.get(terminal_id).map(|conn| TerminalState {
            terminal_id: conn.id.clone(),
            session_id: conn.session_id.clone(),
            created_at: conn.created_at,
            last_activity: conn.last_activity,
            command_count: conn.history.len(),
            state: conn.state.clone(),
            tutorial: conn.tutorial.as_ref().map(runner_to_info),
        })
    }

    /// Route an HTTP request to the appropriate handler and return a response.
    ///
    /// Supported routes:
    /// - `POST /api/playground/terminal` — create a terminal
    /// - `POST /api/playground/terminal/{id}/command` — execute a command
    /// - `GET  /api/playground/terminal/{id}/history` — get command history
    /// - `GET  /api/playground/terminal/{id}/tutorial` — get current tutorial step
    /// - `POST /api/playground/terminal/{id}/tutorial/next` — advance tutorial
    /// - `DELETE /api/playground/terminal/{id}` — close a terminal
    pub fn handle_http_request(&self, path: &str, body: &str) -> HttpResponse {
        // Normalize path: strip trailing slash
        let path = path.trim_end_matches('/');

        // POST /api/playground/terminal — create
        if path == "/api/playground/terminal" {
            let options: TerminalOptions = serde_json::from_str(body).unwrap_or_default();
            return match self.create_terminal(options) {
                Ok(handle) => HttpResponse::created(&handle),
                Err(e) => terminal_error_to_response(&e),
            };
        }

        // Routes that include a terminal ID
        let prefix = "/api/playground/terminal/";
        if !path.starts_with(prefix) {
            return HttpResponse::not_found("unknown endpoint");
        }

        let rest = &path[prefix.len()..];

        // Parse terminal_id and optional sub-path
        let (terminal_id, sub_path) = match rest.find('/') {
            Some(pos) => (&rest[..pos], &rest[pos..]),
            None => (rest, ""),
        };

        if terminal_id.is_empty() {
            return HttpResponse::error(400, "missing terminal id");
        }

        match sub_path {
            "/command" => {
                // POST — send command
                #[derive(Deserialize)]
                struct CmdBody {
                    command: String,
                }
                match serde_json::from_str::<CmdBody>(body) {
                    Ok(cmd) => match self.send_command(terminal_id, &cmd.command) {
                        Ok(output) => HttpResponse::ok(&output),
                        Err(e) => terminal_error_to_response(&e),
                    },
                    Err(_) => HttpResponse::error(400, "invalid JSON: expected {\"command\": \"...\"}"),
                }
            }
            "/history" => {
                // GET — history
                match self.get_history(terminal_id) {
                    Ok(history) => HttpResponse::ok(&history),
                    Err(e) => terminal_error_to_response(&e),
                }
            }
            "/tutorial" => {
                // GET — current tutorial step
                match self.tutorial_step(terminal_id) {
                    Ok(step) => HttpResponse::ok(&step),
                    Err(e) => terminal_error_to_response(&e),
                }
            }
            "/tutorial/next" => {
                // POST — advance tutorial
                match self.advance_tutorial(terminal_id) {
                    Ok(step) => HttpResponse::ok(&step),
                    Err(e) => terminal_error_to_response(&e),
                }
            }
            "" => {
                // DELETE — close terminal
                match self.close_terminal(terminal_id) {
                    Ok(()) => HttpResponse::json(200, r#"{"status":"closed"}"#),
                    Err(e) => terminal_error_to_response(&e),
                }
            }
            _ => HttpResponse::not_found("unknown endpoint"),
        }
    }

    /// Remove terminals that have been idle longer than `idle_timeout_secs`.
    ///
    /// Returns the number of terminals removed.
    pub fn cleanup_stale(&self) -> usize {
        let now = current_timestamp_secs();
        let timeout = self.config.idle_timeout_secs;

        let mut conns = self.connections.write();
        let stale_ids: Vec<String> = conns
            .iter()
            .filter(|(_, c)| {
                c.state != TerminalConnectionState::Closed
                    && now.saturating_sub(c.last_activity) >= timeout
            })
            .map(|(id, _)| id.clone())
            .collect();

        let count = stale_ids.len();
        for id in &stale_ids {
            if let Some(conn) = conns.remove(id) {
                let _ = self.session_manager.destroy_session(&conn.session_id);
            }
        }
        drop(conns);

        self.stats
            .terminals_closed
            .fetch_add(count as u64, Ordering::Relaxed);
        count
    }

    /// Get the current tutorial step for a terminal.
    pub fn tutorial_step(
        &self,
        terminal_id: &str,
    ) -> Result<Option<WebTutorialStep>, TerminalError> {
        let conns = self.connections.read();
        let conn = conns
            .get(terminal_id)
            .ok_or_else(|| TerminalError::NotFound(terminal_id.to_string()))?;

        Ok(conn.tutorial.as_ref().and_then(|runner| {
            if runner.completed {
                None
            } else {
                runner.steps.get(runner.current_step).cloned()
            }
        }))
    }

    /// Advance the tutorial to the next step and return it.
    pub fn advance_tutorial(
        &self,
        terminal_id: &str,
    ) -> Result<Option<WebTutorialStep>, TerminalError> {
        let mut conns = self.connections.write();
        let conn = conns
            .get_mut(terminal_id)
            .ok_or_else(|| TerminalError::NotFound(terminal_id.to_string()))?;

        let runner = conn
            .tutorial
            .as_mut()
            .ok_or_else(|| TerminalError::TutorialNotFound("no tutorial active".to_string()))?;

        if runner.completed {
            return Ok(None);
        }

        // Mark current step as completed
        if let Some(step) = runner.steps.get_mut(runner.current_step) {
            step.completed = true;
        }

        runner.current_step += 1;
        if runner.current_step >= runner.steps.len() {
            runner.completed = true;
            return Ok(None);
        }

        Ok(runner.steps.get(runner.current_step).cloned())
    }

    /// Take a snapshot of the aggregate statistics.
    pub fn stats(&self) -> TerminalStatsSnapshot {
        let active = self.active_terminals();
        TerminalStatsSnapshot {
            terminals_created: self.stats.terminals_created.load(Ordering::Relaxed),
            terminals_closed: self.stats.terminals_closed.load(Ordering::Relaxed),
            commands_executed: self.stats.commands_executed.load(Ordering::Relaxed),
            commands_failed: self.stats.commands_failed.load(Ordering::Relaxed),
            active_connections: active,
        }
    }

    // -- private helpers ----------------------------------------------------

    /// Generate a unique terminal ID.
    fn generate_terminal_id(&self, timestamp: u64) -> String {
        let seq = self.id_counter.fetch_add(1, Ordering::Relaxed);
        format!("term-{:x}-{:x}", timestamp, seq)
    }

    /// Build a [`TutorialRunner`] for the given tutorial ID.
    ///
    /// Currently only the built-in `"getting-started"` tutorial is supported.
    fn build_tutorial_runner(&self, tutorial_id: &str) -> Option<TutorialRunner> {
        if tutorial_id == "getting-started" {
            Some(TutorialRunner {
                tutorial_id: tutorial_id.to_string(),
                steps: self.builtin_tutorial_steps.clone(),
                current_step: 0,
                completed: false,
            })
        } else {
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Free helper functions
// ---------------------------------------------------------------------------

/// Build the five steps for the built-in "Getting Started" tutorial.
fn build_getting_started_steps() -> Vec<WebTutorialStep> {
    let total = 5;
    vec![
        WebTutorialStep {
            step_number: 1,
            total_steps: total,
            title: "Connect and PING".to_string(),
            description: "Send a PING command to verify the connection.".to_string(),
            expected_command: Some("PING".to_string()),
            hint: "Type PING and press Enter.".to_string(),
            completed: false,
        },
        WebTutorialStep {
            step_number: 2,
            total_steps: total,
            title: "Store a value".to_string(),
            description: "Use the SET command to store a greeting.".to_string(),
            expected_command: Some("SET greeting \"Hello Ferrite!\"".to_string()),
            hint: "Type: SET greeting \"Hello Ferrite!\"".to_string(),
            completed: false,
        },
        WebTutorialStep {
            step_number: 3,
            total_steps: total,
            title: "Retrieve a value".to_string(),
            description: "Use GET to read back the value you just stored.".to_string(),
            expected_command: Some("GET greeting".to_string()),
            hint: "Type: GET greeting".to_string(),
            completed: false,
        },
        WebTutorialStep {
            step_number: 4,
            total_steps: total,
            title: "Work with lists".to_string(),
            description: "Push multiple items onto a list with LPUSH.".to_string(),
            expected_command: Some("LPUSH colors red blue green".to_string()),
            hint: "Type: LPUSH colors red blue green".to_string(),
            completed: false,
        },
        WebTutorialStep {
            step_number: 5,
            total_steps: total,
            title: "Explore keys".to_string(),
            description: "List all keys in the database with KEYS *.".to_string(),
            expected_command: Some("KEYS *".to_string()),
            hint: "Type: KEYS *".to_string(),
            completed: false,
        },
    ]
}

/// Convert a [`TutorialRunner`] to a [`WebTutorialInfo`] summary.
fn runner_to_info(runner: &TutorialRunner) -> WebTutorialInfo {
    let title = match runner.tutorial_id.as_str() {
        "getting-started" => "Getting Started".to_string(),
        other => other.to_string(),
    };
    WebTutorialInfo {
        id: runner.tutorial_id.clone(),
        title,
        current_step: runner.current_step,
        total_steps: runner.steps.len(),
        completed: runner.completed,
    }
}

/// Classify output for syntax highlighting.
fn classify_output(output: &str) -> String {
    if output.starts_with("ERR") || output.starts_with("WRONGTYPE") {
        "error".to_string()
    } else if output.starts_with("(integer)") {
        "integer".to_string()
    } else if output == "OK" || output == "PONG" {
        "string".to_string()
    } else if output.starts_with("1)") || output.starts_with("(empty") {
        "array".to_string()
    } else if output.starts_with('"') || output.starts_with('\'') {
        "string".to_string()
    } else {
        "string".to_string()
    }
}

/// Map a [`TerminalError`] to an [`HttpResponse`].
fn terminal_error_to_response(err: &TerminalError) -> HttpResponse {
    match err {
        TerminalError::NotFound(msg) => HttpResponse::not_found(msg),
        TerminalError::SessionExpired(msg) => HttpResponse::error(410, msg),
        TerminalError::MaxConnectionsReached => {
            HttpResponse::error(429, "maximum connections reached")
        }
        TerminalError::CommandFailed(msg) => HttpResponse::error(500, msg),
        TerminalError::TutorialNotFound(msg) => HttpResponse::not_found(msg),
        TerminalError::InvalidRequest(msg) => HttpResponse::error(400, msg),
    }
}

/// Get current Unix timestamp in seconds.
fn current_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::playground::session_manager::SessionConfig;

    fn test_adapter() -> WebTerminalAdapter {
        let session_mgr = Arc::new(SessionManager::new(SessionConfig::default()));
        WebTerminalAdapter::new(session_mgr, WebTerminalConfig::default())
    }

    fn test_adapter_with_config(
        session_config: SessionConfig,
        terminal_config: WebTerminalConfig,
    ) -> WebTerminalAdapter {
        let session_mgr = Arc::new(SessionManager::new(session_config));
        WebTerminalAdapter::new(session_mgr, terminal_config)
    }

    #[test]
    fn test_create_terminal() {
        let adapter = test_adapter();
        let handle = adapter.create_terminal(TerminalOptions::default()).unwrap();
        assert!(handle.terminal_id.starts_with("term-"));
        assert!(handle.session_id.starts_with("sess-"));
        assert_eq!(handle.welcome_message, WebTerminalConfig::default().welcome_message);
        assert_eq!(handle.prompt, "ferrite> ");
        assert!(handle.tutorial_info.is_none());
    }

    #[test]
    fn test_create_terminal_with_tutorial() {
        let adapter = test_adapter();
        let options = TerminalOptions {
            tutorial: Some("getting-started".to_string()),
            ..Default::default()
        };
        let handle = adapter.create_terminal(options).unwrap();
        let info = handle.tutorial_info.unwrap();
        assert_eq!(info.id, "getting-started");
        assert_eq!(info.total_steps, 5);
        assert!(!info.completed);
    }

    #[test]
    fn test_send_command_ping() {
        let adapter = test_adapter();
        let handle = adapter.create_terminal(TerminalOptions::default()).unwrap();
        let output = adapter.send_command(&handle.terminal_id, "PING").unwrap();
        assert_eq!(output.output, "PONG");
        assert!(!output.is_error);
        assert_eq!(output.prompt, "ferrite> ");
    }

    #[test]
    fn test_send_command_set_get() {
        let adapter = test_adapter();
        let handle = adapter.create_terminal(TerminalOptions::default()).unwrap();
        let output = adapter
            .send_command(&handle.terminal_id, "SET foo bar")
            .unwrap();
        assert_eq!(output.output, "OK");
        assert!(!output.is_error);
    }

    #[test]
    fn test_send_command_error() {
        let adapter = test_adapter();
        let handle = adapter.create_terminal(TerminalOptions::default()).unwrap();
        let output = adapter
            .send_command(&handle.terminal_id, "UNKNOWNCMD")
            .unwrap();
        assert!(output.is_error);
    }

    #[test]
    fn test_send_command_not_found() {
        let adapter = test_adapter();
        let result = adapter.send_command("nonexistent", "PING");
        assert!(matches!(result, Err(TerminalError::NotFound(_))));
    }

    #[test]
    fn test_get_history() {
        let adapter = test_adapter();
        let handle = adapter.create_terminal(TerminalOptions::default()).unwrap();
        adapter.send_command(&handle.terminal_id, "PING").unwrap();
        adapter
            .send_command(&handle.terminal_id, "SET k v")
            .unwrap();

        let history = adapter.get_history(&handle.terminal_id).unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].input, "PING");
        assert_eq!(history[0].output, "PONG");
        assert_eq!(history[1].input, "SET k v");
    }

    #[test]
    fn test_close_terminal() {
        let adapter = test_adapter();
        let handle = adapter.create_terminal(TerminalOptions::default()).unwrap();
        assert_eq!(adapter.active_terminals(), 1);

        adapter.close_terminal(&handle.terminal_id).unwrap();
        assert_eq!(adapter.active_terminals(), 0);

        // Cannot send commands after close
        let result = adapter.send_command(&handle.terminal_id, "PING");
        assert!(matches!(result, Err(TerminalError::NotFound(_))));
    }

    #[test]
    fn test_close_terminal_not_found() {
        let adapter = test_adapter();
        let result = adapter.close_terminal("nonexistent");
        assert!(matches!(result, Err(TerminalError::NotFound(_))));
    }

    #[test]
    fn test_active_terminals_count() {
        let adapter = test_adapter();
        assert_eq!(adapter.active_terminals(), 0);
        let h1 = adapter.create_terminal(TerminalOptions::default()).unwrap();
        let _h2 = adapter.create_terminal(TerminalOptions::default()).unwrap();
        assert_eq!(adapter.active_terminals(), 2);
        adapter.close_terminal(&h1.terminal_id).unwrap();
        assert_eq!(adapter.active_terminals(), 1);
    }

    #[test]
    fn test_get_terminal_state() {
        let adapter = test_adapter();
        let handle = adapter.create_terminal(TerminalOptions::default()).unwrap();
        adapter.send_command(&handle.terminal_id, "PING").unwrap();

        let state = adapter.get_terminal_state(&handle.terminal_id).unwrap();
        assert_eq!(state.terminal_id, handle.terminal_id);
        assert_eq!(state.session_id, handle.session_id);
        assert_eq!(state.command_count, 1);
        assert_eq!(state.state, TerminalConnectionState::Active);
    }

    #[test]
    fn test_get_terminal_state_not_found() {
        let adapter = test_adapter();
        assert!(adapter.get_terminal_state("nonexistent").is_none());
    }

    #[test]
    fn test_max_connections_reached() {
        let terminal_config = WebTerminalConfig {
            max_connections: 2,
            ..Default::default()
        };
        let adapter = test_adapter_with_config(SessionConfig::default(), terminal_config);
        adapter.create_terminal(TerminalOptions::default()).unwrap();
        adapter.create_terminal(TerminalOptions::default()).unwrap();
        let result = adapter.create_terminal(TerminalOptions::default());
        assert!(matches!(result, Err(TerminalError::MaxConnectionsReached)));
    }

    #[test]
    fn test_tutorial_step_and_advance() {
        let adapter = test_adapter();
        let options = TerminalOptions {
            tutorial: Some("getting-started".to_string()),
            ..Default::default()
        };
        let handle = adapter.create_terminal(options).unwrap();

        // Step 1
        let step = adapter
            .tutorial_step(&handle.terminal_id)
            .unwrap()
            .unwrap();
        assert_eq!(step.step_number, 1);
        assert_eq!(step.title, "Connect and PING");
        assert!(!step.completed);

        // Advance to step 2
        let step = adapter
            .advance_tutorial(&handle.terminal_id)
            .unwrap()
            .unwrap();
        assert_eq!(step.step_number, 2);
        assert_eq!(step.title, "Store a value");

        // Advance through remaining steps
        let _ = adapter.advance_tutorial(&handle.terminal_id).unwrap(); // step 3
        let _ = adapter.advance_tutorial(&handle.terminal_id).unwrap(); // step 4
        let _ = adapter.advance_tutorial(&handle.terminal_id).unwrap(); // step 5

        // All done — returns None
        let step = adapter.advance_tutorial(&handle.terminal_id).unwrap();
        assert!(step.is_none());
    }

    #[test]
    fn test_tutorial_not_active() {
        let adapter = test_adapter();
        let handle = adapter.create_terminal(TerminalOptions::default()).unwrap();
        let result = adapter.advance_tutorial(&handle.terminal_id);
        assert!(matches!(result, Err(TerminalError::TutorialNotFound(_))));
    }

    #[test]
    fn test_stats_snapshot() {
        let adapter = test_adapter();
        let handle = adapter.create_terminal(TerminalOptions::default()).unwrap();
        adapter.send_command(&handle.terminal_id, "PING").unwrap();
        adapter.close_terminal(&handle.terminal_id).unwrap();

        let stats = adapter.stats();
        assert_eq!(stats.terminals_created, 1);
        assert_eq!(stats.terminals_closed, 1);
        assert_eq!(stats.commands_executed, 1);
        assert_eq!(stats.active_connections, 0);
    }

    #[test]
    fn test_cleanup_stale() {
        let terminal_config = WebTerminalConfig {
            idle_timeout_secs: 0, // expire immediately
            ..Default::default()
        };
        let adapter = test_adapter_with_config(SessionConfig::default(), terminal_config);
        adapter.create_terminal(TerminalOptions::default()).unwrap();
        adapter.create_terminal(TerminalOptions::default()).unwrap();

        let removed = adapter.cleanup_stale();
        assert_eq!(removed, 2);
        assert_eq!(adapter.active_terminals(), 0);
    }

    #[test]
    fn test_syntax_classification() {
        assert_eq!(classify_output("PONG"), "string");
        assert_eq!(classify_output("OK"), "string");
        assert_eq!(classify_output("(integer) 3"), "integer");
        assert_eq!(classify_output("ERR unknown command"), "error");
        assert_eq!(classify_output("1) \"foo\""), "array");
    }

    #[test]
    fn test_http_create_terminal() {
        let adapter = test_adapter();
        let resp = adapter.handle_http_request("/api/playground/terminal", "{}");
        assert_eq!(resp.status, 201);
        assert_eq!(resp.content_type, "application/json");
        assert!(resp.body.contains("terminal_id"));
    }

    #[test]
    fn test_http_send_command() {
        let adapter = test_adapter();
        let resp = adapter.handle_http_request("/api/playground/terminal", "{}");
        assert_eq!(resp.status, 201);

        // Extract terminal_id from response
        let handle: TerminalHandle = serde_json::from_str(&resp.body).unwrap();
        let path = format!("/api/playground/terminal/{}/command", handle.terminal_id);
        let body = r#"{"command":"PING"}"#;
        let resp = adapter.handle_http_request(&path, body);
        assert_eq!(resp.status, 200);
        assert!(resp.body.contains("PONG"));
    }

    #[test]
    fn test_http_get_history() {
        let adapter = test_adapter();
        let handle = adapter.create_terminal(TerminalOptions::default()).unwrap();
        adapter.send_command(&handle.terminal_id, "PING").unwrap();

        let path = format!("/api/playground/terminal/{}/history", handle.terminal_id);
        let resp = adapter.handle_http_request(&path, "");
        assert_eq!(resp.status, 200);
        assert!(resp.body.contains("PING"));
    }

    #[test]
    fn test_http_close_terminal() {
        let adapter = test_adapter();
        let handle = adapter.create_terminal(TerminalOptions::default()).unwrap();

        let path = format!("/api/playground/terminal/{}", handle.terminal_id);
        let resp = adapter.handle_http_request(&path, "");
        assert_eq!(resp.status, 200);
        assert!(resp.body.contains("closed"));
        assert_eq!(adapter.active_terminals(), 0);
    }

    #[test]
    fn test_http_unknown_endpoint() {
        let adapter = test_adapter();
        let resp = adapter.handle_http_request("/api/unknown", "");
        assert_eq!(resp.status, 404);
    }

    #[test]
    fn test_http_tutorial_endpoints() {
        let adapter = test_adapter();
        let body = r#"{"tutorial":"getting-started"}"#;
        let resp = adapter.handle_http_request("/api/playground/terminal", body);
        assert_eq!(resp.status, 201);
        let handle: TerminalHandle = serde_json::from_str(&resp.body).unwrap();

        // Get current tutorial step
        let path = format!("/api/playground/terminal/{}/tutorial", handle.terminal_id);
        let resp = adapter.handle_http_request(&path, "");
        assert_eq!(resp.status, 200);
        assert!(resp.body.contains("Connect and PING"));

        // Advance tutorial
        let path = format!(
            "/api/playground/terminal/{}/tutorial/next",
            handle.terminal_id
        );
        let resp = adapter.handle_http_request(&path, "");
        assert_eq!(resp.status, 200);
        assert!(resp.body.contains("Store a value"));
    }

    #[test]
    fn test_terminal_error_display() {
        assert!(TerminalError::NotFound("x".into())
            .to_string()
            .contains('x'));
        assert!(TerminalError::SessionExpired("x".into())
            .to_string()
            .contains('x'));
        assert_eq!(
            TerminalError::MaxConnectionsReached.to_string(),
            "maximum connections reached"
        );
        assert!(TerminalError::CommandFailed("fail".into())
            .to_string()
            .contains("fail"));
        assert!(TerminalError::TutorialNotFound("t".into())
            .to_string()
            .contains('t'));
        assert!(TerminalError::InvalidRequest("bad".into())
            .to_string()
            .contains("bad"));
    }

    #[test]
    fn test_config_defaults() {
        let config = WebTerminalConfig::default();
        assert_eq!(config.max_connections, 200);
        assert_eq!(config.idle_timeout_secs, 600);
        assert_eq!(config.max_output_buffer, 64 * 1024);
        assert!(config.enable_syntax_highlighting);
        assert!(config.welcome_message.contains("Ferrite Playground"));
        assert_eq!(config.prompt, "ferrite> ");
    }
}
