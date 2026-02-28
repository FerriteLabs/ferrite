//! Session Manager for Playground
//!
//! Manages ephemeral playground sessions with resource limits, rate limiting,
//! and automatic cleanup of expired sessions.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Manages ephemeral playground sessions with resource limits and auto-cleanup.
pub struct SessionManager {
    /// Configuration for session management
    config: SessionConfig,
    /// Active sessions indexed by session ID
    sessions: RwLock<HashMap<String, PlaygroundSession>>,
    /// Aggregate statistics tracked with atomics
    stats: SessionStats,
    /// Counter used for generating unique session IDs
    id_counter: AtomicU64,
}

impl SessionManager {
    /// Create a new session manager with the given configuration.
    pub fn new(config: SessionConfig) -> Self {
        Self {
            config,
            sessions: RwLock::new(HashMap::new()),
            stats: SessionStats::new(),
            id_counter: AtomicU64::new(0),
        }
    }

    /// Create a new playground session with the given options.
    ///
    /// Returns a [`SessionHandle`] containing the session ID and expiry time,
    /// or a [`SessionError`] if limits are exceeded.
    pub fn create_session(
        &self,
        options: SessionOptions,
    ) -> Result<SessionHandle, SessionError> {
        let mut sessions = self.sessions.write();

        if sessions.len() >= self.config.max_concurrent_sessions {
            return Err(SessionError::MaxSessionsReached);
        }

        let now = current_timestamp_secs();
        let id = self.generate_id(now);
        let expires_at = now + self.config.default_ttl_secs;

        let tutorial_state = options.tutorial.as_ref().map(|tid| TutorialProgress {
            tutorial_id: tid.clone(),
            current_step: 0,
            total_steps: 0,
            completed_steps: Vec::new(),
            started_at: now,
        });

        let session = PlaygroundSession {
            id: id.clone(),
            created_at: now,
            expires_at,
            last_activity: now,
            command_count: 0,
            memory_used: 0,
            key_count: 0,
            command_history: Vec::new(),
            tutorial_state,
            options,
        };

        sessions.insert(id.clone(), session);
        self.stats.sessions_created.fetch_add(1, Ordering::Relaxed);

        Ok(SessionHandle {
            id,
            expires_at,
            endpoint: String::from("/playground/session"),
        })
    }

    /// Retrieve information about a session by ID, or `None` if not found.
    pub fn get_session(&self, id: &str) -> Option<SessionInfo> {
        let sessions = self.sessions.read();
        sessions.get(id).map(|s| session_to_info(s))
    }

    /// Execute a command within a session.
    ///
    /// Validates the session is active, checks command restrictions and rate limits,
    /// records the command in history, and returns simulated output.
    pub fn execute_command(
        &self,
        session_id: &str,
        command: &str,
    ) -> Result<CommandOutput, SessionError> {
        if command.len() > self.config.max_command_length {
            return Err(SessionError::CommandTooLong);
        }

        let trimmed = command.trim();
        if trimmed.is_empty() {
            return Err(SessionError::InvalidCommand(
                "empty command".to_string(),
            ));
        }

        // Check blocked commands
        let upper = trimmed.to_uppercase();
        for blocked in &self.config.blocked_commands {
            if upper.starts_with(&blocked.to_uppercase()) {
                self.stats.commands_blocked.fetch_add(1, Ordering::Relaxed);
                return Err(SessionError::CommandBlocked(blocked.clone()));
            }
        }

        // If an allowlist is configured, verify the command is in it
        if let Some(ref allowed) = self.config.allowed_commands {
            let cmd_name = upper.split_whitespace().next().unwrap_or("");
            if !allowed.iter().any(|a| a.to_uppercase() == cmd_name) {
                self.stats.commands_blocked.fetch_add(1, Ordering::Relaxed);
                return Err(SessionError::CommandBlocked(cmd_name.to_string()));
            }
        }

        let now = current_timestamp_secs();
        let start_us = current_timestamp_us();

        let mut sessions = self.sessions.write();
        let session = sessions
            .get_mut(session_id)
            .ok_or_else(|| SessionError::SessionNotFound(session_id.to_string()))?;

        if now >= session.expires_at {
            return Err(SessionError::SessionExpired(session_id.to_string()));
        }

        // Simple rate limiting: check commands per second window
        if self.config.rate_limit_commands_per_sec > 0 && session.last_activity == now {
            let recent = session
                .command_history
                .iter()
                .rev()
                .take_while(|e| e.timestamp == now)
                .count() as u32;
            if recent >= self.config.rate_limit_commands_per_sec {
                return Err(SessionError::RateLimitExceeded);
            }
        }

        // Simulate execution and resource tracking
        let (result_str, is_error, mem_delta, key_delta) = simulate_command(trimmed);

        // Check memory limit
        let new_memory = session.memory_used.saturating_add(mem_delta);
        if new_memory > self.config.max_memory_per_session_bytes {
            return Err(SessionError::MemoryLimitExceeded);
        }

        // Check key limit
        let new_keys = (session.key_count as isize + key_delta).max(0) as usize;
        if new_keys > self.config.max_keys_per_session {
            return Err(SessionError::KeyLimitExceeded);
        }

        let duration_us = current_timestamp_us().saturating_sub(start_us);

        session.command_count += 1;
        session.last_activity = now;
        session.memory_used = new_memory;
        session.key_count = new_keys;
        session.command_history.push(CommandHistoryEntry {
            command: trimmed.to_string(),
            result: result_str.clone(),
            timestamp: now,
            duration_us,
        });

        self.stats
            .commands_executed
            .fetch_add(1, Ordering::Relaxed);
        self.stats
            .total_memory_used
            .fetch_add(mem_delta as u64, Ordering::Relaxed);

        Ok(CommandOutput {
            result: result_str,
            execution_time_us: duration_us,
            is_error,
        })
    }

    /// Destroy a session by ID, freeing its resources.
    pub fn destroy_session(&self, id: &str) -> Result<(), SessionError> {
        let mut sessions = self.sessions.write();
        let session = sessions
            .remove(id)
            .ok_or_else(|| SessionError::SessionNotFound(id.to_string()))?;

        self.stats
            .sessions_destroyed
            .fetch_add(1, Ordering::Relaxed);
        self.stats
            .total_memory_used
            .fetch_sub(session.memory_used as u64, Ordering::Relaxed);

        Ok(())
    }

    /// Return the number of currently active (non-expired) sessions.
    pub fn active_sessions(&self) -> usize {
        let sessions = self.sessions.read();
        let now = current_timestamp_secs();
        sessions.values().filter(|s| s.expires_at > now).count()
    }

    /// Remove all sessions whose TTL has expired. Returns the number removed.
    pub fn cleanup_expired(&self) -> usize {
        let mut sessions = self.sessions.write();
        let now = current_timestamp_secs();
        let before = sessions.len();

        let expired_mem: u64 = sessions
            .values()
            .filter(|s| s.expires_at <= now)
            .map(|s| s.memory_used as u64)
            .sum();

        sessions.retain(|_, s| s.expires_at > now);
        let removed = before - sessions.len();

        if removed > 0 {
            self.stats
                .sessions_expired
                .fetch_add(removed as u64, Ordering::Relaxed);
            self.stats
                .total_memory_used
                .fetch_sub(expired_mem, Ordering::Relaxed);
        }

        removed
    }

    /// List all sessions with their current info.
    pub fn list_sessions(&self) -> Vec<SessionInfo> {
        let sessions = self.sessions.read();
        sessions.values().map(session_to_info).collect()
    }

    /// Extend a session's lifetime by the given number of seconds.
    ///
    /// The new expiry time will not exceed `max_ttl_secs` from the session's creation time.
    pub fn extend_session(&self, id: &str, extra_secs: u64) -> Result<(), SessionError> {
        let mut sessions = self.sessions.write();
        let session = sessions
            .get_mut(id)
            .ok_or_else(|| SessionError::SessionNotFound(id.to_string()))?;

        let max_expires = session.created_at + self.config.max_ttl_secs;
        session.expires_at = (session.expires_at + extra_secs).min(max_expires);
        Ok(())
    }

    /// Take a snapshot of the aggregate session statistics.
    pub fn stats(&self) -> SessionStatsSnapshot {
        SessionStatsSnapshot {
            sessions_created: self.stats.sessions_created.load(Ordering::Relaxed),
            sessions_expired: self.stats.sessions_expired.load(Ordering::Relaxed),
            sessions_destroyed: self.stats.sessions_destroyed.load(Ordering::Relaxed),
            commands_executed: self.stats.commands_executed.load(Ordering::Relaxed),
            commands_blocked: self.stats.commands_blocked.load(Ordering::Relaxed),
            total_memory_used: self.stats.total_memory_used.load(Ordering::Relaxed),
        }
    }

    /// Generate a unique session ID from a counter and timestamp.
    fn generate_id(&self, timestamp: u64) -> String {
        let seq = self.id_counter.fetch_add(1, Ordering::Relaxed);
        format!("sess-{:x}-{:x}", timestamp, seq)
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the [`SessionManager`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionConfig {
    /// Maximum number of concurrent sessions (default: 100)
    pub max_concurrent_sessions: usize,
    /// Default session time-to-live in seconds (default: 900 = 15 min)
    pub default_ttl_secs: u64,
    /// Maximum session time-to-live in seconds (default: 3600 = 1 hour)
    pub max_ttl_secs: u64,
    /// Maximum memory per session in bytes (default: 64 MB)
    pub max_memory_per_session_bytes: usize,
    /// Maximum number of keys per session (default: 10,000)
    pub max_keys_per_session: usize,
    /// Maximum command string length in bytes (default: 4096)
    pub max_command_length: usize,
    /// Optional whitelist of allowed commands. `None` means all commands are allowed.
    pub allowed_commands: Option<Vec<String>>,
    /// Commands that are always blocked (case-insensitive prefix match).
    pub blocked_commands: Vec<String>,
    /// Maximum commands per second per session (default: 100)
    pub rate_limit_commands_per_sec: u32,
    /// Whether session data is persisted (default: false)
    pub enable_persistence: bool,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            max_concurrent_sessions: 100,
            default_ttl_secs: 900,
            max_ttl_secs: 3600,
            max_memory_per_session_bytes: 64 * 1024 * 1024,
            max_keys_per_session: 10_000,
            max_command_length: 4096,
            allowed_commands: None,
            blocked_commands: vec![
                "FLUSHALL".to_string(),
                "SHUTDOWN".to_string(),
                "DEBUG".to_string(),
                "CONFIG SET".to_string(),
                "SLAVEOF".to_string(),
                "REPLICAOF".to_string(),
                "MODULE".to_string(),
            ],
            rate_limit_commands_per_sec: 100,
            enable_persistence: false,
        }
    }
}

// ---------------------------------------------------------------------------
// Session types
// ---------------------------------------------------------------------------

/// An ephemeral playground session with resource tracking.
#[derive(Clone, Debug)]
pub struct PlaygroundSession {
    /// Unique session identifier
    pub id: String,
    /// Unix timestamp (seconds) when the session was created
    pub created_at: u64,
    /// Unix timestamp (seconds) when the session expires
    pub expires_at: u64,
    /// Unix timestamp (seconds) of the last command execution
    pub last_activity: u64,
    /// Total number of commands executed in this session
    pub command_count: u64,
    /// Estimated memory used by this session in bytes
    pub memory_used: usize,
    /// Estimated number of keys stored in this session
    pub key_count: usize,
    /// History of executed commands
    pub command_history: Vec<CommandHistoryEntry>,
    /// Tutorial progress, if the session is running a tutorial
    pub tutorial_state: Option<TutorialProgress>,
    /// Options provided when the session was created
    pub options: SessionOptions,
}

/// Options provided when creating a session.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SessionOptions {
    /// Start with a specific tutorial
    pub tutorial: Option<String>,
    /// Preload a sample dataset (e.g., `"sample_ecommerce"`, `"sample_iot"`)
    pub preload_dataset: Option<String>,
    /// User-provided label for the session
    pub label: Option<String>,
}

/// Handle returned after successfully creating a session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionHandle {
    /// Session identifier
    pub id: String,
    /// Unix timestamp (seconds) when the session will expire
    pub expires_at: u64,
    /// API endpoint for this session
    pub endpoint: String,
}

/// Read-only snapshot of a session's state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionInfo {
    /// Session identifier
    pub id: String,
    /// Unix timestamp (seconds) when the session was created
    pub created_at: u64,
    /// Unix timestamp (seconds) when the session expires
    pub expires_at: u64,
    /// Unix timestamp (seconds) of the last activity
    pub last_activity: u64,
    /// Total number of commands executed
    pub command_count: u64,
    /// Estimated memory used in bytes
    pub memory_used: usize,
    /// Estimated number of keys stored
    pub key_count: usize,
    /// Whether the session has expired
    pub is_expired: bool,
    /// Tutorial ID if the session is running a tutorial
    pub tutorial: Option<String>,
}

/// Output from executing a command in a session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommandOutput {
    /// The result string returned by the command
    pub result: String,
    /// Execution time in microseconds
    pub execution_time_us: u64,
    /// Whether the result is an error
    pub is_error: bool,
}

/// A single entry in the command history of a session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommandHistoryEntry {
    /// The command that was executed
    pub command: String,
    /// The result string
    pub result: String,
    /// Unix timestamp (seconds) of execution
    pub timestamp: u64,
    /// Duration in microseconds
    pub duration_us: u64,
}

/// Tutorial progress within a session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TutorialProgress {
    /// Tutorial identifier
    pub tutorial_id: String,
    /// Index of the current step
    pub current_step: usize,
    /// Total number of steps in the tutorial
    pub total_steps: usize,
    /// Indices of completed steps
    pub completed_steps: Vec<usize>,
    /// Unix timestamp (seconds) when the tutorial was started
    pub started_at: u64,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can occur during session operations.
#[derive(Debug, Clone, thiserror::Error)]
pub enum SessionError {
    /// Maximum number of concurrent sessions has been reached
    #[error("maximum concurrent sessions reached")]
    MaxSessionsReached,
    /// Session with the given ID was not found
    #[error("session not found: {0}")]
    SessionNotFound(String),
    /// Session has expired
    #[error("session expired: {0}")]
    SessionExpired(String),
    /// Command is blocked by policy
    #[error("command blocked: {0}")]
    CommandBlocked(String),
    /// Rate limit exceeded
    #[error("rate limit exceeded")]
    RateLimitExceeded,
    /// Memory limit exceeded
    #[error("memory limit exceeded")]
    MemoryLimitExceeded,
    /// Key limit exceeded
    #[error("key limit exceeded")]
    KeyLimitExceeded,
    /// Command exceeds maximum length
    #[error("command too long")]
    CommandTooLong,
    /// Command is invalid
    #[error("invalid command: {0}")]
    InvalidCommand(String),
}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

/// Atomic counters for aggregate session statistics.
struct SessionStats {
    sessions_created: AtomicU64,
    sessions_expired: AtomicU64,
    sessions_destroyed: AtomicU64,
    commands_executed: AtomicU64,
    commands_blocked: AtomicU64,
    total_memory_used: AtomicU64,
}

impl SessionStats {
    fn new() -> Self {
        Self {
            sessions_created: AtomicU64::new(0),
            sessions_expired: AtomicU64::new(0),
            sessions_destroyed: AtomicU64::new(0),
            commands_executed: AtomicU64::new(0),
            commands_blocked: AtomicU64::new(0),
            total_memory_used: AtomicU64::new(0),
        }
    }
}

/// A point-in-time snapshot of session statistics.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionStatsSnapshot {
    /// Total sessions created since startup
    pub sessions_created: u64,
    /// Total sessions that expired
    pub sessions_expired: u64,
    /// Total sessions explicitly destroyed
    pub sessions_destroyed: u64,
    /// Total commands executed across all sessions
    pub commands_executed: u64,
    /// Total commands blocked by policy
    pub commands_blocked: u64,
    /// Aggregate memory used across all active sessions
    pub total_memory_used: u64,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Get current Unix timestamp in seconds.
fn current_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Get current Unix timestamp in microseconds.
fn current_timestamp_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

/// Convert a [`PlaygroundSession`] to a [`SessionInfo`] snapshot.
fn session_to_info(session: &PlaygroundSession) -> SessionInfo {
    let now = current_timestamp_secs();
    SessionInfo {
        id: session.id.clone(),
        created_at: session.created_at,
        expires_at: session.expires_at,
        last_activity: session.last_activity,
        command_count: session.command_count,
        memory_used: session.memory_used,
        key_count: session.key_count,
        is_expired: now >= session.expires_at,
        tutorial: session
            .tutorial_state
            .as_ref()
            .map(|t| t.tutorial_id.clone()),
    }
}

/// Simulate command execution and return (result, is_error, memory_delta, key_delta).
///
/// In production this would delegate to the real Ferrite engine. Here we provide
/// a lightweight simulation that recognises the most common Redis-compatible
/// commands so that tests and the playground UI can function.
fn simulate_command(command: &str) -> (String, bool, usize, isize) {
    let parts: Vec<&str> = command.split_whitespace().collect();
    if parts.is_empty() {
        return ("ERR empty command".to_string(), true, 0, 0);
    }

    let cmd = parts[0].to_uppercase();
    match cmd.as_str() {
        "PING" => ("PONG".to_string(), false, 0, 0),
        "SET" => {
            if parts.len() < 3 {
                ("ERR wrong number of arguments for 'SET'".to_string(), true, 0, 0)
            } else {
                let mem = parts[1].len() + parts[2].len();
                ("OK".to_string(), false, mem, 1)
            }
        }
        "GET" => {
            if parts.len() < 2 {
                ("ERR wrong number of arguments for 'GET'".to_string(), true, 0, 0)
            } else {
                ("(simulated value)".to_string(), false, 0, 0)
            }
        }
        "DEL" => {
            let count = parts.len().saturating_sub(1);
            (format!("(integer) {count}"), false, 0, -(count as isize))
        }
        "INCR" | "DECR" => {
            if parts.len() < 2 {
                (format!("ERR wrong number of arguments for '{cmd}'"), true, 0, 0)
            } else {
                ("(integer) 1".to_string(), false, 8, 0)
            }
        }
        "LPUSH" | "RPUSH" | "SADD" | "ZADD" | "HSET" => {
            let mem = parts[1..].iter().map(|p| p.len()).sum();
            ("OK".to_string(), false, mem, 1)
        }
        "MSET" => {
            if parts.len() < 3 || parts.len() % 2 == 0 {
                ("ERR wrong number of arguments for 'MSET'".to_string(), true, 0, 0)
            } else {
                let pairs = (parts.len() - 1) / 2;
                let mem: usize = parts[1..].iter().map(|p| p.len()).sum();
                ("OK".to_string(), false, mem, pairs as isize)
            }
        }
        "KEYS" | "DBSIZE" | "INFO" | "TTL" | "TYPE" | "EXISTS" | "LLEN" | "SCARD"
        | "ZCARD" | "HLEN" | "STRLEN" | "LRANGE" | "SMEMBERS" | "ZRANGE" | "HGETALL"
        | "HKEYS" | "HVALS" | "MGET" | "LINDEX" | "HEXISTS" | "SISMEMBER" | "ZSCORE"
        | "ZRANK" | "HGET" => ("(simulated result)".to_string(), false, 0, 0),
        "LPOP" | "RPOP" => ("(simulated value)".to_string(), false, 0, 0),
        "SREM" | "HDEL" => {
            let count = parts.len().saturating_sub(2);
            (format!("(integer) {count}"), false, 0, 0)
        }
        "APPEND" => {
            if parts.len() < 3 {
                ("ERR wrong number of arguments for 'APPEND'".to_string(), true, 0, 0)
            } else {
                let mem = parts[2].len();
                (format!("(integer) {mem}"), false, mem, 0)
            }
        }
        "SETNX" => {
            if parts.len() < 3 {
                ("ERR wrong number of arguments for 'SETNX'".to_string(), true, 0, 0)
            } else {
                let mem = parts[1].len() + parts[2].len();
                ("(integer) 1".to_string(), false, mem, 1)
            }
        }
        "INCRBY" | "DECRBY" => {
            if parts.len() < 3 {
                (format!("ERR wrong number of arguments for '{cmd}'"), true, 0, 0)
            } else {
                ("(integer) 10".to_string(), false, 0, 0)
            }
        }
        _ => (format!("ERR unknown command '{cmd}'"), true, 0, 0),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> SessionConfig {
        SessionConfig {
            max_concurrent_sessions: 5,
            default_ttl_secs: 60,
            max_ttl_secs: 120,
            max_memory_per_session_bytes: 1024,
            max_keys_per_session: 10,
            ..SessionConfig::default()
        }
    }

    #[test]
    fn test_create_and_get_session() {
        let mgr = SessionManager::new(test_config());
        let handle = mgr.create_session(SessionOptions::default()).unwrap();
        assert!(handle.id.starts_with("sess-"));

        let info = mgr.get_session(&handle.id).unwrap();
        assert_eq!(info.id, handle.id);
        assert!(!info.is_expired);
        assert_eq!(info.command_count, 0);
    }

    #[test]
    fn test_max_sessions_reached() {
        let mgr = SessionManager::new(test_config());
        for _ in 0..5 {
            mgr.create_session(SessionOptions::default()).unwrap();
        }
        let result = mgr.create_session(SessionOptions::default());
        assert!(matches!(result, Err(SessionError::MaxSessionsReached)));
    }

    #[test]
    fn test_execute_command_basic() {
        let mgr = SessionManager::new(test_config());
        let handle = mgr.create_session(SessionOptions::default()).unwrap();

        let output = mgr.execute_command(&handle.id, "SET foo bar").unwrap();
        assert_eq!(output.result, "OK");
        assert!(!output.is_error);

        let info = mgr.get_session(&handle.id).unwrap();
        assert_eq!(info.command_count, 1);
    }

    #[test]
    fn test_execute_command_blocked() {
        let mgr = SessionManager::new(test_config());
        let handle = mgr.create_session(SessionOptions::default()).unwrap();

        let result = mgr.execute_command(&handle.id, "SHUTDOWN NOSAVE");
        assert!(matches!(result, Err(SessionError::CommandBlocked(_))));

        let result = mgr.execute_command(&handle.id, "FLUSHALL");
        assert!(matches!(result, Err(SessionError::CommandBlocked(_))));

        let stats = mgr.stats();
        assert_eq!(stats.commands_blocked, 2);
    }

    #[test]
    fn test_execute_command_too_long() {
        let config = SessionConfig {
            max_command_length: 10,
            ..test_config()
        };
        let mgr = SessionManager::new(config);
        let handle = mgr.create_session(SessionOptions::default()).unwrap();

        let result = mgr.execute_command(&handle.id, "SET very_long_key very_long_value");
        assert!(matches!(result, Err(SessionError::CommandTooLong)));
    }

    #[test]
    fn test_execute_command_invalid_empty() {
        let mgr = SessionManager::new(test_config());
        let handle = mgr.create_session(SessionOptions::default()).unwrap();

        let result = mgr.execute_command(&handle.id, "   ");
        assert!(matches!(result, Err(SessionError::InvalidCommand(_))));
    }

    #[test]
    fn test_execute_command_session_not_found() {
        let mgr = SessionManager::new(test_config());

        let result = mgr.execute_command("nonexistent", "PING");
        assert!(matches!(result, Err(SessionError::SessionNotFound(_))));
    }

    #[test]
    fn test_destroy_session() {
        let mgr = SessionManager::new(test_config());
        let handle = mgr.create_session(SessionOptions::default()).unwrap();

        mgr.destroy_session(&handle.id).unwrap();
        assert!(mgr.get_session(&handle.id).is_none());

        let stats = mgr.stats();
        assert_eq!(stats.sessions_destroyed, 1);
    }

    #[test]
    fn test_destroy_session_not_found() {
        let mgr = SessionManager::new(test_config());
        let result = mgr.destroy_session("nonexistent");
        assert!(matches!(result, Err(SessionError::SessionNotFound(_))));
    }

    #[test]
    fn test_cleanup_expired() {
        let mgr = SessionManager::new(SessionConfig {
            default_ttl_secs: 0, // expire immediately
            ..test_config()
        });

        mgr.create_session(SessionOptions::default()).unwrap();
        mgr.create_session(SessionOptions::default()).unwrap();

        // Sessions have ttl=0, so they expire at creation time
        let removed = mgr.cleanup_expired();
        assert_eq!(removed, 2);
        assert_eq!(mgr.list_sessions().len(), 0);

        let stats = mgr.stats();
        assert_eq!(stats.sessions_expired, 2);
    }

    #[test]
    fn test_list_sessions() {
        let mgr = SessionManager::new(test_config());
        mgr.create_session(SessionOptions::default()).unwrap();
        mgr.create_session(SessionOptions {
            label: Some("test".to_string()),
            ..Default::default()
        })
        .unwrap();

        let list = mgr.list_sessions();
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn test_extend_session() {
        let mgr = SessionManager::new(test_config());
        let handle = mgr.create_session(SessionOptions::default()).unwrap();

        let info_before = mgr.get_session(&handle.id).unwrap();
        mgr.extend_session(&handle.id, 30).unwrap();
        let info_after = mgr.get_session(&handle.id).unwrap();

        assert!(info_after.expires_at > info_before.expires_at);
    }

    #[test]
    fn test_extend_session_capped_by_max_ttl() {
        let mgr = SessionManager::new(test_config());
        let handle = mgr.create_session(SessionOptions::default()).unwrap();
        let info = mgr.get_session(&handle.id).unwrap();
        let max_allowed = info.created_at + mgr.config.max_ttl_secs;

        // Try to extend far beyond max_ttl
        mgr.extend_session(&handle.id, 999_999).unwrap();
        let info_after = mgr.get_session(&handle.id).unwrap();

        assert_eq!(info_after.expires_at, max_allowed);
    }

    #[test]
    fn test_active_sessions_count() {
        let mgr = SessionManager::new(test_config());
        mgr.create_session(SessionOptions::default()).unwrap();
        mgr.create_session(SessionOptions::default()).unwrap();
        assert_eq!(mgr.active_sessions(), 2);
    }

    #[test]
    fn test_stats_snapshot() {
        let mgr = SessionManager::new(test_config());
        let handle = mgr.create_session(SessionOptions::default()).unwrap();
        mgr.execute_command(&handle.id, "PING").unwrap();
        mgr.execute_command(&handle.id, "SET k v").unwrap();

        let stats = mgr.stats();
        assert_eq!(stats.sessions_created, 1);
        assert_eq!(stats.commands_executed, 2);
    }

    #[test]
    fn test_session_with_tutorial() {
        let mgr = SessionManager::new(test_config());
        let handle = mgr
            .create_session(SessionOptions {
                tutorial: Some("getting-started".to_string()),
                ..Default::default()
            })
            .unwrap();

        let info = mgr.get_session(&handle.id).unwrap();
        assert_eq!(info.tutorial, Some("getting-started".to_string()));
    }

    #[test]
    fn test_memory_limit_exceeded() {
        let mgr = SessionManager::new(SessionConfig {
            max_memory_per_session_bytes: 10,
            ..test_config()
        });
        let handle = mgr.create_session(SessionOptions::default()).unwrap();

        // This should exceed 10 bytes of memory
        let result = mgr.execute_command(&handle.id, "SET a]very_long_value_exceeding_limit");
        // First SET will add memory; if it exceeds, we get the error
        assert!(
            result.is_ok() || matches!(result, Err(SessionError::MemoryLimitExceeded)),
            "expected Ok or MemoryLimitExceeded"
        );

        // Keep adding until limit is hit
        for _ in 0..10 {
            let r = mgr.execute_command(&handle.id, "SET key value123456");
            if matches!(r, Err(SessionError::MemoryLimitExceeded)) {
                return; // success
            }
        }
        panic!("expected MemoryLimitExceeded to be triggered");
    }

    #[test]
    fn test_key_limit_exceeded() {
        let mgr = SessionManager::new(SessionConfig {
            max_keys_per_session: 2,
            ..test_config()
        });
        let handle = mgr.create_session(SessionOptions::default()).unwrap();

        mgr.execute_command(&handle.id, "SET k1 v1").unwrap();
        mgr.execute_command(&handle.id, "SET k2 v2").unwrap();
        let result = mgr.execute_command(&handle.id, "SET k3 v3");
        assert!(matches!(result, Err(SessionError::KeyLimitExceeded)));
    }

    #[test]
    fn test_allowed_commands_whitelist() {
        let config = SessionConfig {
            allowed_commands: Some(vec!["PING".to_string(), "GET".to_string()]),
            ..test_config()
        };
        let mgr = SessionManager::new(config);
        let handle = mgr.create_session(SessionOptions::default()).unwrap();

        mgr.execute_command(&handle.id, "PING").unwrap();
        let result = mgr.execute_command(&handle.id, "SET foo bar");
        assert!(matches!(result, Err(SessionError::CommandBlocked(_))));
    }

    #[test]
    fn test_session_config_default() {
        let config = SessionConfig::default();
        assert_eq!(config.max_concurrent_sessions, 100);
        assert_eq!(config.default_ttl_secs, 900);
        assert_eq!(config.max_ttl_secs, 3600);
        assert_eq!(config.max_memory_per_session_bytes, 64 * 1024 * 1024);
        assert_eq!(config.max_keys_per_session, 10_000);
        assert_eq!(config.max_command_length, 4096);
        assert!(config.allowed_commands.is_none());
        assert!(!config.blocked_commands.is_empty());
        assert_eq!(config.rate_limit_commands_per_sec, 100);
        assert!(!config.enable_persistence);
    }

    #[test]
    fn test_session_error_display() {
        assert_eq!(
            SessionError::MaxSessionsReached.to_string(),
            "maximum concurrent sessions reached"
        );
        assert!(SessionError::SessionNotFound("x".into())
            .to_string()
            .contains('x'));
        assert!(SessionError::SessionExpired("x".into())
            .to_string()
            .contains('x'));
        assert!(SessionError::CommandBlocked("SHUTDOWN".into())
            .to_string()
            .contains("SHUTDOWN"));
        assert_eq!(
            SessionError::RateLimitExceeded.to_string(),
            "rate limit exceeded"
        );
        assert_eq!(
            SessionError::MemoryLimitExceeded.to_string(),
            "memory limit exceeded"
        );
        assert_eq!(
            SessionError::KeyLimitExceeded.to_string(),
            "key limit exceeded"
        );
        assert_eq!(
            SessionError::CommandTooLong.to_string(),
            "command too long"
        );
        assert!(SessionError::InvalidCommand("bad".into())
            .to_string()
            .contains("bad"));
    }

    #[test]
    fn test_simulate_command_variants() {
        let (r, err, _, _) = simulate_command("PING");
        assert_eq!(r, "PONG");
        assert!(!err);

        let (r, err, _, _) = simulate_command("SET foo bar");
        assert_eq!(r, "OK");
        assert!(!err);

        let (r, _, _, k) = simulate_command("DEL a b");
        assert!(r.contains('2'));
        assert_eq!(k, -2);

        let (_, err, _, _) = simulate_command("UNKNOWNCMD");
        assert!(err);
    }
}
