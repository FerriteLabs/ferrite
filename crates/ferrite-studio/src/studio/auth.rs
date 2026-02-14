//! Authentication system for Ferrite Studio
//!
//! Provides session-based authentication with role-based access control,
//! CSRF protection, and integration with Ferrite's ACL system.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Roles supported by the authentication system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Role {
    /// Full access to all operations including configuration changes.
    Admin,
    /// Can execute commands and modify keys but not server config.
    Operator,
    /// Can only view data and metrics; no mutations allowed.
    ReadOnly,
}

impl Role {
    /// Returns `true` if this role can perform write operations.
    pub fn can_write(&self) -> bool {
        matches!(self, Role::Admin | Role::Operator)
    }

    /// Returns `true` if this role can change server configuration.
    pub fn can_admin(&self) -> bool {
        matches!(self, Role::Admin)
    }
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Role::Admin => write!(f, "admin"),
            Role::Operator => write!(f, "operator"),
            Role::ReadOnly => write!(f, "read-only"),
        }
    }
}

/// Configuration for the authentication subsystem.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// Whether authentication is enabled.
    pub enabled: bool,
    /// Session time-to-live.
    pub session_ttl: Duration,
    /// Maximum concurrent sessions per user.
    pub max_sessions_per_user: usize,
    /// Maximum total sessions.
    pub max_sessions: usize,
    /// Whether to require CSRF tokens on mutating requests.
    pub csrf_enabled: bool,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            session_ttl: Duration::from_secs(3600),
            max_sessions_per_user: 5,
            max_sessions: 1000,
            csrf_enabled: true,
        }
    }
}

/// An active session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    /// Unique session identifier (opaque token).
    pub id: String,
    /// Authenticated username.
    pub username: String,
    /// Role granted for this session.
    pub role: Role,
    /// CSRF token bound to this session.
    pub csrf_token: String,
    /// When this session was created (epoch millis for serialization).
    pub created_at_ms: u64,
    /// When this session expires (epoch millis for serialization).
    pub expires_at_ms: u64,
    /// Internal: creation instant (not serialized).
    #[serde(skip)]
    pub created_at: Option<Instant>,
    /// Internal: expiry instant (not serialized).
    #[serde(skip)]
    pub expires_at: Option<Instant>,
}

impl Session {
    /// Check if this session has expired.
    pub fn is_expired(&self) -> bool {
        match self.expires_at {
            Some(exp) => Instant::now() >= exp,
            None => {
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                now_ms >= self.expires_at_ms
            }
        }
    }
}

/// Credentials for login.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

/// Successful login response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginResponse {
    pub session_id: String,
    pub csrf_token: String,
    pub role: Role,
    pub expires_in_secs: u64,
}

/// A stored user (for the built-in user registry).
#[derive(Debug, Clone)]
struct StoredUser {
    username: String,
    /// Password stored as-is in this placeholder (production would use argon2).
    password_hash: String,
    role: Role,
}

/// Authentication manager.
pub struct AuthManager {
    config: AuthConfig,
    sessions: RwLock<HashMap<String, Session>>,
    users: RwLock<HashMap<String, StoredUser>>,
}

impl AuthManager {
    /// Create a new authentication manager.
    pub fn new(config: AuthConfig) -> Self {
        let manager = Self {
            config,
            sessions: RwLock::new(HashMap::new()),
            users: RwLock::new(HashMap::new()),
        };
        // Seed a default admin user for development.
        manager.add_user("admin", "admin", Role::Admin);
        manager
    }

    /// Register a user (for ACL integration).
    pub fn add_user(&self, username: &str, password: &str, role: Role) {
        let mut users = self.users.write();
        users.insert(
            username.to_string(),
            StoredUser {
                username: username.to_string(),
                password_hash: password.to_string(),
                role,
            },
        );
    }

    /// Remove a user and invalidate their sessions.
    pub fn remove_user(&self, username: &str) -> bool {
        let removed = self.users.write().remove(username).is_some();
        if removed {
            let mut sessions = self.sessions.write();
            sessions.retain(|_, s| s.username != username);
        }
        removed
    }

    /// Attempt to log in with username/password.
    pub fn login(&self, request: &LoginRequest) -> Result<LoginResponse, AuthError> {
        if !self.config.enabled {
            return Err(AuthError::Disabled);
        }

        let users = self.users.read();
        let user = users
            .get(&request.username)
            .ok_or(AuthError::InvalidCredentials)?;

        // Constant-time-ish comparison (placeholder; production uses argon2 verify).
        if user.password_hash != request.password {
            return Err(AuthError::InvalidCredentials);
        }

        let role = user.role;
        drop(users);

        // Enforce max sessions per user.
        {
            let sessions = self.sessions.read();
            let user_session_count = sessions
                .values()
                .filter(|s| s.username == request.username && !s.is_expired())
                .count();
            if user_session_count >= self.config.max_sessions_per_user {
                return Err(AuthError::TooManySessions);
            }
        }

        // Enforce global max sessions.
        self.cleanup_expired();
        {
            let sessions = self.sessions.read();
            if sessions.len() >= self.config.max_sessions {
                return Err(AuthError::TooManySessions);
            }
        }

        let session_id = generate_token();
        let csrf_token = generate_token();
        let now = Instant::now();
        let now_ms = epoch_ms();
        let ttl = self.config.session_ttl;

        let session = Session {
            id: session_id.clone(),
            username: request.username.clone(),
            role,
            csrf_token: csrf_token.clone(),
            created_at_ms: now_ms,
            expires_at_ms: now_ms + ttl.as_millis() as u64,
            created_at: Some(now),
            expires_at: Some(now + ttl),
        };

        self.sessions.write().insert(session_id.clone(), session);

        Ok(LoginResponse {
            session_id,
            csrf_token,
            role,
            expires_in_secs: ttl.as_secs(),
        })
    }

    /// Validate a session token and return the session if valid.
    pub fn validate_session(&self, session_id: &str) -> Result<Session, AuthError> {
        if !self.config.enabled {
            return Err(AuthError::Disabled);
        }

        let sessions = self.sessions.read();
        let session = sessions
            .get(session_id)
            .ok_or(AuthError::InvalidSession)?;

        if session.is_expired() {
            drop(sessions);
            self.sessions.write().remove(session_id);
            return Err(AuthError::SessionExpired);
        }

        Ok(session.clone())
    }

    /// Validate a CSRF token against a session.
    pub fn validate_csrf(&self, session_id: &str, csrf_token: &str) -> Result<(), AuthError> {
        if !self.config.csrf_enabled {
            return Ok(());
        }

        let session = self.validate_session(session_id)?;
        if session.csrf_token != csrf_token {
            return Err(AuthError::InvalidCsrf);
        }
        Ok(())
    }

    /// Log out (destroy session).
    pub fn logout(&self, session_id: &str) -> bool {
        self.sessions.write().remove(session_id).is_some()
    }

    /// List active (non-expired) sessions.
    pub fn active_sessions(&self) -> Vec<Session> {
        self.cleanup_expired();
        self.sessions.read().values().cloned().collect()
    }

    /// Number of active sessions.
    pub fn session_count(&self) -> usize {
        self.sessions.read().len()
    }

    /// Remove expired sessions.
    pub fn cleanup_expired(&self) {
        self.sessions.write().retain(|_, s| !s.is_expired());
    }

    /// Check if authentication is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }
}

/// Authentication errors.
#[derive(Debug, Clone, thiserror::Error)]
pub enum AuthError {
    #[error("authentication is disabled")]
    Disabled,

    #[error("invalid username or password")]
    InvalidCredentials,

    #[error("invalid session")]
    InvalidSession,

    #[error("session expired")]
    SessionExpired,

    #[error("invalid CSRF token")]
    InvalidCsrf,

    #[error("too many active sessions")]
    TooManySessions,

    #[error("insufficient permissions")]
    Forbidden,
}

/// Generate a random hex token.
fn generate_token() -> String {
    use rand::RngCore;
    let mut rng = rand::thread_rng();
    let mut bytes = [0u8; 32];
    rng.fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Current epoch milliseconds.
fn epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> AuthConfig {
        AuthConfig {
            enabled: true,
            session_ttl: Duration::from_secs(3600),
            max_sessions_per_user: 3,
            max_sessions: 100,
            csrf_enabled: true,
        }
    }

    #[test]
    fn test_login_success() {
        let mgr = AuthManager::new(test_config());
        let resp = mgr
            .login(&LoginRequest {
                username: "admin".into(),
                password: "admin".into(),
            })
            .unwrap();
        assert_eq!(resp.role, Role::Admin);
        assert!(!resp.session_id.is_empty());
        assert!(!resp.csrf_token.is_empty());
    }

    #[test]
    fn test_login_wrong_password() {
        let mgr = AuthManager::new(test_config());
        let result = mgr.login(&LoginRequest {
            username: "admin".into(),
            password: "wrong".into(),
        });
        assert!(matches!(result, Err(AuthError::InvalidCredentials)));
    }

    #[test]
    fn test_login_unknown_user() {
        let mgr = AuthManager::new(test_config());
        let result = mgr.login(&LoginRequest {
            username: "nobody".into(),
            password: "admin".into(),
        });
        assert!(matches!(result, Err(AuthError::InvalidCredentials)));
    }

    #[test]
    fn test_session_validation() {
        let mgr = AuthManager::new(test_config());
        let resp = mgr
            .login(&LoginRequest {
                username: "admin".into(),
                password: "admin".into(),
            })
            .unwrap();

        let session = mgr.validate_session(&resp.session_id).unwrap();
        assert_eq!(session.username, "admin");
        assert_eq!(session.role, Role::Admin);
    }

    #[test]
    fn test_invalid_session() {
        let mgr = AuthManager::new(test_config());
        assert!(matches!(
            mgr.validate_session("nonexistent"),
            Err(AuthError::InvalidSession)
        ));
    }

    #[test]
    fn test_session_expiry() {
        let config = AuthConfig {
            session_ttl: Duration::from_millis(1),
            ..test_config()
        };
        let mgr = AuthManager::new(config);
        let resp = mgr
            .login(&LoginRequest {
                username: "admin".into(),
                password: "admin".into(),
            })
            .unwrap();

        std::thread::sleep(Duration::from_millis(10));
        assert!(matches!(
            mgr.validate_session(&resp.session_id),
            Err(AuthError::SessionExpired)
        ));
    }

    #[test]
    fn test_csrf_validation() {
        let mgr = AuthManager::new(test_config());
        let resp = mgr
            .login(&LoginRequest {
                username: "admin".into(),
                password: "admin".into(),
            })
            .unwrap();

        assert!(mgr
            .validate_csrf(&resp.session_id, &resp.csrf_token)
            .is_ok());
        assert!(matches!(
            mgr.validate_csrf(&resp.session_id, "wrong-token"),
            Err(AuthError::InvalidCsrf)
        ));
    }

    #[test]
    fn test_csrf_disabled() {
        let config = AuthConfig {
            csrf_enabled: false,
            ..test_config()
        };
        let mgr = AuthManager::new(config);
        let resp = mgr
            .login(&LoginRequest {
                username: "admin".into(),
                password: "admin".into(),
            })
            .unwrap();

        // Any token accepted when CSRF is disabled.
        assert!(mgr.validate_csrf(&resp.session_id, "anything").is_ok());
    }

    #[test]
    fn test_logout() {
        let mgr = AuthManager::new(test_config());
        let resp = mgr
            .login(&LoginRequest {
                username: "admin".into(),
                password: "admin".into(),
            })
            .unwrap();

        assert!(mgr.logout(&resp.session_id));
        assert!(mgr.validate_session(&resp.session_id).is_err());
        // Logout of already-removed session returns false.
        assert!(!mgr.logout(&resp.session_id));
    }

    #[test]
    fn test_max_sessions_per_user() {
        let config = AuthConfig {
            max_sessions_per_user: 2,
            ..test_config()
        };
        let mgr = AuthManager::new(config);
        let req = LoginRequest {
            username: "admin".into(),
            password: "admin".into(),
        };
        mgr.login(&req).unwrap();
        mgr.login(&req).unwrap();
        assert!(matches!(mgr.login(&req), Err(AuthError::TooManySessions)));
    }

    #[test]
    fn test_role_permissions() {
        assert!(Role::Admin.can_write());
        assert!(Role::Admin.can_admin());
        assert!(Role::Operator.can_write());
        assert!(!Role::Operator.can_admin());
        assert!(!Role::ReadOnly.can_write());
        assert!(!Role::ReadOnly.can_admin());
    }

    #[test]
    fn test_role_display() {
        assert_eq!(Role::Admin.to_string(), "admin");
        assert_eq!(Role::Operator.to_string(), "operator");
        assert_eq!(Role::ReadOnly.to_string(), "read-only");
    }

    #[test]
    fn test_add_and_remove_user() {
        let mgr = AuthManager::new(test_config());
        mgr.add_user("operator1", "pass", Role::Operator);

        let resp = mgr
            .login(&LoginRequest {
                username: "operator1".into(),
                password: "pass".into(),
            })
            .unwrap();
        assert_eq!(resp.role, Role::Operator);

        // Remove user invalidates sessions.
        assert!(mgr.remove_user("operator1"));
        assert!(mgr.validate_session(&resp.session_id).is_err());
    }

    #[test]
    fn test_active_sessions_count() {
        let mgr = AuthManager::new(test_config());
        assert_eq!(mgr.session_count(), 0);

        mgr.login(&LoginRequest {
            username: "admin".into(),
            password: "admin".into(),
        })
        .unwrap();
        assert_eq!(mgr.session_count(), 1);

        let sessions = mgr.active_sessions();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].username, "admin");
    }

    #[test]
    fn test_disabled_auth() {
        let config = AuthConfig {
            enabled: false,
            ..test_config()
        };
        let mgr = AuthManager::new(config);
        assert!(!mgr.is_enabled());
        assert!(matches!(
            mgr.login(&LoginRequest {
                username: "admin".into(),
                password: "admin".into(),
            }),
            Err(AuthError::Disabled)
        ));
    }

    #[test]
    fn test_default_auth_config() {
        let config = AuthConfig::default();
        assert!(config.enabled);
        assert!(config.csrf_enabled);
        assert_eq!(config.session_ttl, Duration::from_secs(3600));
    }

    #[test]
    fn test_login_response_serialization() {
        let resp = LoginResponse {
            session_id: "abc123".into(),
            csrf_token: "csrf456".into(),
            role: Role::ReadOnly,
            expires_in_secs: 3600,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("abc123"));
        let deserialized: LoginResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.role, Role::ReadOnly);
    }

    #[test]
    fn test_cleanup_expired() {
        let config = AuthConfig {
            session_ttl: Duration::from_millis(1),
            ..test_config()
        };
        let mgr = AuthManager::new(config);
        mgr.login(&LoginRequest {
            username: "admin".into(),
            password: "admin".into(),
        })
        .unwrap();
        assert_eq!(mgr.session_count(), 1);

        std::thread::sleep(Duration::from_millis(10));
        mgr.cleanup_expired();
        assert_eq!(mgr.session_count(), 0);
    }

    #[test]
    fn test_multiple_roles() {
        let mgr = AuthManager::new(test_config());
        mgr.add_user("readonly", "pass", Role::ReadOnly);
        mgr.add_user("ops", "pass", Role::Operator);

        let r1 = mgr
            .login(&LoginRequest {
                username: "readonly".into(),
                password: "pass".into(),
            })
            .unwrap();
        assert_eq!(r1.role, Role::ReadOnly);

        let r2 = mgr
            .login(&LoginRequest {
                username: "ops".into(),
                password: "pass".into(),
            })
            .unwrap();
        assert_eq!(r2.role, Role::Operator);
    }
}
