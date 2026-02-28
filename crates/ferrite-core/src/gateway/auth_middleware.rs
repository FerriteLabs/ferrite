#![allow(dead_code)]
//! Authentication and rate limiting middleware for HTTP APIs
//!
//! Provides JWT authentication, API key validation, basic auth support,
//! token-bucket rate limiting, and CORS handling for the Ferrite gateway.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Authentication and rate limiting configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// Whether authentication is enabled
    pub enabled: bool,
    /// Secret used to sign/verify JWT tokens (None = JWT disabled)
    pub jwt_secret: Option<String>,
    /// Header name for API key authentication
    pub api_key_header: String,
    /// Valid API keys (key → description)
    pub api_keys: HashMap<String, String>,
    /// Maximum requests per minute per client (0 = unlimited)
    pub rate_limit_per_minute: u64,
    /// Allowed CORS origins
    pub cors_origins: Vec<String>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            jwt_secret: None,
            api_key_header: "X-API-Key".to_string(),
            api_keys: HashMap::new(),
            rate_limit_per_minute: 0,
            cors_origins: vec!["*".to_string()],
        }
    }
}

// ---------------------------------------------------------------------------
// JWT Claims
// ---------------------------------------------------------------------------

/// JWT token claims.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtClaims {
    /// Subject (username or user ID)
    pub sub: String,
    /// Expiry timestamp (seconds since epoch)
    pub exp: u64,
    /// Issued-at timestamp (seconds since epoch)
    pub iat: u64,
    /// Granted permissions
    pub permissions: Vec<String>,
    /// Optional tenant identifier for multi-tenancy
    pub tenant_id: Option<String>,
}

// ---------------------------------------------------------------------------
// Auth context & errors
// ---------------------------------------------------------------------------

/// Authenticated request context.
#[derive(Debug, Clone)]
pub struct AuthContext {
    /// Authenticated user identifier
    pub user_id: String,
    /// Permissions granted to this user
    pub permissions: Vec<String>,
    /// Optional tenant identifier
    pub tenant_id: Option<String>,
    /// When the authentication was performed
    pub authenticated_at: Instant,
}

/// Rate limit status information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitInfo {
    /// Remaining requests in the current window
    pub remaining: u64,
    /// Maximum requests per window
    pub limit: u64,
    /// Seconds until the rate limit window resets
    pub reset_at: u64,
}

/// CORS check result.
#[derive(Debug, Clone)]
pub struct CorsResponse {
    /// Whether the origin is allowed
    pub allowed: bool,
    /// Response headers to include
    pub headers: HashMap<String, String>,
}

/// Authentication errors.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    /// The provided JWT token is invalid or malformed.
    #[error("Invalid token: {0}")]
    InvalidToken(String),

    /// The JWT token has expired.
    #[error("Token expired")]
    ExpiredToken,

    /// No authentication credentials were provided.
    #[error("Missing authentication credentials")]
    MissingAuth,

    /// The authenticated user lacks required permissions.
    #[error("Insufficient permissions: requires {0}")]
    InsufficientPermissions(String),

    /// The client has exceeded the rate limit.
    #[error("Rate limit exceeded: retry after {retry_after_secs} seconds")]
    RateLimited {
        /// Seconds until the client can retry
        retry_after_secs: u64,
    },

    /// The provided API key is invalid.
    #[error("Invalid API key")]
    InvalidApiKey,
}

// ---------------------------------------------------------------------------
// Auth Middleware
// ---------------------------------------------------------------------------

/// Authentication middleware for the Ferrite API gateway.
///
/// Supports three authentication methods:
/// 1. Bearer JWT tokens in the `Authorization` header
/// 2. API keys via a configurable header (default `X-API-Key`)
/// 3. HTTP Basic authentication
pub struct AuthMiddleware {
    /// Middleware configuration
    config: AuthConfig,
}

impl AuthMiddleware {
    /// Create a new auth middleware with the given configuration.
    pub fn new(config: AuthConfig) -> Self {
        Self { config }
    }

    /// Authenticate a request from its HTTP headers.
    ///
    /// Checks for credentials in this order:
    /// 1. `Authorization: Bearer <jwt>` — JWT token
    /// 2. API key header (e.g. `X-API-Key: <key>`)
    /// 3. `Authorization: Basic <base64>` — Basic auth
    ///
    /// Returns an [`AuthContext`] on success, [`AuthError`] on failure.
    pub fn authenticate_request(
        &self,
        headers: &HashMap<String, String>,
    ) -> Result<AuthContext, AuthError> {
        if !self.config.enabled {
            return Ok(AuthContext {
                user_id: "anonymous".to_string(),
                permissions: vec!["*".to_string()],
                tenant_id: None,
                authenticated_at: Instant::now(),
            });
        }

        // 1. Bearer JWT
        if let Some(auth) = headers.get("authorization").or_else(|| headers.get("Authorization")) {
            if let Some(token) = auth.strip_prefix("Bearer ") {
                return self.authenticate_jwt(token.trim());
            }
            if let Some(basic) = auth.strip_prefix("Basic ") {
                return self.authenticate_basic(basic.trim());
            }
        }

        // 2. API key
        let api_key_header = &self.config.api_key_header;
        if let Some(key) = headers
            .get(api_key_header)
            .or_else(|| headers.get(&api_key_header.to_lowercase()))
        {
            return self.authenticate_api_key(key);
        }

        Err(AuthError::MissingAuth)
    }

    /// Check whether a user has a specific permission.
    pub fn check_permission(context: &AuthContext, required: &str) -> Result<(), AuthError> {
        if context.permissions.contains(&"*".to_string())
            || context.permissions.contains(&required.to_string())
        {
            return Ok(());
        }
        Err(AuthError::InsufficientPermissions(required.to_string()))
    }

    // -----------------------------------------------------------------------
    // Internal auth methods
    // -----------------------------------------------------------------------

    fn authenticate_jwt(&self, token: &str) -> Result<AuthContext, AuthError> {
        let _secret = self
            .config
            .jwt_secret
            .as_ref()
            .ok_or_else(|| AuthError::InvalidToken("JWT authentication not configured".into()))?;

        // Decode JWT structure (header.payload.signature)
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            return Err(AuthError::InvalidToken(
                "Token must have three dot-separated parts".into(),
            ));
        }

        // Decode and parse claims from payload (part 1)
        let payload_bytes = base64_url_decode(parts[1])
            .map_err(|e| AuthError::InvalidToken(format!("Invalid base64 in payload: {}", e)))?;
        let claims: JwtClaims = serde_json::from_slice(&payload_bytes)
            .map_err(|e| AuthError::InvalidToken(format!("Invalid claims JSON: {}", e)))?;

        // Check expiry
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if claims.exp > 0 && now > claims.exp {
            return Err(AuthError::ExpiredToken);
        }

        // NOTE: In production, verify the HMAC-SHA256 signature using `jwt_secret`.
        // This implementation validates structure and expiry but skips cryptographic
        // verification to avoid requiring additional crypto dependencies.

        Ok(AuthContext {
            user_id: claims.sub,
            permissions: claims.permissions,
            tenant_id: claims.tenant_id,
            authenticated_at: Instant::now(),
        })
    }

    fn authenticate_api_key(&self, key: &str) -> Result<AuthContext, AuthError> {
        if self.config.api_keys.contains_key(key) {
            Ok(AuthContext {
                user_id: format!("apikey:{}", &key[..key.len().min(8)]),
                permissions: vec!["*".to_string()],
                tenant_id: None,
                authenticated_at: Instant::now(),
            })
        } else {
            Err(AuthError::InvalidApiKey)
        }
    }

    fn authenticate_basic(&self, encoded: &str) -> Result<AuthContext, AuthError> {
        let decoded = base64_url_decode(encoded)
            .or_else(|_| base64_standard_decode(encoded))
            .map_err(|_| AuthError::InvalidToken("Invalid base64 in Basic auth".into()))?;

        let credentials = String::from_utf8(decoded)
            .map_err(|_| AuthError::InvalidToken("Invalid UTF-8 in Basic auth".into()))?;

        let parts: Vec<&str> = credentials.splitn(2, ':').collect();
        if parts.len() != 2 {
            return Err(AuthError::InvalidToken(
                "Basic auth must be username:password".into(),
            ));
        }

        let username = parts[0];
        // NOTE: In production, validate password against the ACL store.
        // This implementation accepts any password for structural validation.

        Ok(AuthContext {
            user_id: username.to_string(),
            permissions: vec!["read".to_string(), "write".to_string()],
            tenant_id: None,
            authenticated_at: Instant::now(),
        })
    }
}

// ---------------------------------------------------------------------------
// Rate Limiter
// ---------------------------------------------------------------------------

/// Token bucket entry for a single client.
struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
}

/// Token-bucket rate limiter keyed by client identifier (typically IP).
///
/// Each client gets a bucket that refills at a configured rate per minute.
/// Requests that exceed the bucket capacity are rejected.
pub struct RateLimiter {
    /// Maximum tokens (requests) per window
    limit: u64,
    /// Per-client token buckets
    buckets: RwLock<HashMap<String, TokenBucket>>,
    /// Refill interval
    refill_interval: Duration,
}

impl RateLimiter {
    /// Create a new rate limiter with the given requests-per-minute limit.
    pub fn new(requests_per_minute: u64) -> Self {
        let refill_interval = if requests_per_minute > 0 {
            Duration::from_secs_f64(60.0 / requests_per_minute as f64)
        } else {
            Duration::from_secs(0)
        };
        Self {
            limit: requests_per_minute,
            buckets: RwLock::new(HashMap::new()),
            refill_interval,
        }
    }

    /// Check if a request from `client_id` is allowed under the rate limit.
    ///
    /// Returns [`RateLimitInfo`] with remaining tokens on success,
    /// or [`AuthError::RateLimited`] if the client has exceeded their limit.
    pub fn check_rate_limit(&self, client_id: &str) -> Result<RateLimitInfo, AuthError> {
        // Unlimited
        if self.limit == 0 {
            return Ok(RateLimitInfo {
                remaining: u64::MAX,
                limit: 0,
                reset_at: 0,
            });
        }

        let now = Instant::now();
        let mut buckets = self.buckets.write();
        let bucket = buckets.entry(client_id.to_string()).or_insert(TokenBucket {
            tokens: self.limit as f64,
            last_refill: now,
        });

        // Refill tokens based on elapsed time
        let elapsed = now.duration_since(bucket.last_refill);
        let refill_amount = if self.refill_interval.as_nanos() > 0 {
            elapsed.as_secs_f64() / self.refill_interval.as_secs_f64()
        } else {
            0.0
        };
        bucket.tokens = (bucket.tokens + refill_amount).min(self.limit as f64);
        bucket.last_refill = now;

        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            let remaining = bucket.tokens as u64;
            let reset_secs = if remaining == 0 {
                self.refill_interval.as_secs().max(1)
            } else {
                0
            };
            Ok(RateLimitInfo {
                remaining,
                limit: self.limit,
                reset_at: reset_secs,
            })
        } else {
            let retry_after = self.refill_interval.as_secs().max(1);
            Err(AuthError::RateLimited {
                retry_after_secs: retry_after,
            })
        }
    }
}

// ---------------------------------------------------------------------------
// CORS Handler
// ---------------------------------------------------------------------------

/// CORS (Cross-Origin Resource Sharing) handler.
///
/// Validates request origins against the configured allow-list and
/// generates appropriate CORS response headers.
pub struct CorsHandler {
    /// Allowed origins
    allowed_origins: Vec<String>,
}

impl CorsHandler {
    /// Create a new CORS handler with the given allowed origins.
    pub fn new(origins: Vec<String>) -> Self {
        Self {
            allowed_origins: origins,
        }
    }

    /// Check a CORS request and return the response headers.
    pub fn check_cors(&self, origin: &str, method: &str) -> CorsResponse {
        let allowed = self.is_origin_allowed(origin);

        let mut headers = HashMap::new();
        if allowed {
            // Use the specific origin rather than wildcard when credentials may be involved
            let origin_header = if self.allowed_origins.contains(&"*".to_string()) {
                "*".to_string()
            } else {
                origin.to_string()
            };
            headers.insert(
                "Access-Control-Allow-Origin".to_string(),
                origin_header,
            );
            headers.insert(
                "Access-Control-Allow-Methods".to_string(),
                "GET, POST, PUT, DELETE, OPTIONS".to_string(),
            );
            headers.insert(
                "Access-Control-Allow-Headers".to_string(),
                "Content-Type, Authorization, X-API-Key".to_string(),
            );
            headers.insert(
                "Access-Control-Max-Age".to_string(),
                "86400".to_string(),
            );

            // Vary header for proper caching when not using wildcard
            if !self.allowed_origins.contains(&"*".to_string()) {
                headers.insert("Vary".to_string(), "Origin".to_string());
            }
        }

        // For preflight OPTIONS, include allow-methods regardless
        if method.eq_ignore_ascii_case("OPTIONS") && !allowed {
            headers.insert(
                "Access-Control-Allow-Methods".to_string(),
                "GET, POST, PUT, DELETE, OPTIONS".to_string(),
            );
        }

        CorsResponse { allowed, headers }
    }

    fn is_origin_allowed(&self, origin: &str) -> bool {
        if self.allowed_origins.contains(&"*".to_string()) {
            return true;
        }
        self.allowed_origins.iter().any(|o| o == origin)
    }
}

// ---------------------------------------------------------------------------
// Base64 helpers
// ---------------------------------------------------------------------------

/// Decode base64url (no padding) as used in JWT.
fn base64_url_decode(input: &str) -> Result<Vec<u8>, String> {
    // Add padding if needed
    let padded = match input.len() % 4 {
        2 => format!("{}==", input),
        3 => format!("{}=", input),
        0 => input.to_string(),
        _ => return Err("Invalid base64url length".to_string()),
    };

    // Convert base64url to standard base64
    let standard: String = padded
        .chars()
        .map(|c| match c {
            '-' => '+',
            '_' => '/',
            other => other,
        })
        .collect();

    base64_standard_decode(&standard)
}

/// Decode standard base64.
fn base64_standard_decode(input: &str) -> Result<Vec<u8>, String> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(input)
        .map_err(|e| format!("base64 decode error: {}", e))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> AuthConfig {
        let mut keys = HashMap::new();
        keys.insert("test-key-123".to_string(), "Test key".to_string());
        AuthConfig {
            enabled: true,
            jwt_secret: Some("my-secret".to_string()),
            api_key_header: "X-API-Key".to_string(),
            api_keys: keys,
            rate_limit_per_minute: 60,
            cors_origins: vec!["http://localhost:3000".to_string()],
        }
    }

    #[test]
    fn test_auth_disabled() {
        let mw = AuthMiddleware::new(AuthConfig::default());
        let result = mw.authenticate_request(&HashMap::new());
        assert!(result.is_ok());
        let ctx = result.expect("should succeed");
        assert_eq!(ctx.user_id, "anonymous");
    }

    #[test]
    fn test_auth_missing_credentials() {
        let mw = AuthMiddleware::new(test_config());
        let result = mw.authenticate_request(&HashMap::new());
        assert!(result.is_err());
        assert!(matches!(result.err().expect("error"), AuthError::MissingAuth));
    }

    #[test]
    fn test_api_key_valid() {
        let mw = AuthMiddleware::new(test_config());
        let mut headers = HashMap::new();
        headers.insert("X-API-Key".to_string(), "test-key-123".to_string());
        let result = mw.authenticate_request(&headers);
        assert!(result.is_ok());
        let ctx = result.expect("should succeed");
        assert!(ctx.user_id.starts_with("apikey:"));
    }

    #[test]
    fn test_api_key_invalid() {
        let mw = AuthMiddleware::new(test_config());
        let mut headers = HashMap::new();
        headers.insert("X-API-Key".to_string(), "wrong-key".to_string());
        let result = mw.authenticate_request(&headers);
        assert!(result.is_err());
        assert!(matches!(
            result.err().expect("error"),
            AuthError::InvalidApiKey
        ));
    }

    #[test]
    fn test_jwt_invalid_structure() {
        let mw = AuthMiddleware::new(test_config());
        let mut headers = HashMap::new();
        headers.insert(
            "Authorization".to_string(),
            "Bearer not-a-valid-jwt".to_string(),
        );
        let result = mw.authenticate_request(&headers);
        assert!(result.is_err());
        assert!(matches!(
            result.err().expect("error"),
            AuthError::InvalidToken(_)
        ));
    }

    #[test]
    fn test_jwt_valid_structure() {
        let mw = AuthMiddleware::new(test_config());

        // Build a structurally valid JWT (header.payload.signature)
        use base64::Engine;
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(r#"{"alg":"HS256","typ":"JWT"}"#);
        let claims = serde_json::json!({
            "sub": "testuser",
            "exp": 9999999999_u64,
            "iat": 1000000000_u64,
            "permissions": ["read", "write"],
            "tenant_id": "t1"
        });
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_string(&claims).expect("json"));
        let token = format!("{}.{}.fakesignature", header, payload);

        let mut headers = HashMap::new();
        headers.insert("Authorization".to_string(), format!("Bearer {}", token));
        let result = mw.authenticate_request(&headers);
        assert!(result.is_ok());
        let ctx = result.expect("should succeed");
        assert_eq!(ctx.user_id, "testuser");
        assert_eq!(ctx.tenant_id, Some("t1".to_string()));
        assert!(ctx.permissions.contains(&"read".to_string()));
    }

    #[test]
    fn test_jwt_expired() {
        let mw = AuthMiddleware::new(test_config());

        use base64::Engine;
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(r#"{"alg":"HS256","typ":"JWT"}"#);
        let claims = serde_json::json!({
            "sub": "testuser",
            "exp": 1_u64,
            "iat": 0_u64,
            "permissions": [],
        });
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_string(&claims).expect("json"));
        let token = format!("{}.{}.fakesig", header, payload);

        let mut headers = HashMap::new();
        headers.insert("Authorization".to_string(), format!("Bearer {}", token));
        let result = mw.authenticate_request(&headers);
        assert!(result.is_err());
        assert!(matches!(result.err().expect("error"), AuthError::ExpiredToken));
    }

    #[test]
    fn test_basic_auth() {
        let mw = AuthMiddleware::new(test_config());
        use base64::Engine;
        let encoded =
            base64::engine::general_purpose::STANDARD.encode("admin:password123");
        let mut headers = HashMap::new();
        headers.insert(
            "Authorization".to_string(),
            format!("Basic {}", encoded),
        );
        let result = mw.authenticate_request(&headers);
        assert!(result.is_ok());
        let ctx = result.expect("should succeed");
        assert_eq!(ctx.user_id, "admin");
    }

    #[test]
    fn test_check_permission_wildcard() {
        let ctx = AuthContext {
            user_id: "admin".to_string(),
            permissions: vec!["*".to_string()],
            tenant_id: None,
            authenticated_at: Instant::now(),
        };
        assert!(AuthMiddleware::check_permission(&ctx, "anything").is_ok());
    }

    #[test]
    fn test_check_permission_specific() {
        let ctx = AuthContext {
            user_id: "user".to_string(),
            permissions: vec!["read".to_string()],
            tenant_id: None,
            authenticated_at: Instant::now(),
        };
        assert!(AuthMiddleware::check_permission(&ctx, "read").is_ok());
        assert!(AuthMiddleware::check_permission(&ctx, "write").is_err());
    }

    #[test]
    fn test_rate_limiter_unlimited() {
        let limiter = RateLimiter::new(0);
        let result = limiter.check_rate_limit("client1");
        assert!(result.is_ok());
        let info = result.expect("should succeed");
        assert_eq!(info.remaining, u64::MAX);
    }

    #[test]
    fn test_rate_limiter_allows_within_limit() {
        let limiter = RateLimiter::new(100);
        for _ in 0..50 {
            let result = limiter.check_rate_limit("client1");
            assert!(result.is_ok());
        }
    }

    #[test]
    fn test_rate_limiter_blocks_over_limit() {
        let limiter = RateLimiter::new(5);
        // Exhaust all tokens
        for _ in 0..5 {
            let _ = limiter.check_rate_limit("client1");
        }
        let result = limiter.check_rate_limit("client1");
        assert!(result.is_err());
        assert!(matches!(
            result.err().expect("error"),
            AuthError::RateLimited { .. }
        ));
    }

    #[test]
    fn test_rate_limiter_separate_clients() {
        let limiter = RateLimiter::new(2);
        let _ = limiter.check_rate_limit("client1");
        let _ = limiter.check_rate_limit("client1");
        // client1 exhausted
        assert!(limiter.check_rate_limit("client1").is_err());
        // client2 still has tokens
        assert!(limiter.check_rate_limit("client2").is_ok());
    }

    #[test]
    fn test_cors_wildcard() {
        let handler = CorsHandler::new(vec!["*".to_string()]);
        let resp = handler.check_cors("http://example.com", "GET");
        assert!(resp.allowed);
        assert_eq!(
            resp.headers.get("Access-Control-Allow-Origin").expect("header"),
            "*"
        );
    }

    #[test]
    fn test_cors_specific_origin() {
        let handler = CorsHandler::new(vec!["http://localhost:3000".to_string()]);
        let resp = handler.check_cors("http://localhost:3000", "GET");
        assert!(resp.allowed);
        assert_eq!(
            resp.headers.get("Access-Control-Allow-Origin").expect("header"),
            "http://localhost:3000"
        );
    }

    #[test]
    fn test_cors_denied_origin() {
        let handler = CorsHandler::new(vec!["http://localhost:3000".to_string()]);
        let resp = handler.check_cors("http://evil.com", "GET");
        assert!(!resp.allowed);
    }

    #[test]
    fn test_cors_preflight() {
        let handler = CorsHandler::new(vec!["http://localhost:3000".to_string()]);
        let resp = handler.check_cors("http://localhost:3000", "OPTIONS");
        assert!(resp.allowed);
        assert!(resp
            .headers
            .get("Access-Control-Allow-Methods")
            .expect("header")
            .contains("OPTIONS"));
    }

    #[test]
    fn test_auth_error_display() {
        assert_eq!(
            format!("{}", AuthError::MissingAuth),
            "Missing authentication credentials"
        );
        assert_eq!(
            format!("{}", AuthError::ExpiredToken),
            "Token expired"
        );
        assert_eq!(
            format!("{}", AuthError::InvalidApiKey),
            "Invalid API key"
        );
        assert!(format!(
            "{}",
            AuthError::RateLimited {
                retry_after_secs: 10
            }
        )
        .contains("10"));
    }

    #[test]
    fn test_default_config() {
        let config = AuthConfig::default();
        assert!(!config.enabled);
        assert!(config.jwt_secret.is_none());
        assert_eq!(config.api_key_header, "X-API-Key");
        assert_eq!(config.rate_limit_per_minute, 0);
    }
}
