//! # API Gateway Extensions
//!
//! Enhancements for the API gateway: per-IP rate limiting with sliding window,
//! HTTP request routing/matching, response caching, API key management, and
//! webhook subscriptions for data-change notifications.
//!
//! These extensions complement the core [`super::api_gateway::ApiGateway`] with
//! production-grade middleware capabilities.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// Sliding-window rate limiter
// ---------------------------------------------------------------------------

/// Per-client sliding-window rate limiter.
pub struct RateLimiter {
    config: RateLimiterConfig,
    windows: RwLock<HashMap<String, SlidingWindow>>,
}

/// Rate limiter configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RateLimiterConfig {
    /// Maximum requests per window.
    pub max_requests: u64,
    /// Window duration.
    pub window: Duration,
    /// Number of sub-buckets for the sliding window.
    pub buckets: usize,
    /// Burst multiplier (allows short bursts above steady rate).
    pub burst_multiplier: f64,
}

impl Default for RateLimiterConfig {
    fn default() -> Self {
        Self {
            max_requests: 100,
            window: Duration::from_secs(60),
            buckets: 6,
            burst_multiplier: 1.5,
        }
    }
}

struct SlidingWindow {
    counts: Vec<u64>,
    bucket_duration: Duration,
    last_bucket_time: Instant,
    current_bucket: usize,
}

impl SlidingWindow {
    fn new(buckets: usize, bucket_duration: Duration) -> Self {
        Self {
            counts: vec![0; buckets],
            bucket_duration,
            last_bucket_time: Instant::now(),
            current_bucket: 0,
        }
    }

    fn advance(&mut self, now: Instant) {
        let elapsed = now.duration_since(self.last_bucket_time);
        let buckets_to_advance =
            (elapsed.as_millis() / self.bucket_duration.as_millis().max(1)) as usize;

        if buckets_to_advance == 0 {
            return;
        }

        let total = self.counts.len();
        let clear_count = buckets_to_advance.min(total);

        for i in 0..clear_count {
            let idx = (self.current_bucket + 1 + i) % total;
            self.counts[idx] = 0;
        }

        self.current_bucket = (self.current_bucket + buckets_to_advance) % total;
        self.last_bucket_time = now;
    }

    fn increment(&mut self) {
        self.counts[self.current_bucket] += 1;
    }

    fn total(&self) -> u64 {
        self.counts.iter().sum()
    }
}

/// Result of a rate-limit check.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RateLimitResult {
    /// Whether the request was allowed.
    pub allowed: bool,
    /// Number of requests remaining in the current window.
    pub remaining: u64,
    /// Maximum requests allowed per window.
    pub limit: u64,
    /// Time until the current window resets.
    pub reset_after: Duration,
}

impl RateLimiter {
    /// Create a new rate limiter.
    pub fn new(config: RateLimiterConfig) -> Self {
        Self {
            config,
            windows: RwLock::new(HashMap::new()),
        }
    }

    /// Check and record a request for a client.
    pub fn check(&self, client_id: &str) -> RateLimitResult {
        let now = Instant::now();
        let bucket_duration = self.config.window / self.config.buckets as u32;
        let burst_limit = (self.config.max_requests as f64 * self.config.burst_multiplier) as u64;

        let mut windows = self.windows.write();
        let window = windows
            .entry(client_id.to_string())
            .or_insert_with(|| SlidingWindow::new(self.config.buckets, bucket_duration));

        window.advance(now);

        let current = window.total();
        if current >= burst_limit {
            return RateLimitResult {
                allowed: false,
                remaining: 0,
                limit: self.config.max_requests,
                reset_after: bucket_duration,
            };
        }

        window.increment();
        let remaining = burst_limit.saturating_sub(current + 1);

        RateLimitResult {
            allowed: true,
            remaining,
            limit: self.config.max_requests,
            reset_after: bucket_duration,
        }
    }

    /// Remove stale entries that haven't been accessed recently.
    pub fn cleanup(&self) {
        let now = Instant::now();
        let mut windows = self.windows.write();
        windows.retain(|_, w| now.duration_since(w.last_bucket_time) < self.config.window * 2);
    }

    /// Number of tracked clients.
    pub fn client_count(&self) -> usize {
        self.windows.read().len()
    }
}

// ---------------------------------------------------------------------------
// HTTP request router/matcher
// ---------------------------------------------------------------------------

/// A matched route with extracted path parameters.
#[derive(Clone, Debug)]
pub struct RouteMatch {
    /// Index of the matched route in the router's route list.
    pub route_index: usize,
    /// Extracted path parameters (e.g. `:id` → `"42"`).
    pub params: HashMap<String, String>,
}

/// Simple path-segment router supporting `:param` patterns.
pub struct Router {
    routes: Vec<RoutePattern>,
}

#[derive(Clone, Debug)]
struct RoutePattern {
    method: String,
    segments: Vec<Segment>,
    index: usize,
}

#[derive(Clone, Debug)]
enum Segment {
    Literal(String),
    Param(String),
    Wildcard,
}

impl Router {
    /// Create a new empty router.
    pub fn new() -> Self {
        Self { routes: Vec::new() }
    }

    /// Add a route. Path segments with `:` prefix are parameters.
    /// A segment of `*` matches anything.
    pub fn add_route(&mut self, method: &str, path: &str) -> usize {
        let index = self.routes.len();
        let segments: Vec<Segment> = path
            .split('/')
            .filter(|s| !s.is_empty())
            .map(|s| {
                if let Some(param) = s.strip_prefix(':') {
                    Segment::Param(param.to_string())
                } else if s == "*" {
                    Segment::Wildcard
                } else {
                    Segment::Literal(s.to_string())
                }
            })
            .collect();

        self.routes.push(RoutePattern {
            method: method.to_uppercase(),
            segments,
            index,
        });
        index
    }

    /// Match a request against registered routes.
    pub fn match_route(&self, method: &str, path: &str) -> Option<RouteMatch> {
        let input_segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

        for route in &self.routes {
            if route.method != method.to_uppercase() {
                continue;
            }

            if route.segments.len() != input_segments.len() {
                // Allow wildcard at end
                let has_wildcard = route
                    .segments
                    .last()
                    .is_some_and(|s| matches!(s, Segment::Wildcard));
                if !has_wildcard || input_segments.len() < route.segments.len() - 1 {
                    continue;
                }
            }

            let mut params = HashMap::new();
            let mut matched = true;

            for (i, seg) in route.segments.iter().enumerate() {
                match seg {
                    Segment::Literal(lit) => {
                        if i >= input_segments.len() || input_segments[i] != lit.as_str() {
                            matched = false;
                            break;
                        }
                    }
                    Segment::Param(name) => {
                        if i >= input_segments.len() {
                            matched = false;
                            break;
                        }
                        params.insert(name.clone(), input_segments[i].to_string());
                    }
                    Segment::Wildcard => {
                        // Matches anything remaining
                        break;
                    }
                }
            }

            if matched {
                return Some(RouteMatch {
                    route_index: route.index,
                    params,
                });
            }
        }

        None
    }

    /// Number of registered routes.
    pub fn route_count(&self) -> usize {
        self.routes.len()
    }
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Response cache
// ---------------------------------------------------------------------------

/// TTL-based response cache for GET requests.
pub struct ResponseCache {
    config: CacheConfig,
    entries: RwLock<HashMap<String, CacheEntry>>,
    hits: AtomicU64,
    misses: AtomicU64,
}

/// Cache configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Default TTL for cached responses.
    pub default_ttl: Duration,
    /// Maximum number of cached entries.
    pub max_entries: usize,
    /// Maximum cached value size in bytes.
    pub max_value_size: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            default_ttl: Duration::from_secs(30),
            max_entries: 10_000,
            max_value_size: 1024 * 1024,
        }
    }
}

struct CacheEntry {
    value: Vec<u8>,
    content_type: String,
    status: u16,
    created_at: Instant,
    ttl: Duration,
}

impl CacheEntry {
    fn is_expired(&self) -> bool {
        self.created_at.elapsed() > self.ttl
    }
}

/// Cache statistics.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CacheStats {
    /// Number of entries currently in the cache.
    pub entries: usize,
    /// Total cache hits.
    pub hits: u64,
    /// Total cache misses.
    pub misses: u64,
    /// Hit rate (hits / total lookups).
    pub hit_rate: f64,
}

impl ResponseCache {
    /// Create a new response cache with the given configuration.
    pub fn new(config: CacheConfig) -> Self {
        Self {
            config,
            entries: RwLock::new(HashMap::new()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Try to get a cached response.
    pub fn get(&self, key: &str) -> Option<CachedResponse> {
        let entries = self.entries.read();
        if let Some(entry) = entries.get(key) {
            if entry.is_expired() {
                self.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Some(CachedResponse {
                body: entry.value.clone(),
                content_type: entry.content_type.clone(),
                status: entry.status,
            });
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Store a response in the cache.
    pub fn put(&self, key: &str, body: Vec<u8>, content_type: &str, status: u16) {
        if body.len() > self.config.max_value_size {
            return;
        }

        let mut entries = self.entries.write();

        // Evict expired entries if at capacity
        if entries.len() >= self.config.max_entries {
            entries.retain(|_, e| !e.is_expired());
        }

        if entries.len() >= self.config.max_entries {
            // Evict oldest entry
            if let Some(oldest_key) = entries
                .iter()
                .min_by_key(|(_, e)| e.created_at)
                .map(|(k, _)| k.clone())
            {
                entries.remove(&oldest_key);
            }
        }

        entries.insert(
            key.to_string(),
            CacheEntry {
                value: body,
                content_type: content_type.to_string(),
                status,
                created_at: Instant::now(),
                ttl: self.config.default_ttl,
            },
        );
    }

    /// Store with a custom TTL.
    pub fn put_with_ttl(
        &self,
        key: &str,
        body: Vec<u8>,
        content_type: &str,
        status: u16,
        ttl: Duration,
    ) {
        if body.len() > self.config.max_value_size {
            return;
        }

        let mut entries = self.entries.write();
        entries.insert(
            key.to_string(),
            CacheEntry {
                value: body,
                content_type: content_type.to_string(),
                status,
                created_at: Instant::now(),
                ttl,
            },
        );
    }

    /// Invalidate a cache entry.
    pub fn invalidate(&self, key: &str) -> bool {
        self.entries.write().remove(key).is_some()
    }

    /// Invalidate all entries matching a prefix.
    pub fn invalidate_prefix(&self, prefix: &str) -> usize {
        let mut entries = self.entries.write();
        let before = entries.len();
        entries.retain(|k, _| !k.starts_with(prefix));
        before - entries.len()
    }

    /// Clear all cached entries.
    pub fn clear(&self) {
        self.entries.write().clear();
    }

    /// Get cache statistics.
    pub fn stats(&self) -> CacheStats {
        let entries = self.entries.read().len();
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        CacheStats {
            entries,
            hits,
            misses,
            hit_rate: if total == 0 {
                0.0
            } else {
                hits as f64 / total as f64
            },
        }
    }
}

/// A cached HTTP response.
#[derive(Clone, Debug)]
pub struct CachedResponse {
    /// Response body bytes.
    pub body: Vec<u8>,
    /// Content-Type header value.
    pub content_type: String,
    /// HTTP status code.
    pub status: u16,
}

// ---------------------------------------------------------------------------
// API key management
// ---------------------------------------------------------------------------

/// Simple API key store with scoped permissions.
pub struct ApiKeyStore {
    keys: RwLock<HashMap<String, ApiKeyEntry>>,
}

/// An API key with metadata.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApiKeyEntry {
    /// The API key string.
    pub key: String,
    /// Human-readable name for the key.
    pub name: String,
    /// Permission scopes granted to this key.
    pub scopes: Vec<ApiScope>,
    /// When the key was created (Unix timestamp seconds).
    pub created_at: u64,
    /// Optional expiration time (Unix timestamp seconds).
    pub expires_at: Option<u64>,
    /// Whether the key is currently enabled.
    pub enabled: bool,
    /// Number of requests made with this key.
    pub request_count: u64,
}

/// Permission scopes for API keys.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApiScope {
    /// Read-only access to REST/GraphQL.
    Read,
    /// Write access (create, update, delete).
    Write,
    /// Admin access (schema management, config).
    Admin,
    /// Access to a specific resource.
    Resource(String),
}

/// Result of API key validation.
#[derive(Clone, Debug)]
pub struct KeyValidation {
    /// Whether the key is valid for the requested scope.
    pub valid: bool,
    /// Name associated with the key (if found).
    pub name: Option<String>,
    /// Scopes granted to the key.
    pub scopes: Vec<ApiScope>,
    /// Reason for rejection (if invalid).
    pub reason: Option<String>,
}

impl ApiKeyStore {
    /// Create a new empty API key store.
    pub fn new() -> Self {
        Self {
            keys: RwLock::new(HashMap::new()),
        }
    }

    /// Register a new API key.
    pub fn register(&self, key: &str, name: &str, scopes: Vec<ApiScope>, expires_at: Option<u64>) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        self.keys.write().insert(
            key.to_string(),
            ApiKeyEntry {
                key: key.to_string(),
                name: name.to_string(),
                scopes,
                created_at: now,
                expires_at,
                enabled: true,
                request_count: 0,
            },
        );
    }

    /// Validate an API key and check for a required scope.
    pub fn validate(&self, key: &str, required_scope: &ApiScope) -> KeyValidation {
        let mut keys = self.keys.write();
        let Some(entry) = keys.get_mut(key) else {
            return KeyValidation {
                valid: false,
                name: None,
                scopes: vec![],
                reason: Some("Unknown API key".to_string()),
            };
        };

        if !entry.enabled {
            return KeyValidation {
                valid: false,
                name: Some(entry.name.clone()),
                scopes: entry.scopes.clone(),
                reason: Some("API key is disabled".to_string()),
            };
        }

        if let Some(exp) = entry.expires_at {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            if now > exp {
                return KeyValidation {
                    valid: false,
                    name: Some(entry.name.clone()),
                    scopes: entry.scopes.clone(),
                    reason: Some("API key has expired".to_string()),
                };
            }
        }

        let has_scope =
            entry.scopes.contains(required_scope) || entry.scopes.contains(&ApiScope::Admin);

        if !has_scope {
            return KeyValidation {
                valid: false,
                name: Some(entry.name.clone()),
                scopes: entry.scopes.clone(),
                reason: Some(format!("Missing scope: {:?}", required_scope)),
            };
        }

        entry.request_count += 1;

        KeyValidation {
            valid: true,
            name: Some(entry.name.clone()),
            scopes: entry.scopes.clone(),
            reason: None,
        }
    }

    /// Revoke an API key.
    pub fn revoke(&self, key: &str) -> bool {
        if let Some(entry) = self.keys.write().get_mut(key) {
            entry.enabled = false;
            true
        } else {
            false
        }
    }

    /// List all API keys (without secret values).
    pub fn list(&self) -> Vec<ApiKeyEntry> {
        self.keys.read().values().cloned().collect()
    }

    /// Number of registered keys.
    pub fn count(&self) -> usize {
        self.keys.read().len()
    }
}

impl Default for ApiKeyStore {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Webhook subscriptions
// ---------------------------------------------------------------------------

/// Webhook subscription manager for data-change notifications.
pub struct WebhookManager {
    subscriptions: RwLock<Vec<WebhookSubscription>>,
    delivery_log: RwLock<Vec<WebhookDelivery>>,
    next_id: AtomicU64,
    deliveries_total: AtomicU64,
    deliveries_failed: AtomicU64,
}

/// A webhook subscription.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WebhookSubscription {
    /// Unique subscription identifier.
    pub id: u64,
    /// URL to deliver webhooks to.
    pub url: String,
    /// Events this subscription listens for.
    pub events: Vec<WebhookEvent>,
    /// Optional key pattern filter (e.g. `"users:*"`).
    pub key_pattern: Option<String>,
    /// Optional HMAC secret for signing payloads.
    pub secret: Option<String>,
    /// Whether the subscription is active.
    pub active: bool,
    /// When the subscription was created (Unix timestamp seconds).
    pub created_at: u64,
    /// Maximum delivery retry attempts.
    pub max_retries: u32,
}

/// Events that can trigger a webhook.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WebhookEvent {
    /// A key was set or updated.
    KeySet,
    /// A key was deleted.
    KeyDelete,
    /// A key expired.
    KeyExpire,
    /// A schema was updated.
    SchemaChange,
    /// A new key pattern was detected.
    NewPattern,
}

/// A webhook delivery payload.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WebhookPayload {
    /// Event that triggered the webhook.
    pub event: WebhookEvent,
    /// Key involved in the event (if applicable).
    pub key: Option<String>,
    /// When the event occurred (Unix timestamp seconds).
    pub timestamp: u64,
    /// Additional event data.
    pub data: Option<serde_json::Value>,
}

/// Record of a webhook delivery attempt.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WebhookDelivery {
    /// ID of the subscription this delivery belongs to.
    pub subscription_id: u64,
    /// Target URL for the delivery.
    pub url: String,
    /// Event that was delivered.
    pub event: WebhookEvent,
    /// Current delivery status.
    pub status: DeliveryStatus,
    /// When the delivery was attempted (Unix timestamp seconds).
    pub attempted_at: u64,
    /// HTTP response code from the target (if received).
    pub response_code: Option<u16>,
}

/// Status of a delivery attempt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeliveryStatus {
    /// Delivery is queued.
    Pending,
    /// Delivery was successful.
    Delivered,
    /// Delivery failed.
    Failed,
    /// Delivery is being retried.
    Retrying,
}

/// Webhook statistics.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WebhookStats {
    /// Number of currently active subscriptions.
    pub active_subscriptions: usize,
    /// Total delivery attempts.
    pub total_deliveries: u64,
    /// Total failed delivery attempts.
    pub failed_deliveries: u64,
    /// Number of deliveries currently pending.
    pub pending_deliveries: usize,
}

impl WebhookManager {
    /// Create a new webhook manager.
    pub fn new() -> Self {
        Self {
            subscriptions: RwLock::new(Vec::new()),
            delivery_log: RwLock::new(Vec::new()),
            next_id: AtomicU64::new(1),
            deliveries_total: AtomicU64::new(0),
            deliveries_failed: AtomicU64::new(0),
        }
    }

    /// Subscribe to events.
    pub fn subscribe(
        &self,
        url: &str,
        events: Vec<WebhookEvent>,
        key_pattern: Option<String>,
        secret: Option<String>,
    ) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        self.subscriptions.write().push(WebhookSubscription {
            id,
            url: url.to_string(),
            events,
            key_pattern,
            secret,
            active: true,
            created_at: now,
            max_retries: 3,
        });

        id
    }

    /// Unsubscribe by ID.
    pub fn unsubscribe(&self, id: u64) -> bool {
        let mut subs = self.subscriptions.write();
        if let Some(sub) = subs.iter_mut().find(|s| s.id == id) {
            sub.active = false;
            true
        } else {
            false
        }
    }

    /// Find subscriptions matching an event and optional key.
    pub fn matching_subscriptions(
        &self,
        event: &WebhookEvent,
        key: Option<&str>,
    ) -> Vec<WebhookSubscription> {
        let subs = self.subscriptions.read();
        subs.iter()
            .filter(|s| {
                if !s.active {
                    return false;
                }
                if !s.events.contains(event) {
                    return false;
                }
                if let (Some(pattern), Some(k)) = (&s.key_pattern, key) {
                    k.starts_with(pattern.trim_end_matches('*'))
                } else {
                    true
                }
            })
            .cloned()
            .collect()
    }

    /// Record a delivery attempt.
    pub fn record_delivery(
        &self,
        subscription_id: u64,
        url: &str,
        event: WebhookEvent,
        status: DeliveryStatus,
        response_code: Option<u16>,
    ) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        self.deliveries_total.fetch_add(1, Ordering::Relaxed);
        if status == DeliveryStatus::Failed {
            self.deliveries_failed.fetch_add(1, Ordering::Relaxed);
        }

        let mut log = self.delivery_log.write();
        log.push(WebhookDelivery {
            subscription_id,
            url: url.to_string(),
            event,
            status,
            attempted_at: now,
            response_code,
        });

        // Keep log bounded
        if log.len() > 10_000 {
            log.drain(..5_000);
        }
    }

    /// Get statistics.
    pub fn stats(&self) -> WebhookStats {
        let subs = self.subscriptions.read();
        let log = self.delivery_log.read();
        WebhookStats {
            active_subscriptions: subs.iter().filter(|s| s.active).count(),
            total_deliveries: self.deliveries_total.load(Ordering::Relaxed),
            failed_deliveries: self.deliveries_failed.load(Ordering::Relaxed),
            pending_deliveries: log
                .iter()
                .filter(|d| d.status == DeliveryStatus::Pending)
                .count(),
        }
    }

    /// List all subscriptions.
    pub fn list_subscriptions(&self) -> Vec<WebhookSubscription> {
        self.subscriptions.read().clone()
    }
}

impl Default for WebhookManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- Rate limiter tests --

    #[test]
    fn test_rate_limiter_allows_within_limit() {
        let limiter = RateLimiter::new(RateLimiterConfig {
            max_requests: 10,
            window: Duration::from_secs(60),
            buckets: 6,
            burst_multiplier: 1.0,
            ..Default::default()
        });

        for _ in 0..10 {
            let result = limiter.check("client1");
            assert!(result.allowed);
        }

        // 11th should be blocked
        let result = limiter.check("client1");
        assert!(!result.allowed);
    }

    #[test]
    fn test_rate_limiter_separate_clients() {
        let limiter = RateLimiter::new(RateLimiterConfig {
            max_requests: 5,
            burst_multiplier: 1.0,
            ..Default::default()
        });

        for _ in 0..5 {
            limiter.check("a");
        }
        // "a" is exhausted, but "b" should work
        let result = limiter.check("b");
        assert!(result.allowed);
    }

    #[test]
    fn test_rate_limiter_burst() {
        let limiter = RateLimiter::new(RateLimiterConfig {
            max_requests: 10,
            burst_multiplier: 2.0,
            ..Default::default()
        });

        // Should allow up to 20 (10 * 2.0)
        for i in 0..20 {
            let result = limiter.check("burst");
            assert!(result.allowed, "request {} should be allowed", i);
        }

        let result = limiter.check("burst");
        assert!(!result.allowed);
    }

    // -- Router tests --

    #[test]
    fn test_router_literal_match() {
        let mut router = Router::new();
        router.add_route("GET", "/api/v1/users");

        let m = router.match_route("GET", "/api/v1/users");
        assert!(m.is_some());
        assert!(m.unwrap().params.is_empty());

        assert!(router.match_route("POST", "/api/v1/users").is_none());
        assert!(router.match_route("GET", "/api/v2/users").is_none());
    }

    #[test]
    fn test_router_param_extraction() {
        let mut router = Router::new();
        router.add_route("GET", "/api/v1/users/:id");

        let m = router.match_route("GET", "/api/v1/users/42").unwrap();
        assert_eq!(m.params.get("id").unwrap(), "42");
    }

    #[test]
    fn test_router_multiple_params() {
        let mut router = Router::new();
        router.add_route("GET", "/api/:version/users/:id");

        let m = router.match_route("GET", "/api/v2/users/99").unwrap();
        assert_eq!(m.params.get("version").unwrap(), "v2");
        assert_eq!(m.params.get("id").unwrap(), "99");
    }

    #[test]
    fn test_router_wildcard() {
        let mut router = Router::new();
        router.add_route("GET", "/static/*");

        assert!(router.match_route("GET", "/static/css/main.css").is_some());
        assert!(router.match_route("GET", "/static/").is_some());
    }

    // -- Response cache tests --

    #[test]
    fn test_cache_put_get() {
        let cache = ResponseCache::new(CacheConfig::default());

        cache.put("/users", b"[{\"id\":1}]".to_vec(), "application/json", 200);

        let resp = cache.get("/users").unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(resp.content_type, "application/json");
    }

    #[test]
    fn test_cache_miss() {
        let cache = ResponseCache::new(CacheConfig::default());
        assert!(cache.get("/nonexistent").is_none());
    }

    #[test]
    fn test_cache_expiry() {
        let cache = ResponseCache::new(CacheConfig {
            default_ttl: Duration::from_millis(0),
            ..Default::default()
        });

        cache.put("/expired", b"data".to_vec(), "text/plain", 200);
        std::thread::sleep(Duration::from_millis(1));
        assert!(cache.get("/expired").is_none());
    }

    #[test]
    fn test_cache_invalidate() {
        let cache = ResponseCache::new(CacheConfig::default());
        cache.put("/a", b"1".to_vec(), "text/plain", 200);
        cache.put("/a/b", b"2".to_vec(), "text/plain", 200);
        cache.put("/x", b"3".to_vec(), "text/plain", 200);

        let removed = cache.invalidate_prefix("/a");
        assert_eq!(removed, 2);
        assert!(cache.get("/a").is_none());
        assert!(cache.get("/x").is_some());
    }

    #[test]
    fn test_cache_stats() {
        let cache = ResponseCache::new(CacheConfig::default());
        cache.put("/k", b"v".to_vec(), "text/plain", 200);
        cache.get("/k");
        cache.get("/miss");

        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert!((stats.hit_rate - 0.5).abs() < f64::EPSILON);
    }

    // -- API key tests --

    #[test]
    fn test_api_key_register_validate() {
        let store = ApiKeyStore::new();
        store.register("sk-test123", "Test Key", vec![ApiScope::Read], None);

        let result = store.validate("sk-test123", &ApiScope::Read);
        assert!(result.valid);
        assert_eq!(result.name, Some("Test Key".to_string()));
    }

    #[test]
    fn test_api_key_missing_scope() {
        let store = ApiKeyStore::new();
        store.register("sk-readonly", "RO", vec![ApiScope::Read], None);

        let result = store.validate("sk-readonly", &ApiScope::Write);
        assert!(!result.valid);
        assert!(result.reason.unwrap().contains("Missing scope"));
    }

    #[test]
    fn test_api_key_admin_bypass() {
        let store = ApiKeyStore::new();
        store.register("sk-admin", "Admin", vec![ApiScope::Admin], None);

        // Admin should have access to any scope
        assert!(store.validate("sk-admin", &ApiScope::Read).valid);
        assert!(store.validate("sk-admin", &ApiScope::Write).valid);
    }

    #[test]
    fn test_api_key_revoke() {
        let store = ApiKeyStore::new();
        store.register("sk-temp", "Temp", vec![ApiScope::Read], None);
        assert!(store.validate("sk-temp", &ApiScope::Read).valid);

        store.revoke("sk-temp");
        assert!(!store.validate("sk-temp", &ApiScope::Read).valid);
    }

    #[test]
    fn test_api_key_expired() {
        let store = ApiKeyStore::new();
        store.register("sk-exp", "Expired", vec![ApiScope::Read], Some(0));

        let result = store.validate("sk-exp", &ApiScope::Read);
        assert!(!result.valid);
        assert!(result.reason.unwrap().contains("expired"));
    }

    // -- Webhook tests --

    #[test]
    fn test_webhook_subscribe() {
        let mgr = WebhookManager::new();
        let id = mgr.subscribe(
            "https://example.com/hook",
            vec![WebhookEvent::KeySet],
            None,
            None,
        );
        assert!(id > 0);
        assert_eq!(mgr.list_subscriptions().len(), 1);
    }

    #[test]
    fn test_webhook_matching() {
        let mgr = WebhookManager::new();
        mgr.subscribe(
            "https://a.com/hook",
            vec![WebhookEvent::KeySet],
            Some("users:*".to_string()),
            None,
        );
        mgr.subscribe(
            "https://b.com/hook",
            vec![WebhookEvent::KeyDelete],
            None,
            None,
        );

        let matches = mgr.matching_subscriptions(&WebhookEvent::KeySet, Some("users:123"));
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].url, "https://a.com/hook");

        // KeyDelete matches the second subscription (no pattern filter)
        let matches = mgr.matching_subscriptions(&WebhookEvent::KeyDelete, Some("anything"));
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn test_webhook_unsubscribe() {
        let mgr = WebhookManager::new();
        let id = mgr.subscribe("https://x.com", vec![WebhookEvent::KeySet], None, None);
        assert!(mgr.unsubscribe(id));

        let stats = mgr.stats();
        assert_eq!(stats.active_subscriptions, 0);
    }

    #[test]
    fn test_webhook_delivery_tracking() {
        let mgr = WebhookManager::new();
        mgr.record_delivery(
            1,
            "https://x.com",
            WebhookEvent::KeySet,
            DeliveryStatus::Delivered,
            Some(200),
        );
        mgr.record_delivery(
            1,
            "https://x.com",
            WebhookEvent::KeySet,
            DeliveryStatus::Failed,
            Some(500),
        );

        let stats = mgr.stats();
        assert_eq!(stats.total_deliveries, 2);
        assert_eq!(stats.failed_deliveries, 1);
    }
}
