//! API Gateway
//!
//! REST-like API layer for managing Ferrite Cloud instances. Provides
//! authentication via API keys, per-key rate limiting, and a unified
//! interface over the provisioning, metering, and billing services.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::billing::BillingService;
use super::metering::MeteringService;
use super::provisioning::{InstanceSpec, InstanceStatus, ProvisionedInstance, ProvisioningService};

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

/// Errors returned by the API gateway.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ApiError {
    /// The API key is missing or invalid.
    #[error("unauthorized: {0}")]
    Unauthorized(String),

    /// The API key lacks the required permission.
    #[error("forbidden: {0}")]
    Forbidden(String),

    /// The requested resource was not found.
    #[error("not found: {0}")]
    NotFound(String),

    /// The caller has exceeded their rate limit.
    #[error("rate limited: try again later")]
    RateLimited,

    /// A conflict with the current state of the resource.
    #[error("conflict: {0}")]
    Conflict(String),

    /// An internal error occurred.
    #[error("internal error: {0}")]
    InternalError(String),

    /// The request was malformed.
    #[error("bad request: {0}")]
    BadRequest(String),
}

// ---------------------------------------------------------------------------
// Permission model
// ---------------------------------------------------------------------------

/// Permissions that can be assigned to an API key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Permission {
    /// Create new instances.
    InstanceCreate,
    /// Read instance details.
    InstanceRead,
    /// Update / scale instances.
    InstanceUpdate,
    /// Delete instances.
    InstanceDelete,
    /// Read instance metrics.
    MetricsRead,
    /// Read billing information.
    BillingRead,
    /// Modify billing configuration.
    BillingWrite,
    /// Full administrative access.
    Admin,
}

// ---------------------------------------------------------------------------
// API key management
// ---------------------------------------------------------------------------

/// Information about an issued API key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKeyInfo {
    /// The API key string.
    pub key: String,
    /// Owning tenant.
    pub tenant_id: String,
    /// Granted permissions.
    pub permissions: Vec<Permission>,
    /// When the key was created.
    pub created_at: SystemTime,
    /// Optional expiry.
    pub expires_at: Option<SystemTime>,
    /// Maximum requests per minute for this key.
    pub rate_limit: u32,
}

// ---------------------------------------------------------------------------
// Scale specification
// ---------------------------------------------------------------------------

/// Describes how to scale an existing instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScaleSpec {
    /// New vCPU count (if changing).
    pub cpu: Option<u32>,
    /// New memory in bytes (if changing).
    pub memory: Option<u64>,
    /// New storage in bytes (if changing).
    pub storage: Option<u64>,
    /// New replica count (if changing).
    pub replicas: Option<u32>,
}

// ---------------------------------------------------------------------------
// Instance info & metrics
// ---------------------------------------------------------------------------

/// Public view of a provisioned instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceInfo {
    /// Unique instance identifier.
    pub id: String,
    /// Current lifecycle status.
    pub status: InstanceStatus,
    /// Instance specification.
    pub spec: InstanceSpec,
    /// When the instance was created.
    pub created_at: SystemTime,
    /// Primary connection endpoint.
    pub endpoint: String,
    /// Deployment region.
    pub region: String,
    /// Service tier.
    pub tier: String,
}

/// Metrics snapshot for an instance over a time window.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceMetrics {
    /// Instance identifier.
    pub instance_id: String,
    /// Duration of the observation window.
    pub window: Duration,
    /// Average CPU utilization (0–100).
    pub cpu_usage_percent: f64,
    /// Memory consumption in bytes.
    pub memory_usage_bytes: u64,
    /// Throughput in commands per second.
    pub commands_per_sec: f64,
    /// Number of active client connections.
    pub connections: u32,
    /// Cache hit rate (0.0–1.0).
    pub hit_rate: f64,
    /// 99th-percentile latency in microseconds.
    pub latency_p99_us: u64,
}

// ---------------------------------------------------------------------------
// Token-bucket rate limiter
// ---------------------------------------------------------------------------

/// Per-key token bucket state.
#[derive(Debug, Clone)]
struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
    capacity: f64,
    refill_rate: f64, // tokens per second
}

/// Simple token-bucket rate limiter keyed by API key.
pub struct RateLimiter {
    buckets: DashMap<String, TokenBucket>,
}

impl RateLimiter {
    fn new() -> Self {
        Self {
            buckets: DashMap::new(),
        }
    }

    /// Check whether the caller identified by `key` may proceed given a
    /// per-minute `limit`. Returns `Err(ApiError::RateLimited)` when the
    /// bucket is empty.
    pub fn check(&self, key: &str, limit: u32) -> Result<(), ApiError> {
        let capacity = limit as f64;
        let refill_rate = capacity / 60.0; // tokens per second

        let mut entry = self.buckets.entry(key.to_string()).or_insert(TokenBucket {
            tokens: capacity,
            last_refill: Instant::now(),
            capacity,
            refill_rate,
        });

        let bucket = entry.value_mut();
        let now = Instant::now();
        let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * bucket.refill_rate).min(bucket.capacity);
        bucket.last_refill = now;

        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            Ok(())
        } else {
            Err(ApiError::RateLimited)
        }
    }
}

// ---------------------------------------------------------------------------
// API Gateway
// ---------------------------------------------------------------------------

/// REST-like gateway for managing Ferrite Cloud instances.
pub struct ApiGateway {
    provisioning: Arc<ProvisioningService>,
    metering: Arc<MeteringService>,
    billing: Arc<BillingService>,
    api_keys: DashMap<String, ApiKeyInfo>,
    rate_limiter: RateLimiter,
}

impl ApiGateway {
    /// Creates a new `ApiGateway` wrapping the provided services.
    pub fn new(
        provisioning: ProvisioningService,
        metering: MeteringService,
        billing: BillingService,
    ) -> Self {
        Self {
            provisioning: Arc::new(provisioning),
            metering: Arc::new(metering),
            billing: Arc::new(billing),
            api_keys: DashMap::new(),
            rate_limiter: RateLimiter::new(),
        }
    }

    // -- API key helpers ----------------------------------------------------

    /// Issues a new API key for the given tenant.
    pub fn create_api_key(&self, tenant_id: &str, permissions: Vec<Permission>) -> ApiKeyInfo {
        let info = ApiKeyInfo {
            key: Uuid::new_v4().to_string(),
            tenant_id: tenant_id.to_string(),
            permissions,
            created_at: SystemTime::now(),
            expires_at: None,
            rate_limit: 600, // default 600 req/min
        };
        self.api_keys.insert(info.key.clone(), info.clone());
        info
    }

    /// Validates an API key and returns the associated metadata.
    pub fn validate_api_key(&self, api_key: &str) -> Result<ApiKeyInfo, ApiError> {
        let info = self
            .api_keys
            .get(api_key)
            .ok_or_else(|| ApiError::Unauthorized("invalid api key".to_string()))?
            .clone();

        if let Some(expires) = info.expires_at {
            if SystemTime::now() > expires {
                return Err(ApiError::Unauthorized("api key expired".to_string()));
            }
        }
        Ok(info)
    }

    /// Authorize a request: validate key, check rate limit, verify permission.
    fn authorize(&self, api_key: &str, required: Permission) -> Result<ApiKeyInfo, ApiError> {
        let info = self.validate_api_key(api_key)?;
        self.rate_limiter.check(api_key, info.rate_limit)?;

        if !info.permissions.contains(&Permission::Admin) && !info.permissions.contains(&required) {
            return Err(ApiError::Forbidden(format!(
                "missing permission: {:?}",
                required
            )));
        }
        Ok(info)
    }

    // -- Instance management ------------------------------------------------

    /// Provision a new Ferrite Cloud instance.
    pub async fn create_instance(
        &self,
        api_key: &str,
        spec: InstanceSpec,
    ) -> Result<ProvisionedInstance, ApiError> {
        self.authorize(api_key, Permission::InstanceCreate)?;

        self.provisioning
            .provision(spec)
            .await
            .map_err(|e| ApiError::InternalError(e.to_string()))
    }

    /// Get details of an instance.
    pub async fn get_instance(
        &self,
        api_key: &str,
        instance_id: &str,
    ) -> Result<InstanceInfo, ApiError> {
        self.authorize(api_key, Permission::InstanceRead)?;

        let inst = self
            .provisioning
            .status(instance_id)
            .await
            .map_err(|_| ApiError::NotFound(instance_id.to_string()))?;

        Ok(to_instance_info(&inst))
    }

    /// List all instances visible to the caller.
    pub async fn list_instances(&self, api_key: &str) -> Result<Vec<InstanceInfo>, ApiError> {
        self.authorize(api_key, Permission::InstanceRead)?;

        // The provisioning service does not expose a list method, so we
        // return an empty list. A real implementation would iterate the
        // backing store.
        Ok(Vec::new())
    }

    /// Scale an existing instance.
    pub async fn scale_instance(
        &self,
        api_key: &str,
        instance_id: &str,
        new_spec: ScaleSpec,
    ) -> Result<InstanceInfo, ApiError> {
        self.authorize(api_key, Permission::InstanceUpdate)?;

        let current = self
            .provisioning
            .status(instance_id)
            .await
            .map_err(|_| ApiError::NotFound(instance_id.to_string()))?;

        let merged = InstanceSpec {
            cpu: new_spec.cpu.unwrap_or(current.spec.cpu),
            memory: new_spec.memory.unwrap_or(current.spec.memory),
            storage: new_spec.storage.unwrap_or(current.spec.storage),
            region: current.spec.region.clone(),
            tier: current.spec.tier.clone(),
        };

        let scaled = self
            .provisioning
            .scale(instance_id, merged)
            .await
            .map_err(|e| ApiError::InternalError(e.to_string()))?;

        Ok(to_instance_info(&scaled))
    }

    /// Delete (deprovision) an instance.
    pub async fn delete_instance(&self, api_key: &str, instance_id: &str) -> Result<(), ApiError> {
        self.authorize(api_key, Permission::InstanceDelete)?;

        self.provisioning
            .deprovision(instance_id)
            .await
            .map_err(|_| ApiError::NotFound(instance_id.to_string()))
    }

    /// Retrieve metrics for an instance over the given window.
    pub async fn get_metrics(
        &self,
        api_key: &str,
        instance_id: &str,
        window: Duration,
    ) -> Result<InstanceMetrics, ApiError> {
        self.authorize(api_key, Permission::MetricsRead)?;

        // Verify the instance exists.
        let _inst = self
            .provisioning
            .status(instance_id)
            .await
            .map_err(|_| ApiError::NotFound(instance_id.to_string()))?;

        // In a real implementation we would query the metering service.
        // Return a placeholder snapshot.
        Ok(InstanceMetrics {
            instance_id: instance_id.to_string(),
            window,
            cpu_usage_percent: 0.0,
            memory_usage_bytes: 0,
            commands_per_sec: 0.0,
            connections: 0,
            hit_rate: 0.0,
            latency_p99_us: 0,
        })
    }
}

/// Convert a [`ProvisionedInstance`] into the public [`InstanceInfo`] view.
fn to_instance_info(inst: &ProvisionedInstance) -> InstanceInfo {
    InstanceInfo {
        id: inst.id.clone(),
        status: inst.status.clone(),
        spec: inst.spec.clone(),
        created_at: inst.created_at.into(),
        endpoint: inst.endpoints.first().cloned().unwrap_or_default(),
        region: inst.spec.region.clone(),
        tier: inst.spec.tier.clone(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cloud::CloudProvider;

    fn make_gateway() -> ApiGateway {
        let prov = ProvisioningService::new(CloudProvider::Aws);
        let meter = MeteringService::new();
        let bill = BillingService::new();
        ApiGateway::new(prov, meter, bill)
    }

    fn sample_spec() -> InstanceSpec {
        InstanceSpec {
            cpu: 4,
            memory: 8 * 1024 * 1024 * 1024,
            storage: 100 * 1024 * 1024 * 1024,
            region: "us-east-1".to_string(),
            tier: "standard".to_string(),
        }
    }

    // -- API key tests ------------------------------------------------------

    #[test]
    fn test_create_and_validate_api_key() {
        let gw = make_gateway();
        let key = gw.create_api_key("tenant-1", vec![Permission::InstanceRead]);
        assert_eq!(key.tenant_id, "tenant-1");

        let validated = gw.validate_api_key(&key.key).unwrap();
        assert_eq!(validated.tenant_id, "tenant-1");
    }

    #[test]
    fn test_validate_invalid_key() {
        let gw = make_gateway();
        assert!(matches!(
            gw.validate_api_key("bogus"),
            Err(ApiError::Unauthorized(_))
        ));
    }

    #[test]
    fn test_expired_key() {
        let gw = make_gateway();
        let mut info = ApiKeyInfo {
            key: "expired-key".to_string(),
            tenant_id: "t1".to_string(),
            permissions: vec![Permission::Admin],
            created_at: SystemTime::now(),
            expires_at: Some(SystemTime::UNIX_EPOCH), // already expired
            rate_limit: 100,
        };
        gw.api_keys.insert(info.key.clone(), info.clone());

        assert!(matches!(
            gw.validate_api_key("expired-key"),
            Err(ApiError::Unauthorized(_))
        ));
    }

    #[test]
    fn test_permission_denied() {
        let gw = make_gateway();
        let key = gw.create_api_key("t1", vec![Permission::InstanceRead]);

        let result = gw.authorize(&key.key, Permission::InstanceCreate);
        assert!(matches!(result, Err(ApiError::Forbidden(_))));
    }

    #[test]
    fn test_admin_bypasses_permissions() {
        let gw = make_gateway();
        let key = gw.create_api_key("t1", vec![Permission::Admin]);

        // Admin should be able to authorize any permission.
        gw.authorize(&key.key, Permission::InstanceCreate).unwrap();
        gw.authorize(&key.key, Permission::BillingWrite).unwrap();
    }

    // -- Rate limiter tests -------------------------------------------------

    #[test]
    fn test_rate_limiter_allows_within_limit() {
        let rl = RateLimiter::new();
        for _ in 0..10 {
            rl.check("k1", 60).unwrap();
        }
    }

    #[test]
    fn test_rate_limiter_rejects_over_limit() {
        let rl = RateLimiter::new();
        // Exhaust the bucket (capacity = 2).
        rl.check("k1", 2).unwrap();
        rl.check("k1", 2).unwrap();
        assert!(matches!(rl.check("k1", 2), Err(ApiError::RateLimited)));
    }

    // -- Instance lifecycle tests -------------------------------------------

    #[tokio::test]
    async fn test_create_and_get_instance() {
        let gw = make_gateway();
        let key = gw.create_api_key(
            "t1",
            vec![Permission::InstanceCreate, Permission::InstanceRead],
        );

        let inst = gw.create_instance(&key.key, sample_spec()).await.unwrap();
        let info = gw.get_instance(&key.key, &inst.id).await.unwrap();
        assert_eq!(info.id, inst.id);
        assert_eq!(info.region, "us-east-1");
    }

    #[tokio::test]
    async fn test_scale_instance() {
        let gw = make_gateway();
        let key = gw.create_api_key(
            "t1",
            vec![
                Permission::InstanceCreate,
                Permission::InstanceRead,
                Permission::InstanceUpdate,
            ],
        );

        let inst = gw.create_instance(&key.key, sample_spec()).await.unwrap();
        let scaled = gw
            .scale_instance(
                &key.key,
                &inst.id,
                ScaleSpec {
                    cpu: Some(8),
                    memory: None,
                    storage: None,
                    replicas: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(scaled.spec.cpu, 8);
        assert_eq!(scaled.status, InstanceStatus::Scaling);
    }

    #[tokio::test]
    async fn test_delete_instance() {
        let gw = make_gateway();
        let key = gw.create_api_key(
            "t1",
            vec![
                Permission::InstanceCreate,
                Permission::InstanceDelete,
                Permission::InstanceRead,
            ],
        );

        let inst = gw.create_instance(&key.key, sample_spec()).await.unwrap();
        gw.delete_instance(&key.key, &inst.id).await.unwrap();
        assert!(matches!(
            gw.get_instance(&key.key, &inst.id).await,
            Err(ApiError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn test_get_metrics() {
        let gw = make_gateway();
        let key = gw.create_api_key(
            "t1",
            vec![Permission::InstanceCreate, Permission::MetricsRead],
        );

        let inst = gw.create_instance(&key.key, sample_spec()).await.unwrap();
        let metrics = gw
            .get_metrics(&key.key, &inst.id, Duration::from_secs(300))
            .await
            .unwrap();
        assert_eq!(metrics.instance_id, inst.id);
    }

    #[tokio::test]
    async fn test_unauthorized_create() {
        let gw = make_gateway();
        let result = gw.create_instance("bad-key", sample_spec()).await;
        assert!(matches!(result, Err(ApiError::Unauthorized(_))));
    }
}
