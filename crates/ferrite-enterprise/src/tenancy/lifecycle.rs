//! Tenant lifecycle management
//!
//! Create, suspend, resume, delete tenants with full state machine.

#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use uuid::Uuid;

use super::quota_resource::QuotaConfig;
use super::TenantTier;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors originating from tenant lifecycle operations
#[derive(Debug, Clone, thiserror::Error)]
pub enum LifecycleError {
    /// A tenant with this ID already exists
    #[error("tenant already exists: {0}")]
    TenantExists(String),

    /// The requested tenant was not found
    #[error("tenant not found: {0}")]
    TenantNotFound(String),

    /// Invalid state transition
    #[error("invalid state transition: cannot move from '{current}' to '{requested}'")]
    InvalidState {
        /// Current state
        current: String,
        /// Requested target state
        requested: String,
    },

    /// Quota exceeded while creating or updating a tenant
    #[error("quota exceeded: {0}")]
    QuotaExceeded(String),

    /// A deletion is already in progress for this tenant
    #[error("deletion already in progress for tenant: {0}")]
    DeletionInProgress(String),
}

// ---------------------------------------------------------------------------
// State machine
// ---------------------------------------------------------------------------

/// Lifecycle state of a tenant.
///
/// Valid transitions:
/// ```text
/// Active → Suspended → Active   (resume)
///                    → Deleting → Archived   (soft delete)
///                    → (removed)             (force delete)
/// Active → Deleting  → Archived
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LifecycleState {
    /// Tenant is active
    Active,
    /// Tenant is suspended (reason stored in [`TenantRecord::suspended_reason`])
    Suspended,
    /// Tenant is being deleted
    Deleting,
    /// Tenant has been soft-deleted and archived
    Archived,
}

impl std::fmt::Display for LifecycleState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => write!(f, "active"),
            Self::Suspended => write!(f, "suspended"),
            Self::Deleting => write!(f, "deleting"),
            Self::Archived => write!(f, "archived"),
        }
    }
}

impl LifecycleState {
    /// Validate that transitioning from `self` to `target` is legal.
    fn validate_transition(&self, target: &LifecycleState) -> Result<(), LifecycleError> {
        let ok = matches!(
            (self, target),
            (LifecycleState::Active, LifecycleState::Suspended)
                | (LifecycleState::Active, LifecycleState::Deleting)
                | (LifecycleState::Suspended, LifecycleState::Active)
                | (LifecycleState::Suspended, LifecycleState::Deleting)
                | (LifecycleState::Deleting, LifecycleState::Archived)
        );
        if ok {
            Ok(())
        } else {
            Err(LifecycleError::InvalidState {
                current: self.to_string(),
                requested: target.to_string(),
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Request / Record types
// ---------------------------------------------------------------------------

/// Request to create a new tenant
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CreateTenantRequest {
    /// Optional id — auto-generated (UUID) when `None`.
    pub id: Option<String>,
    /// Display name
    pub name: String,
    /// Billing / feature tier
    pub tier: TenantTier,
    /// Optional quota override (uses tier defaults when `None`)
    pub quota_override: Option<QuotaConfig>,
    /// Arbitrary key-value metadata
    pub metadata: HashMap<String, String>,
}

/// Persistent record for a tenant
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TenantRecord {
    /// Unique identifier
    pub id: String,
    /// Display name
    pub name: String,
    /// Current lifecycle state
    pub state: LifecycleState,
    /// Billing / feature tier
    pub tier: TenantTier,
    /// Resource quota configuration
    pub quota: QuotaConfig,
    /// Arbitrary metadata
    pub metadata: HashMap<String, String>,
    /// Creation timestamp (ms since epoch)
    pub created_at: u64,
    /// Last update timestamp (ms since epoch)
    pub updated_at: u64,
    /// Reason the tenant was suspended (if applicable)
    pub suspended_reason: Option<String>,
}

/// Filter criteria for listing tenants
#[derive(Clone, Debug, Default)]
pub struct TenantFilter {
    /// Only return tenants in this state
    pub state: Option<LifecycleState>,
    /// Only return tenants of this tier
    pub tier: Option<TenantTier>,
    /// Name prefix filter
    pub name_prefix: Option<String>,
    /// Created after this timestamp (ms since epoch)
    pub created_after: Option<u64>,
    /// Created before this timestamp (ms since epoch)
    pub created_before: Option<u64>,
}

impl TenantFilter {
    /// Check if a record matches all filter criteria.
    fn matches(&self, record: &TenantRecord) -> bool {
        if let Some(ref state) = self.state {
            if &record.state != state {
                return false;
            }
        }
        if let Some(ref tier) = self.tier {
            if &record.tier != tier {
                return false;
            }
        }
        if let Some(ref prefix) = self.name_prefix {
            if !record.name.starts_with(prefix.as_str()) {
                return false;
            }
        }
        if let Some(after) = self.created_after {
            if record.created_at < after {
                return false;
            }
        }
        if let Some(before) = self.created_before {
            if record.created_at > before {
                return false;
            }
        }
        true
    }
}

/// Point-in-time statistics for a single tenant
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TenantStats {
    /// Memory currently used (bytes)
    pub memory_used: u64,
    /// Number of keys stored
    pub keys_count: u64,
    /// Active connections
    pub connections_active: u32,
    /// Observed operations per second
    pub ops_per_sec: u64,
    /// Uptime in seconds since tenant creation
    pub uptime_secs: u64,
    /// Timestamp of last activity (ms since epoch)
    pub last_activity: u64,
}

// ---------------------------------------------------------------------------
// TenantLifecycleManager
// ---------------------------------------------------------------------------

/// Manages the full lifecycle of tenants: creation, suspension, resumption,
/// soft/force deletion, listing and quota updates.
pub struct TenantLifecycleManager {
    /// All known tenants indexed by ID
    tenants: Arc<RwLock<HashMap<String, TenantRecord>>>,
}

impl TenantLifecycleManager {
    /// Create a new, empty lifecycle manager.
    pub fn new() -> Self {
        Self {
            tenants: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a new tenant from the given request.
    pub fn create_tenant(
        &self,
        request: CreateTenantRequest,
    ) -> Result<TenantRecord, LifecycleError> {
        let id = request.id.unwrap_or_else(|| Uuid::new_v4().to_string());

        let mut tenants = self.tenants.write();

        if tenants.contains_key(&id) {
            return Err(LifecycleError::TenantExists(id));
        }

        let now = current_timestamp_ms();

        let quota = request
            .quota_override
            .unwrap_or_else(|| default_quota_for_tier(&request.tier));

        let record = TenantRecord {
            id: id.clone(),
            name: request.name,
            state: LifecycleState::Active,
            tier: request.tier,
            quota,
            metadata: request.metadata,
            created_at: now,
            updated_at: now,
            suspended_reason: None,
        };

        tenants.insert(id, record.clone());
        Ok(record)
    }

    /// Suspend an active tenant.
    pub fn suspend_tenant(&self, tenant_id: &str, reason: &str) -> Result<(), LifecycleError> {
        let mut tenants = self.tenants.write();
        let record = tenants
            .get_mut(tenant_id)
            .ok_or_else(|| LifecycleError::TenantNotFound(tenant_id.to_string()))?;

        record
            .state
            .validate_transition(&LifecycleState::Suspended)?;

        record.state = LifecycleState::Suspended;
        record.suspended_reason = Some(reason.to_string());
        record.updated_at = current_timestamp_ms();
        Ok(())
    }

    /// Resume a suspended tenant back to active.
    pub fn resume_tenant(&self, tenant_id: &str) -> Result<(), LifecycleError> {
        let mut tenants = self.tenants.write();
        let record = tenants
            .get_mut(tenant_id)
            .ok_or_else(|| LifecycleError::TenantNotFound(tenant_id.to_string()))?;

        record.state.validate_transition(&LifecycleState::Active)?;

        record.state = LifecycleState::Active;
        record.suspended_reason = None;
        record.updated_at = current_timestamp_ms();
        Ok(())
    }

    /// Delete a tenant.
    ///
    /// * `force = false` — soft delete: transitions through `Deleting → Archived`.
    /// * `force = true` — immediate removal from the map.
    pub fn delete_tenant(&self, tenant_id: &str, force: bool) -> Result<(), LifecycleError> {
        let mut tenants = self.tenants.write();

        if force {
            tenants
                .remove(tenant_id)
                .ok_or_else(|| LifecycleError::TenantNotFound(tenant_id.to_string()))?;
            return Ok(());
        }

        let record = tenants
            .get_mut(tenant_id)
            .ok_or_else(|| LifecycleError::TenantNotFound(tenant_id.to_string()))?;

        if record.state == LifecycleState::Deleting {
            return Err(LifecycleError::DeletionInProgress(tenant_id.to_string()));
        }

        // Transition to Deleting first
        record
            .state
            .validate_transition(&LifecycleState::Deleting)?;
        record.state = LifecycleState::Deleting;
        record.updated_at = current_timestamp_ms();

        // Then immediately archive (in a real system there would be an async
        // cleanup step between Deleting and Archived).
        record.state = LifecycleState::Archived;
        record.updated_at = current_timestamp_ms();
        Ok(())
    }

    /// Look up a tenant by ID.
    pub fn get_tenant(&self, tenant_id: &str) -> Option<TenantRecord> {
        let tenants = self.tenants.read();
        tenants.get(tenant_id).cloned()
    }

    /// List tenants matching the given filter.
    pub fn list_tenants(&self, filter: TenantFilter) -> Vec<TenantRecord> {
        let tenants = self.tenants.read();
        tenants
            .values()
            .filter(|r| filter.matches(r))
            .cloned()
            .collect()
    }

    /// Update the quota configuration for a tenant.
    pub fn update_quota(&self, tenant_id: &str, quota: QuotaConfig) -> Result<(), LifecycleError> {
        let mut tenants = self.tenants.write();
        let record = tenants
            .get_mut(tenant_id)
            .ok_or_else(|| LifecycleError::TenantNotFound(tenant_id.to_string()))?;

        record.quota = quota;
        record.updated_at = current_timestamp_ms();
        Ok(())
    }

    /// Get stats for a tenant (stub — returns defaults populated from the record).
    pub fn tenant_stats(&self, tenant_id: &str) -> Option<TenantStats> {
        let tenants = self.tenants.read();
        let record = tenants.get(tenant_id)?;

        let now = current_timestamp_ms();
        Some(TenantStats {
            uptime_secs: (now.saturating_sub(record.created_at)) / 1000,
            last_activity: record.updated_at,
            ..Default::default()
        })
    }
}

impl Default for TenantLifecycleManager {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Current time in milliseconds since UNIX epoch.
fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Return sensible default quota for the given tier.
fn default_quota_for_tier(tier: &TenantTier) -> QuotaConfig {
    match tier {
        TenantTier::Free => QuotaConfig {
            max_memory_bytes: 64 * 1024 * 1024,
            max_keys: 10_000,
            max_connections: 10,
            max_ops_per_second: 100,
            ..QuotaConfig::default()
        },
        TenantTier::Basic => QuotaConfig {
            max_memory_bytes: 512 * 1024 * 1024,
            max_keys: 100_000,
            max_connections: 50,
            max_ops_per_second: 1_000,
            ..QuotaConfig::default()
        },
        TenantTier::Pro => QuotaConfig {
            max_memory_bytes: 4 * 1024 * 1024 * 1024,
            max_keys: 10_000_000,
            max_connections: 500,
            max_ops_per_second: 50_000,
            ..QuotaConfig::default()
        },
        TenantTier::Enterprise | TenantTier::Custom(_) => QuotaConfig {
            max_memory_bytes: 64 * 1024 * 1024 * 1024,
            max_keys: 1_000_000_000,
            max_connections: 10_000,
            max_ops_per_second: 1_000_000,
            ..QuotaConfig::default()
        },
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn req(name: &str, tier: TenantTier) -> CreateTenantRequest {
        CreateTenantRequest {
            id: Some(name.to_string()),
            name: name.to_string(),
            tier,
            quota_override: None,
            metadata: HashMap::new(),
        }
    }

    #[test]
    fn test_create_tenant() {
        let mgr = TenantLifecycleManager::new();
        let record = mgr.create_tenant(req("acme", TenantTier::Pro)).unwrap();
        assert_eq!(record.id, "acme");
        assert_eq!(record.state, LifecycleState::Active);
    }

    #[test]
    fn test_create_duplicate() {
        let mgr = TenantLifecycleManager::new();
        mgr.create_tenant(req("acme", TenantTier::Free)).unwrap();
        let err = mgr
            .create_tenant(req("acme", TenantTier::Free))
            .unwrap_err();
        assert!(matches!(err, LifecycleError::TenantExists(_)));
    }

    #[test]
    fn test_create_auto_id() {
        let mgr = TenantLifecycleManager::new();
        let record = mgr
            .create_tenant(CreateTenantRequest {
                id: None,
                name: "Auto".to_string(),
                tier: TenantTier::Basic,
                quota_override: None,
                metadata: HashMap::new(),
            })
            .unwrap();
        assert!(!record.id.is_empty());
    }

    #[test]
    fn test_suspend_resume() {
        let mgr = TenantLifecycleManager::new();
        mgr.create_tenant(req("t1", TenantTier::Free)).unwrap();

        mgr.suspend_tenant("t1", "maintenance").unwrap();
        let r = mgr.get_tenant("t1").unwrap();
        assert_eq!(r.state, LifecycleState::Suspended);
        assert_eq!(r.suspended_reason.as_deref(), Some("maintenance"));

        mgr.resume_tenant("t1").unwrap();
        let r = mgr.get_tenant("t1").unwrap();
        assert_eq!(r.state, LifecycleState::Active);
        assert!(r.suspended_reason.is_none());
    }

    #[test]
    fn test_invalid_transition() {
        let mgr = TenantLifecycleManager::new();
        mgr.create_tenant(req("t1", TenantTier::Free)).unwrap();

        // Cannot resume an active tenant
        let err = mgr.resume_tenant("t1").unwrap_err();
        assert!(matches!(err, LifecycleError::InvalidState { .. }));
    }

    #[test]
    fn test_soft_delete() {
        let mgr = TenantLifecycleManager::new();
        mgr.create_tenant(req("t1", TenantTier::Free)).unwrap();

        mgr.delete_tenant("t1", false).unwrap();
        let r = mgr.get_tenant("t1").unwrap();
        assert_eq!(r.state, LifecycleState::Archived);
    }

    #[test]
    fn test_force_delete() {
        let mgr = TenantLifecycleManager::new();
        mgr.create_tenant(req("t1", TenantTier::Free)).unwrap();

        mgr.delete_tenant("t1", true).unwrap();
        assert!(mgr.get_tenant("t1").is_none());
    }

    #[test]
    fn test_list_tenants_with_filter() {
        let mgr = TenantLifecycleManager::new();
        mgr.create_tenant(req("t1", TenantTier::Free)).unwrap();
        mgr.create_tenant(req("t2", TenantTier::Pro)).unwrap();
        mgr.create_tenant(req("t3", TenantTier::Free)).unwrap();

        let free = mgr.list_tenants(TenantFilter {
            tier: Some(TenantTier::Free),
            ..Default::default()
        });
        assert_eq!(free.len(), 2);
    }

    #[test]
    fn test_update_quota() {
        let mgr = TenantLifecycleManager::new();
        mgr.create_tenant(req("t1", TenantTier::Free)).unwrap();

        let new_quota = QuotaConfig {
            max_memory_bytes: 999,
            ..QuotaConfig::default()
        };
        mgr.update_quota("t1", new_quota).unwrap();
        let r = mgr.get_tenant("t1").unwrap();
        assert_eq!(r.quota.max_memory_bytes, 999);
    }

    #[test]
    fn test_tenant_stats() {
        let mgr = TenantLifecycleManager::new();
        mgr.create_tenant(req("t1", TenantTier::Free)).unwrap();
        let stats = mgr.tenant_stats("t1").unwrap();
        assert!(stats.last_activity > 0);
    }

    #[test]
    fn test_not_found() {
        let mgr = TenantLifecycleManager::new();
        assert!(matches!(
            mgr.suspend_tenant("nope", "x").unwrap_err(),
            LifecycleError::TenantNotFound(_)
        ));
        assert!(matches!(
            mgr.resume_tenant("nope").unwrap_err(),
            LifecycleError::TenantNotFound(_)
        ));
        assert!(matches!(
            mgr.delete_tenant("nope", false).unwrap_err(),
            LifecycleError::TenantNotFound(_)
        ));
        assert!(mgr.tenant_stats("nope").is_none());
    }
}
