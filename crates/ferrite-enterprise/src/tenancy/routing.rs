//! Tenant Routing
//!
//! Routes incoming requests to the correct tenant based on
//! authentication credentials or connection metadata. Supports:
//!
//! - Tenant identification via AUTH command (`AUTH <tenant_id> <password>`)
//! - Tenant identification via connection metadata (e.g. TLS SNI, proxy header)
//! - Tenant switching within a session (`TENANT USE <id>`)

use super::{TenancyError, TenantContext, TenantManager};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// How a tenant was identified for a connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TenantIdentity {
    /// Identified via AUTH command extension: `AUTH <tenant_id> <password>`
    Auth {
        tenant_id: String,
        username: Option<String>,
    },
    /// Identified via connection metadata (e.g. TLS SNI, proxy protocol header)
    ConnectionMeta { tenant_id: String, source: String },
    /// Identified via explicit `TENANT USE <id>` command
    Explicit { tenant_id: String },
}

impl TenantIdentity {
    /// Get the tenant ID regardless of identification method.
    pub fn tenant_id(&self) -> &str {
        match self {
            Self::Auth { tenant_id, .. } => tenant_id,
            Self::ConnectionMeta { tenant_id, .. } => tenant_id,
            Self::Explicit { tenant_id } => tenant_id,
        }
    }
}

/// Per-connection tenant session state.
pub struct TenantSession {
    /// Current active tenant context (if any)
    current: RwLock<Option<Arc<TenantContext>>>,
    /// How the tenant was identified
    identity: RwLock<Option<TenantIdentity>>,
    /// Connection ID for tracking
    connection_id: String,
}

impl TenantSession {
    /// Create a new session with no tenant selected.
    pub fn new(connection_id: impl Into<String>) -> Self {
        Self {
            current: RwLock::new(None),
            identity: RwLock::new(None),
            connection_id: connection_id.into(),
        }
    }

    /// Get the currently active tenant context.
    pub async fn current_tenant(&self) -> Option<Arc<TenantContext>> {
        self.current.read().await.clone()
    }

    /// Get the currently active tenant context, or error if none selected.
    pub async fn require_tenant(&self) -> Result<Arc<TenantContext>, TenancyError> {
        self.current
            .read()
            .await
            .clone()
            .ok_or(TenancyError::NoTenantSelected)
    }

    /// Set the active tenant for this session.
    pub async fn set_tenant(&self, ctx: Arc<TenantContext>, identity: TenantIdentity) {
        // Decrement connection count on old tenant
        if let Some(old) = self.current.read().await.as_ref() {
            old.record_connection_close();
        }

        // Increment on new tenant
        ctx.record_connection_open();

        tracing::debug!(
            connection = %self.connection_id,
            tenant = %ctx.tenant_id,
            "tenant session switched"
        );

        *self.current.write().await = Some(ctx);
        *self.identity.write().await = Some(identity);
    }

    /// Clear the active tenant (disconnect).
    pub async fn clear(&self) {
        if let Some(old) = self.current.write().await.take() {
            old.record_connection_close();
        }
        *self.identity.write().await = None;
    }

    /// Get the connection ID.
    pub fn connection_id(&self) -> &str {
        &self.connection_id
    }

    /// Get the current identity.
    pub async fn identity(&self) -> Option<TenantIdentity> {
        self.identity.read().await.clone()
    }
}

/// Routes requests to the correct tenant.
pub struct TenantRouter {
    /// Reference to the tenant manager
    manager: Arc<TenantManager>,
    /// Static tenant mapping: auth-credential → tenant ID
    auth_map: RwLock<HashMap<String, String>>,
    /// Whether auto-provisioning of unknown tenants is enabled
    auto_provision: bool,
}

impl TenantRouter {
    /// Create a new tenant router.
    pub fn new(manager: Arc<TenantManager>, auto_provision: bool) -> Self {
        Self {
            manager,
            auth_map: RwLock::new(HashMap::new()),
            auto_provision,
        }
    }

    /// Register a static mapping from auth credential to tenant ID.
    pub async fn register_auth_mapping(&self, credential: String, tenant_id: String) {
        self.auth_map.write().await.insert(credential, tenant_id);
    }

    /// Route via AUTH command: resolve credential to tenant, return context.
    pub async fn route_auth(
        &self,
        credential: &str,
        username: Option<&str>,
    ) -> Result<(Arc<TenantContext>, TenantIdentity), TenancyError> {
        // Look up tenant ID from credential
        let tenant_id = {
            let map = self.auth_map.read().await;
            map.get(credential).cloned()
        };

        let tenant_id = tenant_id.ok_or_else(|| {
            TenancyError::PermissionDenied("unknown auth credential".to_string())
        })?;

        let ctx = self.resolve_tenant(&tenant_id).await?;
        let identity = TenantIdentity::Auth {
            tenant_id,
            username: username.map(|s| s.to_string()),
        };
        Ok((ctx, identity))
    }

    /// Route via connection metadata (e.g., TLS SNI or proxy header).
    pub async fn route_connection_meta(
        &self,
        tenant_id: &str,
        source: &str,
    ) -> Result<(Arc<TenantContext>, TenantIdentity), TenancyError> {
        let ctx = self.resolve_tenant(tenant_id).await?;
        let identity = TenantIdentity::ConnectionMeta {
            tenant_id: tenant_id.to_string(),
            source: source.to_string(),
        };
        Ok((ctx, identity))
    }

    /// Route via explicit `TENANT USE <id>` command.
    pub async fn route_explicit(
        &self,
        tenant_id: &str,
    ) -> Result<(Arc<TenantContext>, TenantIdentity), TenancyError> {
        let ctx = self.resolve_tenant(tenant_id).await?;
        let identity = TenantIdentity::Explicit {
            tenant_id: tenant_id.to_string(),
        };
        Ok((ctx, identity))
    }

    /// Switch tenant within an existing session.
    pub async fn switch_tenant(
        &self,
        session: &TenantSession,
        new_tenant_id: &str,
    ) -> Result<(), TenancyError> {
        let (ctx, identity) = self.route_explicit(new_tenant_id).await?;
        session.set_tenant(ctx, identity).await;
        Ok(())
    }

    /// Resolve a tenant ID to a context, optionally auto-provisioning.
    async fn resolve_tenant(
        &self,
        tenant_id: &str,
    ) -> Result<Arc<TenantContext>, TenancyError> {
        if self.auto_provision {
            self.manager.get_or_create(tenant_id).await
        } else {
            self.manager
                .get_tenant(tenant_id)
                .await
                .ok_or_else(|| TenancyError::TenantNotFound(tenant_id.to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup() -> (Arc<TenantManager>, Arc<TenantRouter>) {
        let manager = Arc::new(TenantManager::default());
        manager
            .create_tenant("acme", "Acme Corp", None)
            .await
            .unwrap();
        manager
            .create_tenant("globex", "Globex Corp", None)
            .await
            .unwrap();

        let router = Arc::new(TenantRouter::new(manager.clone(), false));
        router
            .register_auth_mapping("token-acme".to_string(), "acme".to_string())
            .await;
        router
            .register_auth_mapping("token-globex".to_string(), "globex".to_string())
            .await;
        (manager, router)
    }

    #[tokio::test]
    async fn test_route_auth_success() {
        let (_mgr, router) = setup().await;

        let (ctx, identity) = router.route_auth("token-acme", Some("admin")).await.unwrap();
        assert_eq!(ctx.tenant_id, "acme");
        assert!(matches!(identity, TenantIdentity::Auth { .. }));
    }

    #[tokio::test]
    async fn test_route_auth_unknown_credential() {
        let (_mgr, router) = setup().await;

        let result = router.route_auth("bad-token", None).await;
        assert!(matches!(result, Err(TenancyError::PermissionDenied(_))));
    }

    #[tokio::test]
    async fn test_route_explicit() {
        let (_mgr, router) = setup().await;

        let (ctx, identity) = router.route_explicit("globex").await.unwrap();
        assert_eq!(ctx.tenant_id, "globex");
        assert!(matches!(identity, TenantIdentity::Explicit { .. }));
    }

    #[tokio::test]
    async fn test_route_explicit_not_found() {
        let (_mgr, router) = setup().await;

        let result = router.route_explicit("nonexistent").await;
        assert!(matches!(result, Err(TenancyError::TenantNotFound(_))));
    }

    #[tokio::test]
    async fn test_route_connection_meta() {
        let (_mgr, router) = setup().await;

        let (ctx, identity) = router
            .route_connection_meta("acme", "tls-sni")
            .await
            .unwrap();
        assert_eq!(ctx.tenant_id, "acme");
        assert!(matches!(identity, TenantIdentity::ConnectionMeta { .. }));
    }

    #[tokio::test]
    async fn test_session_lifecycle() {
        let (_mgr, router) = setup().await;
        let session = TenantSession::new("conn-1");

        // No tenant initially
        assert!(session.current_tenant().await.is_none());
        assert!(session.require_tenant().await.is_err());

        // Set tenant
        let (ctx, identity) = router.route_explicit("acme").await.unwrap();
        session.set_tenant(ctx, identity).await;

        let current = session.require_tenant().await.unwrap();
        assert_eq!(current.tenant_id, "acme");

        // Clear
        session.clear().await;
        assert!(session.current_tenant().await.is_none());
    }

    #[tokio::test]
    async fn test_session_tenant_switch() {
        let (_mgr, router) = setup().await;
        let session = TenantSession::new("conn-2");

        // Start with acme
        let (ctx, id) = router.route_explicit("acme").await.unwrap();
        session.set_tenant(ctx, id).await;
        assert_eq!(session.require_tenant().await.unwrap().tenant_id, "acme");

        // Switch to globex
        router.switch_tenant(&session, "globex").await.unwrap();
        assert_eq!(session.require_tenant().await.unwrap().tenant_id, "globex");
    }

    #[tokio::test]
    async fn test_auto_provision() {
        let manager = Arc::new(TenantManager::default());
        let router = TenantRouter::new(manager.clone(), true);

        // Tenant doesn't exist yet — should be auto-created
        let (ctx, _) = router.route_explicit("new-tenant").await.unwrap();
        assert_eq!(ctx.tenant_id, "new-tenant");

        // Verify it now exists
        assert!(manager.get_tenant("new-tenant").await.is_some());
    }
}
