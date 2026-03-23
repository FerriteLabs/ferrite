//! Branch registry: tracks branch metadata + parent links.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

pub type BranchId = String;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BranchMeta {
    pub id: BranchId,
    pub parent: Option<BranchId>,
    pub tenant: String,
    pub created_at_ms: u64,
    pub ttl_ms: Option<u64>,
}

impl BranchMeta {
    /// True if `now_ms` is past `created_at_ms + ttl_ms`.  Branches
    /// without a TTL never expire.
    pub fn is_expired(&self, now_ms: u64) -> bool {
        self.ttl_ms
            .is_some_and(|ttl| now_ms >= self.created_at_ms.saturating_add(ttl))
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RegistryError {
    #[error("branch '{0}' not found")]
    NotFound(BranchId),
    #[error("parent branch '{0}' not found")]
    ParentNotFound(BranchId),
    #[error("branch '{0}' already exists")]
    Duplicate(BranchId),
    #[error("tenant boundary violation: branch '{branch}' is in tenant '{owner}', requested by '{caller}'")]
    TenantMismatch {
        branch: BranchId,
        owner: String,
        caller: String,
    },
}

#[derive(Debug, Default, Clone)]
pub struct BranchRegistry {
    inner: Arc<RwLock<HashMap<BranchId, BranchMeta>>>,
    counter: Arc<RwLock<u64>>,
}

impl BranchRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a branch.  `parent = None` means "branch from main".
    pub fn create(
        &self,
        parent: Option<BranchId>,
        tenant: impl Into<String>,
        now_ms: u64,
    ) -> Result<BranchMeta, RegistryError> {
        let tenant = tenant.into();
        if let Some(p) = &parent {
            let g = self.inner.read();
            let pm = g
                .get(p)
                .ok_or_else(|| RegistryError::ParentNotFound(p.clone()))?;
            if pm.tenant != tenant {
                return Err(RegistryError::TenantMismatch {
                    branch: p.clone(),
                    owner: pm.tenant.clone(),
                    caller: tenant,
                });
            }
        }
        let id = {
            let mut c = self.counter.write();
            *c += 1;
            format!("b-{}", *c)
        };
        let meta = BranchMeta {
            id: id.clone(),
            parent,
            tenant,
            created_at_ms: now_ms,
            ttl_ms: None,
        };
        let mut g = self.inner.write();
        g.insert(id, meta.clone());
        Ok(meta)
    }

    pub fn get(&self, id: &str) -> Option<BranchMeta> {
        self.inner.read().get(id).cloned()
    }

    pub fn drop(&self, id: &str) -> bool {
        self.inner.write().remove(id).is_some()
    }

    pub fn list(&self, tenant: &str) -> Vec<BranchMeta> {
        self.inner
            .read()
            .values()
            .filter(|m| m.tenant == tenant)
            .cloned()
            .collect()
    }

    /// Dump every branch's metadata regardless of tenant — used for snapshots.
    pub fn all(&self) -> Vec<BranchMeta> {
        self.inner.read().values().cloned().collect()
    }

    /// Insert a previously-dumped `BranchMeta`.  Used to restore state from
    /// an external snapshot.  Will silently overwrite an existing entry with
    /// the same id.
    pub fn restore(&self, meta: BranchMeta) {
        let id = meta.id.clone();
        // Bump the counter past any restored ids of the form `b-N` so future
        // create() calls don't collide.
        if let Some(rest) = id.strip_prefix("b-") {
            if let Ok(n) = rest.parse::<u64>() {
                let mut c = self.counter.write();
                if n > *c {
                    *c = n;
                }
            }
        }
        self.inner.write().insert(id, meta);
    }

    /// Walk the parent chain root-first.  Returns ids ordered [root, ..., id].
    pub fn ancestry(&self, id: &str) -> Result<Vec<BranchId>, RegistryError> {
        let g = self.inner.read();
        let mut chain = Vec::new();
        let mut cursor = Some(id.to_string());
        while let Some(c) = cursor {
            let meta = g
                .get(&c)
                .ok_or_else(|| RegistryError::NotFound(c.clone()))?;
            chain.push(c.clone());
            cursor = meta.parent.clone();
        }
        chain.reverse();
        Ok(chain)
    }

    /// Set or clear a branch's TTL (milliseconds since `created_at_ms`).
    pub fn set_ttl(&self, id: &str, ttl_ms: Option<u64>) -> Result<(), RegistryError> {
        let mut g = self.inner.write();
        let meta = g
            .get_mut(id)
            .ok_or_else(|| RegistryError::NotFound(id.into()))?;
        meta.ttl_ms = ttl_ms;
        Ok(())
    }

    /// Drop every branch whose TTL elapsed at or before `now_ms`.
    /// Returns the dropped branch ids.
    pub fn reap_expired(&self, now_ms: u64) -> Vec<BranchId> {
        let mut g = self.inner.write();
        let expired: Vec<BranchId> = g
            .values()
            .filter(|m| m.is_expired(now_ms))
            .map(|m| m.id.clone())
            .collect();
        for id in &expired {
            g.remove(id);
        }
        expired
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_get() {
        let r = BranchRegistry::new();
        let m = r.create(None, "t", 1).unwrap();
        assert_eq!(r.get(&m.id).unwrap(), m);
    }

    #[test]
    fn child_inherits_tenant_and_parent() {
        let r = BranchRegistry::new();
        let p = r.create(None, "t", 1).unwrap();
        let c = r.create(Some(p.id.clone()), "t", 2).unwrap();
        assert_eq!(c.parent, Some(p.id.clone()));
        let chain = r.ancestry(&c.id).unwrap();
        assert_eq!(chain, vec![p.id, c.id]);
    }

    #[test]
    fn cross_tenant_branch_is_rejected() {
        let r = BranchRegistry::new();
        let p = r.create(None, "acme", 1).unwrap();
        let err = r.create(Some(p.id), "evil", 2);
        assert!(matches!(err, Err(RegistryError::TenantMismatch { .. })));
    }

    #[test]
    fn drop_removes_branch() {
        let r = BranchRegistry::new();
        let m = r.create(None, "t", 1).unwrap();
        assert!(r.drop(&m.id));
        assert!(r.get(&m.id).is_none());
    }

    #[test]
    fn list_filters_by_tenant() {
        let r = BranchRegistry::new();
        r.create(None, "a", 1).unwrap();
        r.create(None, "b", 1).unwrap();
        r.create(None, "a", 1).unwrap();
        assert_eq!(r.list("a").len(), 2);
        assert_eq!(r.list("b").len(), 1);
    }

    #[test]
    fn ttl_marks_branch_expired() {
        let r = BranchRegistry::new();
        let m = r.create(None, "t", 100).unwrap();
        r.set_ttl(&m.id, Some(50)).unwrap();
        assert!(!r.get(&m.id).unwrap().is_expired(140));
        assert!(r.get(&m.id).unwrap().is_expired(150));
        assert!(r.get(&m.id).unwrap().is_expired(999));
    }

    #[test]
    fn reap_drops_only_expired_branches() {
        let r = BranchRegistry::new();
        let a = r.create(None, "t", 0).unwrap();
        let b = r.create(None, "t", 0).unwrap();
        r.set_ttl(&a.id, Some(10)).unwrap();
        // b has no TTL.
        let dropped = r.reap_expired(20);
        assert_eq!(dropped, vec![a.id.clone()]);
        assert!(r.get(&a.id).is_none());
        assert!(r.get(&b.id).is_some());
    }

    #[test]
    fn no_ttl_means_never_expires() {
        let r = BranchRegistry::new();
        let m = r.create(None, "t", 0).unwrap();
        assert!(!r.get(&m.id).unwrap().is_expired(u64::MAX));
        assert!(r.reap_expired(u64::MAX).is_empty());
    }
}
