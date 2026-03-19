//! In-memory `MnemoStore` — the reference implementation of the Mnemo data plane.
//!
//! Production deployments back this trait with the real Ferrite storage engine
//! (P3 work); the in-memory impl is what unit tests, the eval harness, and
//! adapter integration tests run against.

use crate::keys::{key_for_record, key_prefix_for_agent, KeyParts};
use crate::schema::{MemoryKind, MemoryRecord, RecordId};
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Tenant-scoped query.  Every Mnemo operation MUST be parameterised by tenant
/// to make the isolation boundary impossible to forget at the call site.
#[derive(Debug, Clone)]
pub struct Scope {
    pub tenant_id: String,
    pub agent_id: String,
    pub session_id: Option<String>,
}

impl Scope {
    pub fn new(tenant_id: impl Into<String>, agent_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            agent_id: agent_id.into(),
            session_id: None,
        }
    }

    pub fn with_session(mut self, s: impl Into<String>) -> Self {
        self.session_id = Some(s.into());
        self
    }
}

/// Filters applied at recall time.
#[derive(Debug, Clone, Default)]
pub struct RecallFilter {
    pub kind: Option<MemoryKind>,
    /// Only return records with importance ≥ this value.
    pub min_importance: Option<f32>,
    /// Cap on number of records returned.  Zero means "no cap".
    pub limit: usize,
}

/// Result of a recall — the records plus a count of how many records were
/// considered before the limit was applied.  Callers use the `scanned` count
/// for observability and pagination heuristics.
#[derive(Debug, Clone)]
pub struct RecallResult {
    pub records: Vec<MemoryRecord>,
    pub scanned: usize,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum StoreError {
    #[error("tenant boundary violation: record tenant '{record}' != scope tenant '{scope}'")]
    TenantMismatch { record: String, scope: String },
    #[error("agent boundary violation: record agent '{record}' != scope agent '{scope}'")]
    AgentMismatch { record: String, scope: String },
    #[error("no such record: {0}")]
    NotFound(RecordId),
}

/// Reference in-memory store.  Thread-safe via parking_lot::RwLock.
///
/// Records are indexed by their full storage key so range queries map directly
/// to BTreeMap range scans — the same access pattern the real engine will use.
#[derive(Debug, Default)]
pub struct InMemoryMnemoStore {
    inner: Arc<RwLock<BTreeMap<String, MemoryRecord>>>,
}

impl InMemoryMnemoStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or overwrite.  Validates that the record's tenant/agent IDs
    /// match the scope to prevent accidental cross-tenant writes.
    pub fn put(&self, scope: &Scope, record: MemoryRecord) -> Result<(), StoreError> {
        if record.tenant_id != scope.tenant_id {
            return Err(StoreError::TenantMismatch {
                record: record.tenant_id.clone(),
                scope: scope.tenant_id.clone(),
            });
        }
        if record.agent_id != scope.agent_id {
            return Err(StoreError::AgentMismatch {
                record: record.agent_id.clone(),
                scope: scope.agent_id.clone(),
            });
        }
        let key = key_for_record(&KeyParts {
            tenant_id: &record.tenant_id,
            agent_id: &record.agent_id,
            session_id: record.session_id.as_deref(),
            record_id: &record.id,
        });
        self.inner.write().insert(key, record);
        Ok(())
    }

    /// Get a single record by ID.  Bumps `access_count` and `last_accessed`
    /// like the real engine will.
    pub fn get(
        &self,
        scope: &Scope,
        record_id: &str,
        now_ms: u64,
    ) -> Result<MemoryRecord, StoreError> {
        let key = key_for_record(&KeyParts {
            tenant_id: &scope.tenant_id,
            agent_id: &scope.agent_id,
            session_id: scope.session_id.as_deref(),
            record_id,
        });
        let mut guard = self.inner.write();
        let rec = guard
            .get_mut(&key)
            .ok_or_else(|| StoreError::NotFound(record_id.into()))?;
        rec.access_count = rec.access_count.saturating_add(1);
        rec.last_accessed = now_ms;
        Ok(rec.clone())
    }

    /// Delete a single record.  Returns `Ok(true)` if it existed.
    pub fn delete(&self, scope: &Scope, record_id: &str) -> bool {
        let key = key_for_record(&KeyParts {
            tenant_id: &scope.tenant_id,
            agent_id: &scope.agent_id,
            session_id: scope.session_id.as_deref(),
            record_id,
        });
        self.inner.write().remove(&key).is_some()
    }

    /// Delete every record belonging to the agent in this scope.  Returns the
    /// number removed.  Used by the GDPR "forget agent" path.
    pub fn forget_agent(&self, tenant_id: &str, agent_id: &str) -> usize {
        let prefix = key_prefix_for_agent(tenant_id, agent_id);
        let mut guard = self.inner.write();
        let to_drop: Vec<String> = guard
            .range(prefix.clone()..)
            .take_while(|(k, _)| k.starts_with(&prefix))
            .map(|(k, _)| k.clone())
            .collect();
        let n = to_drop.len();
        for k in to_drop {
            guard.remove(&k);
        }
        n
    }

    /// Recall: return records matching the filter, scoring by an "importance
    /// then recency" tiebreak.  This is the P1 baseline — P2 swaps in the
    /// hybrid retrieval scorer.
    pub fn recall(&self, scope: &Scope, now_ms: u64, filter: &RecallFilter) -> RecallResult {
        let prefix = key_prefix_for_agent(&scope.tenant_id, &scope.agent_id);
        let guard = self.inner.read();
        let mut hits: Vec<MemoryRecord> = guard
            .range(prefix.clone()..)
            .take_while(|(k, _)| k.starts_with(&prefix))
            .map(|(_, v)| v.clone())
            .filter(|r| !r.is_expired(now_ms))
            .filter(|r| filter.kind.map_or(true, |k| r.kind == k))
            .filter(|r| filter.min_importance.map_or(true, |m| r.importance >= m))
            .collect();
        let scanned = hits.len();
        // Sort: importance desc, then last_accessed desc.
        hits.sort_by(|a, b| {
            b.importance
                .partial_cmp(&a.importance)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| b.last_accessed.cmp(&a.last_accessed))
        });
        if filter.limit > 0 && hits.len() > filter.limit {
            hits.truncate(filter.limit);
        }
        RecallResult {
            records: hits,
            scanned,
        }
    }

    /// Recall an agent's memories scored by the hybrid retrieval scorer
    /// (`scorer::score_records`).  Pass `None` for `query_embedding` to
    /// fall back on the non-semantic components (importance + recency +
    /// frequency).
    pub fn recall_with_embedding(
        &self,
        scope: &Scope,
        now_ms: u64,
        query_embedding: Option<&[f32]>,
        weights: crate::scorer::ScorerWeights,
        filter: &RecallFilter,
    ) -> RecallResult {
        let prefix = key_prefix_for_agent(&scope.tenant_id, &scope.agent_id);
        let guard = self.inner.read();
        let candidates: Vec<MemoryRecord> = guard
            .range(prefix.clone()..)
            .take_while(|(k, _)| k.starts_with(&prefix))
            .map(|(_, v)| v.clone())
            .filter(|r| !r.is_expired(now_ms))
            .filter(|r| filter.kind.map_or(true, |k| r.kind == k))
            .filter(|r| filter.min_importance.map_or(true, |m| r.importance >= m))
            .collect();
        let scanned = candidates.len();
        let scored = crate::scorer::score_records(&candidates, query_embedding, now_ms, &weights);
        let mut records: Vec<MemoryRecord> = scored.into_iter().map(|s| s.record).collect();
        if filter.limit > 0 && records.len() > filter.limit {
            records.truncate(filter.limit);
        }
        RecallResult { records, scanned }
    }

    /// Remove every record whose `expires_at` has passed for `now_ms`.
    /// Returns the number of records evicted.  Single-pass O(N) over the
    /// in-memory map; the production engine's TTL sweep is incremental and
    /// runs on the background-tasks executor.
    pub fn sweep_expired(&self, now_ms: u64) -> usize {
        let mut guard = self.inner.write();
        let to_drop: Vec<String> = guard
            .iter()
            .filter(|(_, v)| v.is_expired(now_ms))
            .map(|(k, _)| k.clone())
            .collect();
        let n = to_drop.len();
        for k in to_drop {
            guard.remove(&k);
        }
        n
    }

    /// Count of records currently held — testing/observability.
    pub fn len(&self) -> usize {
        self.inner.read().len()
    }
    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::MemoryRecordBuilder;

    fn rec(
        tenant: &str,
        agent: &str,
        id: &str,
        kind: MemoryKind,
        importance: f32,
        ts: u64,
    ) -> MemoryRecord {
        MemoryRecordBuilder::new()
            .id(id)
            .tenant(tenant)
            .agent(agent)
            .kind(kind)
            .content(format!("content-{id}"))
            .importance(importance)
            .created_at(ts)
            .build()
            .unwrap()
    }

    #[test]
    fn put_get_delete_roundtrip() {
        let s = InMemoryMnemoStore::new();
        let scope = Scope::new("t", "a");
        s.put(&scope, rec("t", "a", "r1", MemoryKind::Semantic, 0.5, 100))
            .unwrap();
        let got = s.get(&scope, "r1", 200).unwrap();
        assert_eq!(got.id, "r1");
        assert_eq!(got.access_count, 1);
        assert_eq!(got.last_accessed, 200);
        assert!(s.delete(&scope, "r1"));
        assert!(matches!(
            s.get(&scope, "r1", 300),
            Err(StoreError::NotFound(_))
        ));
    }

    #[test]
    fn put_rejects_cross_tenant_write() {
        let s = InMemoryMnemoStore::new();
        let scope = Scope::new("acme", "a");
        let bad = rec("evil", "a", "r1", MemoryKind::Semantic, 0.5, 100);
        let err = s.put(&scope, bad).expect_err("should reject");
        assert!(matches!(err, StoreError::TenantMismatch { .. }));
    }

    #[test]
    fn put_rejects_cross_agent_write() {
        let s = InMemoryMnemoStore::new();
        let scope = Scope::new("t", "alice");
        let bad = rec("t", "bob", "r1", MemoryKind::Semantic, 0.5, 100);
        let err = s.put(&scope, bad).expect_err("should reject");
        assert!(matches!(err, StoreError::AgentMismatch { .. }));
    }

    #[test]
    fn forget_agent_removes_only_that_agents_records() {
        let s = InMemoryMnemoStore::new();
        s.put(
            &Scope::new("t", "alice"),
            rec("t", "alice", "r1", MemoryKind::Semantic, 0.5, 1),
        )
        .unwrap();
        s.put(
            &Scope::new("t", "alice"),
            rec("t", "alice", "r2", MemoryKind::Semantic, 0.5, 1),
        )
        .unwrap();
        s.put(
            &Scope::new("t", "bob"),
            rec("t", "bob", "r1", MemoryKind::Semantic, 0.5, 1),
        )
        .unwrap();
        assert_eq!(s.forget_agent("t", "alice"), 2);
        assert_eq!(s.len(), 1);
        // Bob untouched.
        assert!(s.get(&Scope::new("t", "bob"), "r1", 2).is_ok());
    }

    #[test]
    fn recall_orders_by_importance_then_recency() {
        let s = InMemoryMnemoStore::new();
        let scope = Scope::new("t", "a");
        s.put(&scope, rec("t", "a", "low", MemoryKind::Semantic, 0.1, 1))
            .unwrap();
        s.put(
            &scope,
            rec("t", "a", "high-old", MemoryKind::Semantic, 0.9, 1),
        )
        .unwrap();
        s.put(
            &scope,
            rec("t", "a", "high-new", MemoryKind::Semantic, 0.9, 1),
        )
        .unwrap();
        // Touch high-new so its last_accessed advances.
        s.get(&scope, "high-new", 1000).unwrap();
        let r = s.recall(
            &scope,
            2000,
            &RecallFilter {
                limit: 2,
                ..Default::default()
            },
        );
        assert_eq!(r.records.len(), 2);
        assert_eq!(r.records[0].id, "high-new");
        assert_eq!(r.records[1].id, "high-old");
        assert_eq!(r.scanned, 3);
    }

    #[test]
    fn recall_filters_by_kind_and_importance() {
        let s = InMemoryMnemoStore::new();
        let scope = Scope::new("t", "a");
        s.put(&scope, rec("t", "a", "ep", MemoryKind::Episodic, 0.9, 1))
            .unwrap();
        s.put(
            &scope,
            rec("t", "a", "se-low", MemoryKind::Semantic, 0.1, 1),
        )
        .unwrap();
        s.put(
            &scope,
            rec("t", "a", "se-high", MemoryKind::Semantic, 0.9, 1),
        )
        .unwrap();
        let r = s.recall(
            &scope,
            2,
            &RecallFilter {
                kind: Some(MemoryKind::Semantic),
                min_importance: Some(0.5),
                limit: 0,
            },
        );
        assert_eq!(r.records.len(), 1);
        assert_eq!(r.records[0].id, "se-high");
    }

    #[test]
    fn recall_skips_expired() {
        let s = InMemoryMnemoStore::new();
        let scope = Scope::new("t", "a");
        let mut r = rec("t", "a", "ttl", MemoryKind::Working, 0.9, 1);
        r.expires_at = Some(100);
        s.put(&scope, r).unwrap();
        let live = s.recall(&scope, 99, &RecallFilter::default());
        assert_eq!(live.records.len(), 1);
        let dead = s.recall(&scope, 200, &RecallFilter::default());
        assert_eq!(dead.records.len(), 0);
    }

    #[test]
    fn tenant_isolation_in_recall() {
        let s = InMemoryMnemoStore::new();
        s.put(
            &Scope::new("acme", "a"),
            rec("acme", "a", "r", MemoryKind::Semantic, 0.5, 1),
        )
        .unwrap();
        s.put(
            &Scope::new("evil", "a"),
            rec("evil", "a", "r", MemoryKind::Semantic, 0.5, 1),
        )
        .unwrap();
        let acme = s.recall(&Scope::new("acme", "a"), 2, &RecallFilter::default());
        assert_eq!(acme.records.len(), 1);
        assert_eq!(acme.records[0].tenant_id, "acme");
    }

    #[test]
    fn sweep_evicts_only_expired() {
        let s = InMemoryMnemoStore::new();
        let scope = Scope::new("t", "a");
        let mut r1 = rec("t", "a", "live", MemoryKind::Working, 0.5, 1);
        r1.expires_at = Some(1_000_000);
        let mut r2 = rec("t", "a", "dead", MemoryKind::Working, 0.5, 1);
        r2.expires_at = Some(50);
        let r3 = rec("t", "a", "no-ttl", MemoryKind::Working, 0.5, 1);
        s.put(&scope, r1).unwrap();
        s.put(&scope, r2).unwrap();
        s.put(&scope, r3).unwrap();
        assert_eq!(s.len(), 3);
        let evicted = s.sweep_expired(100);
        assert_eq!(evicted, 1);
        assert_eq!(s.len(), 2);
        assert!(s.get(&scope, "live", 200).is_ok());
        assert!(s.get(&scope, "no-ttl", 200).is_ok());
    }

    #[test]
    fn recall_with_embedding_prefers_semantic_match_over_recency() {
        use crate::scorer::ScorerWeights;
        let s = InMemoryMnemoStore::new();
        let scope = Scope::new("t", "a");
        // "match" has a similar embedding to the query but is OLD.
        let r_match = MemoryRecordBuilder::new()
            .id("match")
            .tenant("t")
            .agent("a")
            .kind(MemoryKind::Semantic)
            .content("c")
            .importance(0.5)
            .created_at(1)
            .embedding(vec![1.0, 0.0, 0.0])
            .build()
            .unwrap();
        // "fresh" has an unrelated embedding but is RECENT.
        let mut r_fresh = MemoryRecordBuilder::new()
            .id("fresh")
            .tenant("t")
            .agent("a")
            .kind(MemoryKind::Semantic)
            .content("c")
            .importance(0.5)
            .created_at(1)
            .embedding(vec![0.0, 1.0, 0.0])
            .build()
            .unwrap();
        r_fresh.last_accessed = 10_000_000;
        s.put(&scope, r_match).unwrap();
        s.put(&scope, r_fresh).unwrap();

        // Heavy semantic weight — semantic match must dominate.
        let weights = ScorerWeights {
            semantic: 0.9,
            importance: 0.05,
            recency: 0.025,
            frequency: 0.025,
            recency_half_life_ms: 24 * 60 * 60 * 1000,
        };
        let q = vec![1.0_f32, 0.0, 0.0];
        let r = s.recall_with_embedding(
            &scope,
            10_000_000,
            Some(&q),
            weights,
            &RecallFilter {
                limit: 2,
                ..Default::default()
            },
        );
        assert_eq!(r.records.len(), 2);
        assert_eq!(r.records[0].id, "match");
    }
}
