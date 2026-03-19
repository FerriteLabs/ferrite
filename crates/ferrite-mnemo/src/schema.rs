//! Mnemo memory record schema.  Stable serialized form per ADR-018.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type RecordId = String;
pub type SessionId = String;

/// Classification of a memory record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryKind {
    Working,
    Semantic,
    Episodic,
    Procedural,
    Summary,
}

/// Mnemo memory record (wire form v1).
///
/// All future additions MUST be optional fields with serde defaults so older
/// serialized records keep loading.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryRecord {
    pub version: u32,
    pub id: RecordId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<RecordId>,
    pub tenant_id: String,
    pub agent_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<SessionId>,
    pub kind: MemoryKind,
    pub content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding: Option<Vec<f32>>,
    #[serde(default)]
    pub metadata: HashMap<String, serde_json::Value>,
    #[serde(default)]
    pub importance: f32,
    #[serde(default)]
    pub access_count: u64,
    pub created_at: u64,
    pub last_accessed: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<u64>,
}

impl MemoryRecord {
    pub fn is_expired(&self, now_ms: u64) -> bool {
        self.expires_at.is_some_and(|t| t <= now_ms)
    }
}

#[derive(Debug, Default)]
pub struct MemoryRecordBuilder {
    id: Option<RecordId>,
    tenant_id: Option<String>,
    agent_id: Option<String>,
    session_id: Option<SessionId>,
    parent_id: Option<RecordId>,
    kind: Option<MemoryKind>,
    content: String,
    embedding: Option<Vec<f32>>,
    metadata: HashMap<String, serde_json::Value>,
    importance: f32,
    created_at: Option<u64>,
    expires_at: Option<u64>,
}

impl MemoryRecordBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn id(mut self, id: impl Into<RecordId>) -> Self {
        self.id = Some(id.into());
        self
    }
    pub fn tenant(mut self, t: impl Into<String>) -> Self {
        self.tenant_id = Some(t.into());
        self
    }
    pub fn agent(mut self, a: impl Into<String>) -> Self {
        self.agent_id = Some(a.into());
        self
    }
    pub fn session(mut self, s: impl Into<SessionId>) -> Self {
        self.session_id = Some(s.into());
        self
    }
    pub fn parent(mut self, p: impl Into<RecordId>) -> Self {
        self.parent_id = Some(p.into());
        self
    }
    pub fn kind(mut self, k: MemoryKind) -> Self {
        self.kind = Some(k);
        self
    }
    pub fn content(mut self, c: impl Into<String>) -> Self {
        self.content = c.into();
        self
    }
    pub fn embedding(mut self, e: Vec<f32>) -> Self {
        self.embedding = Some(e);
        self
    }
    pub fn metadata(mut self, k: impl Into<String>, v: serde_json::Value) -> Self {
        self.metadata.insert(k.into(), v);
        self
    }
    pub fn importance(mut self, i: f32) -> Self {
        self.importance = i.clamp(0.0, 1.0);
        self
    }
    pub fn created_at(mut self, ts: u64) -> Self {
        self.created_at = Some(ts);
        self
    }
    pub fn expires_at(mut self, ts: u64) -> Self {
        self.expires_at = Some(ts);
        self
    }

    pub fn build(self) -> Result<MemoryRecord, BuildError> {
        let id = self.id.ok_or(BuildError::MissingField("id"))?;
        let tenant_id = self
            .tenant_id
            .ok_or(BuildError::MissingField("tenant_id"))?;
        let agent_id = self.agent_id.ok_or(BuildError::MissingField("agent_id"))?;
        let kind = self.kind.ok_or(BuildError::MissingField("kind"))?;
        let created_at = self
            .created_at
            .ok_or(BuildError::MissingField("created_at"))?;
        Ok(MemoryRecord {
            version: 1,
            id,
            parent_id: self.parent_id,
            tenant_id,
            agent_id,
            session_id: self.session_id,
            kind,
            content: self.content,
            embedding: self.embedding,
            metadata: self.metadata,
            importance: self.importance,
            access_count: 0,
            created_at,
            last_accessed: created_at,
            expires_at: self.expires_at,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("missing required field: {0}")]
    MissingField(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_roundtrip_is_stable() {
        let record = MemoryRecordBuilder::new()
            .id("rec-1")
            .tenant("tenant-a")
            .agent("agent-x")
            .session("sess-1")
            .kind(MemoryKind::Episodic)
            .content("the user prefers tabs")
            .importance(0.7)
            .created_at(1_700_000_000_000)
            .metadata("source", serde_json::json!("chat"))
            .build()
            .expect("build");
        let json = serde_json::to_string(&record).unwrap();
        let back: MemoryRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, "rec-1");
        assert_eq!(back.kind, MemoryKind::Episodic);
        assert_eq!(back.version, 1);
    }

    #[test]
    fn missing_required_field_errors() {
        let err = MemoryRecordBuilder::new()
            .id("rec-1")
            .build()
            .expect_err("should fail");
        assert!(matches!(err, BuildError::MissingField("tenant_id")));
    }

    #[test]
    fn expiration_check() {
        let mut rec = MemoryRecordBuilder::new()
            .id("rec-2")
            .tenant("t")
            .agent("a")
            .kind(MemoryKind::Working)
            .created_at(1_000)
            .expires_at(2_000)
            .build()
            .unwrap();
        assert!(!rec.is_expired(1_999));
        assert!(rec.is_expired(2_000));
        rec.expires_at = None;
        assert!(!rec.is_expired(u64::MAX));
    }

    #[test]
    fn forward_compatible_unknown_field() {
        let json = r#"{
            "version": 1, "id": "r", "tenant_id": "t", "agent_id": "a",
            "kind": "semantic", "content": "hi", "importance": 0.5,
            "access_count": 0, "created_at": 1, "last_accessed": 1,
            "future_field": 42
        }"#;
        let rec: MemoryRecord = serde_json::from_str(json).unwrap();
        assert_eq!(rec.id, "r");
    }
}
