//! Cross-agent memory sharing.
//!
//! Enables agents to share memories with configurable visibility and
//! permission-based access control.

use std::collections::HashMap;
use std::time::SystemTime;

use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use super::MemoryEntry;

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

/// Visibility level for a shared memory.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Visibility {
    /// Only the owning agent can access.
    Private,
    /// Only agents in the specified list can access.
    Team(Vec<String>),
    /// Any agent can access.
    Public,
}

/// A memory that has been shared by an agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharedMemory {
    /// Unique identifier for this shared memory.
    pub id: String,
    /// The underlying memory entry.
    pub entry: MemoryEntry,
    /// Agent that shared this memory.
    pub shared_by: String,
    /// When the memory was shared.
    pub shared_at: SystemTime,
    /// Who can access this memory.
    pub visibility: Visibility,
    /// Number of times this shared memory has been accessed.
    pub access_count: u64,
}

/// Permission granted to an agent for memory sharing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharingPermission {
    /// The agent this permission applies to.
    pub agent_id: String,
    /// Whether the agent can read shared memories.
    pub can_read: bool,
    /// Whether the agent can write/share memories.
    pub can_write: bool,
    /// Whether the agent can re-share memories.
    pub can_share: bool,
}

/// Errors from memory sharing operations.
#[derive(Debug, Clone, thiserror::Error)]
pub enum SharingError {
    #[error("shared memory not found: {0}")]
    NotFound(String),

    #[error("permission denied for agent: {0}")]
    PermissionDenied(String),

    #[error("memory already shared: {0}")]
    AlreadyShared(String),
}

// ---------------------------------------------------------------------------
// MemorySharingHub
// ---------------------------------------------------------------------------

/// Central hub for cross-agent memory sharing.
pub struct MemorySharingHub {
    shared_memories: DashMap<String, SharedMemory>,
    agent_permissions: RwLock<HashMap<String, Vec<SharingPermission>>>,
}

impl MemorySharingHub {
    /// Creates a new memory sharing hub.
    pub fn new() -> Self {
        Self {
            shared_memories: DashMap::new(),
            agent_permissions: RwLock::new(HashMap::new()),
        }
    }

    /// Shares a memory entry with the specified visibility. Returns the shared memory ID.
    pub fn share_memory(
        &self,
        from_agent: &str,
        memory: MemoryEntry,
        visibility: Visibility,
    ) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        let shared = SharedMemory {
            id: id.clone(),
            entry: memory,
            shared_by: from_agent.to_string(),
            shared_at: SystemTime::now(),
            visibility,
            access_count: 0,
        };

        self.shared_memories.insert(id.clone(), shared);
        id
    }

    /// Accesses a shared memory, checking visibility and permissions.
    pub fn access_shared(
        &self,
        agent_id: &str,
        memory_id: &str,
    ) -> Result<MemoryEntry, SharingError> {
        let mut shared = self
            .shared_memories
            .get_mut(memory_id)
            .ok_or_else(|| SharingError::NotFound(memory_id.to_string()))?;

        // Check visibility
        match &shared.visibility {
            Visibility::Public => {}
            Visibility::Private => {
                if shared.shared_by != agent_id {
                    return Err(SharingError::PermissionDenied(agent_id.to_string()));
                }
            }
            Visibility::Team(members) => {
                if shared.shared_by != agent_id && !members.contains(&agent_id.to_string()) {
                    return Err(SharingError::PermissionDenied(agent_id.to_string()));
                }
            }
        }

        // Check explicit permissions
        let perms = self.agent_permissions.read();
        if let Some(agent_perms) = perms.get(agent_id) {
            let has_read = agent_perms
                .iter()
                .any(|p| p.agent_id == agent_id && p.can_read);
            // If permissions exist but none grant read, deny unless public or owner
            if !has_read && shared.visibility != Visibility::Public && shared.shared_by != agent_id
            {
                return Err(SharingError::PermissionDenied(agent_id.to_string()));
            }
        }

        shared.access_count += 1;
        Ok(shared.entry.clone())
    }

    /// Lists all shared memories visible to the given agent.
    pub fn list_shared(&self, agent_id: &str) -> Vec<SharedMemory> {
        self.shared_memories
            .iter()
            .filter(|entry| {
                let shared = entry.value();
                match &shared.visibility {
                    Visibility::Public => true,
                    Visibility::Private => shared.shared_by == agent_id,
                    Visibility::Team(members) => {
                        shared.shared_by == agent_id || members.contains(&agent_id.to_string())
                    }
                }
            })
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Revokes sharing of a memory. Returns `true` if found and removed.
    pub fn revoke_sharing(&self, memory_id: &str) -> bool {
        self.shared_memories.remove(memory_id).is_some()
    }

    /// Grants a permission to an agent.
    pub fn grant_permission(&self, agent_id: &str, permission: SharingPermission) {
        let mut perms = self.agent_permissions.write();
        perms
            .entry(agent_id.to_string())
            .or_default()
            .push(permission);
    }
}

impl Default for MemorySharingHub {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(content: &str) -> MemoryEntry {
        MemoryEntry {
            content: content.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn test_share_and_access_public() {
        let hub = MemorySharingHub::new();

        let id = hub.share_memory("agent-1", make_entry("shared fact"), Visibility::Public);

        let entry = hub.access_shared("agent-2", &id).unwrap();
        assert_eq!(entry.content, "shared fact");
    }

    #[test]
    fn test_private_visibility() {
        let hub = MemorySharingHub::new();

        let id = hub.share_memory("agent-1", make_entry("secret"), Visibility::Private);

        // Owner can access
        assert!(hub.access_shared("agent-1", &id).is_ok());

        // Others cannot
        let result = hub.access_shared("agent-2", &id);
        assert!(matches!(result, Err(SharingError::PermissionDenied(_))));
    }

    #[test]
    fn test_team_visibility() {
        let hub = MemorySharingHub::new();

        let id = hub.share_memory(
            "agent-1",
            make_entry("team fact"),
            Visibility::Team(vec!["agent-2".to_string(), "agent-3".to_string()]),
        );

        // Team members can access
        assert!(hub.access_shared("agent-2", &id).is_ok());
        assert!(hub.access_shared("agent-3", &id).is_ok());

        // Non-members cannot
        let result = hub.access_shared("agent-4", &id);
        assert!(matches!(result, Err(SharingError::PermissionDenied(_))));
    }

    #[test]
    fn test_list_shared() {
        let hub = MemorySharingHub::new();

        hub.share_memory("agent-1", make_entry("public"), Visibility::Public);
        hub.share_memory("agent-1", make_entry("private"), Visibility::Private);
        hub.share_memory(
            "agent-1",
            make_entry("team"),
            Visibility::Team(vec!["agent-2".to_string()]),
        );

        // agent-1 (owner) sees all 3
        let agent1_list = hub.list_shared("agent-1");
        assert_eq!(agent1_list.len(), 3);

        // agent-2 sees public + team = 2
        let agent2_list = hub.list_shared("agent-2");
        assert_eq!(agent2_list.len(), 2);

        // agent-3 sees only public = 1
        let agent3_list = hub.list_shared("agent-3");
        assert_eq!(agent3_list.len(), 1);
    }

    #[test]
    fn test_revoke_sharing() {
        let hub = MemorySharingHub::new();

        let id = hub.share_memory("agent-1", make_entry("will be revoked"), Visibility::Public);
        assert!(hub.revoke_sharing(&id));
        assert!(!hub.revoke_sharing(&id)); // Already revoked

        let result = hub.access_shared("agent-2", &id);
        assert!(matches!(result, Err(SharingError::NotFound(_))));
    }

    #[test]
    fn test_grant_permission() {
        let hub = MemorySharingHub::new();

        hub.grant_permission(
            "agent-2",
            SharingPermission {
                agent_id: "agent-2".to_string(),
                can_read: true,
                can_write: false,
                can_share: false,
            },
        );

        let id = hub.share_memory(
            "agent-1",
            make_entry("team secret"),
            Visibility::Team(vec!["agent-2".to_string()]),
        );

        // agent-2 has read permission and is in the team
        let result = hub.access_shared("agent-2", &id);
        assert!(result.is_ok());
    }

    #[test]
    fn test_access_count_increments() {
        let hub = MemorySharingHub::new();

        let id = hub.share_memory("agent-1", make_entry("tracked"), Visibility::Public);

        hub.access_shared("agent-2", &id).unwrap();
        hub.access_shared("agent-3", &id).unwrap();

        let shared = hub.shared_memories.get(&id).unwrap();
        assert_eq!(shared.access_count, 2);
    }

    #[test]
    fn test_not_found() {
        let hub = MemorySharingHub::new();
        let result = hub.access_shared("agent-1", "nonexistent-id");
        assert!(matches!(result, Err(SharingError::NotFound(_))));
    }

    #[test]
    fn test_default_trait() {
        let hub = MemorySharingHub::default();
        assert!(hub.list_shared("any").is_empty());
    }
}
