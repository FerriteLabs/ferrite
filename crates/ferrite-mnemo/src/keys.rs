//! Storage key layout for Mnemo records (ADR-018 §Tenancy & isolation).
//!
//! All Mnemo state lives under the `__ferrite:mnemo:` reserved prefix per
//! the handler-state convention established by ADR-016.  Keys are deterministic
//! and tenant-prefixed so a missing or wrong tenant ID can never cross the
//! isolation boundary.

pub const KEY_PREFIX: &str = "__ferrite:mnemo";

#[derive(Debug, Clone)]
pub struct KeyParts<'a> {
    pub tenant_id: &'a str,
    pub agent_id: &'a str,
    pub session_id: Option<&'a str>,
    pub record_id: &'a str,
}

/// `__ferrite:mnemo:r:<tenant>:<agent>:<session|_>:<id>`
pub fn key_for_record(parts: &KeyParts<'_>) -> String {
    let session = parts.session_id.unwrap_or("_");
    format!(
        "{KEY_PREFIX}:r:{}:{}:{}:{}",
        parts.tenant_id, parts.agent_id, session, parts.record_id,
    )
}

pub fn key_for_session(tenant_id: &str, agent_id: &str, session_id: &str) -> String {
    format!("{KEY_PREFIX}:s:{tenant_id}:{agent_id}:{session_id}")
}

pub fn key_prefix_for_agent(tenant_id: &str, agent_id: &str) -> String {
    format!("{KEY_PREFIX}:r:{tenant_id}:{agent_id}:")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_key_with_session() {
        let k = key_for_record(&KeyParts {
            tenant_id: "acme",
            agent_id: "bot1",
            session_id: Some("conv42"),
            record_id: "abc",
        });
        assert_eq!(k, "__ferrite:mnemo:r:acme:bot1:conv42:abc");
    }

    #[test]
    fn record_key_without_session_uses_underscore() {
        let k = key_for_record(&KeyParts {
            tenant_id: "acme",
            agent_id: "bot1",
            session_id: None,
            record_id: "abc",
        });
        assert_eq!(k, "__ferrite:mnemo:r:acme:bot1:_:abc");
    }

    #[test]
    fn agent_prefix_is_tenant_safe() {
        let p = key_prefix_for_agent("acme", "bot1");
        let k = key_for_record(&KeyParts {
            tenant_id: "acme",
            agent_id: "bot1",
            session_id: Some("s"),
            record_id: "r",
        });
        assert!(k.starts_with(&p));
        let other = key_for_record(&KeyParts {
            tenant_id: "evil",
            agent_id: "bot1",
            session_id: Some("s"),
            record_id: "r",
        });
        assert!(!other.starts_with(&p));
    }
}
