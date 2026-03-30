//! Shared input validation limits and error codes for all moonshot command families.
//!
//! Centralizes key/value/name length limits, structured error codes (6000–6009),
//! and validation helpers that return `Result<(), Frame>` for consistent
//! error responses across all moonshot handlers.

use crate::protocol::Frame;

use super::err_frame;

// ─── Constants ───────────────────────────────────────────────────────────

/// Maximum length of a key argument in bytes.
pub const MAX_KEY_LEN: usize = 1024;

/// Maximum length of a value argument in bytes (1 MiB).
pub const MAX_VALUE_LEN: usize = 1024 * 1024;

/// Maximum length of a module name for Forge.
pub const MAX_MODULE_NAME_LEN: usize = 256;

/// Maximum number of arguments for any moonshot command.
pub const MAX_ARGS: usize = 64;

/// Maximum length of a branch name for Chronicle.
pub const MAX_BRANCH_NAME_LEN: usize = 256;

/// Maximum length of a tenant ID.
pub const MAX_TENANT_LEN: usize = 128;

/// Maximum length of an agent ID for Mnemo.
pub const MAX_AGENT_ID_LEN: usize = 256;

/// Maximum metadata JSON size for Mnemo (16 KiB).
pub const MAX_META_LEN: usize = 16 * 1024;

// ─── Error codes (6000–6009) ─────────────────────────────────────────────

pub const ERR_MOONSHOT_DISABLED: u16 = 6000;
pub const ERR_MOONSHOT_QUOTA: u16 = 6001;
pub const ERR_MOONSHOT_RATE_LIMIT: u16 = 6002;
pub const ERR_MOONSHOT_MODULE_NOT_FOUND: u16 = 6003;
pub const ERR_MOONSHOT_SIGNING_REQUIRED: u16 = 6004;
pub const ERR_MOONSHOT_BRANCH_NOT_FOUND: u16 = 6005;
pub const ERR_MOONSHOT_TENANT_MISMATCH: u16 = 6006;
pub const ERR_MOONSHOT_KEY_TOO_LONG: u16 = 6007;
pub const ERR_MOONSHOT_VALUE_TOO_LONG: u16 = 6008;
pub const ERR_MOONSHOT_INVALID_ARGS: u16 = 6009;

// ─── Validation helpers ──────────────────────────────────────────────────

/// Validate a key argument (non-empty, within length limit).
pub fn validate_key(key: &str) -> Result<(), Frame> {
    if key.is_empty() {
        return Err(err_frame(&format!(
            "MOONSHOT-{ERR_MOONSHOT_KEY_TOO_LONG} key must not be empty"
        )));
    }
    if key.len() > MAX_KEY_LEN {
        return Err(err_frame(&format!(
            "MOONSHOT-{ERR_MOONSHOT_KEY_TOO_LONG} key exceeds maximum length ({MAX_KEY_LEN} bytes)"
        )));
    }
    Ok(())
}

/// Validate a value argument (within length limit).
pub fn validate_value(value: &str) -> Result<(), Frame> {
    if value.len() > MAX_VALUE_LEN {
        return Err(err_frame(&format!(
            "MOONSHOT-{ERR_MOONSHOT_VALUE_TOO_LONG} value exceeds maximum length ({MAX_VALUE_LEN} bytes)"
        )));
    }
    Ok(())
}

/// Validate a Forge module name.
pub fn validate_module_name(name: &str) -> Result<(), Frame> {
    if name.is_empty() {
        return Err(err_frame(&format!(
            "MOONSHOT-{ERR_MOONSHOT_INVALID_ARGS} module name must not be empty"
        )));
    }
    if name.len() > MAX_MODULE_NAME_LEN {
        return Err(err_frame(&format!(
            "MOONSHOT-{ERR_MOONSHOT_INVALID_ARGS} module name exceeds maximum length ({MAX_MODULE_NAME_LEN} bytes)"
        )));
    }
    Ok(())
}

/// Validate a Chronicle branch name.
pub fn validate_branch_name(name: &str) -> Result<(), Frame> {
    if name.is_empty() {
        return Err(err_frame(&format!(
            "MOONSHOT-{ERR_MOONSHOT_INVALID_ARGS} branch name must not be empty"
        )));
    }
    if name.len() > MAX_BRANCH_NAME_LEN {
        return Err(err_frame(&format!(
            "MOONSHOT-{ERR_MOONSHOT_INVALID_ARGS} branch name exceeds maximum length ({MAX_BRANCH_NAME_LEN} bytes)"
        )));
    }
    Ok(())
}

/// Validate a tenant ID.
pub fn validate_tenant(tenant: &str) -> Result<(), Frame> {
    if tenant.is_empty() {
        return Err(err_frame(&format!(
            "MOONSHOT-{ERR_MOONSHOT_TENANT_MISMATCH} tenant ID must not be empty"
        )));
    }
    if tenant.len() > MAX_TENANT_LEN {
        return Err(err_frame(&format!(
            "MOONSHOT-{ERR_MOONSHOT_TENANT_MISMATCH} tenant ID exceeds maximum length ({MAX_TENANT_LEN} bytes)"
        )));
    }
    Ok(())
}

/// Validate a Mnemo agent ID.
pub fn validate_agent_id(agent: &str) -> Result<(), Frame> {
    if agent.is_empty() {
        return Err(err_frame(&format!(
            "MOONSHOT-{ERR_MOONSHOT_INVALID_ARGS} agent ID must not be empty"
        )));
    }
    if agent.len() > MAX_AGENT_ID_LEN {
        return Err(err_frame(&format!(
            "MOONSHOT-{ERR_MOONSHOT_INVALID_ARGS} agent ID exceeds maximum length ({MAX_AGENT_ID_LEN} bytes)"
        )));
    }
    Ok(())
}

/// Validate Mnemo metadata JSON size.
pub fn validate_meta(meta: &str) -> Result<(), Frame> {
    if meta.len() > MAX_META_LEN {
        return Err(err_frame(&format!(
            "MOONSHOT-{ERR_MOONSHOT_INVALID_ARGS} metadata exceeds maximum size ({MAX_META_LEN} bytes)"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_key_accepts_normal() {
        assert!(validate_key("normal-key").is_ok());
    }

    #[test]
    fn validate_key_rejects_empty() {
        assert!(validate_key("").is_err());
    }

    #[test]
    fn validate_key_rejects_oversized() {
        let big = "x".repeat(MAX_KEY_LEN + 1);
        assert!(validate_key(&big).is_err());
    }

    #[test]
    fn validate_value_accepts_normal() {
        assert!(validate_value("hello").is_ok());
    }

    #[test]
    fn validate_value_rejects_oversized() {
        let big = "x".repeat(MAX_VALUE_LEN + 1);
        assert!(validate_value(&big).is_err());
    }

    #[test]
    fn validate_module_name_accepts_normal() {
        assert!(validate_module_name("my-module").is_ok());
    }

    #[test]
    fn validate_module_name_rejects_empty() {
        assert!(validate_module_name("").is_err());
    }

    #[test]
    fn validate_module_name_rejects_oversized() {
        let big = "x".repeat(MAX_MODULE_NAME_LEN + 1);
        assert!(validate_module_name(&big).is_err());
    }

    #[test]
    fn validate_branch_name_accepts_normal() {
        assert!(validate_branch_name("feature-branch").is_ok());
    }

    #[test]
    fn validate_tenant_accepts_normal() {
        assert!(validate_tenant("tenant-1").is_ok());
    }

    #[test]
    fn validate_agent_id_accepts_normal() {
        assert!(validate_agent_id("agent-1").is_ok());
    }

    #[test]
    fn validate_meta_accepts_normal() {
        assert!(validate_meta(r#"{"key":"value"}"#).is_ok());
    }

    #[test]
    fn validate_meta_rejects_oversized() {
        let big = "x".repeat(MAX_META_LEN + 1);
        assert!(validate_meta(&big).is_err());
    }

    #[test]
    fn error_codes_are_in_range() {
        assert!(ERR_MOONSHOT_DISABLED >= 6000);
        assert!(ERR_MOONSHOT_INVALID_ARGS <= 6009);
    }
}
