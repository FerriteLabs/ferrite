//! User management for ACL
//!
//! This module defines the User struct and related permission types.

#[cfg(feature = "crypto")]
use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// User account for ACL
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    /// Username
    username: String,
    /// Hashed password (None means no password required, or NOPASS)
    password_hash: Option<String>,
    /// Whether the user is enabled
    enabled: bool,
    /// Command permissions
    commands: CommandPermission,
    /// Key patterns the user can access
    keys: Vec<KeyPattern>,
    /// Pub/Sub channel patterns the user can access
    channels: Vec<String>,
    /// User flags
    flags: UserFlags,
}

/// User flags
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UserFlags {
    /// User can run all commands
    pub allcommands: bool,
    /// User can access all keys
    pub allkeys: bool,
    /// User can access all channels
    pub allchannels: bool,
    /// No password required
    pub nopass: bool,
    /// Reset user to default state
    pub reset: bool,
}

/// Command permission settings
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CommandPermission {
    /// Allowed commands (if not using allcommands)
    allowed: HashSet<String>,
    /// Denied commands (overrides allowed)
    denied: HashSet<String>,
    /// Allowed command categories
    categories: HashSet<String>,
    /// Denied command categories
    denied_categories: HashSet<String>,
}

impl CommandPermission {
    /// Create new command permissions
    pub fn new() -> Self {
        Self::default()
    }

    /// Allow a specific command
    pub fn allow_command(&mut self, command: &str) {
        self.allowed.insert(command.to_uppercase());
        self.denied.remove(&command.to_uppercase());
    }

    /// Deny a specific command
    pub fn deny_command(&mut self, command: &str) {
        self.denied.insert(command.to_uppercase());
        self.allowed.remove(&command.to_uppercase());
    }

    /// Allow a command category
    pub fn allow_category(&mut self, category: &str) {
        self.categories.insert(category.to_lowercase());
        self.denied_categories.remove(&category.to_lowercase());
    }

    /// Deny a command category
    pub fn deny_category(&mut self, category: &str) {
        self.denied_categories.insert(category.to_lowercase());
        self.categories.remove(&category.to_lowercase());
    }

    /// Check if a command is allowed
    pub fn is_command_allowed(&self, command: &str, category: Option<&str>) -> bool {
        let cmd_upper = command.to_uppercase();

        // Explicitly denied commands are never allowed
        if self.denied.contains(&cmd_upper) {
            return false;
        }

        // Check if category is denied
        if let Some(cat) = category {
            if self.denied_categories.contains(&cat.to_lowercase()) {
                return false;
            }
        }

        // Check if explicitly allowed
        if self.allowed.contains(&cmd_upper) {
            return true;
        }

        // Check if category is allowed
        if let Some(cat) = category {
            if self.categories.contains(&cat.to_lowercase()) {
                return true;
            }
        }

        false
    }

    /// Reset permissions to empty
    pub fn reset(&mut self) {
        self.allowed.clear();
        self.denied.clear();
        self.categories.clear();
        self.denied_categories.clear();
    }
}

/// Key pattern for access control
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KeyPattern {
    /// Match all keys
    All,
    /// Exact key match
    Exact(String),
    /// Prefix match (e.g., "user:*")
    Prefix(String),
    /// Pattern match with glob-style wildcards
    Pattern(String),
    /// Read-only access to pattern
    ReadOnly(Box<KeyPattern>),
    /// Write-only access to pattern
    WriteOnly(Box<KeyPattern>),
}

impl KeyPattern {
    /// Parse a key pattern from string
    pub fn parse(pattern: &str) -> Self {
        // Handle read/write modifiers
        if let Some(inner) = pattern.strip_prefix("%R~") {
            return KeyPattern::ReadOnly(Box::new(KeyPattern::parse(inner)));
        }
        if let Some(inner) = pattern.strip_prefix("%W~") {
            return KeyPattern::WriteOnly(Box::new(KeyPattern::parse(inner)));
        }
        if let Some(inner) = pattern.strip_prefix("%RW~") {
            return KeyPattern::parse(inner);
        }
        if let Some(inner) = pattern.strip_prefix("~") {
            return KeyPattern::parse(inner);
        }

        if pattern == "*" {
            KeyPattern::All
        } else if pattern.contains('*') || pattern.contains('?') {
            if pattern.ends_with('*') && !pattern[..pattern.len() - 1].contains('*') {
                KeyPattern::Prefix(pattern[..pattern.len() - 1].to_string())
            } else {
                KeyPattern::Pattern(pattern.to_string())
            }
        } else {
            KeyPattern::Exact(pattern.to_string())
        }
    }

    /// Check if a key matches this pattern
    pub fn matches(&self, key: &str) -> bool {
        match self {
            KeyPattern::All => true,
            KeyPattern::Exact(exact) => key == exact,
            KeyPattern::Prefix(prefix) => key.starts_with(prefix),
            KeyPattern::Pattern(pattern) => Self::glob_match(pattern, key),
            KeyPattern::ReadOnly(inner) => inner.matches(key),
            KeyPattern::WriteOnly(inner) => inner.matches(key),
        }
    }

    /// Check if this pattern allows read access
    pub fn allows_read(&self) -> bool {
        !matches!(self, KeyPattern::WriteOnly(_))
    }

    /// Check if this pattern allows write access
    pub fn allows_write(&self) -> bool {
        !matches!(self, KeyPattern::ReadOnly(_))
    }

    /// Simple glob matching (supports * and ?)
    fn glob_match(pattern: &str, text: &str) -> bool {
        let mut p_chars = pattern.chars().peekable();
        let mut t_chars = text.chars().peekable();

        while p_chars.peek().is_some() || t_chars.peek().is_some() {
            match (p_chars.peek(), t_chars.peek()) {
                (Some('*'), _) => {
                    p_chars.next();
                    if p_chars.peek().is_none() {
                        return true; // * at end matches everything
                    }
                    // Try matching zero or more characters
                    let remaining_pattern: String = p_chars.collect();
                    let remaining_text: String = t_chars.collect();
                    for i in 0..=remaining_text.len() {
                        if Self::glob_match(&remaining_pattern, &remaining_text[i..]) {
                            return true;
                        }
                    }
                    return false;
                }
                (Some('?'), Some(_)) => {
                    p_chars.next();
                    t_chars.next();
                }
                (Some(pc), Some(tc)) if *pc == *tc => {
                    p_chars.next();
                    t_chars.next();
                }
                (None, None) => return true,
                _ => return false,
            }
        }
        true
    }
}

/// Permission type for checking access
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Permission {
    /// Read permission
    Read,
    /// Write permission
    Write,
    /// Both read and write
    ReadWrite,
}

impl User {
    /// Create a new user with default settings (disabled, no permissions)
    pub fn new(username: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password_hash: None,
            enabled: false,
            commands: CommandPermission::new(),
            keys: Vec::new(),
            channels: Vec::new(),
            flags: UserFlags::default(),
        }
    }

    /// Create the default user (usually "default")
    pub fn default_user() -> Self {
        let mut user = Self::new("default");
        user.enabled = true;
        user.flags.nopass = true;
        user.flags.allkeys = true;
        user.flags.allcommands = true;
        user.flags.allchannels = true;
        user
    }

    /// Get the username
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Check if the user is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Enable the user
    pub fn enable(&mut self) {
        self.enabled = true;
    }

    /// Disable the user
    pub fn disable(&mut self) {
        self.enabled = false;
    }

    /// Set the password (stores hash)
    pub fn set_password(&mut self, password: &str) {
        self.password_hash = Some(Self::hash_password(password));
        self.flags.nopass = false;
    }

    /// Remove password requirement (NOPASS)
    pub fn set_nopass(&mut self) {
        self.password_hash = None;
        self.flags.nopass = true;
    }

    /// Check if a password is correct
    pub fn check_password(&self, password: &str) -> bool {
        if self.flags.nopass {
            return true;
        }
        match &self.password_hash {
            Some(hash) => Self::verify_password(password, hash),
            None => false,
        }
    }

    /// Verify a password against a stored hash
    ///
    /// Supports both legacy (DefaultHasher) and modern (argon2) hash formats.
    /// Legacy hashes are 16 hex characters, argon2 hashes start with "$argon2".
    fn verify_password(password: &str, hash: &str) -> bool {
        #[cfg(feature = "crypto")]
        if hash.starts_with("$argon2") {
            // Modern argon2 verification
            match PasswordHash::new(hash) {
                Ok(parsed_hash) => {
                    return Argon2::default()
                        .verify_password(password.as_bytes(), &parsed_hash)
                        .is_ok();
                }
                Err(_) => return false,
            }
        }
        // Legacy DefaultHasher format (for backwards compatibility)
        Self::legacy_hash_password(password) == hash
    }

    /// Hash a password using argon2 (or legacy hash if crypto feature is disabled)
    fn hash_password(password: &str) -> String {
        #[cfg(feature = "crypto")]
        {
            let salt = SaltString::generate(&mut OsRng);
            let argon2 = Argon2::default();

            argon2
                .hash_password(password.as_bytes(), &salt)
                .expect("Failed to hash password")
                .to_string()
        }
        #[cfg(not(feature = "crypto"))]
        {
            Self::legacy_hash_password(password)
        }
    }

    /// Legacy password hashing for backwards compatibility
    ///
    /// This should only be used for verifying old passwords, not for creating new hashes.
    fn legacy_hash_password(password: &str) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        password.hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    }

    /// Check if the password hash uses the legacy format
    ///
    /// Returns true if the user's password should be migrated to argon2.
    pub fn needs_password_migration(&self) -> bool {
        #[cfg(not(feature = "crypto"))]
        {
            return false;
        }
        #[cfg(feature = "crypto")]
        match &self.password_hash {
            Some(hash) => !hash.starts_with("$argon2"),
            None => false,
        }
    }

    /// Migrate a legacy password hash to argon2
    ///
    /// This requires the plaintext password to re-hash with argon2.
    /// Call this after successful authentication with a legacy hash.
    pub fn migrate_password(&mut self, password: &str) {
        if self.needs_password_migration() && self.check_password(password) {
            self.password_hash = Some(Self::hash_password(password));
        }
    }

    /// Check if a command is allowed
    pub fn can_execute_command(&self, command: &str, category: Option<&str>) -> bool {
        if !self.enabled {
            return false;
        }
        if self.flags.allcommands {
            return !self.commands.denied.contains(&command.to_uppercase());
        }
        self.commands.is_command_allowed(command, category)
    }

    /// Check if a key can be accessed
    pub fn can_access_key(&self, key: &str, permission: Permission) -> bool {
        if !self.enabled {
            return false;
        }
        if self.flags.allkeys {
            return true;
        }
        for pattern in &self.keys {
            if pattern.matches(key) {
                match permission {
                    Permission::Read => {
                        if pattern.allows_read() {
                            return true;
                        }
                    }
                    Permission::Write => {
                        if pattern.allows_write() {
                            return true;
                        }
                    }
                    Permission::ReadWrite => {
                        if pattern.allows_read() && pattern.allows_write() {
                            return true;
                        }
                    }
                }
            }
        }
        false
    }

    /// Check if a channel can be accessed
    pub fn can_access_channel(&self, channel: &str) -> bool {
        if !self.enabled {
            return false;
        }
        if self.flags.allchannels {
            return true;
        }
        for pattern in &self.channels {
            if KeyPattern::Pattern(pattern.clone()).matches(channel) {
                return true;
            }
        }
        false
    }

    /// Add a key pattern
    pub fn add_key_pattern(&mut self, pattern: KeyPattern) {
        self.flags.allkeys = false;
        self.keys.push(pattern);
    }

    /// Set all keys access
    pub fn set_allkeys(&mut self, value: bool) {
        self.flags.allkeys = value;
        if value {
            self.keys.clear();
        }
    }

    /// Add a channel pattern
    pub fn add_channel_pattern(&mut self, pattern: String) {
        self.flags.allchannels = false;
        self.channels.push(pattern);
    }

    /// Set all channels access
    pub fn set_allchannels(&mut self, value: bool) {
        self.flags.allchannels = value;
        if value {
            self.channels.clear();
        }
    }

    /// Allow a command
    pub fn allow_command(&mut self, command: &str) {
        self.flags.allcommands = false;
        self.commands.allow_command(command);
    }

    /// Deny a command
    pub fn deny_command(&mut self, command: &str) {
        self.commands.deny_command(command);
    }

    /// Allow a command category
    pub fn allow_category(&mut self, category: &str) {
        self.flags.allcommands = false;
        self.commands.allow_category(category);
    }

    /// Deny a command category
    pub fn deny_category(&mut self, category: &str) {
        self.commands.deny_category(category);
    }

    /// Set all commands access
    pub fn set_allcommands(&mut self, value: bool) {
        self.flags.allcommands = value;
        if value {
            self.commands.reset();
        }
    }

    /// Reset user to default state
    pub fn reset(&mut self) {
        self.password_hash = None;
        self.enabled = false;
        self.commands = CommandPermission::new();
        self.keys.clear();
        self.channels.clear();
        self.flags = UserFlags::default();
    }

    /// Get ACL string representation
    pub fn to_acl_string(&self) -> String {
        let mut parts = Vec::new();

        parts.push(format!("user {}", self.username));

        if self.enabled {
            parts.push("on".to_string());
        } else {
            parts.push("off".to_string());
        }

        if self.flags.nopass {
            parts.push("nopass".to_string());
        } else if self.password_hash.is_some() {
            parts.push("#<hash>".to_string());
        }

        if self.flags.allkeys {
            parts.push("~*".to_string());
        } else {
            for pattern in &self.keys {
                parts.push(format!("~{:?}", pattern));
            }
        }

        if self.flags.allchannels {
            parts.push("&*".to_string());
        }

        if self.flags.allcommands {
            parts.push("+@all".to_string());
        }

        parts.join(" ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_new() {
        let user = User::new("testuser");
        assert_eq!(user.username(), "testuser");
        assert!(!user.is_enabled());
    }

    #[test]
    fn test_default_user() {
        let user = User::default_user();
        assert_eq!(user.username(), "default");
        assert!(user.is_enabled());
        assert!(user.flags.nopass);
        assert!(user.flags.allkeys);
        assert!(user.flags.allcommands);
    }

    #[test]
    fn test_password() {
        let mut user = User::new("testuser");
        user.enable();

        user.set_password("secret");
        assert!(user.check_password("secret"));
        assert!(!user.check_password("wrong"));

        user.set_nopass();
        assert!(user.check_password("anything"));
    }

    #[cfg(feature = "crypto")]
    #[test]
    fn test_argon2_password_hash() {
        let mut user = User::new("testuser");
        user.enable();

        // New passwords should use argon2
        user.set_password("mysecretpassword");

        // Password hash should start with $argon2
        assert!(user.password_hash.as_ref().unwrap().starts_with("$argon2"));

        // Should verify correctly
        assert!(user.check_password("mysecretpassword"));
        assert!(!user.check_password("wrongpassword"));

        // Should not need migration (already using argon2)
        assert!(!user.needs_password_migration());
    }

    #[cfg(feature = "crypto")]
    #[test]
    fn test_legacy_password_compatibility() {
        let mut user = User::new("testuser");
        user.enable();

        // Simulate a legacy hash (16 hex chars)
        let legacy_hash = User::legacy_hash_password("legacypassword");
        user.password_hash = Some(legacy_hash.clone());
        user.flags.nopass = false;

        // Should still verify with legacy format
        assert!(user.check_password("legacypassword"));
        assert!(!user.check_password("wrongpassword"));

        // Should indicate migration is needed
        assert!(user.needs_password_migration());
    }

    #[cfg(feature = "crypto")]
    #[test]
    fn test_password_migration() {
        let mut user = User::new("testuser");
        user.enable();

        // Set up with legacy hash
        let legacy_hash = User::legacy_hash_password("migratepassword");
        user.password_hash = Some(legacy_hash.clone());
        user.flags.nopass = false;

        assert!(user.needs_password_migration());

        // Migrate the password
        user.migrate_password("migratepassword");

        // Should now use argon2
        assert!(!user.needs_password_migration());
        assert!(user.password_hash.as_ref().unwrap().starts_with("$argon2"));

        // Password should still work
        assert!(user.check_password("migratepassword"));
    }

    #[cfg(feature = "crypto")]
    #[test]
    fn test_argon2_different_salts() {
        // Each hash should be unique even for the same password
        let hash1 = User::hash_password("samepassword");
        let hash2 = User::hash_password("samepassword");

        // Hashes should be different (different salts)
        assert_ne!(hash1, hash2);

        // But both should verify correctly
        assert!(User::verify_password("samepassword", &hash1));
        assert!(User::verify_password("samepassword", &hash2));
    }

    #[test]
    fn test_command_permission() {
        let mut user = User::new("testuser");
        user.enable();

        // User with no commands allowed
        assert!(!user.can_execute_command("GET", None));

        // Allow specific command
        user.allow_command("GET");
        assert!(user.can_execute_command("GET", None));
        assert!(!user.can_execute_command("SET", None));

        // Deny overrides allow
        user.deny_command("GET");
        assert!(!user.can_execute_command("GET", None));
    }

    #[test]
    fn test_key_access() {
        let mut user = User::new("testuser");
        user.enable();

        // No key access
        assert!(!user.can_access_key("foo", Permission::Read));

        // All keys access
        user.set_allkeys(true);
        assert!(user.can_access_key("any:key", Permission::ReadWrite));

        // Specific pattern
        user.set_allkeys(false);
        user.add_key_pattern(KeyPattern::Prefix("user:".to_string()));
        assert!(user.can_access_key("user:123", Permission::Read));
        assert!(!user.can_access_key("admin:123", Permission::Read));
    }

    #[test]
    fn test_key_pattern_parse() {
        // All keys
        assert!(matches!(KeyPattern::parse("*"), KeyPattern::All));

        // Exact match
        assert!(matches!(KeyPattern::parse("foo"), KeyPattern::Exact(_)));

        // Prefix
        let pattern = KeyPattern::parse("user:*");
        assert!(pattern.matches("user:123"));
        assert!(!pattern.matches("admin:123"));
    }

    #[test]
    fn test_glob_match() {
        assert!(KeyPattern::glob_match("foo*", "foobar"));
        assert!(KeyPattern::glob_match("*bar", "foobar"));
        assert!(KeyPattern::glob_match("foo?ar", "foobar"));
        assert!(KeyPattern::glob_match("*", "anything"));
        assert!(!KeyPattern::glob_match("foo", "bar"));
    }

    #[test]
    fn test_category_permission() {
        let mut perm = CommandPermission::new();
        perm.allow_category("read");

        assert!(perm.is_command_allowed("GET", Some("read")));
        assert!(!perm.is_command_allowed("SET", Some("write")));

        // Deny category overrides
        perm.deny_category("read");
        assert!(!perm.is_command_allowed("GET", Some("read")));
    }

    #[test]
    fn test_reset() {
        let mut user = User::default_user();
        user.reset();

        assert!(!user.is_enabled());
        assert!(!user.flags.allkeys);
        assert!(!user.flags.allcommands);
        assert!(!user.flags.nopass);
    }
}
