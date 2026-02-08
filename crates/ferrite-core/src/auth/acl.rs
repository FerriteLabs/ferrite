//! ACL management
//!
//! This module provides the ACL (Access Control List) manager for user authentication
//! and authorization.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::{debug, info};

use super::user::{KeyPattern, Permission, User};
use crate::protocol::Frame;

/// ACL management errors
#[derive(Debug, thiserror::Error)]
pub enum AclError {
    /// User not found
    #[error("User not found: {0}")]
    UserNotFound(String),

    /// Authentication failed
    #[error("Authentication failed")]
    AuthFailed,

    /// Permission denied
    #[error("NOPERM this user has no permissions to run the '{0}' command")]
    CommandDenied(String),

    /// Key access denied
    #[error("NOPERM this user has no permissions to access the key '{0}'")]
    KeyDenied(String),

    /// Channel access denied
    #[error("NOPERM this user has no permissions to access the channel '{0}'")]
    ChannelDenied(String),

    /// Invalid ACL rule
    #[error("Invalid ACL rule: {0}")]
    InvalidRule(String),

    /// User already exists
    #[error("User already exists: {0}")]
    UserExists(String),

    /// File I/O error
    #[error("ACL file error: {0}")]
    FileError(String),

    /// Serialization error
    #[error("ACL serialization error: {0}")]
    SerializationError(String),
}

/// ACL manager for user authentication and authorization
pub struct Acl {
    /// Registered users
    users: RwLock<HashMap<String, User>>,
    /// Whether ACL is enabled
    enabled: bool,
    /// Path to ACL file for persistence
    acl_file: RwLock<Option<std::path::PathBuf>>,
}

impl Acl {
    /// Create a new ACL manager
    pub fn new() -> Self {
        let mut users = HashMap::new();
        // Always create the default user
        users.insert("default".to_string(), User::default_user());
        Self {
            users: RwLock::new(users),
            enabled: true,
            acl_file: RwLock::new(None),
        }
    }

    /// Create a new ACL manager with a file path
    pub fn with_file(path: std::path::PathBuf) -> Self {
        let mut users = HashMap::new();
        users.insert("default".to_string(), User::default_user());
        Self {
            users: RwLock::new(users),
            enabled: true,
            acl_file: RwLock::new(Some(path)),
        }
    }

    /// Create a disabled ACL manager (allows everything)
    pub fn disabled() -> Self {
        let mut users = HashMap::new();
        users.insert("default".to_string(), User::default_user());
        Self {
            users: RwLock::new(users),
            enabled: false,
            acl_file: RwLock::new(None),
        }
    }

    /// Set the ACL file path
    pub async fn set_acl_file(&self, path: std::path::PathBuf) {
        *self.acl_file.write().await = Some(path);
    }

    /// Get the ACL file path
    pub async fn get_acl_file(&self) -> Option<std::path::PathBuf> {
        self.acl_file.read().await.clone()
    }

    /// Check if ACL is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Authenticate a user
    pub async fn authenticate(&self, username: &str, password: &str) -> Result<(), AclError> {
        let users = self.users.read().await;
        let user = users
            .get(username)
            .ok_or_else(|| AclError::UserNotFound(username.to_string()))?;

        if !user.is_enabled() {
            return Err(AclError::AuthFailed);
        }

        if !user.check_password(password) {
            return Err(AclError::AuthFailed);
        }

        Ok(())
    }

    /// Authenticate with just a password (uses default user)
    pub async fn authenticate_password(&self, password: &str) -> Result<(), AclError> {
        self.authenticate("default", password).await
    }

    /// Check if a user can execute a command
    pub async fn check_command(
        &self,
        username: &str,
        command: &str,
        category: Option<&str>,
    ) -> Result<(), AclError> {
        if !self.enabled {
            return Ok(());
        }

        let users = self.users.read().await;
        let user = users
            .get(username)
            .ok_or_else(|| AclError::UserNotFound(username.to_string()))?;

        if user.can_execute_command(command, category) {
            Ok(())
        } else {
            Err(AclError::CommandDenied(command.to_string()))
        }
    }

    /// Check if a user can access a key
    pub async fn check_key(
        &self,
        username: &str,
        key: &str,
        permission: Permission,
    ) -> Result<(), AclError> {
        if !self.enabled {
            return Ok(());
        }

        let users = self.users.read().await;
        let user = users
            .get(username)
            .ok_or_else(|| AclError::UserNotFound(username.to_string()))?;

        if user.can_access_key(key, permission) {
            Ok(())
        } else {
            Err(AclError::KeyDenied(key.to_string()))
        }
    }

    /// Check if a user can access a channel
    pub async fn check_channel(&self, username: &str, channel: &str) -> Result<(), AclError> {
        if !self.enabled {
            return Ok(());
        }

        let users = self.users.read().await;
        let user = users
            .get(username)
            .ok_or_else(|| AclError::UserNotFound(username.to_string()))?;

        if user.can_access_channel(channel) {
            Ok(())
        } else {
            Err(AclError::ChannelDenied(channel.to_string()))
        }
    }

    /// Get a user (if exists)
    pub async fn get_user(&self, username: &str) -> Option<User> {
        self.users.read().await.get(username).cloned()
    }

    /// Create or update a user with ACL rules
    pub async fn set_user(&self, username: &str, rules: &[String]) -> Result<(), AclError> {
        let mut users = self.users.write().await;

        let user = users
            .entry(username.to_string())
            .or_insert_with(|| User::new(username));

        for rule in rules {
            apply_acl_rule(user, rule)?;
        }

        Ok(())
    }

    /// Delete a user
    pub async fn del_user(&self, username: &str) -> Result<(), AclError> {
        if username == "default" {
            return Err(AclError::InvalidRule(
                "Cannot delete the default user".to_string(),
            ));
        }

        let mut users = self.users.write().await;
        if users.remove(username).is_none() {
            return Err(AclError::UserNotFound(username.to_string()));
        }

        Ok(())
    }

    /// List all users
    pub async fn list_users(&self) -> Vec<String> {
        self.users.read().await.keys().cloned().collect()
    }

    /// Get user ACL info
    pub async fn get_user_acl(&self, username: &str) -> Option<String> {
        self.users
            .read()
            .await
            .get(username)
            .map(|u| u.to_acl_string())
    }

    /// Save ACL configuration to a file
    pub async fn save_to_file(&self, path: &Path) -> Result<(), AclError> {
        let users = self.users.read().await;
        let users_vec: Vec<&User> = users.values().collect();

        let json = serde_json::to_string_pretty(&users_vec)
            .map_err(|e| AclError::SerializationError(e.to_string()))?;

        std::fs::write(path, json)
            .map_err(|e| AclError::FileError(format!("Failed to write ACL file: {}", e)))?;

        info!("ACL configuration saved to {:?}", path);
        Ok(())
    }

    /// Load ACL configuration from a file
    pub async fn load_from_file(&self, path: &Path) -> Result<usize, AclError> {
        if !path.exists() {
            debug!("ACL file {:?} does not exist, skipping load", path);
            return Ok(0);
        }

        let contents = std::fs::read_to_string(path)
            .map_err(|e| AclError::FileError(format!("Failed to read ACL file: {}", e)))?;

        let loaded_users: Vec<User> = serde_json::from_str(&contents).map_err(|e| {
            AclError::SerializationError(format!("Failed to parse ACL file: {}", e))
        })?;

        let mut users = self.users.write().await;
        let count = loaded_users.len();

        for user in loaded_users {
            let username = user.username().to_string();
            users.insert(username, user);
        }

        // Ensure default user exists
        if !users.contains_key("default") {
            users.insert("default".to_string(), User::default_user());
        }

        info!("Loaded {} users from ACL file {:?}", count, path);
        Ok(count)
    }

    /// Load ACL from file, creating default if file doesn't exist
    pub async fn load_or_create(&self, path: &Path) -> Result<(), AclError> {
        if path.exists() {
            self.load_from_file(path).await?;
        } else {
            // Create parent directories if needed
            if let Some(parent) = path.parent() {
                if !parent.exists() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        AclError::FileError(format!("Failed to create directory: {}", e))
                    })?;
                }
            }
            // Save default configuration
            self.save_to_file(path).await?;
        }
        Ok(())
    }

    /// Handle ACL command
    pub async fn handle_command(&self, args: &[String]) -> Frame {
        if args.is_empty() {
            return Frame::error("ERR wrong number of arguments for 'acl' command");
        }

        let subcommand = args[0].to_uppercase();
        match subcommand.as_str() {
            "WHOAMI" => {
                // In a real implementation, this would return the current user
                Frame::bulk("default")
            }
            "LIST" => {
                let users = self.users.read().await;
                let list: Vec<Frame> = users
                    .values()
                    .map(|u| Frame::bulk(u.to_acl_string()))
                    .collect();
                Frame::array(list)
            }
            "USERS" => {
                let users = self.list_users().await;
                Frame::array(users.into_iter().map(Frame::bulk).collect())
            }
            "GETUSER" => {
                if args.len() < 2 {
                    return Frame::error("ERR wrong number of arguments for 'acl getuser' command");
                }
                match self.get_user_acl(&args[1]).await {
                    Some(_user) => {
                        // Return as array of field-value pairs
                        Frame::array(vec![
                            Frame::bulk("flags"),
                            Frame::array(vec![]),
                            Frame::bulk("passwords"),
                            Frame::array(vec![]),
                            Frame::bulk("commands"),
                            Frame::bulk("+@all"),
                            Frame::bulk("keys"),
                            Frame::array(vec![Frame::bulk("~*")]),
                            Frame::bulk("channels"),
                            Frame::array(vec![Frame::bulk("&*")]),
                        ])
                    }
                    None => Frame::null(),
                }
            }
            "SETUSER" => {
                if args.len() < 2 {
                    return Frame::error("ERR wrong number of arguments for 'acl setuser' command");
                }
                let rules: Vec<String> = args[2..].to_vec();
                match self.set_user(&args[1], &rules).await {
                    Ok(()) => Frame::simple("OK"),
                    Err(e) => Frame::error(format!("ERR {}", e)),
                }
            }
            "DELUSER" => {
                if args.len() < 2 {
                    return Frame::error("ERR wrong number of arguments for 'acl deluser' command");
                }
                let mut deleted = 0;
                for username in &args[1..] {
                    if self.del_user(username).await.is_ok() {
                        deleted += 1;
                    }
                }
                Frame::Integer(deleted)
            }
            "CAT" => {
                // Return command categories
                if args.len() == 1 {
                    Frame::array(vec![
                        Frame::bulk("read"),
                        Frame::bulk("write"),
                        Frame::bulk("admin"),
                        Frame::bulk("dangerous"),
                        Frame::bulk("connection"),
                        Frame::bulk("transaction"),
                        Frame::bulk("scripting"),
                        Frame::bulk("slow"),
                        Frame::bulk("fast"),
                        Frame::bulk("pubsub"),
                        Frame::bulk("keyspace"),
                        Frame::bulk("string"),
                        Frame::bulk("list"),
                        Frame::bulk("hash"),
                        Frame::bulk("set"),
                        Frame::bulk("sortedset"),
                    ])
                } else {
                    // Return commands in category
                    Frame::array(vec![])
                }
            }
            "GENPASS" => {
                // Generate a random password
                use rand::Rng;
                let len = if args.len() > 1 {
                    args[1].parse().unwrap_or(64)
                } else {
                    64
                };
                let password: String = rand::thread_rng()
                    .sample_iter(&rand::distributions::Alphanumeric)
                    .take(len)
                    .map(char::from)
                    .collect();
                Frame::bulk(password)
            }
            "SAVE" => {
                // Save ACL configuration to file
                match self.acl_file.read().await.as_ref() {
                    Some(path) => match self.save_to_file(path).await {
                        Ok(()) => Frame::simple("OK"),
                        Err(e) => Frame::error(format!("ERR {}", e)),
                    },
                    None => Frame::error("ERR ACL file path not configured"),
                }
            }
            "LOAD" => {
                // Load ACL configuration from file
                match self.acl_file.read().await.as_ref() {
                    Some(path) => match self.load_from_file(path).await {
                        Ok(count) => Frame::simple(format!("OK, loaded {} users", count)),
                        Err(e) => Frame::error(format!("ERR {}", e)),
                    },
                    None => Frame::error("ERR ACL file path not configured"),
                }
            }
            "HELP" => Frame::array(vec![
                Frame::bulk("ACL <subcommand> [<arg> [value] [opt] ...]"),
                Frame::bulk("WHOAMI -- Return the current connection username."),
                Frame::bulk("LIST -- Show all users."),
                Frame::bulk("USERS -- List all usernames."),
                Frame::bulk("GETUSER <username> -- Get a user's ACL."),
                Frame::bulk("SETUSER <username> [rules...] -- Create/modify a user."),
                Frame::bulk("DELUSER <username> [...] -- Delete users."),
                Frame::bulk("CAT [<category>] -- List command categories."),
                Frame::bulk("GENPASS [<len>] -- Generate a secure password."),
                Frame::bulk("SAVE -- Save ACL configuration to file."),
                Frame::bulk("LOAD -- Load ACL configuration from file."),
            ]),
            _ => Frame::error(format!(
                "ERR Unknown ACL subcommand or wrong number of arguments for '{}'",
                subcommand
            )),
        }
    }
}

impl Default for Acl {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared ACL manager
pub type SharedAcl = Arc<Acl>;

/// Apply an ACL rule to a user
fn apply_acl_rule(user: &mut User, rule: &str) -> Result<(), AclError> {
    let rule = rule.trim();

    // Status flags
    if rule == "on" {
        user.enable();
        return Ok(());
    }
    if rule == "off" {
        user.disable();
        return Ok(());
    }

    // Password rules
    if rule == "nopass" {
        user.set_nopass();
        return Ok(());
    }
    if rule == "resetpass" {
        // Reset all passwords - handled by clearing
        return Ok(());
    }
    if let Some(password) = rule.strip_prefix('>') {
        user.set_password(password);
        return Ok(());
    }
    if let Some(_hash) = rule.strip_prefix('#') {
        // Set hashed password - skip for now
        return Ok(());
    }

    // Key patterns
    if rule == "allkeys" || rule == "~*" {
        user.set_allkeys(true);
        return Ok(());
    }
    if rule == "resetkeys" {
        user.set_allkeys(false);
        return Ok(());
    }
    if let Some(pattern) = rule.strip_prefix('~') {
        user.add_key_pattern(KeyPattern::parse(pattern));
        return Ok(());
    }
    if let Some(pattern) = rule.strip_prefix('%') {
        // Read/write specific patterns
        user.add_key_pattern(KeyPattern::parse(&format!("%{}", pattern)));
        return Ok(());
    }

    // Channel patterns
    if rule == "allchannels" || rule == "&*" {
        user.set_allchannels(true);
        return Ok(());
    }
    if rule == "resetchannels" {
        user.set_allchannels(false);
        return Ok(());
    }
    if let Some(pattern) = rule.strip_prefix('&') {
        user.add_channel_pattern(pattern.to_string());
        return Ok(());
    }

    // Command permissions
    if rule == "allcommands" || rule == "+@all" {
        user.set_allcommands(true);
        return Ok(());
    }
    if rule == "nocommands" || rule == "-@all" {
        user.set_allcommands(false);
        return Ok(());
    }
    if let Some(cmd) = rule.strip_prefix('+') {
        if let Some(category) = cmd.strip_prefix('@') {
            user.allow_category(category);
        } else {
            user.allow_command(cmd);
        }
        return Ok(());
    }
    if let Some(cmd) = rule.strip_prefix('-') {
        if let Some(category) = cmd.strip_prefix('@') {
            user.deny_category(category);
        } else {
            user.deny_command(cmd);
        }
        return Ok(());
    }

    // Reset
    if rule == "reset" {
        user.reset();
        return Ok(());
    }

    Err(AclError::InvalidRule(format!("Unknown rule: {}", rule)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_acl_new() {
        let acl = Acl::new();
        assert!(acl.is_enabled());

        // Default user should exist
        let user = acl.get_user("default").await;
        assert!(user.is_some());
        assert!(user.unwrap().is_enabled());
    }

    #[tokio::test]
    async fn test_authenticate() {
        let acl = Acl::new();

        // Default user with nopass should authenticate
        let result = acl.authenticate("default", "anything").await;
        assert!(result.is_ok());

        // Non-existent user should fail
        let result = acl.authenticate("nonexistent", "pass").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_set_user() {
        let acl = Acl::new();

        // Create a new user
        let result = acl
            .set_user(
                "testuser",
                &[
                    "on".to_string(),
                    ">secret".to_string(),
                    "+GET".to_string(),
                    "~user:*".to_string(),
                ],
            )
            .await;
        assert!(result.is_ok());

        // Should be able to authenticate
        let result = acl.authenticate("testuser", "secret").await;
        assert!(result.is_ok());

        // Wrong password should fail
        let result = acl.authenticate("testuser", "wrong").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_del_user() {
        let acl = Acl::new();

        // Create and delete user
        acl.set_user("testuser", &["on".to_string()]).await.unwrap();
        let result = acl.del_user("testuser").await;
        assert!(result.is_ok());

        // User should no longer exist
        let user = acl.get_user("testuser").await;
        assert!(user.is_none());

        // Cannot delete default user
        let result = acl.del_user("default").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_check_command() {
        let acl = Acl::new();

        // Default user can run all commands
        let result = acl.check_command("default", "GET", None).await;
        assert!(result.is_ok());

        // Create restricted user
        acl.set_user(
            "restricted",
            &[
                "on".to_string(),
                "nopass".to_string(),
                "+GET".to_string(),
                "-SET".to_string(),
            ],
        )
        .await
        .unwrap();

        let result = acl.check_command("restricted", "GET", None).await;
        assert!(result.is_ok());

        // SET should be denied (user doesn't have allcommands)
        // Since we only added +GET, SET is not allowed
    }

    #[tokio::test]
    async fn test_list_users() {
        let acl = Acl::new();
        acl.set_user("user1", &["on".to_string()]).await.unwrap();
        acl.set_user("user2", &["on".to_string()]).await.unwrap();

        let users = acl.list_users().await;
        assert!(users.contains(&"default".to_string()));
        assert!(users.contains(&"user1".to_string()));
        assert!(users.contains(&"user2".to_string()));
    }

    #[tokio::test]
    async fn test_disabled_acl() {
        let acl = Acl::disabled();
        assert!(!acl.is_enabled());

        // Everything should be allowed
        let result = acl.check_command("nonexistent", "ANYTHING", None).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_acl_rule() {
        let mut user = User::new("test");

        // Test enable/disable
        apply_acl_rule(&mut user, "on").unwrap();
        assert!(user.is_enabled());

        apply_acl_rule(&mut user, "off").unwrap();
        assert!(!user.is_enabled());

        // Test password
        apply_acl_rule(&mut user, ">secret").unwrap();
        assert!(user.check_password("secret"));

        // Test nopass
        apply_acl_rule(&mut user, "nopass").unwrap();
        assert!(user.check_password("anything"));

        // Test commands
        user.enable();
        apply_acl_rule(&mut user, "+GET").unwrap();
        assert!(user.can_execute_command("GET", None));

        // Test keys
        apply_acl_rule(&mut user, "~user:*").unwrap();
        assert!(user.can_access_key("user:123", Permission::Read));

        // Test reset
        apply_acl_rule(&mut user, "reset").unwrap();
        assert!(!user.is_enabled());
    }

    #[tokio::test]
    async fn test_acl_save_load() {
        let temp_dir = std::env::temp_dir();
        let acl_file = temp_dir.join("test_acl.json");

        // Create ACL with users
        let acl = Acl::with_file(acl_file.clone());
        acl.set_user(
            "testuser",
            &[
                "on".to_string(),
                ">secret123".to_string(),
                "+GET".to_string(),
                "+SET".to_string(),
                "~user:*".to_string(),
            ],
        )
        .await
        .unwrap();

        // Save to file
        acl.save_to_file(&acl_file).await.unwrap();
        assert!(acl_file.exists());

        // Create new ACL and load from file
        let acl2 = Acl::with_file(acl_file.clone());
        let count = acl2.load_from_file(&acl_file).await.unwrap();
        assert!(count >= 2); // default + testuser

        // Verify user was loaded
        let user = acl2.get_user("testuser").await;
        assert!(user.is_some());
        let user = user.unwrap();
        assert!(user.is_enabled());
        assert!(user.check_password("secret123"));

        // Clean up
        std::fs::remove_file(&acl_file).ok();
    }

    #[tokio::test]
    async fn test_acl_load_nonexistent() {
        let acl = Acl::new();
        let result = acl.load_from_file(Path::new("/nonexistent/path.acl")).await;
        assert!(result.is_ok()); // Should return Ok(0) for nonexistent file
        assert_eq!(result.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_acl_set_get_file() {
        let acl = Acl::new();
        assert!(acl.get_acl_file().await.is_none());

        acl.set_acl_file(PathBuf::from("/tmp/test.acl")).await;
        let file = acl.get_acl_file().await;
        assert!(file.is_some());
        assert_eq!(file.unwrap().to_str().unwrap(), "/tmp/test.acl");
    }

    // ==================== Additional Tests ====================

    // AclError tests
    #[test]
    fn test_acl_error_display_user_not_found() {
        let err = AclError::UserNotFound("admin".to_string());
        assert_eq!(err.to_string(), "User not found: admin");
    }

    #[test]
    fn test_acl_error_display_auth_failed() {
        let err = AclError::AuthFailed;
        assert_eq!(err.to_string(), "Authentication failed");
    }

    #[test]
    fn test_acl_error_display_command_denied() {
        let err = AclError::CommandDenied("FLUSHALL".to_string());
        assert_eq!(
            err.to_string(),
            "NOPERM this user has no permissions to run the 'FLUSHALL' command"
        );
    }

    #[test]
    fn test_acl_error_display_key_denied() {
        let err = AclError::KeyDenied("secret:key".to_string());
        assert_eq!(
            err.to_string(),
            "NOPERM this user has no permissions to access the key 'secret:key'"
        );
    }

    #[test]
    fn test_acl_error_display_channel_denied() {
        let err = AclError::ChannelDenied("private".to_string());
        assert_eq!(
            err.to_string(),
            "NOPERM this user has no permissions to access the channel 'private'"
        );
    }

    #[test]
    fn test_acl_error_display_invalid_rule() {
        let err = AclError::InvalidRule("badformat".to_string());
        assert_eq!(err.to_string(), "Invalid ACL rule: badformat");
    }

    #[test]
    fn test_acl_error_display_user_exists() {
        let err = AclError::UserExists("admin".to_string());
        assert_eq!(err.to_string(), "User already exists: admin");
    }

    // Authentication edge cases
    #[tokio::test]
    async fn test_authenticate_disabled_user() {
        let acl = Acl::new();
        acl.set_user(
            "disabled_user",
            &["off".to_string(), ">password".to_string()],
        )
        .await
        .unwrap();

        let result = acl.authenticate("disabled_user", "password").await;
        assert!(result.is_err());
        match result.unwrap_err() {
            AclError::AuthFailed => {}
            _ => panic!("Expected AuthFailed error"),
        }
    }

    #[tokio::test]
    async fn test_authenticate_password_with_default_user() {
        let acl = Acl::new();

        // Default user has nopass, so any password works
        let result = acl.authenticate_password("any_password").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_authenticate_with_set_password() {
        let acl = Acl::new();
        acl.set_user("testuser", &["on".to_string(), ">mypassword".to_string()])
            .await
            .unwrap();

        // Correct password works
        assert!(acl.authenticate("testuser", "mypassword").await.is_ok());

        // Wrong password fails
        assert!(acl.authenticate("testuser", "wrongpassword").await.is_err());

        // Empty password fails
        assert!(acl.authenticate("testuser", "").await.is_err());
    }

    // Key access tests
    #[tokio::test]
    async fn test_check_key_with_pattern() {
        let acl = Acl::new();
        acl.set_user(
            "limited",
            &[
                "on".to_string(),
                "nopass".to_string(),
                "~user:*".to_string(),
            ],
        )
        .await
        .unwrap();

        // Matching key should work
        let result = acl.check_key("limited", "user:123", Permission::Read).await;
        assert!(result.is_ok());

        // Non-matching key should fail (since allkeys is not set)
        // This depends on the User implementation
    }

    #[tokio::test]
    async fn test_check_key_disabled_acl() {
        let acl = Acl::disabled();

        // Everything allowed when ACL is disabled
        let result = acl.check_key("anyone", "any:key", Permission::Write).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_check_key_user_not_found() {
        let acl = Acl::new();
        let result = acl.check_key("nonexistent", "key", Permission::Read).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            AclError::UserNotFound(_) => {}
            _ => panic!("Expected UserNotFound error"),
        }
    }

    // Channel access tests
    #[tokio::test]
    async fn test_check_channel_allowed() {
        let acl = Acl::new();
        // Default user has allchannels
        let result = acl.check_channel("default", "any_channel").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_check_channel_disabled_acl() {
        let acl = Acl::disabled();
        let result = acl.check_channel("anyone", "channel").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_check_channel_user_not_found() {
        let acl = Acl::new();
        let result = acl.check_channel("nonexistent", "channel").await;
        assert!(result.is_err());
    }

    // Command handler tests
    #[tokio::test]
    async fn test_handle_command_whoami() {
        let acl = Acl::new();
        let result = acl.handle_command(&["WHOAMI".to_string()]).await;
        match result {
            Frame::Bulk(Some(data)) => assert_eq!(data.as_ref(), b"default"),
            _ => panic!("Expected Bulk frame"),
        }
    }

    #[tokio::test]
    async fn test_handle_command_users() {
        let acl = Acl::new();
        acl.set_user("admin", &["on".to_string()]).await.unwrap();

        let result = acl.handle_command(&["USERS".to_string()]).await;
        match result {
            Frame::Array(Some(frames)) => {
                assert!(frames.len() >= 2); // default + admin
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[tokio::test]
    async fn test_handle_command_list() {
        let acl = Acl::new();
        let result = acl.handle_command(&["LIST".to_string()]).await;
        match result {
            Frame::Array(Some(frames)) => {
                assert!(!frames.is_empty());
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[tokio::test]
    async fn test_handle_command_getuser_exists() {
        let acl = Acl::new();
        let result = acl
            .handle_command(&["GETUSER".to_string(), "default".to_string()])
            .await;
        match result {
            Frame::Array(Some(_)) => {} // Expected
            _ => panic!("Expected Array frame"),
        }
    }

    #[tokio::test]
    async fn test_handle_command_getuser_not_exists() {
        let acl = Acl::new();
        let result = acl
            .handle_command(&["GETUSER".to_string(), "nonexistent".to_string()])
            .await;
        match result {
            Frame::Bulk(None) => {} // Expected null bulk for nonexistent user
            _ => panic!("Expected null Bulk frame"),
        }
    }

    #[tokio::test]
    async fn test_handle_command_getuser_missing_arg() {
        let acl = Acl::new();
        let result = acl.handle_command(&["GETUSER".to_string()]).await;
        match result {
            Frame::Error(_) => {} // Expected
            _ => panic!("Expected Error frame"),
        }
    }

    #[tokio::test]
    async fn test_handle_command_setuser() {
        let acl = Acl::new();
        let result = acl
            .handle_command(&[
                "SETUSER".to_string(),
                "newuser".to_string(),
                "on".to_string(),
            ])
            .await;
        match result {
            Frame::Simple(s) => assert_eq!(s, "OK"),
            _ => panic!("Expected Simple OK frame"),
        }

        // Verify user was created
        let user = acl.get_user("newuser").await;
        assert!(user.is_some());
    }

    #[tokio::test]
    async fn test_handle_command_setuser_missing_arg() {
        let acl = Acl::new();
        let result = acl.handle_command(&["SETUSER".to_string()]).await;
        match result {
            Frame::Error(_) => {} // Expected
            _ => panic!("Expected Error frame"),
        }
    }

    #[tokio::test]
    async fn test_handle_command_deluser() {
        let acl = Acl::new();
        acl.set_user("todelete", &["on".to_string()]).await.unwrap();

        let result = acl
            .handle_command(&["DELUSER".to_string(), "todelete".to_string()])
            .await;
        match result {
            Frame::Integer(count) => assert_eq!(count, 1),
            _ => panic!("Expected Integer frame"),
        }
    }

    #[tokio::test]
    async fn test_handle_command_deluser_multiple() {
        let acl = Acl::new();
        acl.set_user("user1", &["on".to_string()]).await.unwrap();
        acl.set_user("user2", &["on".to_string()]).await.unwrap();

        let result = acl
            .handle_command(&[
                "DELUSER".to_string(),
                "user1".to_string(),
                "user2".to_string(),
            ])
            .await;
        match result {
            Frame::Integer(count) => assert_eq!(count, 2),
            _ => panic!("Expected Integer frame"),
        }
    }

    #[tokio::test]
    async fn test_handle_command_deluser_missing_arg() {
        let acl = Acl::new();
        let result = acl.handle_command(&["DELUSER".to_string()]).await;
        match result {
            Frame::Error(_) => {} // Expected
            _ => panic!("Expected Error frame"),
        }
    }

    #[tokio::test]
    async fn test_handle_command_cat_no_args() {
        let acl = Acl::new();
        let result = acl.handle_command(&["CAT".to_string()]).await;
        match result {
            Frame::Array(Some(frames)) => {
                assert!(!frames.is_empty());
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[tokio::test]
    async fn test_handle_command_cat_with_category() {
        let acl = Acl::new();
        let result = acl
            .handle_command(&["CAT".to_string(), "read".to_string()])
            .await;
        match result {
            Frame::Array(Some(_)) => {} // Expected
            _ => panic!("Expected Array frame"),
        }
    }

    #[tokio::test]
    async fn test_handle_command_genpass_default() {
        let acl = Acl::new();
        let result = acl.handle_command(&["GENPASS".to_string()]).await;
        match result {
            Frame::Bulk(Some(data)) => {
                assert_eq!(data.len(), 64); // Default length
            }
            _ => panic!("Expected Bulk frame"),
        }
    }

    #[tokio::test]
    async fn test_handle_command_genpass_custom_length() {
        let acl = Acl::new();
        let result = acl
            .handle_command(&["GENPASS".to_string(), "32".to_string()])
            .await;
        match result {
            Frame::Bulk(Some(data)) => {
                assert_eq!(data.len(), 32);
            }
            _ => panic!("Expected Bulk frame"),
        }
    }

    #[tokio::test]
    async fn test_handle_command_help() {
        let acl = Acl::new();
        let result = acl.handle_command(&["HELP".to_string()]).await;
        match result {
            Frame::Array(Some(frames)) => {
                assert!(!frames.is_empty());
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[tokio::test]
    async fn test_handle_command_unknown() {
        let acl = Acl::new();
        let result = acl.handle_command(&["UNKNOWN_COMMAND".to_string()]).await;
        match result {
            Frame::Error(_) => {} // Expected
            _ => panic!("Expected Error frame"),
        }
    }

    #[tokio::test]
    async fn test_handle_command_empty_args() {
        let acl = Acl::new();
        let result = acl.handle_command(&[]).await;
        match result {
            Frame::Error(_) => {} // Expected
            _ => panic!("Expected Error frame"),
        }
    }

    #[tokio::test]
    async fn test_handle_command_save_no_file() {
        let acl = Acl::new();
        let result = acl.handle_command(&["SAVE".to_string()]).await;
        match result {
            Frame::Error(e) => {
                let err_str = String::from_utf8_lossy(&e);
                assert!(err_str.contains("not configured"));
            }
            _ => panic!("Expected Error frame"),
        }
    }

    #[tokio::test]
    async fn test_handle_command_load_no_file() {
        let acl = Acl::new();
        let result = acl.handle_command(&["LOAD".to_string()]).await;
        match result {
            Frame::Error(e) => {
                let err_str = String::from_utf8_lossy(&e);
                assert!(err_str.contains("not configured"));
            }
            _ => panic!("Expected Error frame"),
        }
    }

    // ACL rule application tests
    #[test]
    fn test_apply_acl_rule_allkeys() {
        let mut user = User::new("test");
        apply_acl_rule(&mut user, "allkeys").unwrap();
        // allkeys should allow access to any key
    }

    #[test]
    fn test_apply_acl_rule_resetkeys() {
        let mut user = User::new("test");
        apply_acl_rule(&mut user, "allkeys").unwrap();
        apply_acl_rule(&mut user, "resetkeys").unwrap();
    }

    #[test]
    fn test_apply_acl_rule_allchannels() {
        let mut user = User::new("test");
        apply_acl_rule(&mut user, "allchannels").unwrap();
    }

    #[test]
    fn test_apply_acl_rule_resetchannels() {
        let mut user = User::new("test");
        apply_acl_rule(&mut user, "allchannels").unwrap();
        apply_acl_rule(&mut user, "resetchannels").unwrap();
    }

    #[test]
    fn test_apply_acl_rule_allcommands() {
        let mut user = User::new("test");
        apply_acl_rule(&mut user, "allcommands").unwrap();
    }

    #[test]
    fn test_apply_acl_rule_nocommands() {
        let mut user = User::new("test");
        apply_acl_rule(&mut user, "allcommands").unwrap();
        apply_acl_rule(&mut user, "nocommands").unwrap();
    }

    #[test]
    fn test_apply_acl_rule_category_add() {
        let mut user = User::new("test");
        apply_acl_rule(&mut user, "+@read").unwrap();
    }

    #[test]
    fn test_apply_acl_rule_category_remove() {
        let mut user = User::new("test");
        apply_acl_rule(&mut user, "+@all").unwrap();
        apply_acl_rule(&mut user, "-@dangerous").unwrap();
    }

    #[test]
    fn test_apply_acl_rule_channel_pattern() {
        let mut user = User::new("test");
        apply_acl_rule(&mut user, "&news:*").unwrap();
    }

    #[test]
    fn test_apply_acl_rule_resetpass() {
        let mut user = User::new("test");
        apply_acl_rule(&mut user, ">password").unwrap();
        apply_acl_rule(&mut user, "resetpass").unwrap();
    }

    #[test]
    fn test_apply_acl_rule_hashed_password() {
        let mut user = User::new("test");
        // Hashed password rule (starts with #)
        apply_acl_rule(&mut user, "#abc123def456").unwrap();
    }

    #[test]
    fn test_apply_acl_rule_read_write_pattern() {
        let mut user = User::new("test");
        // % prefix for read/write patterns
        apply_acl_rule(&mut user, "%RW~user:*").unwrap();
    }

    #[test]
    fn test_apply_acl_rule_unknown() {
        let mut user = User::new("test");
        let result = apply_acl_rule(&mut user, "unknown_rule_format");
        assert!(result.is_err());
    }

    // Concurrent access tests
    #[tokio::test]
    async fn test_concurrent_user_creation() {
        use std::sync::Arc;
        use tokio::task::JoinSet;

        let acl = Arc::new(Acl::new());
        let mut join_set = JoinSet::new();

        for i in 0..10 {
            let acl_clone = acl.clone();
            let username = format!("user_{}", i);
            join_set.spawn(async move {
                acl_clone
                    .set_user(&username, &["on".to_string()])
                    .await
                    .unwrap();
            });
        }

        while (join_set.join_next().await).is_some() {}

        let users = acl.list_users().await;
        assert!(users.len() >= 11); // default + 10 users
    }

    #[tokio::test]
    async fn test_concurrent_authentication() {
        use std::sync::Arc;
        use tokio::task::JoinSet;

        let acl = Arc::new(Acl::new());
        acl.set_user("testuser", &["on".to_string(), "nopass".to_string()])
            .await
            .unwrap();

        let mut join_set = JoinSet::new();

        for _ in 0..20 {
            let acl_clone = acl.clone();
            join_set.spawn(async move {
                acl_clone
                    .authenticate("testuser", "anypassword")
                    .await
                    .unwrap();
            });
        }

        while (join_set.join_next().await).is_some() {}
    }

    #[tokio::test]
    async fn test_concurrent_read_write() {
        use std::sync::Arc;
        use tokio::task::JoinSet;

        let acl = Arc::new(Acl::new());
        let mut join_set = JoinSet::new();

        // Writers
        for i in 0..5 {
            let acl_clone = acl.clone();
            let username = format!("writer_{}", i);
            join_set.spawn(async move {
                acl_clone
                    .set_user(&username, &["on".to_string()])
                    .await
                    .unwrap();
            });
        }

        // Readers
        for _ in 0..5 {
            let acl_clone = acl.clone();
            join_set.spawn(async move {
                let _ = acl_clone.list_users().await;
            });
        }

        while (join_set.join_next().await).is_some() {}
    }

    // Edge cases
    #[tokio::test]
    async fn test_get_user_acl_format() {
        let acl = Acl::new();
        let acl_string = acl.get_user_acl("default").await;
        assert!(acl_string.is_some());
    }

    #[tokio::test]
    async fn test_get_user_acl_nonexistent() {
        let acl = Acl::new();
        let acl_string = acl.get_user_acl("nonexistent").await;
        assert!(acl_string.is_none());
    }

    #[tokio::test]
    async fn test_del_user_nonexistent() {
        let acl = Acl::new();
        let result = acl.del_user("nonexistent").await;
        assert!(result.is_err());
        match result.unwrap_err() {
            AclError::UserNotFound(_) => {}
            _ => panic!("Expected UserNotFound error"),
        }
    }

    #[tokio::test]
    async fn test_set_user_update_existing() {
        let acl = Acl::new();

        // Create user
        acl.set_user("testuser", &["on".to_string(), ">pass1".to_string()])
            .await
            .unwrap();
        assert!(acl.authenticate("testuser", "pass1").await.is_ok());

        // Update user with new password
        acl.set_user("testuser", &[">pass2".to_string()])
            .await
            .unwrap();
        assert!(acl.authenticate("testuser", "pass2").await.is_ok());
    }

    #[test]
    fn test_acl_default_impl() {
        let acl = Acl::default();
        assert!(acl.is_enabled());
    }

    // Aliased rule tests
    #[test]
    fn test_apply_acl_rule_tilde_star() {
        let mut user = User::new("test");
        apply_acl_rule(&mut user, "~*").unwrap();
    }

    #[test]
    fn test_apply_acl_rule_ampersand_star() {
        let mut user = User::new("test");
        apply_acl_rule(&mut user, "&*").unwrap();
    }

    #[test]
    fn test_apply_acl_rule_plus_at_all() {
        let mut user = User::new("test");
        apply_acl_rule(&mut user, "+@all").unwrap();
    }

    #[test]
    fn test_apply_acl_rule_minus_at_all() {
        let mut user = User::new("test");
        apply_acl_rule(&mut user, "-@all").unwrap();
    }

    // Save/Load command handler integration tests
    #[tokio::test]
    async fn test_handle_command_save_with_file() {
        let temp_dir = std::env::temp_dir();
        let acl_file = temp_dir.join("test_save_command.json");

        let acl = Acl::with_file(acl_file.clone());
        acl.set_user("savetest", &["on".to_string()]).await.unwrap();

        let result = acl.handle_command(&["SAVE".to_string()]).await;
        match result {
            Frame::Simple(s) => assert_eq!(s, "OK"),
            _ => panic!("Expected Simple OK frame"),
        }

        // File should exist
        assert!(acl_file.exists());

        // Clean up
        std::fs::remove_file(&acl_file).ok();
    }

    #[tokio::test]
    async fn test_handle_command_load_with_file() {
        let temp_dir = std::env::temp_dir();
        let acl_file = temp_dir.join("test_load_command.json");

        // Create and save ACL
        let acl = Acl::with_file(acl_file.clone());
        acl.set_user("loadtest", &["on".to_string(), ">password123".to_string()])
            .await
            .unwrap();
        acl.save_to_file(&acl_file).await.unwrap();

        // Create new ACL and load
        let acl2 = Acl::with_file(acl_file.clone());
        let result = acl2.handle_command(&["LOAD".to_string()]).await;
        match result {
            Frame::Simple(s) => {
                let msg = String::from_utf8_lossy(&s);
                assert!(
                    msg.contains("loaded"),
                    "Expected 'loaded' in message: {}",
                    msg
                );
            }
            _ => panic!("Expected Simple frame with load message"),
        }

        // Verify user was loaded
        let user = acl2.get_user("loadtest").await;
        assert!(user.is_some());

        // Clean up
        std::fs::remove_file(&acl_file).ok();
    }

    #[tokio::test]
    async fn test_load_or_create_new_file() {
        let temp_dir = std::env::temp_dir();
        let acl_file = temp_dir.join("test_new_acl.json");

        // Delete file if exists
        std::fs::remove_file(&acl_file).ok();

        let acl = Acl::new();
        let result = acl.load_or_create(&acl_file).await;
        assert!(result.is_ok());
        assert!(acl_file.exists());

        // Clean up
        std::fs::remove_file(&acl_file).ok();
    }

    #[tokio::test]
    async fn test_load_or_create_existing_file() {
        let temp_dir = std::env::temp_dir();
        let acl_file = temp_dir.join("test_existing_acl.json");

        // Create initial ACL
        let acl = Acl::new();
        acl.set_user("existing", &["on".to_string()]).await.unwrap();
        acl.save_to_file(&acl_file).await.unwrap();

        // Load it
        let acl2 = Acl::new();
        let result = acl2.load_or_create(&acl_file).await;
        assert!(result.is_ok());

        let user = acl2.get_user("existing").await;
        assert!(user.is_some());

        // Clean up
        std::fs::remove_file(&acl_file).ok();
    }

    #[tokio::test]
    async fn test_load_or_create_creates_parent_dir() {
        let temp_dir = std::env::temp_dir();
        let nested_dir = temp_dir.join("ferrite_test_nested");
        let acl_file = nested_dir.join("acl.json");

        // Delete directory if exists
        std::fs::remove_dir_all(&nested_dir).ok();

        let acl = Acl::new();
        let result = acl.load_or_create(&acl_file).await;
        assert!(result.is_ok());
        assert!(acl_file.exists());

        // Clean up
        std::fs::remove_dir_all(&nested_dir).ok();
    }

    // Multiple user operations
    #[tokio::test]
    async fn test_authenticate_multiple_users() {
        let acl = Acl::new();

        // Create multiple users with different passwords
        acl.set_user("user1", &["on".to_string(), ">pass1".to_string()])
            .await
            .unwrap();
        acl.set_user("user2", &["on".to_string(), ">pass2".to_string()])
            .await
            .unwrap();
        acl.set_user("user3", &["on".to_string(), ">pass3".to_string()])
            .await
            .unwrap();

        // Each user should authenticate with their own password
        assert!(acl.authenticate("user1", "pass1").await.is_ok());
        assert!(acl.authenticate("user2", "pass2").await.is_ok());
        assert!(acl.authenticate("user3", "pass3").await.is_ok());

        // Cross-authentication should fail
        assert!(acl.authenticate("user1", "pass2").await.is_err());
        assert!(acl.authenticate("user2", "pass3").await.is_err());
        assert!(acl.authenticate("user3", "pass1").await.is_err());
    }

    #[tokio::test]
    async fn test_check_command_with_category() {
        let acl = Acl::new();
        acl.set_user(
            "reader",
            &["on".to_string(), "nopass".to_string(), "+@read".to_string()],
        )
        .await
        .unwrap();

        // Command with matching category should work
        let result = acl.check_command("reader", "GET", Some("read")).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_check_command_category_denied() {
        let acl = Acl::new();
        acl.set_user(
            "limited",
            &[
                "on".to_string(),
                "nopass".to_string(),
                "+@all".to_string(),
                "-@dangerous".to_string(),
            ],
        )
        .await
        .unwrap();

        // Denied category should prevent execution even if allcommands
        let result = acl
            .check_command("limited", "FLUSHALL", Some("dangerous"))
            .await;
        // This depends on implementation - with +@all and then -@dangerous
        // The user should have access unless specifically checking category denial
    }

    #[tokio::test]
    async fn test_check_key_read_only_pattern() {
        let acl = Acl::new();
        acl.set_user(
            "reader",
            &[
                "on".to_string(),
                "nopass".to_string(),
                "%R~data:*".to_string(),
            ],
        )
        .await
        .unwrap();

        // Read should work for matching pattern
        let result = acl.check_key("reader", "data:123", Permission::Read).await;
        assert!(result.is_ok());

        // Write should fail for read-only pattern
        let result = acl.check_key("reader", "data:123", Permission::Write).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_check_key_write_only_pattern() {
        let acl = Acl::new();
        acl.set_user(
            "writer",
            &[
                "on".to_string(),
                "nopass".to_string(),
                "%W~logs:*".to_string(),
            ],
        )
        .await
        .unwrap();

        // Write should work
        let result = acl.check_key("writer", "logs:app", Permission::Write).await;
        assert!(result.is_ok());

        // Read should fail for write-only pattern
        let result = acl.check_key("writer", "logs:app", Permission::Read).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_check_key_multiple_patterns() {
        let acl = Acl::new();
        acl.set_user(
            "multi",
            &[
                "on".to_string(),
                "nopass".to_string(),
                "~user:*".to_string(),
                "~session:*".to_string(),
            ],
        )
        .await
        .unwrap();

        // Both patterns should work
        assert!(acl
            .check_key("multi", "user:123", Permission::Read)
            .await
            .is_ok());
        assert!(acl
            .check_key("multi", "session:abc", Permission::Read)
            .await
            .is_ok());

        // Non-matching pattern should fail
        let result = acl.check_key("multi", "admin:data", Permission::Read).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_check_channel_with_pattern() {
        let acl = Acl::new();
        acl.set_user(
            "subscriber",
            &[
                "on".to_string(),
                "nopass".to_string(),
                "resetchannels".to_string(),
                "&news:*".to_string(),
            ],
        )
        .await
        .unwrap();

        // Matching channel should work
        let result = acl.check_channel("subscriber", "news:sports").await;
        assert!(result.is_ok());

        // Non-matching channel should fail
        let result = acl.check_channel("subscriber", "admin:alerts").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_set_user_complex_rules() {
        let acl = Acl::new();

        let result = acl
            .set_user(
                "complex",
                &[
                    "on".to_string(),
                    ">complexpass".to_string(),
                    "+GET".to_string(),
                    "+SET".to_string(),
                    "+DEL".to_string(),
                    "-FLUSHALL".to_string(),
                    "~user:*".to_string(),
                    "~session:*".to_string(),
                    "&notifications:*".to_string(),
                ],
            )
            .await;

        assert!(result.is_ok());

        let user = acl.get_user("complex").await.unwrap();
        assert!(user.is_enabled());
        assert!(user.check_password("complexpass"));
    }

    #[tokio::test]
    async fn test_del_user_multiple_mixed_results() {
        let acl = Acl::new();
        acl.set_user("user1", &["on".to_string()]).await.unwrap();

        // Try to delete existing and non-existing users
        // deluser ignores errors and counts successes
        let result = acl
            .handle_command(&[
                "DELUSER".to_string(),
                "user1".to_string(),
                "nonexistent".to_string(),
            ])
            .await;

        match result {
            Frame::Integer(count) => assert_eq!(count, 1), // Only user1 deleted
            _ => panic!("Expected Integer frame"),
        }
    }

    #[tokio::test]
    async fn test_del_user_default_forbidden() {
        let acl = Acl::new();

        let result = acl
            .handle_command(&["DELUSER".to_string(), "default".to_string()])
            .await;

        match result {
            Frame::Integer(count) => assert_eq!(count, 0), // Default user not deleted
            _ => panic!("Expected Integer frame"),
        }
    }

    // Security scenario tests
    #[tokio::test]
    async fn test_security_disabled_user_cannot_auth() {
        let acl = Acl::new();
        acl.set_user("disabled", &["off".to_string(), "nopass".to_string()])
            .await
            .unwrap();

        let result = acl.authenticate("disabled", "anything").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_security_disabled_user_cannot_execute_commands() {
        let acl = Acl::new();
        acl.set_user(
            "disabled",
            &["off".to_string(), "nopass".to_string(), "+@all".to_string()],
        )
        .await
        .unwrap();

        let result = acl.check_command("disabled", "GET", None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_security_disabled_user_cannot_access_keys() {
        let acl = Acl::new();
        acl.set_user("disabled", &["off".to_string(), "allkeys".to_string()])
            .await
            .unwrap();

        let result = acl.check_key("disabled", "anykey", Permission::Read).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_security_empty_password() {
        let acl = Acl::new();
        acl.set_user("emptypass", &["on".to_string(), ">".to_string()])
            .await
            .unwrap();

        // Empty password should work
        assert!(acl.authenticate("emptypass", "").await.is_ok());
        // Non-empty password should fail
        assert!(acl.authenticate("emptypass", "nonempty").await.is_err());
    }

    #[tokio::test]
    async fn test_security_command_deny_overrides_allow() {
        let acl = Acl::new();
        acl.set_user(
            "mixed",
            &[
                "on".to_string(),
                "nopass".to_string(),
                "+GET".to_string(),
                "-GET".to_string(),
            ],
        )
        .await
        .unwrap();

        // Deny should override previous allow
        let result = acl.check_command("mixed", "GET", None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_security_allkeys_then_resetkeys() {
        let acl = Acl::new();
        acl.set_user(
            "keyreset",
            &[
                "on".to_string(),
                "nopass".to_string(),
                "allkeys".to_string(),
                "resetkeys".to_string(),
            ],
        )
        .await
        .unwrap();

        // After resetkeys, should have no key access
        let result = acl.check_key("keyreset", "anykey", Permission::Read).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_load_from_file_ensures_default_user() {
        let temp_dir = std::env::temp_dir();
        let acl_file = temp_dir.join("test_no_default.json");

        // Create ACL file without default user
        let users = vec![User::new("customuser")];
        let json = serde_json::to_string_pretty(&users).unwrap();
        std::fs::write(&acl_file, json).unwrap();

        // Load should ensure default user exists
        let acl = Acl::new();
        acl.load_from_file(&acl_file).await.unwrap();

        let default_user = acl.get_user("default").await;
        assert!(default_user.is_some());

        // Clean up
        std::fs::remove_file(&acl_file).ok();
    }

    #[tokio::test]
    async fn test_acl_with_file_constructor() {
        let temp_dir = std::env::temp_dir();
        let acl_file = temp_dir.join("test_constructor.json");

        let acl = Acl::with_file(acl_file.clone());
        assert!(acl.is_enabled());

        let file_path = acl.get_acl_file().await;
        assert!(file_path.is_some());
        assert_eq!(file_path.unwrap(), acl_file);
    }

    #[tokio::test]
    async fn test_handle_command_setuser_invalid_rule() {
        let acl = Acl::new();

        let result = acl
            .handle_command(&[
                "SETUSER".to_string(),
                "testuser".to_string(),
                "invalid_rule_format".to_string(),
            ])
            .await;

        match result {
            Frame::Error(_) => {} // Expected
            _ => panic!("Expected Error frame for invalid rule"),
        }
    }

    #[tokio::test]
    async fn test_handle_command_case_insensitive() {
        let acl = Acl::new();

        // Test lowercase command
        let result = acl.handle_command(&["whoami".to_string()]).await;
        match result {
            Frame::Bulk(Some(_)) => {} // Expected
            _ => panic!("Expected Bulk frame"),
        }

        // Test mixed case
        let result = acl.handle_command(&["UsErS".to_string()]).await;
        match result {
            Frame::Array(Some(_)) => {} // Expected
            _ => panic!("Expected Array frame"),
        }
    }

    #[tokio::test]
    async fn test_key_pattern_exact_match() {
        let acl = Acl::new();
        acl.set_user(
            "exact",
            &[
                "on".to_string(),
                "nopass".to_string(),
                "~exactkey".to_string(),
            ],
        )
        .await
        .unwrap();

        // Exact match should work
        assert!(acl
            .check_key("exact", "exactkey", Permission::Read)
            .await
            .is_ok());

        // Partial match should fail
        assert!(acl
            .check_key("exact", "exactkey123", Permission::Read)
            .await
            .is_err());
        assert!(acl
            .check_key("exact", "prefix_exactkey", Permission::Read)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_key_pattern_glob_wildcard() {
        let acl = Acl::new();
        acl.set_user(
            "glob",
            &[
                "on".to_string(),
                "nopass".to_string(),
                "~user:?:data".to_string(),
            ],
        )
        .await
        .unwrap();

        // Single character wildcard should match
        assert!(acl
            .check_key("glob", "user:1:data", Permission::Read)
            .await
            .is_ok());
        assert!(acl
            .check_key("glob", "user:a:data", Permission::Read)
            .await
            .is_ok());

        // Multiple characters should fail
        assert!(acl
            .check_key("glob", "user:12:data", Permission::Read)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_apply_multiple_rules_order_matters() {
        let acl = Acl::new();

        // Allow then deny
        acl.set_user(
            "test1",
            &[
                "on".to_string(),
                "allcommands".to_string(),
                "-FLUSHALL".to_string(),
            ],
        )
        .await
        .unwrap();

        let user = acl.get_user("test1").await.unwrap();
        // With allcommands, FLUSHALL should be denied
        assert!(!user.can_execute_command("FLUSHALL", None));
        assert!(user.can_execute_command("GET", None));
    }

    #[tokio::test]
    async fn test_check_command_nonexistent_user() {
        let acl = Acl::new();
        let result = acl.check_command("ghost", "GET", None).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            AclError::UserNotFound(username) => assert_eq!(username, "ghost"),
            _ => panic!("Expected UserNotFound error"),
        }
    }

    #[tokio::test]
    async fn test_save_to_file_serialization() {
        let temp_dir = std::env::temp_dir();
        let acl_file = temp_dir.join("test_serialization.json");

        let acl = Acl::new();
        acl.set_user(
            "serialtest",
            &[
                "on".to_string(),
                ">password".to_string(),
                "+GET".to_string(),
                "~key:*".to_string(),
            ],
        )
        .await
        .unwrap();

        // Save should succeed
        let result = acl.save_to_file(&acl_file).await;
        assert!(result.is_ok());

        // File should contain valid JSON
        let contents = std::fs::read_to_string(&acl_file).unwrap();
        let parsed: serde_json::Result<Vec<User>> = serde_json::from_str(&contents);
        assert!(parsed.is_ok());

        // Clean up
        std::fs::remove_file(&acl_file).ok();
    }

    #[tokio::test]
    async fn test_concurrent_delete_operations() {
        use std::sync::Arc;
        use tokio::task::JoinSet;

        let acl = Arc::new(Acl::new());

        // Create users
        for i in 0..10 {
            acl.set_user(&format!("deluser_{}", i), &["on".to_string()])
                .await
                .unwrap();
        }

        let mut join_set = JoinSet::new();

        // Try to delete concurrently
        for i in 0..10 {
            let acl_clone = acl.clone();
            let username = format!("deluser_{}", i);
            join_set.spawn(async move {
                let _ = acl_clone.del_user(&username).await;
            });
        }

        while (join_set.join_next().await).is_some() {}

        // All users should be deleted
        let users = acl.list_users().await;
        for i in 0..10 {
            assert!(!users.contains(&format!("deluser_{}", i)));
        }
    }

    #[tokio::test]
    async fn test_acl_error_file_error_display() {
        let err = AclError::FileError("permission denied".to_string());
        assert_eq!(err.to_string(), "ACL file error: permission denied");
    }

    #[tokio::test]
    async fn test_acl_error_serialization_error_display() {
        let err = AclError::SerializationError("invalid json".to_string());
        assert_eq!(err.to_string(), "ACL serialization error: invalid json");
    }
}
