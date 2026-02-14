//! Policy action execution

use serde::{Deserialize, Serialize};

use super::rule::PolicyAction;

/// Result of executing an action
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionResult {
    /// Action that was executed
    pub action: String,
    /// Whether execution succeeded
    pub success: bool,
    /// Error message if failed
    pub error: Option<String>,
    /// Modified value (if applicable)
    pub modified_value: Option<Vec<u8>>,
}

impl ActionResult {
    /// Create a success result
    pub fn success(action: &str) -> Self {
        Self {
            action: action.to_string(),
            success: true,
            error: None,
            modified_value: None,
        }
    }

    /// Create a failure result
    pub fn failure(action: &str, error: impl Into<String>) -> Self {
        Self {
            action: action.to_string(),
            success: false,
            error: Some(error.into()),
            modified_value: None,
        }
    }

    /// Set modified value
    pub fn with_value(mut self, value: Vec<u8>) -> Self {
        self.modified_value = Some(value);
        self
    }
}

/// Executes policy actions
pub struct ActionExecutor {
    /// Notification handlers
    notification_handlers: parking_lot::RwLock<Vec<Box<dyn NotificationHandler>>>,
}

/// Handler for notifications
pub trait NotificationHandler: Send + Sync {
    /// Handle a notification
    fn notify(&self, message: &str) -> bool;
}

impl ActionExecutor {
    /// Create a new action executor
    pub fn new() -> Self {
        Self {
            notification_handlers: parking_lot::RwLock::new(Vec::new()),
        }
    }

    /// Execute an action
    pub fn execute(&self, action: &PolicyAction, value: Option<&[u8]>) -> ActionResult {
        match action {
            PolicyAction::Allow => ActionResult::success("allow"),
            PolicyAction::Deny(reason) => ActionResult::failure("deny", reason),
            PolicyAction::SetTtl(ttl) => {
                // TTL is handled by the caller
                ActionResult::success("set_ttl").with_value(ttl.to_be_bytes().to_vec())
            }
            PolicyAction::RequireEncryption => {
                // Encryption is handled by the caller
                ActionResult::success("require_encryption")
            }
            PolicyAction::Log(message) => {
                // Log the operation
                tracing::info!(policy_action = "log", message = %message);
                ActionResult::success("log")
            }
            PolicyAction::Notify(target) => {
                self.send_notification(target);
                ActionResult::success("notify")
            }
            PolicyAction::Redact => {
                // Redact the entire value
                ActionResult::success("redact").with_value(b"[REDACTED]".to_vec())
            }
            PolicyAction::Mask(pattern) => {
                // Mask value according to pattern
                let masked = if let Some(v) = value {
                    self.mask_value(v, pattern)
                } else {
                    b"****".to_vec()
                };
                ActionResult::success("mask").with_value(masked)
            }
            PolicyAction::RateLimit(limit) => {
                // Rate limiting is handled externally
                ActionResult::success("rate_limit").with_value(limit.to_be_bytes().to_vec())
            }
            PolicyAction::Custom(name) => {
                // Custom actions need external handling
                ActionResult::success(&format!("custom:{}", name))
            }
        }
    }

    /// Execute multiple actions
    pub fn execute_all(&self, actions: &[PolicyAction], value: Option<&[u8]>) -> Vec<ActionResult> {
        let mut current_value = value.map(|v| v.to_vec());

        actions
            .iter()
            .map(|action| {
                let result = self.execute(action, current_value.as_deref());

                // Update value if modified
                if let Some(ref modified) = result.modified_value {
                    current_value = Some(modified.clone());
                }

                result
            })
            .collect()
    }

    /// Register a notification handler
    pub fn register_notification_handler(&self, handler: Box<dyn NotificationHandler>) {
        self.notification_handlers.write().push(handler);
    }

    /// Send notification
    fn send_notification(&self, target: &str) {
        let handlers = self.notification_handlers.read();
        for handler in handlers.iter() {
            handler.notify(target);
        }
    }

    /// Mask a value according to pattern
    fn mask_value(&self, value: &[u8], pattern: &str) -> Vec<u8> {
        // Simple masking - keep first and last chars, mask middle
        let value_str = String::from_utf8_lossy(value);
        let chars: Vec<char> = value_str.chars().collect();

        if chars.len() <= 2 {
            return pattern.repeat(chars.len()).into_bytes();
        }

        let mask_char = pattern.chars().next().unwrap_or('*');
        let mut result = String::new();
        result.push(chars[0]);

        for _ in 1..chars.len() - 1 {
            result.push(mask_char);
        }

        result.push(chars[chars.len() - 1]);
        result.into_bytes()
    }
}

impl Default for ActionExecutor {
    fn default() -> Self {
        Self::new()
    }
}

/// Simple logging notification handler
pub struct LoggingNotificationHandler;

impl NotificationHandler for LoggingNotificationHandler {
    fn notify(&self, message: &str) -> bool {
        tracing::info!(notification = %message, "Policy notification");
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execute_allow() {
        let executor = ActionExecutor::new();
        let result = executor.execute(&PolicyAction::Allow, None);
        assert!(result.success);
    }

    #[test]
    fn test_execute_deny() {
        let executor = ActionExecutor::new();
        let result = executor.execute(&PolicyAction::Deny("not allowed".to_string()), None);
        assert!(!result.success);
    }

    #[test]
    fn test_execute_redact() {
        let executor = ActionExecutor::new();
        let result = executor.execute(&PolicyAction::Redact, Some(b"secret"));
        assert!(result.success);
        assert_eq!(result.modified_value, Some(b"[REDACTED]".to_vec()));
    }

    #[test]
    fn test_mask_value() {
        let executor = ActionExecutor::new();
        let masked = executor.mask_value(b"secret123", "*");
        let masked_str = String::from_utf8(masked).unwrap();
        assert_eq!(masked_str, "s*******3");
    }
}
