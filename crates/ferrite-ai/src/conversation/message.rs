//! Message types for conversation memory
//!
//! Defines the structure of individual messages in a conversation.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Unique message identifier
pub type MessageId = String;

/// Role of the message sender
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// System message (instructions, context)
    System,
    /// User message
    User,
    /// Assistant/AI response
    Assistant,
    /// Tool/function call result
    Tool,
    /// Function call (deprecated, use Tool)
    Function,
}

impl Role {
    /// Get string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            Role::System => "system",
            Role::User => "user",
            Role::Assistant => "assistant",
            Role::Tool => "tool",
            Role::Function => "function",
        }
    }
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Metadata associated with a message
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MessageMetadata {
    /// Token count (if computed)
    pub token_count: Option<usize>,
    /// Model used (for assistant messages)
    pub model: Option<String>,
    /// Finish reason (for assistant messages)
    pub finish_reason: Option<String>,
    /// Tool call ID (for tool messages)
    pub tool_call_id: Option<String>,
    /// Tool name (for tool messages)
    pub tool_name: Option<String>,
    /// Generation latency in milliseconds
    pub latency_ms: Option<u64>,
    /// Custom key-value pairs
    pub custom: HashMap<String, String>,
}

impl MessageMetadata {
    /// Create empty metadata
    pub fn new() -> Self {
        Self::default()
    }

    /// Set token count
    pub fn with_token_count(mut self, count: usize) -> Self {
        self.token_count = Some(count);
        self
    }

    /// Set model
    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    /// Set tool call ID
    pub fn with_tool_call_id(mut self, id: impl Into<String>) -> Self {
        self.tool_call_id = Some(id.into());
        self
    }

    /// Set custom metadata
    pub fn with_custom(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.custom.insert(key.into(), value.into());
        self
    }
}

/// A single message in a conversation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    /// Unique message ID
    pub id: MessageId,
    /// Message role
    pub role: Role,
    /// Message content
    pub content: String,
    /// Creation timestamp (Unix millis)
    pub created_at: u64,
    /// Message metadata
    pub metadata: MessageMetadata,
    /// Whether this message is a summary of older messages
    pub is_summary: bool,
    /// IDs of messages this summarizes (if is_summary is true)
    pub summarizes: Vec<MessageId>,
}

impl Message {
    /// Create a new message
    pub fn new(role: Role, content: impl Into<String>) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            id: generate_message_id(),
            role,
            content: content.into(),
            created_at: now,
            metadata: MessageMetadata::default(),
            is_summary: false,
            summarizes: Vec::new(),
        }
    }

    /// Create a system message
    pub fn system(content: impl Into<String>) -> Self {
        Self::new(Role::System, content)
    }

    /// Create a user message
    pub fn user(content: impl Into<String>) -> Self {
        Self::new(Role::User, content)
    }

    /// Create an assistant message
    pub fn assistant(content: impl Into<String>) -> Self {
        Self::new(Role::Assistant, content)
    }

    /// Create a tool result message
    pub fn tool(content: impl Into<String>, tool_call_id: impl Into<String>) -> Self {
        let mut msg = Self::new(Role::Tool, content);
        msg.metadata.tool_call_id = Some(tool_call_id.into());
        msg
    }

    /// Set message metadata
    pub fn with_metadata(mut self, metadata: MessageMetadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// Set token count
    pub fn with_token_count(mut self, count: usize) -> Self {
        self.metadata.token_count = Some(count);
        self
    }

    /// Set model
    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.metadata.model = Some(model.into());
        self
    }

    /// Mark as summary
    pub fn as_summary(mut self, summarized_ids: Vec<MessageId>) -> Self {
        self.is_summary = true;
        self.summarizes = summarized_ids;
        self
    }

    /// Get token count (estimated if not set)
    pub fn token_count(&self) -> usize {
        self.metadata.token_count.unwrap_or_else(|| {
            // Rough estimate: ~4 chars per token for English text
            self.content.len() / 4 + 1
        })
    }

    /// Estimate tokens for a given encoding
    pub fn estimate_tokens(&self) -> usize {
        // More accurate estimate based on role overhead + content
        let role_tokens = 4; // ~4 tokens for role marker
        let content_tokens = (self.content.len() as f32 / 3.5).ceil() as usize; // ~3.5 chars/token
        role_tokens + content_tokens
    }
}

/// Generate a unique message ID
fn generate_message_id() -> MessageId {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("msg-{:x}-{:04x}", now, counter & 0xFFFF)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_creation() {
        let msg = Message::user("Hello, world!");
        assert_eq!(msg.role, Role::User);
        assert_eq!(msg.content, "Hello, world!");
        assert!(!msg.is_summary);
    }

    #[test]
    fn test_role_display() {
        assert_eq!(Role::User.to_string(), "user");
        assert_eq!(Role::Assistant.to_string(), "assistant");
        assert_eq!(Role::System.to_string(), "system");
    }

    #[test]
    fn test_message_with_metadata() {
        let msg = Message::assistant("Response")
            .with_model("gpt-4")
            .with_token_count(10);

        assert_eq!(msg.metadata.model, Some("gpt-4".to_string()));
        assert_eq!(msg.metadata.token_count, Some(10));
    }

    #[test]
    fn test_tool_message() {
        let msg = Message::tool("Result", "call-123");
        assert_eq!(msg.role, Role::Tool);
        assert_eq!(msg.metadata.tool_call_id, Some("call-123".to_string()));
    }

    #[test]
    fn test_summary_message() {
        let msg = Message::assistant("Summary of conversation...")
            .as_summary(vec!["msg-1".to_string(), "msg-2".to_string()]);

        assert!(msg.is_summary);
        assert_eq!(msg.summarizes.len(), 2);
    }

    #[test]
    fn test_token_estimation() {
        let msg = Message::user("Hello, world!");
        let tokens = msg.estimate_tokens();
        assert!(tokens > 0);
        assert!(tokens < 20); // Should be reasonable
    }
}
