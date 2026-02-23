//! Conversation session management
//!
//! Tracks individual conversation sessions with their messages and state.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use super::message::{Message, MessageId, Role};

/// Unique conversation identifier
pub type ConversationId = String;

/// Conversation state
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConversationState {
    /// Active conversation
    #[default]
    Active,
    /// Paused/suspended
    Paused,
    /// Archived (read-only)
    Archived,
    /// Deleted (pending cleanup)
    Deleted,
}

/// A conversation session containing multiple messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Conversation {
    /// Unique conversation ID
    pub id: ConversationId,
    /// User or session identifier
    pub user_id: String,
    /// Optional title/name
    pub title: Option<String>,
    /// Conversation state
    pub state: ConversationState,
    /// Creation timestamp (Unix millis)
    pub created_at: u64,
    /// Last activity timestamp (Unix millis)
    pub updated_at: u64,
    /// Messages in order
    messages: Vec<Message>,
    /// Message ID to index mapping
    #[serde(skip)]
    message_index: HashMap<MessageId, usize>,
    /// System prompt (prepended to all contexts)
    pub system_prompt: Option<String>,
    /// Total token count (approximate)
    pub total_tokens: usize,
    /// Custom metadata
    pub metadata: HashMap<String, String>,
    /// Time-to-live in seconds (0 = no expiry)
    pub ttl_secs: u64,
}

impl Conversation {
    /// Create a new conversation
    pub fn new(user_id: impl Into<String>) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            id: generate_conversation_id(),
            user_id: user_id.into(),
            title: None,
            state: ConversationState::Active,
            created_at: now,
            updated_at: now,
            messages: Vec::new(),
            message_index: HashMap::new(),
            system_prompt: None,
            total_tokens: 0,
            metadata: HashMap::new(),
            ttl_secs: 0,
        }
    }

    /// Create with specific ID
    pub fn with_id(id: impl Into<String>, user_id: impl Into<String>) -> Self {
        let mut conv = Self::new(user_id);
        conv.id = id.into();
        conv
    }

    /// Set title
    pub fn with_title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    /// Set system prompt
    pub fn with_system_prompt(mut self, prompt: impl Into<String>) -> Self {
        self.system_prompt = Some(prompt.into());
        self
    }

    /// Set TTL
    pub fn with_ttl(mut self, secs: u64) -> Self {
        self.ttl_secs = secs;
        self
    }

    /// Add a message to the conversation
    pub fn add_message(&mut self, message: Message) {
        let token_count = message.token_count();
        let id = message.id.clone();
        let idx = self.messages.len();

        self.messages.push(message);
        self.message_index.insert(id, idx);
        self.total_tokens += token_count;
        self.update_timestamp();
    }

    /// Get a message by ID
    pub fn get_message(&self, id: &str) -> Option<&Message> {
        self.message_index
            .get(id)
            .and_then(|&idx| self.messages.get(idx))
    }

    /// Get all messages
    pub fn messages(&self) -> &[Message] {
        &self.messages
    }

    /// Get message count
    pub fn message_count(&self) -> usize {
        self.messages.len()
    }

    /// Get the last N messages
    pub fn last_messages(&self, n: usize) -> &[Message] {
        if n >= self.messages.len() {
            &self.messages
        } else {
            &self.messages[self.messages.len() - n..]
        }
    }

    /// Get messages by role
    pub fn messages_by_role(&self, role: Role) -> Vec<&Message> {
        self.messages.iter().filter(|m| m.role == role).collect()
    }

    /// Get messages within token limit
    pub fn messages_within_tokens(&self, max_tokens: usize) -> Vec<&Message> {
        let mut result = Vec::new();
        let mut token_count = 0;

        // Include system prompt tokens if present
        if let Some(ref prompt) = self.system_prompt {
            token_count += prompt.len() / 4 + 4; // Rough estimate
        }

        // Iterate from most recent
        for msg in self.messages.iter().rev() {
            let msg_tokens = msg.token_count();
            if token_count + msg_tokens > max_tokens {
                break;
            }
            result.push(msg);
            token_count += msg_tokens;
        }

        // Reverse to get chronological order
        result.reverse();
        result
    }

    /// Clear all messages
    pub fn clear_messages(&mut self) {
        self.messages.clear();
        self.message_index.clear();
        self.total_tokens = 0;
        self.update_timestamp();
    }

    /// Remove messages older than the specified index
    pub fn trim_to(&mut self, keep_last: usize) {
        if keep_last >= self.messages.len() {
            return;
        }

        let remove_count = self.messages.len() - keep_last;
        let removed: Vec<_> = self.messages.drain(..remove_count).collect();

        // Update token count
        for msg in &removed {
            self.total_tokens = self.total_tokens.saturating_sub(msg.token_count());
        }

        // Rebuild index
        self.message_index.clear();
        for (idx, msg) in self.messages.iter().enumerate() {
            self.message_index.insert(msg.id.clone(), idx);
        }

        self.update_timestamp();
    }

    /// Replace older messages with a summary
    pub fn summarize(&mut self, summary: Message, summarized_count: usize) {
        if summarized_count == 0 || summarized_count >= self.messages.len() {
            return;
        }

        // Remove old messages
        let removed: Vec<_> = self.messages.drain(..summarized_count).collect();
        for msg in &removed {
            self.total_tokens = self.total_tokens.saturating_sub(msg.token_count());
        }

        // Add summary at the beginning
        self.total_tokens += summary.token_count();
        self.messages.insert(0, summary);

        // Rebuild index
        self.message_index.clear();
        for (idx, msg) in self.messages.iter().enumerate() {
            self.message_index.insert(msg.id.clone(), idx);
        }

        self.update_timestamp();
    }

    /// Check if conversation is expired
    pub fn is_expired(&self) -> bool {
        if self.ttl_secs == 0 {
            return false;
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        now > self.updated_at + (self.ttl_secs * 1000)
    }

    /// Update the timestamp
    fn update_timestamp(&mut self) {
        self.updated_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
    }

    /// Get conversation statistics
    pub fn stats(&self) -> ConversationStats {
        let mut user_messages = 0;
        let mut assistant_messages = 0;
        let mut system_messages = 0;
        let mut tool_messages = 0;

        for msg in &self.messages {
            match msg.role {
                Role::User => user_messages += 1,
                Role::Assistant => assistant_messages += 1,
                Role::System => system_messages += 1,
                Role::Tool | Role::Function => tool_messages += 1,
            }
        }

        ConversationStats {
            message_count: self.messages.len(),
            user_messages,
            assistant_messages,
            system_messages,
            tool_messages,
            total_tokens: self.total_tokens,
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

/// Statistics for a conversation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversationStats {
    /// Total message count
    pub message_count: usize,
    /// User messages
    pub user_messages: usize,
    /// Assistant messages
    pub assistant_messages: usize,
    /// System messages
    pub system_messages: usize,
    /// Tool/function messages
    pub tool_messages: usize,
    /// Total tokens
    pub total_tokens: usize,
    /// Creation time
    pub created_at: u64,
    /// Last update time
    pub updated_at: u64,
}

/// Generate a unique conversation ID
fn generate_conversation_id() -> ConversationId {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("conv-{:x}-{:04x}", now, counter & 0xFFFF)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_conversation_creation() {
        let conv = Conversation::new("user-123");
        assert_eq!(conv.user_id, "user-123");
        assert_eq!(conv.state, ConversationState::Active);
        assert_eq!(conv.message_count(), 0);
    }

    #[test]
    fn test_add_messages() {
        let mut conv = Conversation::new("user-123");

        conv.add_message(Message::user("Hello"));
        conv.add_message(Message::assistant("Hi there!"));

        assert_eq!(conv.message_count(), 2);
        assert!(conv.total_tokens > 0);
    }

    #[test]
    fn test_get_message() {
        let mut conv = Conversation::new("user-123");
        let msg = Message::user("Test message");
        let msg_id = msg.id.clone();

        conv.add_message(msg);

        let retrieved = conv.get_message(&msg_id);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().content, "Test message");
    }

    #[test]
    fn test_last_messages() {
        let mut conv = Conversation::new("user-123");

        conv.add_message(Message::user("One"));
        conv.add_message(Message::assistant("Two"));
        conv.add_message(Message::user("Three"));

        let last_two = conv.last_messages(2);
        assert_eq!(last_two.len(), 2);
        assert_eq!(last_two[0].content, "Two");
        assert_eq!(last_two[1].content, "Three");
    }

    #[test]
    fn test_trim_messages() {
        let mut conv = Conversation::new("user-123");

        conv.add_message(Message::user("One"));
        conv.add_message(Message::assistant("Two"));
        conv.add_message(Message::user("Three"));
        conv.add_message(Message::assistant("Four"));

        conv.trim_to(2);

        assert_eq!(conv.message_count(), 2);
        assert_eq!(conv.messages()[0].content, "Three");
    }

    #[test]
    fn test_conversation_stats() {
        let mut conv = Conversation::new("user-123");

        conv.add_message(Message::system("You are helpful"));
        conv.add_message(Message::user("Hello"));
        conv.add_message(Message::assistant("Hi!"));
        conv.add_message(Message::user("Bye"));

        let stats = conv.stats();
        assert_eq!(stats.message_count, 4);
        assert_eq!(stats.user_messages, 2);
        assert_eq!(stats.assistant_messages, 1);
        assert_eq!(stats.system_messages, 1);
    }
}
