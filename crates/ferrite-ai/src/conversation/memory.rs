//! Conversation memory store
//!
//! Central store for managing multiple conversations with persistence.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use super::message::{Message, MessageId};
use super::session::{Conversation, ConversationId, ConversationState};
use super::summarizer::{Summarizer, SummaryConfig};
use super::window::{ContextWindow, WindowConfig};
use super::{ConversationError, Result};

/// Configuration for conversation store
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversationConfig {
    /// Maximum conversations per user
    pub max_conversations_per_user: usize,
    /// Default TTL for conversations (seconds, 0 = no expiry)
    pub default_ttl_secs: u64,
    /// Window configuration
    pub window: WindowConfig,
    /// Summary configuration
    pub summary: SummaryConfig,
    /// Enable automatic summarization
    pub auto_summarize: bool,
    /// Enable persistence
    pub persist: bool,
}

impl Default for ConversationConfig {
    fn default() -> Self {
        Self {
            max_conversations_per_user: 100,
            default_ttl_secs: 86400 * 7, // 1 week
            window: WindowConfig::default(),
            summary: SummaryConfig::default(),
            auto_summarize: true,
            persist: true,
        }
    }
}

impl ConversationConfig {
    /// Set max conversations per user
    pub fn with_max_per_user(mut self, max: usize) -> Self {
        self.max_conversations_per_user = max;
        self
    }

    /// Set default TTL
    pub fn with_ttl(mut self, secs: u64) -> Self {
        self.default_ttl_secs = secs;
        self
    }

    /// Set window config
    pub fn with_window(mut self, window: WindowConfig) -> Self {
        self.window = window;
        self
    }

    /// Disable auto summarization
    pub fn without_auto_summarize(mut self) -> Self {
        self.auto_summarize = false;
        self
    }
}

/// Conversation memory store statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConversationStats {
    /// Total conversations
    pub total_conversations: u64,
    /// Active conversations
    pub active_conversations: u64,
    /// Total messages
    pub total_messages: u64,
    /// Total tokens
    pub total_tokens: u64,
    /// Summaries created
    pub summaries_created: u64,
    /// Conversations expired
    pub conversations_expired: u64,
}

/// Central conversation memory store
pub struct ConversationStore {
    config: ConversationConfig,
    conversations: RwLock<HashMap<ConversationId, Conversation>>,
    user_conversations: RwLock<HashMap<String, Vec<ConversationId>>>,
    window: ContextWindow,
    summarizer: Summarizer,
    // Stats
    total_conversations: AtomicU64,
    total_messages: AtomicU64,
    total_tokens: AtomicU64,
    summaries_created: AtomicU64,
    expired: AtomicU64,
}

impl ConversationStore {
    /// Create a new conversation store
    pub fn new(config: ConversationConfig) -> Self {
        let window = ContextWindow::new(config.window.clone());
        let summarizer = Summarizer::new(config.summary.clone());

        Self {
            config,
            conversations: RwLock::new(HashMap::new()),
            user_conversations: RwLock::new(HashMap::new()),
            window,
            summarizer,
            total_conversations: AtomicU64::new(0),
            total_messages: AtomicU64::new(0),
            total_tokens: AtomicU64::new(0),
            summaries_created: AtomicU64::new(0),
            expired: AtomicU64::new(0),
        }
    }

    /// Create with default configuration
    pub fn with_defaults() -> Self {
        Self::new(ConversationConfig::default())
    }

    /// Create a new conversation
    pub fn create(&self, user_id: impl Into<String>) -> Result<ConversationId> {
        let user_id = user_id.into();

        // Check per-user limit
        let user_convs = self.user_conversations.read();
        if let Some(convs) = user_convs.get(&user_id) {
            if convs.len() >= self.config.max_conversations_per_user {
                return Err(ConversationError::InvalidOperation(
                    "maximum conversations per user reached".to_string(),
                ));
            }
        }
        drop(user_convs);

        // Create conversation
        let mut conversation = Conversation::new(&user_id);
        if self.config.default_ttl_secs > 0 {
            conversation = conversation.with_ttl(self.config.default_ttl_secs);
        }

        let id = conversation.id.clone();

        // Store conversation
        self.conversations.write().insert(id.clone(), conversation);

        // Update user index
        self.user_conversations
            .write()
            .entry(user_id)
            .or_default()
            .push(id.clone());

        self.total_conversations.fetch_add(1, Ordering::Relaxed);

        Ok(id)
    }

    /// Get a conversation by ID
    pub fn get(&self, id: &str) -> Option<Conversation> {
        self.conversations.read().get(id).cloned()
    }

    /// Check if conversation exists
    pub fn exists(&self, id: &str) -> bool {
        self.conversations.read().contains_key(id)
    }

    /// Delete a conversation
    pub fn delete(&self, id: &str) -> bool {
        let mut conversations = self.conversations.write();
        if let Some(conv) = conversations.remove(id) {
            // Update user index
            let mut user_convs = self.user_conversations.write();
            if let Some(convs) = user_convs.get_mut(&conv.user_id) {
                convs.retain(|c| c != id);
            }
            return true;
        }
        false
    }

    /// List conversations for a user
    pub fn list_for_user(&self, user_id: &str) -> Vec<ConversationId> {
        self.user_conversations
            .read()
            .get(user_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Add a message to a conversation
    pub fn add_message(&self, conversation_id: &str, message: Message) -> Result<MessageId> {
        let mut conversations = self.conversations.write();
        let conversation = conversations
            .get_mut(conversation_id)
            .ok_or_else(|| ConversationError::NotFound(conversation_id.to_string()))?;

        let msg_id = message.id.clone();
        let tokens = message.token_count();

        conversation.add_message(message);

        self.total_messages.fetch_add(1, Ordering::Relaxed);
        self.total_tokens
            .fetch_add(tokens as u64, Ordering::Relaxed);

        // Check if auto-summarization is needed
        if self.config.auto_summarize {
            let threshold = self.config.window.max_tokens;
            if self.summarizer.needs_summarization(conversation, threshold) {
                // Perform summarization
                let to_summarize = self.summarizer.messages_to_summarize(conversation);
                if !to_summarize.is_empty() {
                    let summary = self.summarizer.create_extractive_summary(&to_summarize);
                    let summary_msg = summary.to_message();
                    let summarize_count = to_summarize.len();

                    conversation.summarize(summary_msg, summarize_count);
                    self.summaries_created.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        Ok(msg_id)
    }

    /// Get context for a conversation
    pub fn get_context(&self, conversation_id: &str) -> Result<Vec<Message>> {
        let conversations = self.conversations.read();
        let conversation = conversations
            .get(conversation_id)
            .ok_or_else(|| ConversationError::NotFound(conversation_id.to_string()))?;

        let context: Vec<Message> = self
            .window
            .get_context(conversation)
            .into_iter()
            .cloned()
            .collect();

        Ok(context)
    }

    /// Get context with token limit
    pub fn get_context_with_limit(
        &self,
        conversation_id: &str,
        max_tokens: usize,
    ) -> Result<Vec<Message>> {
        let conversations = self.conversations.read();
        let conversation = conversations
            .get(conversation_id)
            .ok_or_else(|| ConversationError::NotFound(conversation_id.to_string()))?;

        let context: Vec<Message> = conversation
            .messages_within_tokens(max_tokens)
            .into_iter()
            .cloned()
            .collect();

        Ok(context)
    }

    /// Set system prompt for a conversation
    pub fn set_system_prompt(
        &self,
        conversation_id: &str,
        prompt: impl Into<String>,
    ) -> Result<()> {
        let mut conversations = self.conversations.write();
        let conversation = conversations
            .get_mut(conversation_id)
            .ok_or_else(|| ConversationError::NotFound(conversation_id.to_string()))?;

        conversation.system_prompt = Some(prompt.into());
        Ok(())
    }

    /// Clear messages in a conversation
    pub fn clear_messages(&self, conversation_id: &str) -> Result<()> {
        let mut conversations = self.conversations.write();
        let conversation = conversations
            .get_mut(conversation_id)
            .ok_or_else(|| ConversationError::NotFound(conversation_id.to_string()))?;

        conversation.clear_messages();
        Ok(())
    }

    /// Set conversation title
    pub fn set_title(&self, conversation_id: &str, title: impl Into<String>) -> Result<()> {
        let mut conversations = self.conversations.write();
        let conversation = conversations
            .get_mut(conversation_id)
            .ok_or_else(|| ConversationError::NotFound(conversation_id.to_string()))?;

        conversation.title = Some(title.into());
        Ok(())
    }

    /// Archive a conversation
    pub fn archive(&self, conversation_id: &str) -> Result<()> {
        let mut conversations = self.conversations.write();
        let conversation = conversations
            .get_mut(conversation_id)
            .ok_or_else(|| ConversationError::NotFound(conversation_id.to_string()))?;

        conversation.state = ConversationState::Archived;
        Ok(())
    }

    /// Get conversation statistics
    pub fn conversation_stats(
        &self,
        conversation_id: &str,
    ) -> Result<super::session::ConversationStats> {
        let conversations = self.conversations.read();
        let conversation = conversations
            .get(conversation_id)
            .ok_or_else(|| ConversationError::NotFound(conversation_id.to_string()))?;

        Ok(conversation.stats())
    }

    /// Cleanup expired conversations
    pub fn cleanup_expired(&self) -> usize {
        let mut conversations = self.conversations.write();
        let mut user_convs = self.user_conversations.write();

        let expired: Vec<_> = conversations
            .iter()
            .filter(|(_, c)| c.is_expired())
            .map(|(id, c)| (id.clone(), c.user_id.clone()))
            .collect();

        let count = expired.len();

        for (id, user_id) in expired {
            conversations.remove(&id);
            if let Some(convs) = user_convs.get_mut(&user_id) {
                convs.retain(|c| c != &id);
            }
        }

        self.expired.fetch_add(count as u64, Ordering::Relaxed);
        count
    }

    /// Get store statistics
    pub fn stats(&self) -> ConversationStats {
        let conversations = self.conversations.read();
        let active = conversations
            .values()
            .filter(|c| c.state == ConversationState::Active)
            .count() as u64;

        ConversationStats {
            total_conversations: self.total_conversations.load(Ordering::Relaxed),
            active_conversations: active,
            total_messages: self.total_messages.load(Ordering::Relaxed),
            total_tokens: self.total_tokens.load(Ordering::Relaxed),
            summaries_created: self.summaries_created.load(Ordering::Relaxed),
            conversations_expired: self.expired.load(Ordering::Relaxed),
        }
    }

    /// Get configuration
    pub fn config(&self) -> &ConversationConfig {
        &self.config
    }

    /// Get number of conversations
    pub fn count(&self) -> usize {
        self.conversations.read().len()
    }
}

impl Default for ConversationStore {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_store_creation() {
        let store = ConversationStore::with_defaults();
        assert_eq!(store.count(), 0);
    }

    #[test]
    fn test_create_conversation() {
        let store = ConversationStore::with_defaults();

        let id = store.create("user-123").unwrap();
        assert!(!id.is_empty());
        assert!(store.exists(&id));
    }

    #[test]
    fn test_add_messages() {
        let store = ConversationStore::with_defaults();
        let conv_id = store.create("user-123").unwrap();

        let msg_id = store.add_message(&conv_id, Message::user("Hello")).unwrap();
        assert!(!msg_id.is_empty());

        store
            .add_message(&conv_id, Message::assistant("Hi there!"))
            .unwrap();

        let conv = store.get(&conv_id).unwrap();
        assert_eq!(conv.message_count(), 2);
    }

    #[test]
    fn test_get_context() {
        let store = ConversationStore::with_defaults();
        let conv_id = store.create("user-123").unwrap();

        store
            .set_system_prompt(&conv_id, "You are helpful")
            .unwrap();
        store.add_message(&conv_id, Message::user("Hello")).unwrap();
        store
            .add_message(&conv_id, Message::assistant("Hi!"))
            .unwrap();

        let context = store.get_context(&conv_id).unwrap();
        assert_eq!(context.len(), 2);
    }

    #[test]
    fn test_list_user_conversations() {
        let store = ConversationStore::with_defaults();

        store.create("user-1").unwrap();
        store.create("user-1").unwrap();
        store.create("user-2").unwrap();

        let user1_convs = store.list_for_user("user-1");
        assert_eq!(user1_convs.len(), 2);

        let user2_convs = store.list_for_user("user-2");
        assert_eq!(user2_convs.len(), 1);
    }

    #[test]
    fn test_delete_conversation() {
        let store = ConversationStore::with_defaults();
        let conv_id = store.create("user-123").unwrap();

        assert!(store.exists(&conv_id));
        assert!(store.delete(&conv_id));
        assert!(!store.exists(&conv_id));
    }

    #[test]
    fn test_store_stats() {
        let store = ConversationStore::with_defaults();
        let conv_id = store.create("user-123").unwrap();

        store.add_message(&conv_id, Message::user("Hello")).unwrap();
        store
            .add_message(&conv_id, Message::assistant("Hi"))
            .unwrap();

        let stats = store.stats();
        assert_eq!(stats.total_conversations, 1);
        assert_eq!(stats.total_messages, 2);
        assert!(stats.total_tokens > 0);
    }

    #[test]
    fn test_max_conversations_limit() {
        let config = ConversationConfig::default().with_max_per_user(2);
        let store = ConversationStore::new(config);

        store.create("user-1").unwrap();
        store.create("user-1").unwrap();

        let result = store.create("user-1");
        assert!(result.is_err());
    }
}
