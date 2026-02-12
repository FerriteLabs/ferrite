//! Context window management
//!
//! Manages the sliding context window for LLM conversations.

use serde::{Deserialize, Serialize};

use super::message::Message;
use super::session::Conversation;

/// Type of context window
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum WindowType {
    /// Fixed number of messages
    FixedMessages,
    /// Sliding window based on token count
    #[default]
    TokenBased,
    /// Sliding window with summarization
    Summarizing,
    /// Keep all messages (no window)
    Unlimited,
}



/// Context window configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowConfig {
    /// Window type
    pub window_type: WindowType,
    /// Maximum tokens in context
    pub max_tokens: usize,
    /// Maximum messages (for FixedMessages type)
    pub max_messages: usize,
    /// Reserved tokens for response
    pub reserved_for_response: usize,
    /// Always include system message
    pub include_system: bool,
    /// Token threshold for summarization
    pub summarize_threshold: usize,
}

impl Default for WindowConfig {
    fn default() -> Self {
        Self {
            window_type: WindowType::TokenBased,
            max_tokens: 4096,
            max_messages: 50,
            reserved_for_response: 1024,
            include_system: true,
            summarize_threshold: 3072,
        }
    }
}

impl WindowConfig {
    /// Create config for GPT-3.5 (4K context)
    pub fn gpt3_5() -> Self {
        Self {
            max_tokens: 4096,
            reserved_for_response: 1024,
            summarize_threshold: 3000,
            ..Default::default()
        }
    }

    /// Create config for GPT-4 (8K context)
    pub fn gpt4() -> Self {
        Self {
            max_tokens: 8192,
            reserved_for_response: 2048,
            summarize_threshold: 6000,
            ..Default::default()
        }
    }

    /// Create config for GPT-4 Turbo (128K context)
    pub fn gpt4_turbo() -> Self {
        Self {
            max_tokens: 128000,
            reserved_for_response: 4096,
            summarize_threshold: 100000,
            ..Default::default()
        }
    }

    /// Create config for Claude (200K context)
    pub fn claude() -> Self {
        Self {
            max_tokens: 200000,
            reserved_for_response: 4096,
            summarize_threshold: 150000,
            ..Default::default()
        }
    }

    /// Set max tokens
    pub fn with_max_tokens(mut self, tokens: usize) -> Self {
        self.max_tokens = tokens;
        self
    }

    /// Set max messages
    pub fn with_max_messages(mut self, count: usize) -> Self {
        self.max_messages = count;
        self
    }

    /// Available tokens for context (excluding reserved)
    pub fn available_tokens(&self) -> usize {
        self.max_tokens.saturating_sub(self.reserved_for_response)
    }
}

/// Context window manager
#[derive(Debug, Clone)]
pub struct ContextWindow {
    config: WindowConfig,
}

impl ContextWindow {
    /// Create a new context window
    pub fn new(config: WindowConfig) -> Self {
        Self { config }
    }

    /// Create with default config
    pub fn with_defaults() -> Self {
        Self::new(WindowConfig::default())
    }

    /// Get messages that fit in the context window
    pub fn get_context<'a>(&self, conversation: &'a Conversation) -> Vec<&'a Message> {
        match self.config.window_type {
            WindowType::FixedMessages => self.fixed_window(conversation),
            WindowType::TokenBased => self.token_window(conversation),
            WindowType::Summarizing => self.summarizing_window(conversation),
            WindowType::Unlimited => conversation.messages().iter().collect(),
        }
    }

    /// Fixed message count window
    fn fixed_window<'a>(&self, conversation: &'a Conversation) -> Vec<&'a Message> {
        let messages = conversation.messages();
        let max = self.config.max_messages;

        if messages.len() <= max {
            messages.iter().collect()
        } else {
            messages[messages.len() - max..].iter().collect()
        }
    }

    /// Token-based sliding window
    fn token_window<'a>(&self, conversation: &'a Conversation) -> Vec<&'a Message> {
        let available = self.config.available_tokens();
        let mut result = Vec::new();
        let mut token_count = 0;

        // Account for system prompt
        if self.config.include_system {
            if let Some(ref prompt) = conversation.system_prompt {
                token_count += estimate_tokens(prompt);
            }
        }

        // Add messages from most recent, working backwards
        for msg in conversation.messages().iter().rev() {
            let msg_tokens = msg.token_count();

            if token_count + msg_tokens > available {
                break;
            }

            result.push(msg);
            token_count += msg_tokens;
        }

        // Reverse to get chronological order
        result.reverse();
        result
    }

    /// Summarizing window (marks when summarization needed)
    fn summarizing_window<'a>(&self, conversation: &'a Conversation) -> Vec<&'a Message> {
        // For now, same as token window
        // Summarization is handled by the store
        self.token_window(conversation)
    }

    /// Check if summarization is needed
    pub fn needs_summarization(&self, conversation: &Conversation) -> bool {
        if self.config.window_type != WindowType::Summarizing {
            return false;
        }

        conversation.total_tokens > self.config.summarize_threshold
    }

    /// Get the number of messages to summarize
    pub fn messages_to_summarize(&self, conversation: &Conversation) -> usize {
        if !self.needs_summarization(conversation) {
            return 0;
        }

        // Summarize approximately half of the messages
        let total = conversation.message_count();
        if total <= 4 {
            return 0;
        }

        total / 2
    }

    /// Build context array for API call
    pub fn build_context(&self, conversation: &Conversation) -> Vec<ContextMessage> {
        let mut result = Vec::new();

        // Add system prompt if present
        if self.config.include_system {
            if let Some(ref prompt) = conversation.system_prompt {
                result.push(ContextMessage {
                    role: "system".to_string(),
                    content: prompt.clone(),
                });
            }
        }

        // Add messages from window
        for msg in self.get_context(conversation) {
            result.push(ContextMessage {
                role: msg.role.to_string(),
                content: msg.content.clone(),
            });
        }

        result
    }

    /// Get configuration
    pub fn config(&self) -> &WindowConfig {
        &self.config
    }

    /// Get token usage stats
    pub fn token_stats(&self, conversation: &Conversation) -> TokenStats {
        let context = self.get_context(conversation);
        let context_tokens: usize = context.iter().map(|m| m.token_count()).sum();

        let system_tokens = conversation
            .system_prompt
            .as_ref()
            .map(|p| estimate_tokens(p))
            .unwrap_or(0);

        TokenStats {
            context_tokens,
            system_tokens,
            total_tokens: context_tokens + system_tokens,
            available_tokens: self.config.available_tokens(),
            max_tokens: self.config.max_tokens,
            usage_percent: ((context_tokens + system_tokens) as f64
                / self.config.available_tokens() as f64
                * 100.0) as u8,
        }
    }
}

/// Message format for API context
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextMessage {
    /// Role
    pub role: String,
    /// Content
    pub content: String,
}

/// Token usage statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenStats {
    /// Tokens used by context messages
    pub context_tokens: usize,
    /// Tokens used by system prompt
    pub system_tokens: usize,
    /// Total tokens used
    pub total_tokens: usize,
    /// Available tokens
    pub available_tokens: usize,
    /// Maximum tokens
    pub max_tokens: usize,
    /// Usage percentage
    pub usage_percent: u8,
}

/// Estimate tokens for text
fn estimate_tokens(text: &str) -> usize {
    // Rough estimate: ~3.5 characters per token for English
    (text.len() as f32 / 3.5).ceil() as usize + 4 // +4 for formatting overhead
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conversation::message::Message;

    #[test]
    fn test_window_config_presets() {
        let gpt3 = WindowConfig::gpt3_5();
        assert_eq!(gpt3.max_tokens, 4096);

        let gpt4 = WindowConfig::gpt4();
        assert_eq!(gpt4.max_tokens, 8192);

        let claude = WindowConfig::claude();
        assert_eq!(claude.max_tokens, 200000);
    }

    #[test]
    fn test_fixed_window() {
        let config = WindowConfig {
            window_type: WindowType::FixedMessages,
            max_messages: 3,
            ..Default::default()
        };
        let window = ContextWindow::new(config);

        let mut conv = Conversation::new("user");
        conv.add_message(Message::user("One"));
        conv.add_message(Message::assistant("Two"));
        conv.add_message(Message::user("Three"));
        conv.add_message(Message::assistant("Four"));
        conv.add_message(Message::user("Five"));

        let context = window.get_context(&conv);
        assert_eq!(context.len(), 3);
        assert_eq!(context[0].content, "Three");
    }

    #[test]
    fn test_token_window() {
        let config = WindowConfig {
            window_type: WindowType::TokenBased,
            max_tokens: 100,
            reserved_for_response: 20,
            ..Default::default()
        };
        let window = ContextWindow::new(config);

        let mut conv = Conversation::new("user");
        conv.add_message(Message::user("Short message"));
        conv.add_message(Message::assistant("Another short one"));

        let context = window.get_context(&conv);
        assert!(!context.is_empty());
    }

    #[test]
    fn test_build_context() {
        let window = ContextWindow::with_defaults();

        let mut conv = Conversation::new("user").with_system_prompt("You are helpful");

        conv.add_message(Message::user("Hello"));
        conv.add_message(Message::assistant("Hi!"));

        let context = window.build_context(&conv);
        assert_eq!(context.len(), 3); // system + 2 messages
        assert_eq!(context[0].role, "system");
    }

    #[test]
    fn test_token_stats() {
        let window = ContextWindow::with_defaults();

        let mut conv = Conversation::new("user");
        conv.add_message(Message::user("Hello world"));
        conv.add_message(Message::assistant("Hi there!"));

        let stats = window.token_stats(&conv);
        assert!(stats.total_tokens > 0);
        assert!(stats.usage_percent < 100);
    }
}
