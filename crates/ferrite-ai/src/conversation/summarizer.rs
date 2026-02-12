//! Conversation summarization
//!
//! Automatic summarization of older messages to manage context windows.

use serde::{Deserialize, Serialize};

use super::message::{Message, MessageId, Role};
use super::session::Conversation;

/// Summary configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryConfig {
    /// Maximum tokens for summary
    pub max_summary_tokens: usize,
    /// Summarization prompt template
    pub prompt_template: String,
    /// Minimum messages to trigger summarization
    pub min_messages: usize,
    /// Whether to preserve system messages
    pub preserve_system: bool,
    /// Whether to preserve recent user message
    pub preserve_last_user: bool,
}

impl Default for SummaryConfig {
    fn default() -> Self {
        Self {
            max_summary_tokens: 500,
            prompt_template: DEFAULT_SUMMARY_PROMPT.to_string(),
            min_messages: 6,
            preserve_system: true,
            preserve_last_user: true,
        }
    }
}

const DEFAULT_SUMMARY_PROMPT: &str = r#"Summarize the following conversation concisely, preserving key information, decisions, and context needed for continuing the conversation:

{{messages}}

Summary:"#;

impl SummaryConfig {
    /// Set max summary tokens
    pub fn with_max_tokens(mut self, tokens: usize) -> Self {
        self.max_summary_tokens = tokens;
        self
    }

    /// Set custom prompt template
    pub fn with_prompt(mut self, template: impl Into<String>) -> Self {
        self.prompt_template = template.into();
        self
    }

    /// Set minimum messages
    pub fn with_min_messages(mut self, count: usize) -> Self {
        self.min_messages = count;
        self
    }
}

/// A summary of conversation messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Summary {
    /// Summary text
    pub text: String,
    /// IDs of messages that were summarized
    pub message_ids: Vec<MessageId>,
    /// Token count of original messages
    pub original_tokens: usize,
    /// Token count of summary
    pub summary_tokens: usize,
    /// Compression ratio
    pub compression_ratio: f32,
    /// Timestamp when summary was created
    pub created_at: u64,
}

impl Summary {
    /// Create a new summary
    pub fn new(text: impl Into<String>, message_ids: Vec<MessageId>) -> Self {
        let text = text.into();
        let summary_tokens = estimate_tokens(&text);

        Self {
            text,
            message_ids,
            original_tokens: 0,
            summary_tokens,
            compression_ratio: 0.0,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        }
    }

    /// Set original token count and calculate compression
    pub fn with_original_tokens(mut self, tokens: usize) -> Self {
        self.original_tokens = tokens;
        if tokens > 0 {
            self.compression_ratio = self.summary_tokens as f32 / tokens as f32;
        }
        self
    }

    /// Convert to a message
    pub fn to_message(&self) -> Message {
        Message::assistant(&self.text).as_summary(self.message_ids.clone())
    }
}

/// Summarization engine
pub struct Summarizer {
    config: SummaryConfig,
}

impl Summarizer {
    /// Create a new summarizer
    pub fn new(config: SummaryConfig) -> Self {
        Self { config }
    }

    /// Create with default config
    pub fn with_defaults() -> Self {
        Self::new(SummaryConfig::default())
    }

    /// Check if summarization is needed
    pub fn needs_summarization(
        &self,
        conversation: &Conversation,
        threshold_tokens: usize,
    ) -> bool {
        conversation.message_count() >= self.config.min_messages
            && conversation.total_tokens > threshold_tokens
    }

    /// Get messages that should be summarized
    pub fn messages_to_summarize<'a>(&self, conversation: &'a Conversation) -> Vec<&'a Message> {
        let messages = conversation.messages();
        if messages.len() < self.config.min_messages {
            return Vec::new();
        }

        // Keep the most recent messages
        let keep_count = messages.len() / 2;
        let summarize_count = messages.len() - keep_count;

        let mut to_summarize: Vec<&Message> = Vec::new();

        for msg in messages.iter().take(summarize_count) {
            // Skip system messages if configured to preserve
            if self.config.preserve_system && msg.role == Role::System {
                continue;
            }
            to_summarize.push(msg);
        }

        to_summarize
    }

    /// Build the summarization prompt
    pub fn build_prompt(&self, messages: &[&Message]) -> String {
        let messages_text: String = messages
            .iter()
            .map(|m| format!("{}: {}", m.role, m.content))
            .collect::<Vec<_>>()
            .join("\n\n");

        self.config
            .prompt_template
            .replace("{{messages}}", &messages_text)
    }

    /// Create a summary from text
    pub fn create_summary(&self, text: impl Into<String>, messages: &[&Message]) -> Summary {
        let message_ids: Vec<MessageId> = messages.iter().map(|m| m.id.clone()).collect();
        let original_tokens: usize = messages.iter().map(|m| m.token_count()).sum();

        Summary::new(text, message_ids).with_original_tokens(original_tokens)
    }

    /// Create a simple extractive summary (no LLM needed)
    pub fn create_extractive_summary(&self, messages: &[&Message]) -> Summary {
        if messages.is_empty() {
            return Summary::new("", vec![]);
        }

        // Extract key information from messages
        let mut summary_parts = Vec::new();

        // Add first user message (usually the initial query)
        if let Some(first_user) = messages.iter().find(|m| m.role == Role::User) {
            summary_parts.push(format!(
                "Initial query: {}",
                truncate(&first_user.content, 100)
            ));
        }

        // Add any decisions or conclusions from assistant
        let assistant_msgs: Vec<_> = messages
            .iter()
            .filter(|m| m.role == Role::Assistant)
            .collect();

        if let Some(last_assistant) = assistant_msgs.last() {
            summary_parts.push(format!(
                "Latest response: {}",
                truncate(&last_assistant.content, 200)
            ));
        }

        // Count message types
        let user_count = messages.iter().filter(|m| m.role == Role::User).count();
        let assistant_count = messages
            .iter()
            .filter(|m| m.role == Role::Assistant)
            .count();

        summary_parts.push(format!(
            "[Summary of {} exchanges]",
            user_count.min(assistant_count)
        ));

        let summary_text = summary_parts.join("\n");
        self.create_summary(summary_text, messages)
    }

    /// Get configuration
    pub fn config(&self) -> &SummaryConfig {
        &self.config
    }
}

/// Estimate token count
fn estimate_tokens(text: &str) -> usize {
    (text.len() as f32 / 3.5).ceil() as usize + 4
}

/// Truncate text to max length
fn truncate(text: &str, max_len: usize) -> String {
    if text.len() <= max_len {
        text.to_string()
    } else {
        format!("{}...", &text[..max_len.saturating_sub(3)])
    }
}

impl Default for Summarizer {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conversation::session::Conversation;

    #[test]
    fn test_summary_config() {
        let config = SummaryConfig::default()
            .with_max_tokens(300)
            .with_min_messages(4);

        assert_eq!(config.max_summary_tokens, 300);
        assert_eq!(config.min_messages, 4);
    }

    #[test]
    fn test_summary_creation() {
        let summary =
            Summary::new("This is a summary", vec!["msg-1".to_string()]).with_original_tokens(100);

        assert!(summary.compression_ratio > 0.0);
        assert!(summary.compression_ratio < 1.0);
    }

    #[test]
    fn test_summarizer_needs_summarization() {
        let summarizer = Summarizer::with_defaults();

        let mut conv = Conversation::new("user");
        // Add fewer than minimum messages
        conv.add_message(Message::user("Hello"));
        conv.add_message(Message::assistant("Hi"));

        assert!(!summarizer.needs_summarization(&conv, 100));

        // Add more messages
        for i in 0..10 {
            conv.add_message(Message::user(format!("Message {}", i)));
            conv.add_message(Message::assistant(format!("Response {}", i)));
        }

        assert!(summarizer.needs_summarization(&conv, 10));
    }

    #[test]
    fn test_build_prompt() {
        let summarizer = Summarizer::with_defaults();

        let msgs = vec![Message::user("Hello"), Message::assistant("Hi!")];
        let msg_refs: Vec<&Message> = msgs.iter().collect();

        let prompt = summarizer.build_prompt(&msg_refs);
        assert!(prompt.contains("Hello"));
        assert!(prompt.contains("Hi!"));
    }

    #[test]
    fn test_extractive_summary() {
        let summarizer = Summarizer::with_defaults();

        let msgs = vec![
            Message::user("What is Rust?"),
            Message::assistant("Rust is a systems programming language."),
            Message::user("Is it fast?"),
            Message::assistant("Yes, it's very fast and memory-safe."),
        ];
        let msg_refs: Vec<&Message> = msgs.iter().collect();

        let summary = summarizer.create_extractive_summary(&msg_refs);
        assert!(!summary.text.is_empty());
        assert_eq!(summary.message_ids.len(), 4);
    }

    #[test]
    fn test_summary_to_message() {
        let summary = Summary::new(
            "Summary text",
            vec!["msg-1".to_string(), "msg-2".to_string()],
        );
        let msg = summary.to_message();

        assert!(msg.is_summary);
        assert_eq!(msg.summarizes.len(), 2);
        assert_eq!(msg.role, Role::Assistant);
    }
}
