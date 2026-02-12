//! Conversation Memory Store
//!
//! Stateful LLM conversation management with configurable context windows,
//! automatic summarization, and RAG integration.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                 Conversation Memory Store                    │
//! ├─────────────────────────────────────────────────────────────┤
//! │  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐ │
//! │  │ Message  │   │ Context  │   │  Token   │   │ Summary  │ │
//! │  │  Store   │──▶│ Window   │──▶│ Counter  │──▶│ Manager  │ │
//! │  └──────────┘   └──────────┘   └──────────┘   └──────────┘ │
//! │       │              │              │              │        │
//! │       ▼              ▼              ▼              ▼        │
//! │  Persistence   Sliding/Fixed   tiktoken/    Auto-Summarize │
//! │                    Window       approx                      │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Features
//!
//! - **Multi-turn Conversations**: Track messages with roles and metadata
//! - **Context Windows**: Sliding, fixed, or token-based window management
//! - **Token Counting**: Accurate token counting for context management
//! - **Summarization**: Automatic summarization of older messages
//! - **Persistence**: Durable storage with optional TTL
//!
//! # Example
//!
//! ```ignore
//! use ferrite::conversation::{ConversationStore, Message, Role};
//!
//! let store = ConversationStore::new();
//!
//! // Create a conversation
//! let conv_id = store.create("user:123")?;
//!
//! // Add messages
//! store.add_message(conv_id, Message::user("Hello!"))?;
//! store.add_message(conv_id, Message::assistant("Hi there!"))?;
//!
//! // Get context window
//! let context = store.get_context(conv_id, 4096)?; // max tokens
//! ```

mod memory;
mod message;
mod session;
mod summarizer;
mod window;

pub use memory::{ConversationConfig, ConversationStats, ConversationStore};
pub use message::{Message, MessageId, MessageMetadata, Role};
pub use session::{Conversation, ConversationId, ConversationState};
pub use summarizer::{Summarizer, Summary, SummaryConfig};
pub use window::{ContextWindow, WindowConfig, WindowType};

/// Error types for conversation operations
#[derive(Debug, Clone, thiserror::Error)]
pub enum ConversationError {
    /// Conversation not found
    #[error("conversation not found: {0}")]
    NotFound(String),

    /// Message not found
    #[error("message not found: {0}")]
    MessageNotFound(String),

    /// Invalid operation
    #[error("invalid operation: {0}")]
    InvalidOperation(String),

    /// Token limit exceeded
    #[error("token limit exceeded: {0} > {1}")]
    TokenLimitExceeded(usize, usize),

    /// Storage error
    #[error("storage error: {0}")]
    StorageError(String),
}

/// Result type for conversation operations
pub type Result<T> = std::result::Result<T, ConversationError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = ConversationError::NotFound("conv-123".to_string());
        assert!(err.to_string().contains("conv-123"));
    }
}
