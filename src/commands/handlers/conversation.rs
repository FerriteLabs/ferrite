//! Conversation Memory command handlers
//!
//! Implements commands for LLM conversation management:
//! - CONV.CREATE - Create a new conversation
//! - CONV.DELETE - Delete a conversation
//! - CONV.MESSAGE - Add a message
//! - CONV.CONTEXT - Get context window
//! - CONV.LIST - List conversations for user
//! - CONV.INFO - Get conversation info
//! - CONV.CLEAR - Clear messages
//! - CONV.SYSTEM - Set system prompt
//! - CONV.STATS - Get statistics

use std::sync::OnceLock;

use bytes::Bytes;

use super::{err_frame, ok_frame, HandlerContext};
use ferrite_ai::conversation::{ConversationConfig, ConversationStore, Message, Role};
use crate::protocol::Frame;

/// Global conversation store singleton
static CONVERSATION_STORE: OnceLock<ConversationStore> = OnceLock::new();

/// Get or initialize the conversation store
fn get_store() -> &'static ConversationStore {
    CONVERSATION_STORE.get_or_init(|| ConversationStore::new(ConversationConfig::default()))
}

/// Handle CONV.CREATE command
///
/// Syntax: CONV.CREATE user_id [TITLE title] [SYSTEM prompt] [TTL secs]
pub fn conv_create(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'CONV.CREATE' command");
    }

    let user_id = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_string(),
        Err(_) => return err_frame("invalid user_id"),
    };

    // Create conversation
    let conv_id = match get_store().create(&user_id) {
        Ok(id) => id,
        Err(e) => return err_frame(&format!("failed to create conversation: {}", e)),
    };

    // Parse optional arguments
    let mut i = 1;
    while i < args.len() {
        let arg = match std::str::from_utf8(&args[i]) {
            Ok(s) => s.to_uppercase(),
            Err(_) => return err_frame("invalid argument"),
        };

        match arg.as_str() {
            "TITLE" => {
                i += 1;
                if i >= args.len() {
                    return err_frame("TITLE requires a value");
                }
                let title = match std::str::from_utf8(&args[i]) {
                    Ok(s) => s.to_string(),
                    Err(_) => return err_frame("invalid title"),
                };
                if let Err(e) = get_store().set_title(&conv_id, title) {
                    return err_frame(&format!("failed to set title: {}", e));
                }
            }
            "SYSTEM" => {
                i += 1;
                if i >= args.len() {
                    return err_frame("SYSTEM requires a value");
                }
                let prompt = match std::str::from_utf8(&args[i]) {
                    Ok(s) => s.to_string(),
                    Err(_) => return err_frame("invalid system prompt"),
                };
                if let Err(e) = get_store().set_system_prompt(&conv_id, prompt) {
                    return err_frame(&format!("failed to set system prompt: {}", e));
                }
            }
            _ => {
                return err_frame(&format!("unknown argument: {}", arg));
            }
        }
        i += 1;
    }

    Frame::bulk(conv_id)
}

/// Handle CONV.DELETE command
///
/// Syntax: CONV.DELETE conversation_id
pub fn conv_delete(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'CONV.DELETE' command");
    }

    let conv_id = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return err_frame("invalid conversation_id"),
    };

    if get_store().delete(conv_id) {
        ok_frame()
    } else {
        err_frame("conversation not found")
    }
}

/// Handle CONV.MESSAGE command
///
/// Syntax: CONV.MESSAGE conversation_id role content [MODEL model] [TOKENS count]
pub fn conv_message(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 3 {
        return err_frame("wrong number of arguments for 'CONV.MESSAGE' command");
    }

    let conv_id = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return err_frame("invalid conversation_id"),
    };

    let role_str = match std::str::from_utf8(&args[1]) {
        Ok(s) => s.to_lowercase(),
        Err(_) => return err_frame("invalid role"),
    };

    let role = match role_str.as_str() {
        "user" => Role::User,
        "assistant" => Role::Assistant,
        "system" => Role::System,
        "tool" => Role::Tool,
        "function" => Role::Function,
        _ => return err_frame("invalid role (use: user, assistant, system, tool)"),
    };

    let content = match std::str::from_utf8(&args[2]) {
        Ok(s) => s.to_string(),
        Err(_) => return err_frame("invalid content"),
    };

    let mut message = Message::new(role, content);

    // Parse optional metadata
    let mut i = 3;
    while i < args.len() {
        let arg = match std::str::from_utf8(&args[i]) {
            Ok(s) => s.to_uppercase(),
            Err(_) => return err_frame("invalid argument"),
        };

        match arg.as_str() {
            "MODEL" => {
                i += 1;
                if i >= args.len() {
                    return err_frame("MODEL requires a value");
                }
                let model = match std::str::from_utf8(&args[i]) {
                    Ok(s) => s.to_string(),
                    Err(_) => return err_frame("invalid model"),
                };
                message = message.with_model(model);
            }
            "TOKENS" => {
                i += 1;
                if i >= args.len() {
                    return err_frame("TOKENS requires a value");
                }
                let tokens: usize = match std::str::from_utf8(&args[i]) {
                    Ok(s) => match s.parse() {
                        Ok(t) => t,
                        Err(_) => return err_frame("invalid token count"),
                    },
                    Err(_) => return err_frame("invalid tokens value"),
                };
                message = message.with_token_count(tokens);
            }
            _ => {
                return err_frame(&format!("unknown argument: {}", arg));
            }
        }
        i += 1;
    }

    match get_store().add_message(conv_id, message) {
        Ok(msg_id) => Frame::bulk(msg_id),
        Err(e) => err_frame(&format!("failed to add message: {}", e)),
    }
}

/// Handle CONV.CONTEXT command
///
/// Syntax: CONV.CONTEXT conversation_id [TOKENS max_tokens]
pub fn conv_context(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'CONV.CONTEXT' command");
    }

    let conv_id = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return err_frame("invalid conversation_id"),
    };

    // Check for token limit
    let mut max_tokens: Option<usize> = None;
    if args.len() >= 3 {
        let arg = match std::str::from_utf8(&args[1]) {
            Ok(s) => s.to_uppercase(),
            Err(_) => return err_frame("invalid argument"),
        };

        if arg == "TOKENS" {
            max_tokens = match std::str::from_utf8(&args[2]) {
                Ok(s) => match s.parse() {
                    Ok(t) => Some(t),
                    Err(_) => return err_frame("invalid token count"),
                },
                Err(_) => return err_frame("invalid tokens value"),
            };
        }
    }

    let messages = if let Some(max) = max_tokens {
        match get_store().get_context_with_limit(conv_id, max) {
            Ok(m) => m,
            Err(e) => return err_frame(&format!("failed to get context: {}", e)),
        }
    } else {
        match get_store().get_context(conv_id) {
            Ok(m) => m,
            Err(e) => return err_frame(&format!("failed to get context: {}", e)),
        }
    };

    // Build response array
    let frames: Vec<Frame> = messages
        .into_iter()
        .map(|m| {
            Frame::array(vec![
                Frame::bulk("role"),
                Frame::bulk(m.role.as_str()),
                Frame::bulk("content"),
                Frame::bulk(m.content),
                Frame::bulk("id"),
                Frame::bulk(m.id),
            ])
        })
        .collect();

    Frame::array(frames)
}

/// Handle CONV.LIST command
///
/// Syntax: CONV.LIST user_id
pub fn conv_list(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'CONV.LIST' command");
    }

    let user_id = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return err_frame("invalid user_id"),
    };

    let conversations = get_store().list_for_user(user_id);
    let frames: Vec<Frame> = conversations
        .into_iter()
        .map(Frame::bulk)
        .collect();

    Frame::array(frames)
}

/// Handle CONV.INFO command
///
/// Syntax: CONV.INFO conversation_id
pub fn conv_info(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'CONV.INFO' command");
    }

    let conv_id = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return err_frame("invalid conversation_id"),
    };

    match get_store().get(conv_id) {
        Some(conv) => {
            let mut result = Vec::new();

            result.push(Frame::bulk("id"));
            result.push(Frame::bulk(conv.id.clone()));

            result.push(Frame::bulk("user_id"));
            result.push(Frame::bulk(conv.user_id.clone()));

            result.push(Frame::bulk("message_count"));
            result.push(Frame::Integer(conv.message_count() as i64));

            result.push(Frame::bulk("total_tokens"));
            result.push(Frame::Integer(conv.total_tokens as i64));

            result.push(Frame::bulk("state"));
            result.push(Frame::bulk(format!("{:?}", conv.state)));

            result.push(Frame::bulk("created_at"));
            result.push(Frame::Integer(conv.created_at as i64));

            result.push(Frame::bulk("updated_at"));
            result.push(Frame::Integer(conv.updated_at as i64));

            if let Some(ref title) = conv.title {
                result.push(Frame::bulk("title"));
                result.push(Frame::bulk(title.clone()));
            }

            if let Some(ref prompt) = conv.system_prompt {
                result.push(Frame::bulk("system_prompt"));
                result.push(Frame::bulk(prompt.clone()));
            }

            Frame::array(result)
        }
        None => Frame::Null,
    }
}

/// Handle CONV.CLEAR command
///
/// Syntax: CONV.CLEAR conversation_id
pub fn conv_clear(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'CONV.CLEAR' command");
    }

    let conv_id = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return err_frame("invalid conversation_id"),
    };

    match get_store().clear_messages(conv_id) {
        Ok(_) => ok_frame(),
        Err(e) => err_frame(&format!("failed to clear: {}", e)),
    }
}

/// Handle CONV.SYSTEM command
///
/// Syntax: CONV.SYSTEM conversation_id prompt
pub fn conv_system(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("wrong number of arguments for 'CONV.SYSTEM' command");
    }

    let conv_id = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return err_frame("invalid conversation_id"),
    };

    let prompt = match std::str::from_utf8(&args[1]) {
        Ok(s) => s.to_string(),
        Err(_) => return err_frame("invalid prompt"),
    };

    match get_store().set_system_prompt(conv_id, prompt) {
        Ok(_) => ok_frame(),
        Err(e) => err_frame(&format!("failed to set system prompt: {}", e)),
    }
}

/// Handle CONV.ARCHIVE command
///
/// Syntax: CONV.ARCHIVE conversation_id
pub fn conv_archive(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'CONV.ARCHIVE' command");
    }

    let conv_id = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return err_frame("invalid conversation_id"),
    };

    match get_store().archive(conv_id) {
        Ok(_) => ok_frame(),
        Err(e) => err_frame(&format!("failed to archive: {}", e)),
    }
}

/// Handle CONV.STATS command
///
/// Syntax: CONV.STATS [conversation_id]
pub fn conv_stats(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if !args.is_empty() {
        // Stats for specific conversation
        let conv_id = match std::str::from_utf8(&args[0]) {
            Ok(s) => s,
            Err(_) => return err_frame("invalid conversation_id"),
        };

        match get_store().conversation_stats(conv_id) {
            Ok(stats) => Frame::array(vec![
                Frame::bulk("message_count"),
                Frame::Integer(stats.message_count as i64),
                Frame::bulk("user_messages"),
                Frame::Integer(stats.user_messages as i64),
                Frame::bulk("assistant_messages"),
                Frame::Integer(stats.assistant_messages as i64),
                Frame::bulk("system_messages"),
                Frame::Integer(stats.system_messages as i64),
                Frame::bulk("total_tokens"),
                Frame::Integer(stats.total_tokens as i64),
            ]),
            Err(e) => err_frame(&format!("failed to get stats: {}", e)),
        }
    } else {
        // Global stats
        let stats = get_store().stats();

        Frame::array(vec![
            Frame::bulk("total_conversations"),
            Frame::Integer(stats.total_conversations as i64),
            Frame::bulk("active_conversations"),
            Frame::Integer(stats.active_conversations as i64),
            Frame::bulk("total_messages"),
            Frame::Integer(stats.total_messages as i64),
            Frame::bulk("total_tokens"),
            Frame::Integer(stats.total_tokens as i64),
            Frame::bulk("summaries_created"),
            Frame::Integer(stats.summaries_created as i64),
            Frame::bulk("conversations_expired"),
            Frame::Integer(stats.conversations_expired as i64),
        ])
    }
}

/// Handle CONV.CLEANUP command
///
/// Syntax: CONV.CLEANUP
pub fn conv_cleanup(_ctx: &HandlerContext<'_>, _args: &[Bytes]) -> Frame {
    let count = get_store().cleanup_expired();
    Frame::Integer(count as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_args(args: &[&str]) -> Vec<Bytes> {
        args.iter().map(|s| Bytes::from(s.to_string())).collect()
    }

    #[test]
    fn test_conv_create() {
        // Note: Tests use global state, so results may vary in concurrent tests
        let store = ConversationStore::with_defaults();
        let _ = store.create("test-user");
    }
}
