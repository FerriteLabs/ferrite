//! Trigger actions
//!
//! Defines the actions that can be executed when a trigger fires.
//! Includes built-in actions (PUBLISH, HTTP, CALL) and WASM-based custom actions.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::TriggerEvent;

/// Simple percent-encoding for URLs
fn percent_encode(s: &str) -> String {
    let mut result = String::with_capacity(s.len() * 3);
    for c in s.chars() {
        match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => {
                result.push(c);
            }
            _ => {
                for byte in c.to_string().as_bytes() {
                    result.push_str(&format!("%{:02X}", byte));
                }
            }
        }
    }
    result
}

/// Result of an action execution
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActionResult {
    /// Action name/type
    pub action: String,
    /// Whether the action succeeded
    pub success: bool,
    /// Result data (if any)
    pub data: Option<serde_json::Value>,
    /// Error message (if failed)
    pub error: Option<String>,
    /// Execution time in microseconds
    pub duration_us: u64,
}

impl ActionResult {
    /// Create a successful result
    pub fn ok(action: impl Into<String>, data: Option<serde_json::Value>) -> Self {
        Self {
            action: action.into(),
            success: true,
            data,
            error: None,
            duration_us: 0,
        }
    }

    /// Create a failed result
    pub fn err(action: impl Into<String>, error: impl Into<String>) -> Self {
        Self {
            action: action.into(),
            success: false,
            data: None,
            error: Some(error.into()),
            duration_us: 0,
        }
    }

    /// Set the duration
    pub fn with_duration(mut self, duration_us: u64) -> Self {
        self.duration_us = duration_us;
        self
    }
}

/// Action to execute when a trigger fires
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Action {
    /// Built-in action
    Builtin(BuiltinAction),
    /// WASM-based custom action
    Wasm(WasmAction),
    /// Execute a Redis command
    Command(CommandAction),
    /// Chain of actions
    Chain(Vec<Action>),
    /// Conditional action
    Conditional {
        /// Condition expression to evaluate
        condition: String,
        /// Action to execute if the condition is true
        then_action: Box<Action>,
        /// Action to execute if the condition is false
        else_action: Option<Box<Action>>,
    },
}

/// Built-in actions
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum BuiltinAction {
    /// Publish a message to a channel
    Publish(PublishAction),
    /// Send an HTTP request
    Http(HttpAction),
    /// Log a message
    Log(LogAction),
    /// Set a key value
    Set(SetAction),
    /// Delete a key
    Delete(DeleteAction),
    /// Increment a counter
    Increment(IncrementAction),
    /// Add to a list
    ListPush(ListPushAction),
    /// Add to a set
    SetAdd(SetAddAction),
    /// No operation (for testing)
    Noop,
}

/// Publish action - sends a message to a pub/sub channel
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PublishAction {
    /// Channel to publish to
    pub channel: String,
    /// Message template (supports $KEY, $VALUE, $OLD_VALUE, $TTL)
    pub message_template: String,
}

impl PublishAction {
    /// Render the message template with event data
    pub fn render_message(&self, event: &TriggerEvent) -> String {
        let mut message = self.message_template.clone();
        message = message.replace("$KEY", &event.key);
        if let Some(value) = &event.value {
            message = message.replace("$VALUE", &String::from_utf8_lossy(value));
        } else {
            message = message.replace("$VALUE", "");
        }
        if let Some(old_value) = &event.old_value {
            message = message.replace("$OLD_VALUE", &String::from_utf8_lossy(old_value));
        } else {
            message = message.replace("$OLD_VALUE", "");
        }
        if let Some(ttl) = event.ttl {
            message = message.replace("$TTL", &ttl.to_string());
        } else {
            message = message.replace("$TTL", "-1");
        }
        message
    }
}

/// HTTP action - sends an HTTP request to a webhook
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HttpAction {
    /// HTTP method (GET, POST, PUT, DELETE, PATCH)
    pub method: HttpMethod,
    /// URL template (supports $KEY, $VALUE, etc.)
    pub url: String,
    /// Request headers
    pub headers: HashMap<String, String>,
    /// Request body template
    pub body_template: Option<String>,
    /// Content type
    pub content_type: Option<String>,
    /// Timeout in milliseconds
    pub timeout_ms: Option<u64>,
    /// Retry count
    pub retries: Option<u32>,
}

impl HttpAction {
    /// Create a simple POST action
    pub fn post(url: impl Into<String>) -> Self {
        Self {
            method: HttpMethod::Post,
            url: url.into(),
            headers: HashMap::new(),
            body_template: Some("{\"key\": \"$KEY\", \"value\": \"$VALUE\"}".to_string()),
            content_type: Some("application/json".to_string()),
            timeout_ms: None,
            retries: None,
        }
    }

    /// Render the URL with event data
    pub fn render_url(&self, event: &TriggerEvent) -> String {
        let mut url = self.url.clone();
        url = url.replace("$KEY", &percent_encode(&event.key));
        if let Some(value) = &event.value {
            url = url.replace("$VALUE", &percent_encode(&String::from_utf8_lossy(value)));
        }
        url
    }

    /// Render the body with event data
    pub fn render_body(&self, event: &TriggerEvent) -> Option<String> {
        self.body_template.as_ref().map(|template| {
            let mut body = template.clone();
            body = body.replace("$KEY", &event.key);
            if let Some(value) = &event.value {
                // Escape JSON special characters
                let escaped = String::from_utf8_lossy(value)
                    .replace('\\', "\\\\")
                    .replace('"', "\\\"")
                    .replace('\n', "\\n")
                    .replace('\r', "\\r")
                    .replace('\t', "\\t");
                body = body.replace("$VALUE", &escaped);
            } else {
                body = body.replace("$VALUE", "null");
            }
            if let Some(old_value) = &event.old_value {
                let escaped = String::from_utf8_lossy(old_value)
                    .replace('\\', "\\\\")
                    .replace('"', "\\\"");
                body = body.replace("$OLD_VALUE", &escaped);
            } else {
                body = body.replace("$OLD_VALUE", "null");
            }
            if let Some(ttl) = event.ttl {
                body = body.replace("$TTL", &ttl.to_string());
            } else {
                body = body.replace("$TTL", "-1");
            }
            body = body.replace("$TIMESTAMP", &event.timestamp.to_string());
            body
        })
    }
}

/// HTTP method
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum HttpMethod {
    /// HTTP GET method
    Get,
    /// HTTP POST method
    Post,
    /// HTTP PUT method
    Put,
    /// HTTP DELETE method
    Delete,
    /// HTTP PATCH method
    Patch,
}

impl std::fmt::Display for HttpMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HttpMethod::Get => write!(f, "GET"),
            HttpMethod::Post => write!(f, "POST"),
            HttpMethod::Put => write!(f, "PUT"),
            HttpMethod::Delete => write!(f, "DELETE"),
            HttpMethod::Patch => write!(f, "PATCH"),
        }
    }
}

/// Log action - logs a message
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogAction {
    /// Log level
    pub level: LogLevel,
    /// Message template
    pub message_template: String,
}

/// Log level
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LogLevel {
    /// Debug level logging
    Debug,
    /// Info level logging
    Info,
    /// Warning level logging
    Warn,
    /// Error level logging
    Error,
}

/// Set action - sets a key value
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SetAction {
    /// Key template
    pub key_template: String,
    /// Value template
    pub value_template: String,
    /// TTL in seconds (optional)
    pub ttl_secs: Option<u64>,
}

/// Delete action - deletes a key
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeleteAction {
    /// Key template
    pub key_template: String,
}

/// Increment action - increments a counter
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IncrementAction {
    /// Key template
    pub key_template: String,
    /// Increment amount
    pub amount: i64,
}

/// List push action - adds to a list
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ListPushAction {
    /// Key template
    pub key_template: String,
    /// Value template
    pub value_template: String,
    /// Push direction (left or right)
    pub direction: ListDirection,
    /// Max list size (optional, triggers LTRIM)
    pub max_size: Option<usize>,
}

/// List direction
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ListDirection {
    /// Push to the left (head of list)
    Left,
    /// Push to the right (tail of list)
    Right,
}

/// Set add action - adds to a set
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SetAddAction {
    /// Key template
    pub key_template: String,
    /// Value template
    pub value_template: String,
}

/// WASM action - executes a WASM function
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WasmAction {
    /// WASM module name
    pub module: String,
    /// Function to call
    pub function: String,
    /// Additional arguments
    pub args: Vec<String>,
}

/// Command action - executes a Redis command
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommandAction {
    /// Command name
    pub command: String,
    /// Command arguments (templates)
    pub args: Vec<String>,
}

impl CommandAction {
    /// Render the arguments with event data
    pub fn render_args(&self, event: &TriggerEvent) -> Vec<String> {
        self.args
            .iter()
            .map(|arg| {
                let mut rendered = arg.clone();
                rendered = rendered.replace("$KEY", &event.key);
                if let Some(value) = &event.value {
                    rendered = rendered.replace("$VALUE", &String::from_utf8_lossy(value));
                }
                if let Some(old_value) = &event.old_value {
                    rendered = rendered.replace("$OLD_VALUE", &String::from_utf8_lossy(old_value));
                }
                if let Some(ttl) = event.ttl {
                    rendered = rendered.replace("$TTL", &ttl.to_string());
                }
                rendered
            })
            .collect()
    }
}

/// Template helper for rendering strings with event data
pub struct TemplateRenderer;

impl TemplateRenderer {
    /// Render a template string with event data
    pub fn render(template: &str, event: &TriggerEvent) -> String {
        let mut result = template.to_string();

        result = result.replace("$KEY", &event.key);

        if let Some(value) = &event.value {
            result = result.replace("$VALUE", &String::from_utf8_lossy(value));
        } else {
            result = result.replace("$VALUE", "");
        }

        if let Some(old_value) = &event.old_value {
            result = result.replace("$OLD_VALUE", &String::from_utf8_lossy(old_value));
        } else {
            result = result.replace("$OLD_VALUE", "");
        }

        if let Some(value_type) = &event.value_type {
            result = result.replace("$TYPE", value_type);
        } else {
            result = result.replace("$TYPE", "unknown");
        }

        if let Some(ttl) = event.ttl {
            result = result.replace("$TTL", &ttl.to_string());
        } else {
            result = result.replace("$TTL", "-1");
        }

        result = result.replace("$TIMESTAMP", &event.timestamp.to_string());
        result = result.replace("$EVENT", &event.event_type.to_string());

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::triggers::EventType;

    fn sample_event() -> TriggerEvent {
        TriggerEvent {
            event_type: EventType::Set,
            key: "users:123".to_string(),
            value: Some(b"John Doe".to_vec()),
            old_value: Some(b"Jane Doe".to_vec()),
            value_type: Some("string".to_string()),
            ttl: Some(3600),
            timestamp: 1700000000,
        }
    }

    #[test]
    fn test_publish_action_render() {
        let action = PublishAction {
            channel: "events".to_string(),
            message_template: "Key $KEY was set to $VALUE (was $OLD_VALUE), TTL=$TTL".to_string(),
        };

        let event = sample_event();
        let message = action.render_message(&event);

        assert_eq!(
            message,
            "Key users:123 was set to John Doe (was Jane Doe), TTL=3600"
        );
    }

    #[test]
    fn test_http_action_render() {
        let action = HttpAction::post("https://api.example.com/webhook");
        let event = sample_event();

        let url = action.render_url(&event);
        assert!(url.contains("api.example.com"));

        let body = action.render_body(&event);
        assert!(body.is_some());
        let body = body.unwrap();
        assert!(body.contains("users:123"));
        assert!(body.contains("John Doe"));
    }

    #[test]
    fn test_command_action_render() {
        let action = CommandAction {
            command: "PUBLISH".to_string(),
            args: vec!["changes".to_string(), "$KEY:$VALUE".to_string()],
        };

        let event = sample_event();
        let args = action.render_args(&event);

        assert_eq!(args.len(), 2);
        assert_eq!(args[0], "changes");
        assert_eq!(args[1], "users:123:John Doe");
    }

    #[test]
    fn test_template_renderer() {
        let event = sample_event();

        let rendered = TemplateRenderer::render("Event: $EVENT on $KEY at $TIMESTAMP", &event);
        assert!(rendered.contains("SET"));
        assert!(rendered.contains("users:123"));
        assert!(rendered.contains("1700000000"));
    }

    #[test]
    fn test_action_result() {
        let ok = ActionResult::ok("test", Some(serde_json::json!({"status": "ok"})));
        assert!(ok.success);
        assert!(ok.error.is_none());

        let err = ActionResult::err("test", "something went wrong");
        assert!(!err.success);
        assert_eq!(err.error, Some("something went wrong".to_string()));
    }
}
