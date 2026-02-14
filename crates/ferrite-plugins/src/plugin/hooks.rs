//! Plugin Event Hooks
//!
//! Provides event-driven plugin execution.

use super::{EventType, PluginError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Event hook for plugins
#[derive(Clone, Debug)]
pub struct EventHook {
    /// Hook name
    pub name: String,
    /// Plugin that registered this hook
    pub plugin_name: String,
    /// Event type to hook
    pub event: EventType,
    /// Priority (lower = earlier)
    pub priority: i32,
}

impl EventHook {
    /// Create a new event hook
    pub fn new(name: &str, plugin_name: &str, event: EventType) -> Self {
        Self {
            name: name.to_string(),
            plugin_name: plugin_name.to_string(),
            event,
            priority: 0,
        }
    }

    /// Create from plugin hook spec
    pub fn from_plugin(plugin_name: String, spec: super::manifest::HookSpec) -> Self {
        Self {
            name: spec.name,
            plugin_name,
            event: parse_event_type(&spec.event),
            priority: spec.priority,
        }
    }

    /// Builder: set priority
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Execute the hook
    pub async fn execute(&self, _context: &HookContext) -> Result<HookResult, PluginError> {
        // In a real implementation, this would call into the WASM plugin
        // For now, we just return Continue
        Ok(HookResult::Continue)
    }
}

/// Context passed to event hooks
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HookContext {
    /// Event type
    pub event: EventType,
    /// Command being executed (if applicable)
    pub command: Option<String>,
    /// Key being accessed (if applicable)
    pub key: Option<String>,
    /// Value being set (if applicable)
    pub value: Option<Vec<u8>>,
    /// Client ID
    pub client_id: Option<u64>,
    /// Database number
    pub db: u8,
    /// Timestamp
    pub timestamp: u64,
    /// Additional data
    pub data: HashMap<String, String>,
}

impl HookContext {
    /// Create a new hook context
    pub fn new(event: EventType) -> Self {
        Self {
            event,
            command: None,
            key: None,
            value: None,
            client_id: None,
            db: 0,
            timestamp: current_timestamp_ms(),
            data: HashMap::new(),
        }
    }

    /// Builder: set command
    pub fn with_command(mut self, cmd: &str) -> Self {
        self.command = Some(cmd.to_string());
        self
    }

    /// Builder: set key
    pub fn with_key(mut self, key: &str) -> Self {
        self.key = Some(key.to_string());
        self
    }

    /// Builder: set value
    pub fn with_value(mut self, value: Vec<u8>) -> Self {
        self.value = Some(value);
        self
    }

    /// Builder: set client ID
    pub fn with_client_id(mut self, id: u64) -> Self {
        self.client_id = Some(id);
        self
    }

    /// Builder: set database
    pub fn with_db(mut self, db: u8) -> Self {
        self.db = db;
        self
    }

    /// Builder: add data
    pub fn with_data(mut self, key: &str, value: &str) -> Self {
        self.data.insert(key.to_string(), value.to_string());
        self
    }

    /// Create context for before command event
    pub fn before_command(cmd: &str, args: &[String]) -> Self {
        Self::new(EventType::BeforeCommand)
            .with_command(cmd)
            .with_data("args", &args.join(" "))
    }

    /// Create context for after command event
    pub fn after_command(cmd: &str, success: bool) -> Self {
        Self::new(EventType::AfterCommand)
            .with_command(cmd)
            .with_data("success", if success { "true" } else { "false" })
    }

    /// Create context for key write event
    pub fn key_write(key: &str, value: Vec<u8>) -> Self {
        Self::new(EventType::OnKeyWrite)
            .with_key(key)
            .with_value(value)
    }

    /// Create context for key read event
    pub fn key_read(key: &str) -> Self {
        Self::new(EventType::OnKeyRead).with_key(key)
    }

    /// Create context for key delete event
    pub fn key_delete(key: &str) -> Self {
        Self::new(EventType::OnKeyDelete).with_key(key)
    }

    /// Create context for key expiry event
    pub fn key_expiry(key: &str) -> Self {
        Self::new(EventType::OnKeyExpiry).with_key(key)
    }

    /// Create context for client connect event
    pub fn client_connect(client_id: u64) -> Self {
        Self::new(EventType::OnClientConnect).with_client_id(client_id)
    }

    /// Create context for client disconnect event
    pub fn client_disconnect(client_id: u64) -> Self {
        Self::new(EventType::OnClientDisconnect).with_client_id(client_id)
    }
}

/// Result from hook execution
#[derive(Clone, Debug)]
pub enum HookResult {
    /// Continue with normal execution
    Continue,
    /// Abort the operation with a reason
    Abort(String),
    /// Continue with modified data
    Modified(ModifiedData),
}

impl HookResult {
    /// Check if result is continue
    pub fn is_continue(&self) -> bool {
        matches!(self, HookResult::Continue)
    }

    /// Check if result is abort
    pub fn is_abort(&self) -> bool {
        matches!(self, HookResult::Abort(_))
    }

    /// Get abort reason if aborted
    pub fn abort_reason(&self) -> Option<&str> {
        match self {
            HookResult::Abort(reason) => Some(reason),
            _ => None,
        }
    }
}

/// Modified data from a hook
#[derive(Clone, Debug)]
pub struct ModifiedData {
    /// Modified key (if applicable)
    pub key: Option<String>,
    /// Modified value (if applicable)
    pub value: Option<Vec<u8>>,
    /// Additional modifications
    pub modifications: HashMap<String, String>,
}

impl ModifiedData {
    /// Create empty modified data
    pub fn new() -> Self {
        Self {
            key: None,
            value: None,
            modifications: HashMap::new(),
        }
    }

    /// Builder: set key
    pub fn with_key(mut self, key: &str) -> Self {
        self.key = Some(key.to_string());
        self
    }

    /// Builder: set value
    pub fn with_value(mut self, value: Vec<u8>) -> Self {
        self.value = Some(value);
        self
    }

    /// Builder: add modification
    pub fn with_modification(mut self, key: &str, value: &str) -> Self {
        self.modifications
            .insert(key.to_string(), value.to_string());
        self
    }
}

impl Default for ModifiedData {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse event type from string
fn parse_event_type(s: &str) -> EventType {
    match s.to_lowercase().as_str() {
        "before_command" | "beforecommand" => EventType::BeforeCommand,
        "after_command" | "aftercommand" => EventType::AfterCommand,
        "on_key_write" | "onkeywrite" => EventType::OnKeyWrite,
        "on_key_read" | "onkeyread" => EventType::OnKeyRead,
        "on_key_expiry" | "onkeyexpiry" => EventType::OnKeyExpiry,
        "on_key_delete" | "onkeydelete" => EventType::OnKeyDelete,
        "on_client_connect" | "onclientconnect" => EventType::OnClientConnect,
        "on_client_disconnect" | "onclientdisconnect" => EventType::OnClientDisconnect,
        "on_server_startup" | "onserverstartup" => EventType::OnServerStartup,
        "on_server_shutdown" | "onservershutdown" => EventType::OnServerShutdown,
        other => EventType::Custom(other.to_string()),
    }
}

/// Get current timestamp in milliseconds
fn current_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_hook_new() {
        let hook = EventHook::new("test_hook", "test_plugin", EventType::BeforeCommand);
        assert_eq!(hook.name, "test_hook");
        assert_eq!(hook.plugin_name, "test_plugin");
        assert_eq!(hook.event, EventType::BeforeCommand);
        assert_eq!(hook.priority, 0);
    }

    #[test]
    fn test_event_hook_with_priority() {
        let hook = EventHook::new("test", "plugin", EventType::AfterCommand).with_priority(-10);
        assert_eq!(hook.priority, -10);
    }

    #[test]
    fn test_hook_context_new() {
        let ctx = HookContext::new(EventType::OnKeyWrite);
        assert_eq!(ctx.event, EventType::OnKeyWrite);
        assert!(ctx.command.is_none());
        assert!(ctx.key.is_none());
    }

    #[test]
    fn test_hook_context_builder() {
        let ctx = HookContext::new(EventType::BeforeCommand)
            .with_command("SET")
            .with_key("mykey")
            .with_db(1)
            .with_data("extra", "value");

        assert_eq!(ctx.command, Some("SET".to_string()));
        assert_eq!(ctx.key, Some("mykey".to_string()));
        assert_eq!(ctx.db, 1);
        assert_eq!(ctx.data.get("extra"), Some(&"value".to_string()));
    }

    #[test]
    fn test_hook_context_before_command() {
        let ctx = HookContext::before_command("SET", &["key".to_string(), "value".to_string()]);
        assert_eq!(ctx.event, EventType::BeforeCommand);
        assert_eq!(ctx.command, Some("SET".to_string()));
        assert_eq!(ctx.data.get("args"), Some(&"key value".to_string()));
    }

    #[test]
    fn test_hook_context_after_command() {
        let ctx = HookContext::after_command("GET", true);
        assert_eq!(ctx.event, EventType::AfterCommand);
        assert_eq!(ctx.data.get("success"), Some(&"true".to_string()));
    }

    #[test]
    fn test_hook_context_key_events() {
        let write = HookContext::key_write("mykey", b"myvalue".to_vec());
        assert_eq!(write.event, EventType::OnKeyWrite);
        assert_eq!(write.key, Some("mykey".to_string()));
        assert_eq!(write.value, Some(b"myvalue".to_vec()));

        let read = HookContext::key_read("mykey");
        assert_eq!(read.event, EventType::OnKeyRead);

        let delete = HookContext::key_delete("mykey");
        assert_eq!(delete.event, EventType::OnKeyDelete);
    }

    #[test]
    fn test_hook_context_client_events() {
        let connect = HookContext::client_connect(123);
        assert_eq!(connect.event, EventType::OnClientConnect);
        assert_eq!(connect.client_id, Some(123));

        let disconnect = HookContext::client_disconnect(456);
        assert_eq!(disconnect.event, EventType::OnClientDisconnect);
        assert_eq!(disconnect.client_id, Some(456));
    }

    #[test]
    fn test_hook_result() {
        let continue_result = HookResult::Continue;
        assert!(continue_result.is_continue());
        assert!(!continue_result.is_abort());
        assert!(continue_result.abort_reason().is_none());

        let abort_result = HookResult::Abort("reason".to_string());
        assert!(!abort_result.is_continue());
        assert!(abort_result.is_abort());
        assert_eq!(abort_result.abort_reason(), Some("reason"));
    }

    #[test]
    fn test_modified_data() {
        let data = ModifiedData::new()
            .with_key("newkey")
            .with_value(b"newvalue".to_vec())
            .with_modification("flag", "true");

        assert_eq!(data.key, Some("newkey".to_string()));
        assert_eq!(data.value, Some(b"newvalue".to_vec()));
        assert_eq!(data.modifications.get("flag"), Some(&"true".to_string()));
    }

    #[test]
    fn test_parse_event_type() {
        assert_eq!(parse_event_type("before_command"), EventType::BeforeCommand);
        assert_eq!(parse_event_type("BeforeCommand"), EventType::BeforeCommand);
        assert_eq!(parse_event_type("after_command"), EventType::AfterCommand);
        assert_eq!(parse_event_type("on_key_write"), EventType::OnKeyWrite);
        assert_eq!(parse_event_type("on_key_read"), EventType::OnKeyRead);
        assert_eq!(
            parse_event_type("custom_event"),
            EventType::Custom("custom_event".to_string())
        );
    }

    #[tokio::test]
    async fn test_event_hook_execute() {
        let hook = EventHook::new("test", "plugin", EventType::BeforeCommand);
        let ctx = HookContext::new(EventType::BeforeCommand);

        let result = hook.execute(&ctx).await.unwrap();
        assert!(result.is_continue());
    }
}
