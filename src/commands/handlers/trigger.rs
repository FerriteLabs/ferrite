//! Trigger (FerriteFunctions) command handlers
//!
//! This module contains handlers for trigger operations:
//! - TRIGGER.CREATE (create a trigger)
//! - TRIGGER.DELETE (delete a trigger)
//! - TRIGGER.GET (get trigger info)
//! - TRIGGER.LIST (list triggers)
//! - TRIGGER.ENABLE/DISABLE (toggle trigger)
//! - TRIGGER.FIRE (manually fire a trigger)
//! - TRIGGER.INFO (system info)
//! - TRIGGER.STATS (usage statistics)
//! - TRIGGER.CONFIG (configuration)

use bytes::Bytes;

use crate::protocol::Frame;

/// Handle TRIGGER.CREATE command
#[allow(clippy::too_many_arguments)]
pub async fn create(
    name: &Bytes,
    event_type: &Bytes,
    pattern: &Bytes,
    actions: &[Bytes],
    wasm_module: Option<&Bytes>,
    wasm_function: Option<&Bytes>,
    priority: Option<i32>,
    description: Option<&Bytes>,
) -> Frame {
    use crate::triggers::{
        Action, BuiltinAction, Condition, EventType, Pattern, PublishAction, Trigger,
        TriggerConfig, TriggerRegistry,
    };

    let name_str = String::from_utf8_lossy(name).to_string();
    let event_str = String::from_utf8_lossy(event_type).to_string();
    let pattern_str = String::from_utf8_lossy(pattern).to_string();

    // Parse event type
    let event = match EventType::parse_str(&event_str) {
        Some(e) => e,
        None => return Frame::error(format!("ERR Unknown event type: {}", event_str)),
    };

    // Parse pattern
    let pat = Pattern::parse_str(&pattern_str);

    // Build actions
    let mut trigger_actions = Vec::new();

    // Parse built-in actions
    for action_bytes in actions {
        let action_str = String::from_utf8_lossy(action_bytes).to_string();
        let parts: Vec<&str> = action_str.split_whitespace().collect();

        if parts.is_empty() {
            continue;
        }

        match parts[0].to_uppercase().as_str() {
            "PUBLISH" => {
                if parts.len() >= 3 {
                    trigger_actions.push(Action::Builtin(BuiltinAction::Publish(PublishAction {
                        channel: parts[1].to_string(),
                        message_template: parts[2..].join(" "),
                    })));
                }
            }
            _ => {
                // Store as generic action string
                trigger_actions.push(Action::Builtin(BuiltinAction::Noop));
            }
        }
    }

    // Handle WASM action
    if let (Some(module), Some(func)) = (wasm_module, wasm_function) {
        trigger_actions.push(Action::Wasm(crate::triggers::actions::WasmAction {
            module: String::from_utf8_lossy(module).to_string(),
            function: String::from_utf8_lossy(func).to_string(),
            args: vec![],
        }));
    }

    // Create trigger
    let mut trigger = Trigger::new(name_str, Condition::new(event, pat), trigger_actions);

    if let Some(p) = priority {
        trigger.priority = p;
    }

    if let Some(desc) = description {
        trigger.description = Some(String::from_utf8_lossy(desc).to_string());
    }

    // In production, this would add to a shared registry
    // For now, just acknowledge creation
    let config = TriggerConfig::default();
    let registry = TriggerRegistry::new(config);

    match registry.create(trigger).await {
        Ok(()) => Frame::simple("OK"),
        Err(e) => Frame::error(format!("ERR {}", e)),
    }
}

/// Handle TRIGGER.DELETE command
pub async fn delete(name: &Bytes) -> Frame {
    use crate::triggers::{TriggerConfig, TriggerRegistry};

    let name_str = String::from_utf8_lossy(name).to_string();
    let config = TriggerConfig::default();
    let registry = TriggerRegistry::new(config);

    match registry.delete(&name_str).await {
        Ok(()) => Frame::simple("OK"),
        Err(e) => Frame::error(format!("ERR {}", e)),
    }
}

/// Handle TRIGGER.GET command
pub async fn get(name: &Bytes) -> Frame {
    use crate::triggers::{TriggerConfig, TriggerRegistry};

    let name_str = String::from_utf8_lossy(name).to_string();
    let config = TriggerConfig::default();
    let registry = TriggerRegistry::new(config);

    match registry.get(&name_str).await {
        Some(trigger) => Frame::array(vec![
            Frame::bulk("name"),
            Frame::bulk(Bytes::from(trigger.name.clone())),
            Frame::bulk("event_type"),
            Frame::bulk(Bytes::from(trigger.condition.event_type.to_string())),
            Frame::bulk("pattern"),
            Frame::bulk(Bytes::from(trigger.condition.pattern.to_string())),
            Frame::bulk("enabled"),
            Frame::bulk(if trigger.enabled { "yes" } else { "no" }),
            Frame::bulk("priority"),
            Frame::Integer(trigger.priority as i64),
            Frame::bulk("actions"),
            Frame::Integer(trigger.actions.len() as i64),
            Frame::bulk("execution_count"),
            Frame::Integer(trigger.execution_count as i64),
        ]),
        None => Frame::null(),
    }
}

/// Handle TRIGGER.LIST command
pub async fn list(pattern: Option<&Bytes>) -> Frame {
    use crate::triggers::{TriggerConfig, TriggerRegistry};

    let pattern_str = pattern.map(|p| String::from_utf8_lossy(p).to_string());
    let config = TriggerConfig::default();
    let registry = TriggerRegistry::new(config);

    let triggers = registry.list(pattern_str.as_deref()).await;

    let items: Vec<Frame> = triggers
        .iter()
        .map(|t| Frame::bulk(Bytes::from(t.name.clone())))
        .collect();

    Frame::array(items)
}

/// Handle TRIGGER.ENABLE command
pub async fn enable(name: &Bytes) -> Frame {
    use crate::triggers::{TriggerConfig, TriggerRegistry};

    let name_str = String::from_utf8_lossy(name).to_string();
    let config = TriggerConfig::default();
    let registry = TriggerRegistry::new(config);

    match registry.enable(&name_str).await {
        Ok(()) => Frame::simple("OK"),
        Err(e) => Frame::error(format!("ERR {}", e)),
    }
}

/// Handle TRIGGER.DISABLE command
pub async fn disable(name: &Bytes) -> Frame {
    use crate::triggers::{TriggerConfig, TriggerRegistry};

    let name_str = String::from_utf8_lossy(name).to_string();
    let config = TriggerConfig::default();
    let registry = TriggerRegistry::new(config);

    match registry.disable(&name_str).await {
        Ok(()) => Frame::simple("OK"),
        Err(e) => Frame::error(format!("ERR {}", e)),
    }
}

/// Handle TRIGGER.FIRE command
pub async fn fire(_name: &Bytes, key: &Bytes, value: Option<&Bytes>, ttl: Option<i64>) -> Frame {
    use crate::triggers::{TriggerConfig, TriggerEvent, TriggerRegistry};

    let key_str = String::from_utf8_lossy(key).to_string();
    let value_vec = value.map(|v| v.to_vec());

    let mut event = TriggerEvent::set(key_str, value_vec.unwrap_or_default());
    if let Some(t) = ttl {
        event = event.with_ttl(t);
    }

    let config = TriggerConfig::default();
    let registry = TriggerRegistry::new(config);

    match registry.fire(event).await {
        Ok(results) => {
            let items: Vec<Frame> = results
                .iter()
                .map(|r| {
                    Frame::array(vec![
                        Frame::bulk("action"),
                        Frame::bulk(Bytes::from(r.action.clone())),
                        Frame::bulk("success"),
                        Frame::bulk(if r.success { "yes" } else { "no" }),
                        Frame::bulk("duration_us"),
                        Frame::Integer(r.duration_us as i64),
                    ])
                })
                .collect();
            Frame::array(items)
        }
        Err(e) => Frame::error(format!("ERR {}", e)),
    }
}

/// Handle TRIGGER.INFO command
pub async fn info() -> Frame {
    use crate::triggers::TriggerConfig;

    let config = TriggerConfig::default();

    Frame::array(vec![
        Frame::bulk("triggers_enabled"),
        Frame::bulk(if config.enabled { "yes" } else { "no" }),
        Frame::bulk("max_triggers"),
        Frame::Integer(config.max_triggers as i64),
        Frame::bulk("max_actions_per_trigger"),
        Frame::Integer(config.max_actions_per_trigger as i64),
        Frame::bulk("default_timeout_ms"),
        Frame::Integer(config.default_timeout_ms as i64),
        Frame::bulk("max_concurrent_executions"),
        Frame::Integer(config.max_concurrent_executions as i64),
        Frame::bulk("async_execution"),
        Frame::bulk(if config.async_execution { "yes" } else { "no" }),
        Frame::bulk("http_timeout_ms"),
        Frame::Integer(config.http_timeout_ms as i64),
        Frame::bulk("max_retries"),
        Frame::Integer(config.max_retries as i64),
        Frame::bulk("log_executions"),
        Frame::bulk(if config.log_executions { "yes" } else { "no" }),
    ])
}

/// Handle TRIGGER.STATS command
pub async fn stats() -> Frame {
    use crate::triggers::{TriggerConfig, TriggerRegistry};

    let config = TriggerConfig::default();
    let registry = TriggerRegistry::new(config);
    let stats = registry.stats();

    Frame::array(vec![
        Frame::bulk("triggers_created"),
        Frame::Integer(stats.triggers_created as i64),
        Frame::bulk("triggers_deleted"),
        Frame::Integer(stats.triggers_deleted as i64),
        Frame::bulk("total_executions"),
        Frame::Integer(stats.total_executions as i64),
        Frame::bulk("successful_executions"),
        Frame::Integer(stats.successful_executions as i64),
        Frame::bulk("failed_executions"),
        Frame::Integer(stats.failed_executions as i64),
        Frame::bulk("total_actions"),
        Frame::Integer(stats.total_actions as i64),
        Frame::bulk("avg_execution_time_us"),
        Frame::Integer(stats.avg_execution_time_us as i64),
    ])
}

/// Handle TRIGGER.CONFIG command
pub async fn config(operation: &Bytes, param: Option<&Bytes>, _value: Option<&Bytes>) -> Frame {
    use crate::triggers::TriggerConfig;

    let op = String::from_utf8_lossy(operation).to_uppercase();
    let trigger_config = TriggerConfig::default();

    match op.as_str() {
        "GET" => {
            if let Some(p) = param {
                let param_name = String::from_utf8_lossy(p).to_lowercase();
                match param_name.as_str() {
                    "enabled" => Frame::bulk(if trigger_config.enabled { "yes" } else { "no" }),
                    "max_triggers" => Frame::Integer(trigger_config.max_triggers as i64),
                    "max_actions_per_trigger" => {
                        Frame::Integer(trigger_config.max_actions_per_trigger as i64)
                    }
                    "default_timeout_ms" => {
                        Frame::Integer(trigger_config.default_timeout_ms as i64)
                    }
                    "max_concurrent_executions" => {
                        Frame::Integer(trigger_config.max_concurrent_executions as i64)
                    }
                    "async_execution" => Frame::bulk(if trigger_config.async_execution {
                        "yes"
                    } else {
                        "no"
                    }),
                    "http_timeout_ms" => Frame::Integer(trigger_config.http_timeout_ms as i64),
                    "max_retries" => Frame::Integer(trigger_config.max_retries as i64),
                    "log_executions" => Frame::bulk(if trigger_config.log_executions {
                        "yes"
                    } else {
                        "no"
                    }),
                    _ => Frame::error(format!("ERR Unknown config parameter: {}", param_name)),
                }
            } else {
                // Return all config
                info().await
            }
        }
        "SET" => {
            // Config SET not implemented yet - would require runtime config modification
            Frame::error("ERR TRIGGER.CONFIG SET not implemented. Use config file.")
        }
        _ => Frame::error(format!("ERR Unknown operation: {}. Use GET or SET.", op)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_list_triggers() {
        let result = list(None).await;
        assert!(matches!(result, Frame::Array(Some(_))));
    }

    #[tokio::test]
    async fn test_info() {
        let result = info().await;
        assert!(matches!(result, Frame::Array(Some(_))));
    }

    #[tokio::test]
    async fn test_stats() {
        let result = stats().await;
        assert!(matches!(result, Frame::Array(Some(_))));
    }

    #[tokio::test]
    async fn test_config_get_enabled() {
        let result = config(&Bytes::from("GET"), Some(&Bytes::from("enabled")), None).await;
        assert!(matches!(result, Frame::Bulk(_)));
    }

    #[tokio::test]
    async fn test_config_set_not_implemented() {
        let result = config(&Bytes::from("SET"), None, None).await;
        assert!(matches!(result, Frame::Error(_)));
    }

    #[tokio::test]
    async fn test_get_nonexistent_trigger() {
        let result = get(&Bytes::from("nonexistent")).await;
        // Frame::null() returns Frame::Bulk(None)
        assert!(matches!(result, Frame::Bulk(None)));
    }
}
