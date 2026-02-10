//! Trigger execution engine
//!
//! Executes trigger actions when events occur.

use std::time::{Duration, Instant};

use bytes::Bytes;

use super::actions::{Action, ActionResult, BuiltinAction, HttpMethod, TemplateRenderer};
use super::conditions::EventType;
use super::{Trigger, TriggerConfig, TriggerError};
use crate::runtime::SharedSubscriptionManager;

/// Event that can trigger actions
#[derive(Clone, Debug)]
pub struct TriggerEvent {
    /// Type of event
    pub event_type: EventType,
    /// Key that was affected
    pub key: String,
    /// New value (if any)
    pub value: Option<Vec<u8>>,
    /// Old value (if any)
    pub old_value: Option<Vec<u8>>,
    /// Value type (string, list, hash, etc.)
    pub value_type: Option<String>,
    /// TTL in seconds (if set)
    pub ttl: Option<i64>,
    /// Timestamp of the event
    pub timestamp: u64,
}

impl TriggerEvent {
    /// Create a SET event
    pub fn set(key: impl Into<String>, value: Vec<u8>) -> Self {
        Self {
            event_type: EventType::Set,
            key: key.into(),
            value: Some(value),
            old_value: None,
            value_type: Some("string".to_string()),
            ttl: None,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    /// Create a DELETE event
    pub fn delete(key: impl Into<String>) -> Self {
        Self {
            event_type: EventType::Delete,
            key: key.into(),
            value: None,
            old_value: None,
            value_type: None,
            ttl: None,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    /// Create an EXPIRE event
    pub fn expire(key: impl Into<String>) -> Self {
        Self {
            event_type: EventType::Expire,
            key: key.into(),
            value: None,
            old_value: None,
            value_type: None,
            ttl: Some(0),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    /// Set the old value
    pub fn with_old_value(mut self, old_value: Vec<u8>) -> Self {
        self.old_value = Some(old_value);
        self
    }

    /// Set the TTL
    pub fn with_ttl(mut self, ttl: i64) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Set the value type
    pub fn with_value_type(mut self, value_type: impl Into<String>) -> Self {
        self.value_type = Some(value_type.into());
        self
    }
}

/// Trigger execution engine
pub struct TriggerEngine {
    /// Configuration
    config: TriggerConfig,
    /// HTTP client for webhook actions
    #[cfg(feature = "cloud")]
    http_client: reqwest::Client,
    /// Optional subscription manager for pub/sub actions
    subscription_manager: Option<SharedSubscriptionManager>,
}

impl TriggerEngine {
    /// Create a new trigger engine
    pub fn new(config: TriggerConfig) -> Self {
        // Create HTTP client with default timeout
        #[cfg(feature = "cloud")]
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_millis(config.http_timeout_ms))
            .build()
            .unwrap_or_default();

        Self {
            config,
            #[cfg(feature = "cloud")]
            http_client,
            subscription_manager: None,
        }
    }

    /// Create a new trigger engine with subscription manager
    pub fn with_subscription_manager(
        config: TriggerConfig,
        subscription_manager: SharedSubscriptionManager,
    ) -> Self {
        #[cfg(feature = "cloud")]
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_millis(config.http_timeout_ms))
            .build()
            .unwrap_or_default();

        Self {
            config,
            #[cfg(feature = "cloud")]
            http_client,
            subscription_manager: Some(subscription_manager),
        }
    }

    /// Set the subscription manager
    pub fn set_subscription_manager(&mut self, manager: SharedSubscriptionManager) {
        self.subscription_manager = Some(manager);
    }

    /// Execute a trigger's actions
    pub async fn execute(
        &self,
        trigger: &Trigger,
        event: &TriggerEvent,
    ) -> Result<Vec<ActionResult>, TriggerError> {
        let start = Instant::now();
        let mut results = Vec::new();

        for action in &trigger.actions {
            let action_start = Instant::now();

            let result = self.execute_action(action, event).await;

            let duration_us = action_start.elapsed().as_micros() as u64;
            let result = match result {
                Ok(mut r) => {
                    r.duration_us = duration_us;
                    r
                }
                Err(e) => ActionResult::err(format!("{:?}", action), e.to_string())
                    .with_duration(duration_us),
            };

            results.push(result);

            // Check timeout
            if start.elapsed().as_millis() as u64 > self.config.default_timeout_ms {
                return Err(TriggerError::Timeout);
            }
        }

        Ok(results)
    }

    /// Execute a single action
    async fn execute_action(
        &self,
        action: &Action,
        event: &TriggerEvent,
    ) -> Result<ActionResult, TriggerError> {
        match action {
            Action::Builtin(builtin) => self.execute_builtin(builtin, event).await,
            Action::Wasm(wasm) => self.execute_wasm(wasm, event).await,
            Action::Command(cmd) => self.execute_command(cmd, event).await,
            Action::Chain(actions) => {
                let mut results = Vec::new();
                for action in actions {
                    let result = Box::pin(self.execute_action(action, event)).await?;
                    results.push(result);
                }
                Ok(ActionResult::ok(
                    "chain",
                    Some(serde_json::json!({
                        "actions": results.len(),
                        "results": results.iter().map(|r| r.success).collect::<Vec<_>>()
                    })),
                ))
            }
            Action::Conditional {
                condition,
                then_action,
                else_action,
            } => {
                let condition_met = self.evaluate_condition(condition, event);
                if condition_met {
                    Box::pin(self.execute_action(then_action, event)).await
                } else if let Some(else_action) = else_action {
                    Box::pin(self.execute_action(else_action, event)).await
                } else {
                    Ok(ActionResult::ok("conditional", None))
                }
            }
        }
    }

    /// Execute a built-in action
    async fn execute_builtin(
        &self,
        action: &BuiltinAction,
        event: &TriggerEvent,
    ) -> Result<ActionResult, TriggerError> {
        match action {
            BuiltinAction::Publish(publish) => {
                let message = publish.render_message(event);

                tracing::debug!(
                    channel = %publish.channel,
                    message = %message,
                    "Trigger publish action"
                );

                // Publish to pub/sub if subscription manager is available
                let receivers = if let Some(ref manager) = self.subscription_manager {
                    let channel_bytes = Bytes::from(publish.channel.clone());
                    let message_bytes = Bytes::from(message.clone());
                    manager.publish(&channel_bytes, &message_bytes)
                } else {
                    tracing::warn!(
                        channel = %publish.channel,
                        "Publish action skipped: no subscription manager configured"
                    );
                    0
                };

                Ok(ActionResult::ok(
                    "publish",
                    Some(serde_json::json!({
                        "channel": publish.channel,
                        "message": message,
                        "receivers": receivers
                    })),
                ))
            }
            BuiltinAction::Http(http) => {
                #[cfg(not(feature = "cloud"))]
                {
                    let _ = http;
                    return Ok(ActionResult::err(
                        "http",
                        "HTTP actions require the 'cloud' feature".to_string(),
                    ));
                }

                #[cfg(feature = "cloud")]
                {
                    let url = http.render_url(event);
                    let body = http.render_body(event);

                    tracing::debug!(
                        method = %http.method,
                        url = %url,
                        "Trigger HTTP action"
                    );

                    // Build the request
                    let mut request = match http.method {
                        HttpMethod::Get => self.http_client.get(&url),
                        HttpMethod::Post => self.http_client.post(&url),
                        HttpMethod::Put => self.http_client.put(&url),
                        HttpMethod::Delete => self.http_client.delete(&url),
                        HttpMethod::Patch => self.http_client.patch(&url),
                    };

                    // Add headers
                    for (key, value) in &http.headers {
                        request = request.header(key, value);
                    }

                    // Set content type
                    if let Some(content_type) = &http.content_type {
                        request = request.header("Content-Type", content_type);
                    }

                    // Add body for methods that support it
                    if let Some(body_content) = &body {
                        if matches!(
                            http.method,
                            HttpMethod::Post | HttpMethod::Put | HttpMethod::Patch
                        ) {
                            request = request.body(body_content.clone());
                        }
                    }

                    // Apply per-action timeout if specified
                    if let Some(timeout_ms) = http.timeout_ms {
                        request = request.timeout(Duration::from_millis(timeout_ms));
                    }

                    // Execute with retries
                    let max_retries = http.retries.unwrap_or(self.config.max_retries);
                    let mut last_error = None;

                    for attempt in 0..=max_retries {
                        if attempt > 0 {
                            tokio::time::sleep(Duration::from_millis(
                                self.config.retry_delay_ms * (1 << (attempt - 1).min(4)),
                            ))
                            .await;
                            tracing::debug!(
                                attempt = attempt,
                                url = %url,
                                "Retrying HTTP request"
                            );
                        }

                        match request
                            .try_clone()
                            .unwrap_or_else(|| {
                                // Rebuild request if clone fails
                                let mut req = match http.method {
                                    HttpMethod::Get => self.http_client.get(&url),
                                    HttpMethod::Post => self.http_client.post(&url),
                                    HttpMethod::Put => self.http_client.put(&url),
                                    HttpMethod::Delete => self.http_client.delete(&url),
                                    HttpMethod::Patch => self.http_client.patch(&url),
                                };
                                for (key, value) in &http.headers {
                                    req = req.header(key, value);
                                }
                                if let Some(ct) = &http.content_type {
                                    req = req.header("Content-Type", ct);
                                }
                                if let Some(b) = &body {
                                    if matches!(
                                        http.method,
                                        HttpMethod::Post | HttpMethod::Put | HttpMethod::Patch
                                    ) {
                                        req = req.body(b.clone());
                                    }
                                }
                                req
                            })
                            .send()
                            .await
                        {
                            Ok(response) => {
                                let status = response.status();
                                let status_code = status.as_u16();

                                // Check if we should retry on this status
                                if status.is_server_error() && attempt < max_retries {
                                    last_error = Some(format!(
                                        "HTTP {} {}",
                                        status_code,
                                        status.canonical_reason().unwrap_or("Server Error")
                                    ));
                                    continue;
                                }

                                // Get response body (limited to 1MB)
                                let response_body = response
                                    .bytes()
                                    .await
                                    .map(|b| {
                                        if b.len() > 1024 * 1024 {
                                            String::from_utf8_lossy(&b[..1024]).to_string()
                                                + "...(truncated)"
                                        } else {
                                            String::from_utf8_lossy(&b).to_string()
                                        }
                                    })
                                    .ok();

                                if status.is_success() {
                                    return Ok(ActionResult::ok(
                                        "http",
                                        Some(serde_json::json!({
                                            "method": http.method.to_string(),
                                            "url": url,
                                            "status": status_code,
                                            "success": true,
                                            "body": response_body
                                        })),
                                    ));
                                } else {
                                    return Ok(ActionResult::err(
                                        "http",
                                        format!(
                                            "HTTP {} {}: {}",
                                            status_code,
                                            status.canonical_reason().unwrap_or("Error"),
                                            response_body.unwrap_or_default()
                                        ),
                                    ));
                                }
                            }
                            Err(e) => {
                                if e.is_timeout() {
                                    last_error = Some(format!("Request timeout: {}", e));
                                } else if e.is_connect() {
                                    last_error = Some(format!("Connection error: {}", e));
                                } else {
                                    last_error = Some(format!("Request error: {}", e));
                                }
                                // Continue to retry
                            }
                        }
                    }

                    // All retries exhausted
                    Ok(ActionResult::err(
                        "http",
                        last_error.unwrap_or_else(|| "Unknown error".to_string()),
                    ))
                } // end #[cfg(feature = "cloud")] block
            }
            BuiltinAction::Log(log) => {
                let message = TemplateRenderer::render(&log.message_template, event);

                match log.level {
                    super::actions::LogLevel::Debug => tracing::debug!("{}", message),
                    super::actions::LogLevel::Info => tracing::info!("{}", message),
                    super::actions::LogLevel::Warn => tracing::warn!("{}", message),
                    super::actions::LogLevel::Error => tracing::error!("{}", message),
                }

                Ok(ActionResult::ok(
                    "log",
                    Some(serde_json::json!({
                        "level": format!("{:?}", log.level),
                        "message": message
                    })),
                ))
            }
            BuiltinAction::Set(set) => {
                let key = TemplateRenderer::render(&set.key_template, event);
                let value = TemplateRenderer::render(&set.value_template, event);

                tracing::debug!(
                    key = %key,
                    value = %value,
                    ttl = ?set.ttl_secs,
                    "Trigger SET action"
                );

                Ok(ActionResult::ok(
                    "set",
                    Some(serde_json::json!({
                        "key": key,
                        "value": value,
                        "ttl": set.ttl_secs
                    })),
                ))
            }
            BuiltinAction::Delete(delete) => {
                let key = TemplateRenderer::render(&delete.key_template, event);

                tracing::debug!(key = %key, "Trigger DELETE action");

                Ok(ActionResult::ok(
                    "delete",
                    Some(serde_json::json!({"key": key})),
                ))
            }
            BuiltinAction::Increment(incr) => {
                let key = TemplateRenderer::render(&incr.key_template, event);

                tracing::debug!(
                    key = %key,
                    amount = %incr.amount,
                    "Trigger INCREMENT action"
                );

                Ok(ActionResult::ok(
                    "increment",
                    Some(serde_json::json!({
                        "key": key,
                        "amount": incr.amount
                    })),
                ))
            }
            BuiltinAction::ListPush(push) => {
                let key = TemplateRenderer::render(&push.key_template, event);
                let value = TemplateRenderer::render(&push.value_template, event);

                tracing::debug!(
                    key = %key,
                    value = %value,
                    direction = ?push.direction,
                    "Trigger LIST PUSH action"
                );

                Ok(ActionResult::ok(
                    "list_push",
                    Some(serde_json::json!({
                        "key": key,
                        "value": value,
                        "direction": format!("{:?}", push.direction)
                    })),
                ))
            }
            BuiltinAction::SetAdd(add) => {
                let key = TemplateRenderer::render(&add.key_template, event);
                let value = TemplateRenderer::render(&add.value_template, event);

                tracing::debug!(
                    key = %key,
                    value = %value,
                    "Trigger SET ADD action"
                );

                Ok(ActionResult::ok(
                    "set_add",
                    Some(serde_json::json!({
                        "key": key,
                        "value": value
                    })),
                ))
            }
            BuiltinAction::Noop => Ok(ActionResult::ok("noop", None)),
        }
    }

    /// Execute a WASM action
    async fn execute_wasm(
        &self,
        wasm: &super::actions::WasmAction,
        event: &TriggerEvent,
    ) -> Result<ActionResult, TriggerError> {
        // In a real implementation, this would:
        // 1. Load the WASM module from the registry
        // 2. Call the specified function with event data
        // 3. Return the result

        tracing::debug!(
            module = %wasm.module,
            function = %wasm.function,
            "Trigger WASM action"
        );

        // Placeholder implementation
        Ok(ActionResult::ok(
            "wasm",
            Some(serde_json::json!({
                "module": wasm.module,
                "function": wasm.function,
                "key": event.key
            })),
        ))
    }

    /// Execute a command action
    async fn execute_command(
        &self,
        cmd: &super::actions::CommandAction,
        event: &TriggerEvent,
    ) -> Result<ActionResult, TriggerError> {
        let args = cmd.render_args(event);

        tracing::debug!(
            command = %cmd.command,
            args = ?args,
            "Trigger COMMAND action"
        );

        // In a real implementation, this would execute the Redis command
        Ok(ActionResult::ok(
            "command",
            Some(serde_json::json!({
                "command": cmd.command,
                "args": args
            })),
        ))
    }

    /// Evaluate a condition expression
    fn evaluate_condition(&self, condition: &str, event: &TriggerEvent) -> bool {
        // Simple condition evaluation
        // Supports: $KEY, $VALUE, $TYPE, $TTL comparisons

        let parts: Vec<&str> = condition.split_whitespace().collect();
        if parts.len() < 3 {
            return false;
        }

        let left = self.resolve_variable(parts[0], event);
        let op = parts[1];
        let right = parts[2..].join(" ");

        match op {
            "==" | "=" => left == right,
            "!=" => left != right,
            "contains" => left.contains(&right),
            "starts_with" => left.starts_with(&right),
            "ends_with" => left.ends_with(&right),
            ">" => left
                .parse::<i64>()
                .ok()
                .zip(right.parse::<i64>().ok())
                .map(|(l, r)| l > r)
                .unwrap_or(false),
            "<" => left
                .parse::<i64>()
                .ok()
                .zip(right.parse::<i64>().ok())
                .map(|(l, r)| l < r)
                .unwrap_or(false),
            ">=" => left
                .parse::<i64>()
                .ok()
                .zip(right.parse::<i64>().ok())
                .map(|(l, r)| l >= r)
                .unwrap_or(false),
            "<=" => left
                .parse::<i64>()
                .ok()
                .zip(right.parse::<i64>().ok())
                .map(|(l, r)| l <= r)
                .unwrap_or(false),
            _ => false,
        }
    }

    /// Resolve a variable reference
    fn resolve_variable(&self, var: &str, event: &TriggerEvent) -> String {
        match var {
            "$KEY" => event.key.clone(),
            "$VALUE" => event
                .value
                .as_ref()
                .map(|v| String::from_utf8_lossy(v).to_string())
                .unwrap_or_default(),
            "$OLD_VALUE" => event
                .old_value
                .as_ref()
                .map(|v| String::from_utf8_lossy(v).to_string())
                .unwrap_or_default(),
            "$TYPE" => event.value_type.clone().unwrap_or_default(),
            "$TTL" => event.ttl.map(|t| t.to_string()).unwrap_or("-1".to_string()),
            "$TIMESTAMP" => event.timestamp.to_string(),
            "$EVENT" => event.event_type.to_string(),
            _ => var.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::triggers::actions::{BuiltinAction, PublishAction};
    use crate::triggers::conditions::{Condition, Pattern};
    use crate::triggers::Trigger;

    fn sample_event() -> TriggerEvent {
        TriggerEvent::set("orders:123", b"order data".to_vec())
            .with_ttl(3600)
            .with_value_type("string")
    }

    #[test]
    fn test_trigger_event_set() {
        let event = TriggerEvent::set("key", b"value".to_vec());
        assert_eq!(event.event_type, EventType::Set);
        assert_eq!(event.key, "key");
        assert_eq!(event.value, Some(b"value".to_vec()));
    }

    #[test]
    fn test_trigger_event_delete() {
        let event = TriggerEvent::delete("key");
        assert_eq!(event.event_type, EventType::Delete);
        assert_eq!(event.key, "key");
        assert!(event.value.is_none());
    }

    #[tokio::test]
    async fn test_engine_execute_publish() {
        let config = TriggerConfig::default();
        let engine = TriggerEngine::new(config);

        let trigger = Trigger::new(
            "test".to_string(),
            Condition::new(EventType::Set, Pattern::Any),
            vec![Action::Builtin(BuiltinAction::Publish(PublishAction {
                channel: "events".to_string(),
                message_template: "Key: $KEY".to_string(),
            }))],
        );

        let event = sample_event();
        let results = engine.execute(&trigger, &event).await.unwrap();

        assert_eq!(results.len(), 1);
        assert!(results[0].success);
        assert_eq!(results[0].action, "publish");
    }

    #[tokio::test]
    async fn test_engine_execute_noop() {
        let config = TriggerConfig::default();
        let engine = TriggerEngine::new(config);

        let trigger = Trigger::new(
            "test".to_string(),
            Condition::new(EventType::Set, Pattern::Any),
            vec![Action::Builtin(BuiltinAction::Noop)],
        );

        let event = sample_event();
        let results = engine.execute(&trigger, &event).await.unwrap();

        assert_eq!(results.len(), 1);
        assert!(results[0].success);
        assert_eq!(results[0].action, "noop");
    }

    #[test]
    fn test_condition_evaluation() {
        let config = TriggerConfig::default();
        let engine = TriggerEngine::new(config);

        let event = sample_event();

        assert!(engine.evaluate_condition("$KEY starts_with orders", &event));
        assert!(!engine.evaluate_condition("$KEY starts_with users", &event));
        assert!(engine.evaluate_condition("$TTL > 1000", &event));
        assert!(engine.evaluate_condition("$TTL < 5000", &event));
    }

    #[tokio::test]
    async fn test_engine_chain_action() {
        let config = TriggerConfig::default();
        let engine = TriggerEngine::new(config);

        let trigger = Trigger::new(
            "test".to_string(),
            Condition::new(EventType::Set, Pattern::Any),
            vec![Action::Chain(vec![
                Action::Builtin(BuiltinAction::Noop),
                Action::Builtin(BuiltinAction::Noop),
            ])],
        );

        let event = sample_event();
        let results = engine.execute(&trigger, &event).await.unwrap();

        assert_eq!(results.len(), 1);
        assert!(results[0].success);
        assert_eq!(results[0].action, "chain");
    }
}
