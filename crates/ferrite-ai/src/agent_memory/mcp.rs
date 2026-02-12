//! Model Context Protocol (MCP) server for AI agent framework integration.
//!
//! Exposes agent memory operations via the MCP protocol, enabling direct
//! integration with Claude, GPT, LangChain, and other AI frameworks.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// MCP tool definition for agent memory operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpTool {
    /// Tool name (e.g., "memory_store", "memory_recall").
    pub name: String,
    /// Human-readable description.
    pub description: String,
    /// JSON Schema for the tool's input parameters.
    pub input_schema: serde_json::Value,
}

/// MCP resource representing an agent's memory state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpResource {
    /// Resource URI (e.g., "memory://agent-1/episodic").
    pub uri: String,
    /// Resource name.
    pub name: String,
    /// MIME type (always application/json).
    pub mime_type: String,
    /// Resource description.
    pub description: String,
}

/// Request types from MCP clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum McpRequest {
    /// Store a memory.
    #[serde(rename = "memory_store")]
    Store {
        agent_id: String,
        content: String,
        memory_type: String,
        importance: Option<f64>,
        metadata: Option<HashMap<String, String>>,
    },
    /// Recall memories.
    #[serde(rename = "memory_recall")]
    Recall {
        agent_id: String,
        query: String,
        limit: Option<usize>,
        memory_type: Option<String>,
    },
    /// Get working memory context.
    #[serde(rename = "memory_context")]
    GetContext {
        agent_id: String,
        max_tokens: Option<usize>,
    },
    /// Clear agent memories.
    #[serde(rename = "memory_clear")]
    Clear {
        agent_id: String,
        memory_type: Option<String>,
    },
}

/// Response types sent to MCP clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpResponse {
    pub success: bool,
    pub data: serde_json::Value,
    pub error: Option<String>,
}

impl McpResponse {
    pub fn ok(data: serde_json::Value) -> Self {
        Self {
            success: true,
            data,
            error: None,
        }
    }

    pub fn err(message: &str) -> Self {
        Self {
            success: false,
            data: serde_json::Value::Null,
            error: Some(message.to_string()),
        }
    }
}

/// MCP server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpServerConfig {
    /// Listen address.
    pub listen_addr: String,
    /// Server name advertised to clients.
    pub server_name: String,
    /// Protocol version.
    pub protocol_version: String,
    /// Maximum request size in bytes.
    pub max_request_size: usize,
}

impl Default for McpServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:3100".to_string(),
            server_name: "ferrite-agent-memory".to_string(),
            protocol_version: "2024-11-05".to_string(),
            max_request_size: 1024 * 1024, // 1MB
        }
    }
}

/// Returns the MCP tool definitions for agent memory operations.
pub fn tool_definitions() -> Vec<McpTool> {
    vec![
        McpTool {
            name: "memory_store".to_string(),
            description: "Store a memory for an AI agent".to_string(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "agent_id": {"type": "string", "description": "Agent identifier"},
                    "content": {"type": "string", "description": "Memory content text"},
                    "memory_type": {
                        "type": "string",
                        "enum": ["episodic", "semantic", "procedural"],
                        "description": "Type of memory"
                    },
                    "importance": {
                        "type": "number",
                        "minimum": 0.0,
                        "maximum": 1.0,
                        "description": "Importance score (0-1)"
                    }
                },
                "required": ["agent_id", "content", "memory_type"]
            }),
        },
        McpTool {
            name: "memory_recall".to_string(),
            description: "Recall relevant memories for a query".to_string(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "agent_id": {"type": "string", "description": "Agent identifier"},
                    "query": {"type": "string", "description": "Search query"},
                    "limit": {"type": "integer", "description": "Max results", "default": 5}
                },
                "required": ["agent_id", "query"]
            }),
        },
        McpTool {
            name: "memory_context".to_string(),
            description: "Get current working memory context for an agent".to_string(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "agent_id": {"type": "string", "description": "Agent identifier"},
                    "max_tokens": {"type": "integer", "description": "Max context tokens"}
                },
                "required": ["agent_id"]
            }),
        },
    ]
}

/// Returns the MCP resource definitions for agent memory state.
pub fn resource_definitions(agent_ids: &[String]) -> Vec<McpResource> {
    agent_ids
        .iter()
        .flat_map(|agent_id| {
            vec![
                McpResource {
                    uri: format!("memory://{}/episodic", agent_id),
                    name: format!("{} - Episodic Memory", agent_id),
                    mime_type: "application/json".to_string(),
                    description: "Event and interaction memories".to_string(),
                },
                McpResource {
                    uri: format!("memory://{}/semantic", agent_id),
                    name: format!("{} - Semantic Memory", agent_id),
                    mime_type: "application/json".to_string(),
                    description: "Knowledge and facts".to_string(),
                },
                McpResource {
                    uri: format!("memory://{}/working", agent_id),
                    name: format!("{} - Working Memory", agent_id),
                    mime_type: "application/json".to_string(),
                    description: "Current active context".to_string(),
                },
            ]
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tool_definitions() {
        let tools = tool_definitions();
        assert_eq!(tools.len(), 3);
        assert_eq!(tools[0].name, "memory_store");
        assert_eq!(tools[1].name, "memory_recall");
        assert_eq!(tools[2].name, "memory_context");
    }

    #[test]
    fn test_resource_definitions() {
        let agents = vec!["agent-1".to_string(), "agent-2".to_string()];
        let resources = resource_definitions(&agents);
        assert_eq!(resources.len(), 6); // 3 per agent
        assert!(resources[0].uri.contains("agent-1"));
    }

    #[test]
    fn test_mcp_response_ok() {
        let resp = McpResponse::ok(serde_json::json!({"stored": true}));
        assert!(resp.success);
        assert!(resp.error.is_none());
    }

    #[test]
    fn test_mcp_response_err() {
        let resp = McpResponse::err("not found");
        assert!(!resp.success);
        assert_eq!(resp.error.unwrap(), "not found");
    }

    #[test]
    fn test_mcp_server_config_default() {
        let config = McpServerConfig::default();
        assert_eq!(config.server_name, "ferrite-agent-memory");
        assert_eq!(config.protocol_version, "2024-11-05");
    }

    #[test]
    fn test_request_serialization() {
        let req = McpRequest::Store {
            agent_id: "a1".to_string(),
            content: "test memory".to_string(),
            memory_type: "episodic".to_string(),
            importance: Some(0.8),
            metadata: None,
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("memory_store"));
    }
}
