//! gRPC service definition for Ferrite
//!
//! Provides a protobuf-compatible service interface without requiring
//! external protobuf compilation. Uses manual message encoding.
#![allow(dead_code)]

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Method definitions
// ---------------------------------------------------------------------------

/// A single gRPC method descriptor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcMethod {
    /// Method name (e.g., "Get", "Set").
    pub name: String,
    /// Protobuf request type name.
    pub request_type: String,
    /// Protobuf response type name.
    pub response_type: String,
    /// Whether this method uses server-side streaming.
    pub is_streaming: bool,
}

// ---------------------------------------------------------------------------
// Request / Response envelopes
// ---------------------------------------------------------------------------

/// Generic gRPC request envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcRequest {
    /// The method to invoke.
    pub method: String,
    /// The request payload as a JSON value.
    pub payload: serde_json::Value,
}

/// Generic gRPC response envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcResponse {
    /// gRPC status code (0 = OK).
    pub status: u32,
    /// The response payload as a JSON value.
    pub payload: serde_json::Value,
    /// Error message, if any.
    pub error: Option<String>,
}

// ---------------------------------------------------------------------------
// Service definition
// ---------------------------------------------------------------------------

/// Service descriptor for Ferrite's gRPC interface.
pub struct GrpcServiceDefinition;

impl GrpcServiceDefinition {
    /// Returns the fully-qualified service name.
    pub fn service_name() -> &'static str {
        "ferrite.v1.Ferrite"
    }

    /// Returns the list of gRPC methods exposed by this service.
    pub fn methods() -> Vec<GrpcMethod> {
        vec![
            GrpcMethod {
                name: "Get".to_string(),
                request_type: "GetRequest".to_string(),
                response_type: "GetResponse".to_string(),
                is_streaming: false,
            },
            GrpcMethod {
                name: "Set".to_string(),
                request_type: "SetRequest".to_string(),
                response_type: "SetResponse".to_string(),
                is_streaming: false,
            },
            GrpcMethod {
                name: "Del".to_string(),
                request_type: "DelRequest".to_string(),
                response_type: "DelResponse".to_string(),
                is_streaming: false,
            },
            GrpcMethod {
                name: "Scan".to_string(),
                request_type: "ScanRequest".to_string(),
                response_type: "ScanResponse".to_string(),
                is_streaming: true,
            },
            GrpcMethod {
                name: "Subscribe".to_string(),
                request_type: "SubscribeRequest".to_string(),
                response_type: "SubscribeResponse".to_string(),
                is_streaming: true,
            },
            GrpcMethod {
                name: "Execute".to_string(),
                request_type: "ExecuteRequest".to_string(),
                response_type: "ExecuteResponse".to_string(),
                is_streaming: false,
            },
        ]
    }
}

// ---------------------------------------------------------------------------
// Service implementation
// ---------------------------------------------------------------------------

/// Ferrite gRPC service handler.
pub struct FerritGrpcService {
    /// Pre-built method lookup table.
    method_map: HashMap<String, GrpcMethod>,
}

impl FerritGrpcService {
    /// Create a new gRPC service instance.
    pub fn new() -> Self {
        let methods = GrpcServiceDefinition::methods();
        let method_map: HashMap<String, GrpcMethod> = methods
            .into_iter()
            .map(|m| (m.name.clone(), m))
            .collect();
        Self { method_map }
    }

    /// Handle an incoming gRPC request.
    pub fn handle_request(&self, method: &str, payload: &serde_json::Value) -> GrpcResponse {
        if !self.method_map.contains_key(method) {
            return GrpcResponse {
                status: 12, // UNIMPLEMENTED
                payload: serde_json::Value::Null,
                error: Some(format!("method not found: {}", method)),
            };
        }

        // Dispatch based on method name
        match method {
            "Get" => self.handle_get(payload),
            "Set" => self.handle_set(payload),
            "Del" => self.handle_del(payload),
            "Scan" => self.handle_scan(payload),
            "Subscribe" => self.handle_subscribe(payload),
            "Execute" => self.handle_execute(payload),
            _ => GrpcResponse {
                status: 12,
                payload: serde_json::Value::Null,
                error: Some(format!("unhandled method: {}", method)),
            },
        }
    }

    /// List all available methods.
    pub fn list_methods(&self) -> Vec<GrpcMethod> {
        self.method_map.values().cloned().collect()
    }

    /// Return the proto3 service definition as a string.
    pub fn service_descriptor(&self) -> String {
        let mut proto = String::new();
        proto.push_str("syntax = \"proto3\";\n\n");
        proto.push_str("package ferrite.v1;\n\n");
        proto.push_str("service Ferrite {\n");

        for method in self.method_map.values() {
            if method.is_streaming {
                proto.push_str(&format!(
                    "  rpc {}({}) returns (stream {}) {{}}\n",
                    method.name, method.request_type, method.response_type
                ));
            } else {
                proto.push_str(&format!(
                    "  rpc {}({}) returns ({}) {{}}\n",
                    method.name, method.request_type, method.response_type
                ));
            }
        }

        proto.push_str("}\n");
        proto
    }

    // --- Individual handlers ---

    fn handle_get(&self, payload: &serde_json::Value) -> GrpcResponse {
        let key = payload
            .get("key")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        GrpcResponse {
            status: 0,
            payload: serde_json::json!({
                "key": key,
                "value": serde_json::Value::Null,
                "found": false
            }),
            error: None,
        }
    }

    fn handle_set(&self, payload: &serde_json::Value) -> GrpcResponse {
        let key = payload
            .get("key")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        GrpcResponse {
            status: 0,
            payload: serde_json::json!({
                "key": key,
                "ok": true
            }),
            error: None,
        }
    }

    fn handle_del(&self, payload: &serde_json::Value) -> GrpcResponse {
        let keys = payload
            .get("keys")
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(0);
        GrpcResponse {
            status: 0,
            payload: serde_json::json!({
                "deleted": keys
            }),
            error: None,
        }
    }

    fn handle_scan(&self, _payload: &serde_json::Value) -> GrpcResponse {
        GrpcResponse {
            status: 0,
            payload: serde_json::json!({
                "keys": [],
                "cursor": "0"
            }),
            error: None,
        }
    }

    fn handle_subscribe(&self, _payload: &serde_json::Value) -> GrpcResponse {
        GrpcResponse {
            status: 0,
            payload: serde_json::json!({
                "subscribed": true
            }),
            error: None,
        }
    }

    fn handle_execute(&self, payload: &serde_json::Value) -> GrpcResponse {
        let command = payload
            .get("command")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        GrpcResponse {
            status: 0,
            payload: serde_json::json!({
                "command": command,
                "result": "OK"
            }),
            error: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_service_name() {
        assert_eq!(GrpcServiceDefinition::service_name(), "ferrite.v1.Ferrite");
    }

    #[test]
    fn test_methods_list() {
        let methods = GrpcServiceDefinition::methods();
        assert_eq!(methods.len(), 6);
        assert!(methods.iter().any(|m| m.name == "Get"));
        assert!(methods.iter().any(|m| m.name == "Subscribe" && m.is_streaming));
    }

    #[test]
    fn test_handle_get() {
        let service = FerritGrpcService::new();
        let resp = service.handle_request("Get", &serde_json::json!({"key": "foo"}));
        assert_eq!(resp.status, 0);
        assert!(resp.error.is_none());
    }

    #[test]
    fn test_handle_set() {
        let service = FerritGrpcService::new();
        let resp = service.handle_request(
            "Set",
            &serde_json::json!({"key": "foo", "value": "bar"}),
        );
        assert_eq!(resp.status, 0);
    }

    #[test]
    fn test_handle_unknown_method() {
        let service = FerritGrpcService::new();
        let resp = service.handle_request("Unknown", &serde_json::json!({}));
        assert_eq!(resp.status, 12);
        assert!(resp.error.is_some());
    }

    #[test]
    fn test_service_descriptor() {
        let service = FerritGrpcService::new();
        let desc = service.service_descriptor();
        assert!(desc.contains("syntax = \"proto3\""));
        assert!(desc.contains("service Ferrite"));
        assert!(desc.contains("rpc Get"));
    }

    #[test]
    fn test_list_methods() {
        let service = FerritGrpcService::new();
        let methods = service.list_methods();
        assert!(!methods.is_empty());
    }

    #[test]
    fn test_handle_execute() {
        let service = FerritGrpcService::new();
        let resp = service.handle_request(
            "Execute",
            &serde_json::json!({"command": "PING"}),
        );
        assert_eq!(resp.status, 0);
    }
}
