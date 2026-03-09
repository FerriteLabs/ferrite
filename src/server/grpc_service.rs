//! gRPC service definition for Ferrite
//!
//! Provides a protobuf-compatible service interface without requiring
//! external protobuf compilation. Uses manual message encoding.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::storage::{Store, Value};

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
    /// Reference to the key-value store.
    store: Option<Arc<Store>>,
}

impl Default for FerritGrpcService {
    fn default() -> Self {
        Self::new()
    }
}

impl FerritGrpcService {
    /// Create a new gRPC service instance (without store — for testing/metadata only).
    pub fn new() -> Self {
        let methods = GrpcServiceDefinition::methods();
        let method_map: HashMap<String, GrpcMethod> =
            methods.into_iter().map(|m| (m.name.clone(), m)).collect();
        Self {
            method_map,
            store: None,
        }
    }

    /// Create a new gRPC service instance connected to a store.
    pub fn with_store(store: Arc<Store>) -> Self {
        let methods = GrpcServiceDefinition::methods();
        let method_map: HashMap<String, GrpcMethod> =
            methods.into_iter().map(|m| (m.name.clone(), m)).collect();
        Self {
            method_map,
            store: Some(store),
        }
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

        if let Some(ref store) = self.store {
            match store.get(0, &Bytes::from(key.to_string())) {
                Some(Value::String(v)) => GrpcResponse {
                    status: 0,
                    payload: serde_json::json!({
                        "key": key,
                        "value": String::from_utf8_lossy(&v).to_string(),
                        "found": true
                    }),
                    error: None,
                },
                Some(_) => GrpcResponse {
                    status: 0,
                    payload: serde_json::json!({
                        "key": key,
                        "value": serde_json::Value::Null,
                        "found": true,
                        "type_error": "WRONGTYPE"
                    }),
                    error: None,
                },
                None => GrpcResponse {
                    status: 0,
                    payload: serde_json::json!({
                        "key": key,
                        "value": serde_json::Value::Null,
                        "found": false
                    }),
                    error: None,
                },
            }
        } else {
            GrpcResponse {
                status: 13, // INTERNAL
                payload: serde_json::Value::Null,
                error: Some("store not available".to_string()),
            }
        }
    }

    fn handle_set(&self, payload: &serde_json::Value) -> GrpcResponse {
        let key = payload
            .get("key")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let value = payload
            .get("value")
            .and_then(|v| v.as_str())
            .unwrap_or_default();

        if let Some(ref store) = self.store {
            store.set(
                0,
                Bytes::from(key.to_string()),
                Value::String(Bytes::from(value.to_string())),
            );
            GrpcResponse {
                status: 0,
                payload: serde_json::json!({
                    "key": key,
                    "ok": true
                }),
                error: None,
            }
        } else {
            GrpcResponse {
                status: 13,
                payload: serde_json::Value::Null,
                error: Some("store not available".to_string()),
            }
        }
    }

    fn handle_del(&self, payload: &serde_json::Value) -> GrpcResponse {
        let keys: Vec<String> = payload
            .get("keys")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        if let Some(ref store) = self.store {
            let byte_keys: Vec<Bytes> = keys.iter().map(|k| Bytes::from(k.clone())).collect();
            let deleted = store.del(0, &byte_keys);
            GrpcResponse {
                status: 0,
                payload: serde_json::json!({
                    "deleted": deleted
                }),
                error: None,
            }
        } else {
            GrpcResponse {
                status: 13,
                payload: serde_json::Value::Null,
                error: Some("store not available".to_string()),
            }
        }
    }

    fn handle_scan(&self, payload: &serde_json::Value) -> GrpcResponse {
        let count = payload.get("count").and_then(|v| v.as_u64()).unwrap_or(10) as usize;

        if let Some(ref store) = self.store {
            let all_keys = store.keys(0);
            let limited: Vec<String> = all_keys
                .into_iter()
                .take(count)
                .map(|k| String::from_utf8_lossy(&k).to_string())
                .collect();
            GrpcResponse {
                status: 0,
                payload: serde_json::json!({
                    "keys": limited,
                    "cursor": "0"
                }),
                error: None,
            }
        } else {
            GrpcResponse {
                status: 13,
                payload: serde_json::Value::Null,
                error: Some("store not available".to_string()),
            }
        }
    }

    fn handle_subscribe(&self, _payload: &serde_json::Value) -> GrpcResponse {
        // Pub/Sub subscriptions require a streaming connection; acknowledge intent
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
            .unwrap_or_default()
            .to_uppercase();
        let args: Vec<String> = payload
            .get("args")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        if let Some(ref store) = self.store {
            let result = match command.as_str() {
                "PING" => {
                    let msg = args.first().map(|s| s.as_str()).unwrap_or("PONG");
                    serde_json::json!({ "result": msg })
                }
                "GET" => {
                    if let Some(key) = args.first() {
                        match store.get(0, &Bytes::from(key.clone())) {
                            Some(Value::String(v)) => {
                                serde_json::json!({ "result": String::from_utf8_lossy(&v).to_string() })
                            }
                            _ => serde_json::json!({ "result": null }),
                        }
                    } else {
                        serde_json::json!({ "error": "ERR wrong number of arguments" })
                    }
                }
                "SET" => {
                    if args.len() >= 2 {
                        store.set(
                            0,
                            Bytes::from(args[0].clone()),
                            Value::String(Bytes::from(args[1].clone())),
                        );
                        serde_json::json!({ "result": "OK" })
                    } else {
                        serde_json::json!({ "error": "ERR wrong number of arguments" })
                    }
                }
                "DEL" => {
                    let byte_keys: Vec<Bytes> =
                        args.iter().map(|a| Bytes::from(a.clone())).collect();
                    let deleted = store.del(0, &byte_keys);
                    serde_json::json!({ "result": deleted })
                }
                "DBSIZE" => {
                    serde_json::json!({ "result": store.keys(0).len() })
                }
                _ => {
                    serde_json::json!({ "error": format!("ERR unsupported command: {}", command) })
                }
            };

            GrpcResponse {
                status: 0,
                payload: serde_json::json!({
                    "command": command,
                    "result": result
                }),
                error: None,
            }
        } else {
            GrpcResponse {
                status: 13,
                payload: serde_json::Value::Null,
                error: Some("store not available".to_string()),
            }
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
        assert!(methods
            .iter()
            .any(|m| m.name == "Subscribe" && m.is_streaming));
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
    fn test_get_set_with_store() {
        let store = Arc::new(Store::new(16));
        let service = FerritGrpcService::with_store(store);

        // SET a key
        let resp = service.handle_request(
            "Set",
            &serde_json::json!({"key": "hello", "value": "world"}),
        );
        assert_eq!(resp.status, 0);

        // GET the key back
        let resp = service.handle_request("Get", &serde_json::json!({"key": "hello"}));
        assert_eq!(resp.status, 0);
        assert_eq!(resp.payload["found"], true);
        assert_eq!(resp.payload["value"], "world");
    }

    #[test]
    fn test_get_missing_key() {
        let store = Arc::new(Store::new(16));
        let service = FerritGrpcService::with_store(store);

        let resp = service.handle_request("Get", &serde_json::json!({"key": "nonexistent"}));
        assert_eq!(resp.status, 0);
        assert_eq!(resp.payload["found"], false);
    }

    #[test]
    fn test_del_with_store() {
        let store = Arc::new(Store::new(16));
        store.set(0, Bytes::from("k1"), Value::String(Bytes::from("v1")));
        let service = FerritGrpcService::with_store(store);

        let resp = service.handle_request("Del", &serde_json::json!({"keys": ["k1", "k2"]}));
        assert_eq!(resp.status, 0);
        assert_eq!(resp.payload["deleted"], 1);
    }

    #[test]
    fn test_scan_with_store() {
        let store = Arc::new(Store::new(16));
        store.set(0, Bytes::from("a"), Value::String(Bytes::from("1")));
        store.set(0, Bytes::from("b"), Value::String(Bytes::from("2")));
        let service = FerritGrpcService::with_store(store);

        let resp = service.handle_request("Scan", &serde_json::json!({"count": 10}));
        assert_eq!(resp.status, 0);
        let keys = resp.payload["keys"].as_array().expect("keys array");
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn test_execute_ping() {
        let store = Arc::new(Store::new(16));
        let service = FerritGrpcService::with_store(store);

        let resp = service.handle_request("Execute", &serde_json::json!({"command": "PING"}));
        assert_eq!(resp.status, 0);
    }
}
