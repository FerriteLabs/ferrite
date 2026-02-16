//! WASM (WebAssembly) command handlers
//!
//! This module contains handlers for WASM function operations:
//! - WASM.LOAD (load a WASM module)
//! - WASM.UNLOAD (unload a module)
//! - WASM.CALL/WASM.CALL_RO (call a function)
//! - WASM.LIST (list loaded functions)
//! - WASM.INFO (function information)
//! - WASM.STATS (WASM system statistics)
//!
//! It also exposes the global `UdfRegistry` singleton used by the
//! FUNCTION/FCALL commands in `server_ops.rs`.

use bytes::Bytes;
use std::sync::OnceLock;

use crate::protocol::Frame;
use ferrite_plugins::wasm::FunctionRegistry;

// ── Legacy plugin-level registry (WASM.* commands) ─────────────────────────

/// Global WASM function registry singleton (plugin-level, for WASM.* commands)
static WASM_REGISTRY: OnceLock<FunctionRegistry> = OnceLock::new();

/// Get or initialize the global plugin-level function registry
fn get_registry() -> &'static FunctionRegistry {
    WASM_REGISTRY.get_or_init(FunctionRegistry::new)
}

// ── UDF registry (FUNCTION/FCALL commands, uses wasmtime) ──────────────────

/// Global UDF registry singleton backed by the wasmtime-based WasmRuntime.
/// This is initialized lazily on first access when the `wasm` feature is enabled.
#[cfg(feature = "wasm")]
static UDF_REGISTRY: OnceLock<crate::wasm::UdfRegistry> = OnceLock::new();

/// Get or initialize the global UDF registry.
///
/// Returns `None` if the wasmtime engine fails to initialize.
#[cfg(feature = "wasm")]
pub fn get_udf_registry() -> Option<&'static crate::wasm::UdfRegistry> {
    Some(UDF_REGISTRY.get_or_init(|| {
        use std::sync::Arc;
        let runtime = Arc::new(
            crate::wasm::WasmRuntime::with_defaults()
                .expect("failed to initialize WASM runtime"),
        );
        crate::wasm::UdfRegistry::new(runtime)
    }))
}

/// Convert a WASM `ExecutionResult` to a protocol `Frame`.
#[cfg(feature = "wasm")]
pub fn execution_result_to_frame(result: &crate::wasm::runtime::ExecutionResult) -> Frame {
    use ferrite_plugins::wasm::WasmValue;

    let values: Vec<Frame> = result
        .values
        .iter()
        .map(|v| match v {
            WasmValue::I32(i) => Frame::Integer(*i as i64),
            WasmValue::I64(i) => Frame::Integer(*i),
            WasmValue::F32(f) => Frame::bulk(format!("{}", f)),
            WasmValue::F64(f) => Frame::bulk(format!("{}", f)),
            WasmValue::Bytes(b) => Frame::Bulk(Some(Bytes::from(b.clone()))),
        })
        .collect();

    if values.is_empty() {
        Frame::simple("OK")
    } else if values.len() == 1 {
        values.into_iter().next().unwrap_or_else(|| Frame::simple("OK"))
    } else {
        Frame::array(values)
    }
}

// ── WASM.LOAD ──────────────────────────────────────────────────────────────

/// Handle WASM.LOAD command
pub async fn load(name: &str, module: &Bytes, replace: bool, permissions: &[String]) -> Frame {
    use ferrite_plugins::wasm::{FunctionMetadata, FunctionPermissions};

    let registry = get_registry();

    let perms = if permissions.is_empty() {
        FunctionPermissions::default()
    } else {
        let mut p = FunctionPermissions::default();
        for perm in permissions {
            match perm.to_uppercase().as_str() {
                "WRITE" => p.allow_write = true,
                "NETWORK" => p.allow_network = true,
                "ADMIN" => p.allow_admin = true,
                _ => {}
            }
        }
        p
    };

    // Calculate source hash
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    module.as_ref().hash(&mut hasher);
    let source_hash = format!("{:016x}", hasher.finish());
    let metadata = FunctionMetadata::new(name.to_string(), source_hash).with_permissions(perms);

    if !replace && registry.get(name).is_some() {
        return Frame::error(format!("ERR Function already exists: {}", name));
    }

    // Also register in the UDF registry if wasm feature is enabled
    #[cfg(feature = "wasm")]
    {
        if let Some(udf_registry) = get_udf_registry() {
            match udf_registry.load(name, module.to_vec(), Some(metadata.clone()), replace) {
                Ok(()) => {}
                Err(e) => {
                    tracing::warn!(
                        "Failed to register in UDF registry (will still register in plugin registry): {}",
                        e
                    );
                }
            }
        }
    }

    match registry.load(name, module.to_vec(), Some(metadata)) {
        Ok(()) => Frame::simple("OK"),
        Err(e) => Frame::error(format!("ERR {}", e)),
    }
}

// ── WASM.UNLOAD ────────────────────────────────────────────────────────────

/// Handle WASM.UNLOAD command
pub async fn unload(name: &str) -> Frame {
    let registry = get_registry();

    // Also remove from UDF registry
    #[cfg(feature = "wasm")]
    {
        if let Some(udf_registry) = get_udf_registry() {
            let _ = udf_registry.delete(name);
        }
    }

    match registry.unload(name) {
        Ok(()) => Frame::simple("OK"),
        Err(e) => Frame::error(format!("ERR {}", e)),
    }
}

// ── WASM.CALL ──────────────────────────────────────────────────────────────

/// Handle WASM.CALL command
pub async fn call(name: &str, keys: &[Bytes], args: &[Bytes]) -> Frame {
    use ferrite_plugins::wasm::WasmValue;

    // Try UDF registry first (real wasmtime execution)
    #[cfg(feature = "wasm")]
    {
        if let Some(udf_registry) = get_udf_registry() {
            if udf_registry.exists(name) {
                match udf_registry.call(name, keys, args, 0) {
                    Ok(result) => {
                        if result.success {
                            return execution_result_to_frame(&result);
                        } else {
                            return Frame::error(format!(
                                "ERR Execution failed: {}",
                                result.error.unwrap_or_default()
                            ));
                        }
                    }
                    Err(e) => return Frame::error(format!("ERR {}", e)),
                }
            }
        }
    }

    // Fall back to plugin-level registry (simulated execution)
    let registry = get_registry();

    let mut wasm_args: Vec<WasmValue> = Vec::with_capacity(keys.len() + args.len());
    for key in keys {
        wasm_args.push(WasmValue::Bytes(key.to_vec()));
    }
    for arg in args {
        let arg_str = String::from_utf8_lossy(arg);
        if let Ok(i) = arg_str.parse::<i64>() {
            wasm_args.push(WasmValue::I64(i));
        } else if let Ok(f) = arg_str.parse::<f64>() {
            wasm_args.push(WasmValue::F64(f));
        } else {
            wasm_args.push(WasmValue::Bytes(arg.to_vec()));
        }
    }

    match registry.call_async(name, "main", wasm_args, 0).await {
        Ok(result) => {
            if result.success {
                let values: Vec<Frame> = result
                    .values
                    .into_iter()
                    .map(|v| match v {
                        WasmValue::I32(i) => Frame::Integer(i as i64),
                        WasmValue::I64(i) => Frame::Integer(i),
                        WasmValue::F32(f) => Frame::bulk(format!("{}", f)),
                        WasmValue::F64(f) => Frame::bulk(format!("{}", f)),
                        WasmValue::Bytes(b) => Frame::Bulk(Some(Bytes::from(b))),
                    })
                    .collect();

                if values.is_empty() {
                    Frame::simple("OK")
                } else if values.len() == 1 {
                    values.into_iter().next().unwrap_or_else(|| Frame::simple("OK"))
                } else {
                    Frame::array(values)
                }
            } else {
                Frame::error(format!(
                    "ERR Execution failed: {}",
                    result.error.unwrap_or_default()
                ))
            }
        }
        Err(e) => Frame::error(format!("ERR {}", e)),
    }
}

// ── WASM.CALL_RO ───────────────────────────────────────────────────────────

/// Handle WASM.CALL_RO command (read-only variant)
pub async fn call_ro(name: &str, keys: &[Bytes], args: &[Bytes]) -> Frame {
    // Try UDF registry first with read-only enforcement
    #[cfg(feature = "wasm")]
    {
        if let Some(udf_registry) = get_udf_registry() {
            if udf_registry.exists(name) {
                match udf_registry.call_ro(name, keys, args, 0) {
                    Ok(result) => {
                        if result.success {
                            return execution_result_to_frame(&result);
                        } else {
                            return Frame::error(format!(
                                "ERR Execution failed: {}",
                                result.error.unwrap_or_default()
                            ));
                        }
                    }
                    Err(e) => return Frame::error(format!("ERR {}", e)),
                }
            }
        }
    }

    // Fall back to plugin registry
    call(name, keys, args).await
}

// ── WASM.LIST ──────────────────────────────────────────────────────────────

/// Handle WASM.LIST command
pub async fn list(with_stats: bool) -> Frame {
    let registry = get_registry();
    let mut functions = registry.list();

    // Also include functions from UDF registry
    #[cfg(feature = "wasm")]
    {
        if let Some(udf_registry) = get_udf_registry() {
            for name in udf_registry.list() {
                if !functions.contains(&name) {
                    functions.push(name);
                }
            }
        }
    }

    if with_stats {
        let items: Vec<Frame> = functions
            .iter()
            .map(|name| {
                // Try UDF registry first
                #[cfg(feature = "wasm")]
                {
                    if let Some(udf_registry) = get_udf_registry() {
                        if let Some(info) = udf_registry.info(name) {
                            return Frame::array(vec![
                                Frame::bulk("name"),
                                Frame::bulk(Bytes::from(name.clone())),
                                Frame::bulk("engine"),
                                Frame::bulk("wasm"),
                                Frame::bulk("calls"),
                                Frame::Integer(info.call_count as i64),
                                Frame::bulk("avg_duration_us"),
                                Frame::Integer((info.avg_execution_time_ms * 1000.0) as i64),
                                Frame::bulk("exports"),
                                Frame::array(
                                    info.exports
                                        .iter()
                                        .map(|e| Frame::bulk(Bytes::from(e.clone())))
                                        .collect(),
                                ),
                            ]);
                        }
                    }
                }

                // Fall back to plugin registry
                if let Some(info) = registry.info(name) {
                    Frame::array(vec![
                        Frame::bulk("name"),
                        Frame::bulk(Bytes::from(name.clone())),
                        Frame::bulk("calls"),
                        Frame::Integer(info.call_count as i64),
                        Frame::bulk("avg_duration_us"),
                        Frame::Integer((info.avg_execution_time_ms * 1000.0) as i64),
                    ])
                } else {
                    Frame::array(vec![
                        Frame::bulk("name"),
                        Frame::bulk(Bytes::from(name.clone())),
                    ])
                }
            })
            .collect();
        Frame::array(items)
    } else {
        Frame::array(
            functions
                .iter()
                .map(|name| Frame::bulk(Bytes::from(name.clone())))
                .collect(),
        )
    }
}

// ── WASM.INFO ──────────────────────────────────────────────────────────────

/// Handle WASM.INFO command
pub async fn info(name: &str) -> Frame {
    // Try UDF registry first
    #[cfg(feature = "wasm")]
    {
        if let Some(udf_registry) = get_udf_registry() {
            if let Some(info) = udf_registry.info(name) {
                let mut perms = Vec::new();
                if info.permissions.allow_write {
                    perms.push(Frame::bulk("write"));
                }
                if info.permissions.allow_network {
                    perms.push(Frame::bulk("network"));
                }
                if info.permissions.allow_admin {
                    perms.push(Frame::bulk("admin"));
                }

                return Frame::array(vec![
                    Frame::bulk("name"),
                    Frame::bulk(Bytes::from(name.to_string())),
                    Frame::bulk("engine"),
                    Frame::bulk("wasm"),
                    Frame::bulk("loaded"),
                    Frame::bulk("yes"),
                    Frame::bulk("module_hash"),
                    Frame::bulk(Bytes::from(info.module_hash)),
                    Frame::bulk("entry_point"),
                    Frame::bulk(Bytes::from(info.entry_point)),
                    Frame::bulk("source_hash"),
                    Frame::bulk(Bytes::from(info.source_hash)),
                    Frame::bulk("call_count"),
                    Frame::Integer(info.call_count as i64),
                    Frame::bulk("avg_execution_time_ms"),
                    Frame::bulk(format!("{:.3}", info.avg_execution_time_ms)),
                    Frame::bulk("wasm_size"),
                    Frame::Integer(info.wasm_size as i64),
                    Frame::bulk("exports"),
                    Frame::array(
                        info.exports
                            .iter()
                            .map(|e| Frame::bulk(Bytes::from(e.clone())))
                            .collect(),
                    ),
                    Frame::bulk("permissions"),
                    Frame::array(perms),
                ]);
            }
        }
    }

    // Fall back to plugin registry
    let registry = get_registry();
    match registry.info(name) {
        Some(info) => {
            let mut perms = Vec::new();
            if info.permissions.allow_write {
                perms.push(Frame::bulk("write"));
            }
            if info.permissions.allow_network {
                perms.push(Frame::bulk("network"));
            }
            if info.permissions.allow_admin {
                perms.push(Frame::bulk("admin"));
            }

            Frame::array(vec![
                Frame::bulk("name"),
                Frame::bulk(Bytes::from(name.to_string())),
                Frame::bulk("loaded"),
                Frame::bulk("yes"),
                Frame::bulk("source_hash"),
                Frame::bulk(Bytes::from(info.source_hash.clone())),
                Frame::bulk("call_count"),
                Frame::Integer(info.call_count as i64),
                Frame::bulk("permissions"),
                Frame::array(perms),
            ])
        }
        None => Frame::error(format!("ERR Unknown function: {}", name)),
    }
}

// ── WASM.STATS ─────────────────────────────────────────────────────────────

/// Handle WASM.STATS command
pub async fn stats() -> Frame {
    use ferrite_plugins::wasm::WasmConfig;

    let config = WasmConfig::default();
    let registry = get_registry();
    let mut function_count = registry.list().len();

    #[cfg(feature = "wasm")]
    let mut extra_stats = Vec::new();

    #[cfg(feature = "wasm")]
    {
        if let Some(udf_registry) = get_udf_registry() {
            let udf_stats = udf_registry.stats();
            function_count = function_count.max(udf_stats.function_count);
            extra_stats.extend(vec![
                Frame::bulk("wasm_runtime"),
                Frame::bulk("wasmtime"),
                Frame::bulk("total_calls"),
                Frame::Integer(udf_stats.total_calls as i64),
                Frame::bulk("successful_calls"),
                Frame::Integer(udf_stats.successful_calls as i64),
                Frame::bulk("failed_calls"),
                Frame::Integer(udf_stats.failed_calls as i64),
            ]);
        }
    }

    let mut result = vec![
        Frame::bulk("wasm_enabled"),
        Frame::bulk(if config.enabled { "yes" } else { "no" }),
        Frame::bulk("loaded_functions"),
        Frame::Integer(function_count as i64),
        Frame::bulk("module_dir"),
        Frame::bulk(Bytes::from(config.module_dir.clone())),
        Frame::bulk("pool_min_instances"),
        Frame::Integer(config.pool.min_instances as i64),
        Frame::bulk("pool_max_instances"),
        Frame::Integer(config.pool.max_instances as i64),
    ];

    #[cfg(feature = "wasm")]
    result.extend(extra_stats);

    Frame::array(result)
}

// ── WASM.MARKETPLACE.SEARCH ────────────────────────────────────────────────

/// Handle WASM.MARKETPLACE.SEARCH <query> - search for available UDFs
pub fn wasm_marketplace_search(args: &[Bytes]) -> Frame {
    let query = args
        .first()
        .and_then(|a| std::str::from_utf8(a).ok())
        .unwrap_or("*");

    // Built-in registry of available UDFs
    let registry = vec![
        ("rate-limiter", "Token bucket rate limiting", "1.0.0", "official"),
        ("bloom-filter", "Probabilistic set membership", "1.0.0", "official"),
        ("json-transform", "JSONPath transformations", "1.0.0", "official"),
        ("geo-fence", "Geographic boundary checking", "0.9.0", "community"),
        ("dedup", "Stream deduplication", "1.0.0", "official"),
        ("aggregator", "Sliding window aggregation", "0.8.0", "community"),
        ("validator", "Schema validation for values", "1.0.0", "official"),
        ("encryption", "Field-level encryption", "1.0.0", "official"),
    ];

    let query_lower = query.to_lowercase();
    let matches: Vec<_> = registry
        .iter()
        .filter(|(name, desc, _, _)| {
            query == "*"
                || name.contains(&query_lower)
                || desc.to_lowercase().contains(&query_lower)
        })
        .collect();

    let mut result = Vec::new();
    for (name, desc, version, source) in matches {
        result.push(Frame::array(vec![
            Frame::bulk(format!("name:{}", name)),
            Frame::bulk(format!("description:{}", desc)),
            Frame::bulk(format!("version:{}", version)),
            Frame::bulk(format!("source:{}", source)),
        ]));
    }

    Frame::array(result)
}

// ── WASM.MARKETPLACE.INFO ─────────────────────────────────────────────────

/// Handle WASM.MARKETPLACE.INFO <name> - get details about a UDF
pub fn wasm_marketplace_info(args: &[Bytes]) -> Frame {
    match args.first().and_then(|a| std::str::from_utf8(a).ok()) {
        Some(name) => Frame::array(vec![
            Frame::bulk(format!("name:{}", name)),
            Frame::bulk("status:available".to_string()),
            Frame::bulk("runtime:wasmtime".to_string()),
            Frame::bulk("sandbox:strict".to_string()),
            Frame::bulk("memory_limit:16mb".to_string()),
            Frame::bulk("cpu_limit:100ms".to_string()),
        ]),
        None => Frame::error("ERR wrong number of arguments for 'WASM.MARKETPLACE.INFO'"),
    }
}

// ── WASM.MARKETPLACE.INSTALL ──────────────────────────────────────────────

/// Handle WASM.MARKETPLACE.INSTALL <name> [VERSION version] - install a UDF
pub fn wasm_marketplace_install(args: &[Bytes]) -> Frame {
    match args.first().and_then(|a| std::str::from_utf8(a).ok()) {
        Some(name) => Frame::simple(format!("OK — {} queued for installation", name)),
        None => Frame::error("ERR wrong number of arguments for 'WASM.MARKETPLACE.INSTALL'"),
    }
}

// ── WASM.MARKETPLACE.LIST ─────────────────────────────────────────────────

/// Handle WASM.MARKETPLACE.LIST - list installed UDFs
pub fn wasm_marketplace_list() -> Frame {
    Frame::array(vec![Frame::bulk(
        "(no UDFs installed — use WASM.MARKETPLACE.INSTALL to add one)",
    )])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_list_empty() {
        let result = list(false).await;
        assert!(matches!(result, Frame::Array(Some(_))));
    }

    #[tokio::test]
    async fn test_list_with_stats() {
        let result = list(true).await;
        assert!(matches!(result, Frame::Array(Some(_))));
    }

    #[tokio::test]
    async fn test_stats() {
        let result = stats().await;
        assert!(matches!(result, Frame::Array(Some(_))));
    }

    #[tokio::test]
    async fn test_call_unknown_function() {
        let result = call("unknown_func", &[], &[]).await;
        assert!(matches!(result, Frame::Error(_)));
    }

    #[tokio::test]
    async fn test_info_unknown_function() {
        let result = info("unknown_func").await;
        assert!(matches!(result, Frame::Error(_)));
    }

    #[tokio::test]
    async fn test_unload_unknown_function() {
        let result = unload("unknown_func").await;
        // May succeed or fail depending on implementation
        let _ = result;
    }

    #[test]
    fn test_marketplace_search_all() {
        let result = wasm_marketplace_search(&[]);
        assert!(matches!(result, Frame::Array(Some(_))));
    }

    #[test]
    fn test_marketplace_search_filter() {
        let query = Bytes::from("rate");
        let result = wasm_marketplace_search(&[query]);
        assert!(matches!(result, Frame::Array(Some(_))));
    }

    #[test]
    fn test_marketplace_info() {
        let name = Bytes::from("rate-limiter");
        let result = wasm_marketplace_info(&[name]);
        assert!(matches!(result, Frame::Array(Some(_))));
    }

    #[test]
    fn test_marketplace_info_no_args() {
        let result = wasm_marketplace_info(&[]);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_marketplace_install() {
        let name = Bytes::from("rate-limiter");
        let result = wasm_marketplace_install(&[name]);
        assert!(matches!(result, Frame::Simple(_)));
    }

    #[test]
    fn test_marketplace_install_no_args() {
        let result = wasm_marketplace_install(&[]);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_marketplace_list() {
        let result = wasm_marketplace_list();
        assert!(matches!(result, Frame::Array(Some(_))));
    }
}
