//! FUNCTION.* command handlers
//!
//! Serverless function deployment, invocation, and management.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;

use bytes::Bytes;

use crate::protocol::Frame;
use ferrite_plugins::faas::serverless::{
    FunctionConfig, FunctionDefinition, FunctionRuntime, FunctionTrigger, ServerlessRuntime,
    TriggerEvent,
};

use super::err_frame;

/// Global serverless runtime singleton.
static SERVERLESS_RT: OnceLock<ServerlessRuntime> = OnceLock::new();

fn get_runtime() -> &'static ServerlessRuntime {
    SERVERLESS_RT.get_or_init(|| ServerlessRuntime::new(FunctionConfig::default()))
}

/// Dispatch a `FUNCTION` subcommand.
pub fn function_command(subcommand: &str, args: &[String]) -> Frame {
    match subcommand.to_uppercase().as_str() {
        "DEPLOY" => function_deploy(args),
        "INVOKE" => function_invoke(args),
        "INVOKE.ASYNC" => function_invoke_async(args),
        "UNDEPLOY" => function_undeploy(args),
        "LIST" => function_list(),
        "INFO" => function_info(args),
        "RESULT" => function_result(args),
        "HISTORY" => function_history(args),
        "ENABLE" => function_enable(args),
        "DISABLE" => function_disable(args),
        "STATS" => function_stats(),
        "HELP" => function_help(),
        _ => err_frame(&format!("unknown FUNCTION subcommand '{}'", subcommand)),
    }
}

/// FUNCTION.DEPLOY name RUNTIME js|ts|python|wasm SOURCE "code..." [TRIGGER event [PATTERN pat]] [TIMEOUT ms] [MEMORY bytes]
fn function_deploy(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("FUNCTION.DEPLOY requires at least: name RUNTIME type SOURCE code");
    }

    let name = args[0].clone();
    let mut runtime: Option<FunctionRuntime> = None;
    let mut source: Option<String> = None;
    let mut triggers = Vec::new();
    let mut timeout = Duration::from_secs(5);
    let mut memory_limit = 64 * 1024 * 1024u64;

    let mut i = 1;
    while i < args.len() {
        match args[i].to_uppercase().as_str() {
            "RUNTIME" if i + 1 < args.len() => {
                runtime = FunctionRuntime::from_str_name(&args[i + 1]);
                if runtime.is_none() {
                    return err_frame(&format!("unknown runtime: {}", args[i + 1]));
                }
                i += 2;
            }
            "SOURCE" if i + 1 < args.len() => {
                source = Some(args[i + 1].clone());
                i += 2;
            }
            "TRIGGER" if i + 1 < args.len() => {
                let event = match TriggerEvent::from_str_name(&args[i + 1]) {
                    Some(e) => e,
                    None => {
                        return err_frame(&format!("unknown trigger event: {}", args[i + 1]));
                    }
                };
                let mut pattern = None;
                i += 2;
                if i + 1 < args.len() && args[i].to_uppercase() == "PATTERN" {
                    pattern = Some(args[i + 1].clone());
                    i += 2;
                }
                triggers.push(FunctionTrigger {
                    event,
                    key_pattern: pattern,
                    filter: None,
                    debounce: None,
                });
            }
            "TIMEOUT" if i + 1 < args.len() => {
                if let Ok(ms) = args[i + 1].parse::<u64>() {
                    timeout = Duration::from_millis(ms);
                }
                i += 2;
            }
            "MEMORY" if i + 1 < args.len() => {
                if let Ok(bytes) = args[i + 1].parse::<u64>() {
                    memory_limit = bytes;
                }
                i += 2;
            }
            _ => {
                i += 1;
            }
        }
    }

    let runtime = match runtime {
        Some(r) => r,
        None => return err_frame("RUNTIME is required"),
    };
    let source = match source {
        Some(s) => s,
        None => return err_frame("SOURCE is required"),
    };

    if triggers.is_empty() {
        triggers.push(FunctionTrigger {
            event: TriggerEvent::Manual,
            key_pattern: None,
            filter: None,
            debounce: None,
        });
    }

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let def = FunctionDefinition {
        name,
        runtime,
        source,
        triggers,
        timeout,
        memory_limit,
        env: HashMap::new(),
        created_at: now_ms,
        updated_at: now_ms,
        version: 1,
        enabled: true,
    };

    match get_runtime().deploy(def) {
        Ok(id) => Frame::bulk(Bytes::from(id)),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// FUNCTION.INVOKE name [EVENT json_payload]
fn function_invoke(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("FUNCTION.INVOKE requires: name [EVENT json]");
    }

    let name = &args[0];
    let mut event = serde_json::json!({});

    let mut i = 1;
    while i < args.len() {
        if args[i].to_uppercase() == "EVENT" && i + 1 < args.len() {
            match serde_json::from_str(&args[i + 1]) {
                Ok(v) => event = v,
                Err(e) => return err_frame(&format!("invalid EVENT JSON: {}", e)),
            }
            i += 2;
        } else {
            i += 1;
        }
    }

    match get_runtime().invoke(name, event) {
        Ok(result) => {
            let output_str = result
                .output
                .as_ref()
                .map(|v| v.to_string())
                .unwrap_or_else(|| "null".to_string());
            Frame::array(vec![
                Frame::bulk(format!("execution_id:{}", result.execution_id)),
                Frame::bulk(format!("status:success")),
                Frame::bulk(format!("output:{}", output_str)),
                Frame::bulk(format!("duration_ms:{}", result.duration_ms)),
            ])
        }
        Err(e) => err_frame(&e.to_string()),
    }
}

/// FUNCTION.INVOKE.ASYNC name [EVENT json_payload]
fn function_invoke_async(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("FUNCTION.INVOKE.ASYNC requires: name [EVENT json]");
    }

    let name = &args[0];
    let mut event = serde_json::json!({});

    let mut i = 1;
    while i < args.len() {
        if args[i].to_uppercase() == "EVENT" && i + 1 < args.len() {
            match serde_json::from_str(&args[i + 1]) {
                Ok(v) => event = v,
                Err(_) => {}
            }
            i += 2;
        } else {
            i += 1;
        }
    }

    match get_runtime().invoke_async(name, event) {
        Ok(exec_id) => Frame::bulk(Bytes::from(exec_id)),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// FUNCTION.UNDEPLOY name
fn function_undeploy(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("FUNCTION.UNDEPLOY requires: name");
    }

    match get_runtime().undeploy(&args[0]) {
        Ok(()) => Frame::Simple(Bytes::from("OK")),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// FUNCTION.LIST
fn function_list() -> Frame {
    let fns = get_runtime().list_functions();
    let items: Vec<Frame> = fns
        .iter()
        .map(|f| {
            Frame::array(vec![
                Frame::bulk(format!("name:{}", f.name)),
                Frame::bulk(format!("runtime:{}", f.runtime.name())),
                Frame::bulk(format!("version:{}", f.version)),
                Frame::bulk(format!("enabled:{}", f.enabled)),
                Frame::bulk(format!("invocations:{}", f.invocations)),
            ])
        })
        .collect();
    Frame::Array(Some(items))
}

/// FUNCTION.INFO name
fn function_info(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("FUNCTION.INFO requires: name");
    }

    match get_runtime().get_function(&args[0]) {
        Some(def) => Frame::array(vec![
            Frame::bulk(format!("name:{}", def.name)),
            Frame::bulk(format!("runtime:{}", def.runtime.name())),
            Frame::bulk(format!("version:{}", def.version)),
            Frame::bulk(format!("enabled:{}", def.enabled)),
            Frame::bulk(format!("triggers:{}", def.triggers.len())),
            Frame::bulk(format!("timeout_ms:{}", def.timeout.as_millis())),
            Frame::bulk(format!("memory_limit:{}", def.memory_limit)),
        ]),
        None => Frame::Null,
    }
}

/// FUNCTION.RESULT execution_id
fn function_result(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("FUNCTION.RESULT requires: execution_id");
    }

    match get_runtime().get_result(&args[0]) {
        Some(r) => {
            let output_str = r
                .output
                .as_ref()
                .map(|v| v.to_string())
                .unwrap_or_else(|| "null".to_string());
            Frame::array(vec![
                Frame::bulk(format!("function:{}", r.function_name)),
                Frame::bulk(format!("execution_id:{}", r.execution_id)),
                Frame::bulk(format!("output:{}", output_str)),
                Frame::bulk(format!("duration_ms:{}", r.duration_ms)),
            ])
        }
        None => Frame::Null,
    }
}

/// FUNCTION.HISTORY name [LIMIT n]
fn function_history(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("FUNCTION.HISTORY requires: name [LIMIT n]");
    }

    let name = &args[0];
    let mut limit = 20usize;

    if args.len() >= 3 && args[1].to_uppercase() == "LIMIT" {
        limit = args[2].parse().unwrap_or(20);
    }

    let results = get_runtime().recent_executions(name, limit);
    let items: Vec<Frame> = results
        .iter()
        .map(|r| {
            Frame::array(vec![
                Frame::bulk(format!("execution_id:{}", r.execution_id)),
                Frame::bulk(format!("duration_ms:{}", r.duration_ms)),
            ])
        })
        .collect();
    Frame::Array(Some(items))
}

/// FUNCTION.ENABLE name
fn function_enable(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("FUNCTION.ENABLE requires: name");
    }

    match get_runtime().enable(&args[0]) {
        Ok(()) => Frame::Simple(Bytes::from("OK")),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// FUNCTION.DISABLE name
fn function_disable(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("FUNCTION.DISABLE requires: name");
    }

    match get_runtime().disable(&args[0]) {
        Ok(()) => Frame::Simple(Bytes::from("OK")),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// FUNCTION.STATS
fn function_stats() -> Frame {
    let stats = get_runtime().stats();
    Frame::array(vec![
        Frame::bulk(format!("deployed_functions:{}", stats.deployed_functions)),
        Frame::bulk(format!("total_invocations:{}", stats.total_invocations)),
        Frame::bulk(format!("active_executions:{}", stats.active_executions)),
        Frame::bulk(format!("avg_cold_start_ms:{:.2}", stats.avg_cold_start_ms)),
        Frame::bulk(format!("avg_execution_ms:{:.2}", stats.avg_execution_ms)),
        Frame::bulk(format!("errors:{}", stats.errors)),
        Frame::bulk(format!("timeouts:{}", stats.timeouts)),
    ])
}

/// FUNCTION.HELP
fn function_help() -> Frame {
    let lines = vec![
        "FUNCTION.DEPLOY name RUNTIME js|ts|python|wasm SOURCE \"code\" [TRIGGER event [PATTERN pat]] [TIMEOUT ms] [MEMORY bytes] - Deploy a function",
        "FUNCTION.INVOKE name [EVENT json] - Invoke a function synchronously",
        "FUNCTION.INVOKE.ASYNC name [EVENT json] - Invoke a function asynchronously",
        "FUNCTION.UNDEPLOY name - Remove a deployed function",
        "FUNCTION.LIST - List all deployed functions",
        "FUNCTION.INFO name - Get function definition details",
        "FUNCTION.RESULT execution_id - Get execution result",
        "FUNCTION.HISTORY name [LIMIT n] - Recent execution history",
        "FUNCTION.ENABLE name - Enable a function",
        "FUNCTION.DISABLE name - Disable a function",
        "FUNCTION.STATS - Runtime statistics",
    ];
    Frame::Array(Some(
        lines
            .into_iter()
            .map(|l| Frame::bulk(l.to_string()))
            .collect(),
    ))
}
