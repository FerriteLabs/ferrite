use bytes::Bytes;

use super::{get_bytes, get_float, get_int, get_string};
use crate::commands::parser::Command;
use crate::error::{FerriteError, Result};
use crate::protocol::Frame;

pub(crate) fn parse_ft_create(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("FT.CREATE".to_string()));
    }

    let index = get_bytes(&args[0])?;
    let mut schema = Vec::new();
    let mut index_type = None;
    let mut dimension = None;
    let mut metric = None;

    let mut i = 1;
    while i < args.len() {
        let arg = get_string(&args[i])?.to_uppercase();
        match arg.as_str() {
            "SCHEMA" => {
                i += 1;
                // Parse field definitions
                while i + 1 < args.len() {
                    let field_name = get_string(&args[i])?;
                    let field_type = get_string(&args[i + 1])?.to_uppercase();

                    // Check if this is a keyword rather than field type
                    if field_type == "VECTOR"
                        || field_type == "TEXT"
                        || field_type == "NUMERIC"
                        || field_type == "TAG"
                    {
                        schema.push((field_name, field_type));
                        i += 2;
                    } else {
                        break;
                    }
                }
            }
            "TYPE" => {
                i += 1;
                if i < args.len() {
                    index_type = Some(get_string(&args[i])?);
                    i += 1;
                }
            }
            "DIM" | "DIMENSION" => {
                i += 1;
                if i < args.len() {
                    dimension = Some(get_int(&args[i])? as usize);
                    i += 1;
                }
            }
            "DISTANCE_METRIC" | "METRIC" => {
                i += 1;
                if i < args.len() {
                    metric = Some(get_string(&args[i])?);
                    i += 1;
                }
            }
            _ => {
                i += 1;
            }
        }
    }

    Ok(Command::FtCreate {
        index,
        schema,
        index_type,
        dimension,
        metric,
    })
}

pub(crate) fn parse_ft_dropindex(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("FT.DROPINDEX".to_string()));
    }

    let index = get_bytes(&args[0])?;
    let mut delete_docs = false;

    for arg in &args[1..] {
        let s = get_string(arg)?.to_uppercase();
        if s == "DD" {
            delete_docs = true;
        }
    }

    Ok(Command::FtDropIndex { index, delete_docs })
}

pub(crate) fn parse_ft_add(args: &[Frame]) -> Result<Command> {
    if args.len() < 3 {
        return Err(FerriteError::WrongArity("FT.ADD".to_string()));
    }

    let index = get_bytes(&args[0])?;
    let key = get_bytes(&args[1])?;

    // Parse vector - can be comma-separated string or array of numbers
    let vector_str = get_string(&args[2])?;
    let vector: Vec<f32> = vector_str
        .split([',', ' '])
        .filter(|s| !s.is_empty())
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    let mut payload = None;
    let mut i = 3;
    while i < args.len() {
        let arg = get_string(&args[i])?.to_uppercase();
        if arg == "PAYLOAD" && i + 1 < args.len() {
            payload = Some(get_bytes(&args[i + 1])?);
            i += 2;
        } else {
            i += 1;
        }
    }

    Ok(Command::FtAdd {
        index,
        key,
        vector,
        payload,
    })
}

pub(crate) fn parse_ft_del(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("FT.DEL".to_string()));
    }

    let index = get_bytes(&args[0])?;
    let key = get_bytes(&args[1])?;

    Ok(Command::FtDel { index, key })
}

pub(crate) fn parse_ft_search(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("FT.SEARCH".to_string()));
    }

    let index = get_bytes(&args[0])?;

    // Parse query vector
    let query_str = get_string(&args[1])?;
    let query: Vec<f32> = query_str
        .split([',', ' '])
        .filter(|s| !s.is_empty())
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    let mut k = 10; // default
    let mut return_fields = Vec::new();
    let mut filter = None;

    let mut i = 2;
    while i < args.len() {
        let arg = get_string(&args[i])?.to_uppercase();
        match arg.as_str() {
            "KNN" | "K" => {
                i += 1;
                if i < args.len() {
                    k = get_int(&args[i])? as usize;
                    i += 1;
                }
            }
            "RETURN" => {
                i += 1;
                if i < args.len() {
                    let count = get_int(&args[i])? as usize;
                    i += 1;
                    for _ in 0..count {
                        if i < args.len() {
                            return_fields.push(get_string(&args[i])?);
                            i += 1;
                        }
                    }
                }
            }
            "FILTER" => {
                i += 1;
                if i < args.len() {
                    filter = Some(get_string(&args[i])?);
                    i += 1;
                }
            }
            _ => {
                i += 1;
            }
        }
    }

    Ok(Command::FtSearch {
        index,
        query,
        k,
        return_fields,
        filter,
    })
}

pub(crate) fn parse_ft_info(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("FT.INFO".to_string()));
    }

    let index = get_bytes(&args[0])?;
    Ok(Command::FtInfo { index })
}

pub(crate) fn parse_crdt_gcounter(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("CRDT.GCOUNTER".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let subcommand = get_string(&args[1])?.to_uppercase();
    let crdt_args: Vec<String> = args[2..]
        .iter()
        .filter_map(|f| get_string(f).ok())
        .collect();

    Ok(Command::CrdtGCounter {
        key,
        subcommand,
        args: crdt_args,
    })
}

pub(crate) fn parse_crdt_pncounter(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("CRDT.PNCOUNTER".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let subcommand = get_string(&args[1])?.to_uppercase();
    let crdt_args: Vec<String> = args[2..]
        .iter()
        .filter_map(|f| get_string(f).ok())
        .collect();

    Ok(Command::CrdtPNCounter {
        key,
        subcommand,
        args: crdt_args,
    })
}

pub(crate) fn parse_crdt_lwwreg(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("CRDT.LWWREG".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let subcommand = get_string(&args[1])?.to_uppercase();
    let crdt_args: Vec<String> = args[2..]
        .iter()
        .filter_map(|f| get_string(f).ok())
        .collect();

    Ok(Command::CrdtLwwRegister {
        key,
        subcommand,
        args: crdt_args,
    })
}

pub(crate) fn parse_crdt_orset(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("CRDT.ORSET".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let subcommand = get_string(&args[1])?.to_uppercase();
    let crdt_args: Vec<String> = args[2..]
        .iter()
        .filter_map(|f| get_string(f).ok())
        .collect();

    Ok(Command::CrdtOrSet {
        key,
        subcommand,
        args: crdt_args,
    })
}

pub(crate) fn parse_crdt_info(args: &[Frame]) -> Result<Command> {
    let key = if args.is_empty() {
        None
    } else {
        Some(get_bytes(&args[0])?)
    };

    Ok(Command::CrdtInfo { key })
}

pub(crate) fn parse_wasm_load(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("WASM.LOAD".to_string()));
    }

    let name = get_string(&args[0])?;
    let module = get_bytes(&args[1])?;
    let mut replace = false;
    let mut permissions = Vec::new();

    let mut i = 2;
    while i < args.len() {
        let arg = get_string(&args[i])?.to_uppercase();
        match arg.as_str() {
            "REPLACE" => {
                replace = true;
                i += 1;
            }
            "PERMISSIONS" => {
                i += 1;
                while i < args.len() {
                    let perm = get_string(&args[i])?;
                    if perm.starts_with('-') || perm.to_uppercase() == "REPLACE" {
                        break;
                    }
                    permissions.push(perm);
                    i += 1;
                }
            }
            _ => {
                i += 1;
            }
        }
    }

    Ok(Command::WasmLoad {
        name,
        module,
        replace,
        permissions,
    })
}

pub(crate) fn parse_wasm_unload(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("WASM.UNLOAD".to_string()));
    }

    let name = get_string(&args[0])?;
    Ok(Command::WasmUnload { name })
}

pub(crate) fn parse_wasm_call(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("WASM.CALL".to_string()));
    }

    let name = get_string(&args[0])?;
    let numkeys = get_int(&args[1])? as usize;

    let mut keys = Vec::new();
    let mut wasm_args = Vec::new();

    let mut i = 2;
    for _ in 0..numkeys {
        if i < args.len() {
            keys.push(get_bytes(&args[i])?);
            i += 1;
        }
    }

    while i < args.len() {
        wasm_args.push(get_bytes(&args[i])?);
        i += 1;
    }

    Ok(Command::WasmCall {
        name,
        keys,
        args: wasm_args,
    })
}

pub(crate) fn parse_wasm_call_ro(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("WASM.CALL_RO".to_string()));
    }

    let name = get_string(&args[0])?;
    let numkeys = get_int(&args[1])? as usize;

    let mut keys = Vec::new();
    let mut wasm_args = Vec::new();

    let mut i = 2;
    for _ in 0..numkeys {
        if i < args.len() {
            keys.push(get_bytes(&args[i])?);
            i += 1;
        }
    }

    while i < args.len() {
        wasm_args.push(get_bytes(&args[i])?);
        i += 1;
    }

    Ok(Command::WasmCallRo {
        name,
        keys,
        args: wasm_args,
    })
}

pub(crate) fn parse_wasm_list(args: &[Frame]) -> Result<Command> {
    let mut with_stats = false;

    for arg in args {
        let s = get_string(arg)?.to_uppercase();
        if s == "WITHSTATS" {
            with_stats = true;
        }
    }

    Ok(Command::WasmList { with_stats })
}

pub(crate) fn parse_wasm_info(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("WASM.INFO".to_string()));
    }

    let name = get_string(&args[0])?;
    Ok(Command::WasmInfo { name })
}

pub(crate) fn parse_semantic_set(args: &[Frame]) -> Result<Command> {
    // SEMANTIC.SET query value embedding [EX seconds] [THRESHOLD threshold]
    if args.len() < 3 {
        return Err(FerriteError::WrongArity("SEMANTIC.SET".to_string()));
    }

    let query = get_bytes(&args[0])?;
    let value = get_bytes(&args[1])?;

    // Parse embedding - it could be a space-separated string or array of floats
    let embedding = parse_embedding(&args[2])?;

    let mut ttl_secs = None;
    let mut threshold = None;

    let mut i = 3;
    while i < args.len() {
        let option = get_string(&args[i])?.to_uppercase();
        match option.as_str() {
            "EX" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                ttl_secs = Some(get_int(&args[i])? as u64);
            }
            "THRESHOLD" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                threshold = Some(get_float(&args[i])? as f32);
            }
            _ => return Err(FerriteError::Syntax),
        }
        i += 1;
    }

    Ok(Command::SemanticSet {
        query,
        value,
        embedding,
        ttl_secs,
        threshold,
    })
}

pub(crate) fn parse_semantic_get(args: &[Frame]) -> Result<Command> {
    // SEMANTIC.GET embedding [THRESHOLD threshold] [COUNT count]
    if args.is_empty() {
        return Err(FerriteError::WrongArity("SEMANTIC.GET".to_string()));
    }

    let embedding = parse_embedding(&args[0])?;

    let mut threshold = None;
    let mut count = None;

    let mut i = 1;
    while i < args.len() {
        let option = get_string(&args[i])?.to_uppercase();
        match option.as_str() {
            "THRESHOLD" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                threshold = Some(get_float(&args[i])? as f32);
            }
            "COUNT" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                count = Some(get_int(&args[i])? as usize);
            }
            _ => return Err(FerriteError::Syntax),
        }
        i += 1;
    }

    Ok(Command::SemanticGet {
        embedding,
        threshold,
        count,
    })
}

pub(crate) fn parse_semantic_gettext(args: &[Frame]) -> Result<Command> {
    // SEMANTIC.GETTEXT query [THRESHOLD threshold] [COUNT count]
    if args.is_empty() {
        return Err(FerriteError::WrongArity("SEMANTIC.GETTEXT".to_string()));
    }

    let query = get_bytes(&args[0])?;

    let mut threshold = None;
    let mut count = None;

    let mut i = 1;
    while i < args.len() {
        let option = get_string(&args[i])?.to_uppercase();
        match option.as_str() {
            "THRESHOLD" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                threshold = Some(get_float(&args[i])? as f32);
            }
            "COUNT" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                count = Some(get_int(&args[i])? as usize);
            }
            _ => return Err(FerriteError::Syntax),
        }
        i += 1;
    }

    Ok(Command::SemanticGetText {
        query,
        threshold,
        count,
    })
}

pub(crate) fn parse_semantic_del(args: &[Frame]) -> Result<Command> {
    // SEMANTIC.DEL id
    if args.is_empty() {
        return Err(FerriteError::WrongArity("SEMANTIC.DEL".to_string()));
    }

    let id = get_int(&args[0])? as u64;
    Ok(Command::SemanticDel { id })
}

pub(crate) fn parse_semantic_config(args: &[Frame]) -> Result<Command> {
    // SEMANTIC.CONFIG GET|SET [param] [value]
    if args.is_empty() {
        return Err(FerriteError::WrongArity("SEMANTIC.CONFIG".to_string()));
    }

    let operation = get_bytes(&args[0])?;
    let param = args.get(1).map(get_bytes).transpose()?;
    let value = args.get(2).map(get_bytes).transpose()?;

    Ok(Command::SemanticConfig {
        operation,
        param,
        value,
    })
}

/// Parse an embedding vector from a frame
/// Supports: space-separated string of floats, or array of floats
pub(crate) fn parse_embedding(frame: &Frame) -> Result<Vec<f32>> {
    match frame {
        Frame::Simple(b) | Frame::Bulk(Some(b)) => {
            // Parse space-separated or comma-separated floats
            let s = String::from_utf8(b.to_vec())
                .map_err(|_| FerriteError::Protocol("invalid UTF-8".to_string()))?;

            let floats: Result<Vec<f32>> = s
                .split(|c: char| c.is_whitespace() || c == ',')
                .filter(|s| !s.is_empty())
                .map(|s| {
                    s.parse::<f32>()
                        .map_err(|_| FerriteError::Protocol(format!("invalid float: {}", s)))
                })
                .collect();

            floats
        }
        Frame::Array(Some(frames)) => {
            // Parse array of floats
            frames
                .iter()
                .map(|f| match f {
                    Frame::Simple(b) | Frame::Bulk(Some(b)) => {
                        let s = String::from_utf8(b.to_vec())
                            .map_err(|_| FerriteError::Protocol("invalid UTF-8".to_string()))?;
                        s.parse::<f32>()
                            .map_err(|_| FerriteError::Protocol(format!("invalid float: {}", s)))
                    }
                    Frame::Integer(i) => Ok(*i as f32),
                    Frame::Double(d) => Ok(*d as f32),
                    _ => Err(FerriteError::Protocol("expected float".to_string())),
                })
                .collect()
        }
        _ => Err(FerriteError::Protocol(
            "expected embedding vector".to_string(),
        )),
    }
}

pub(crate) fn parse_trigger_create(args: &[Frame]) -> Result<Command> {
    // TRIGGER.CREATE name ON event pattern [DO action...] [WASM module func] [PRIORITY n] [DESC text]
    if args.len() < 4 {
        return Err(FerriteError::WrongArity("TRIGGER.CREATE".to_string()));
    }

    let name = get_bytes(&args[0])?;

    // Expect "ON"
    let on_keyword = get_string(&args[1])?.to_uppercase();
    if on_keyword != "ON" {
        return Err(FerriteError::Syntax);
    }

    let event_type = get_bytes(&args[2])?;
    let pattern = get_bytes(&args[3])?;

    let mut actions = Vec::new();
    let mut wasm_module = None;
    let mut wasm_function = None;
    let mut priority = None;
    let mut description = None;

    let mut i = 4;
    while i < args.len() {
        let keyword = get_string(&args[i])?.to_uppercase();
        match keyword.as_str() {
            "DO" => {
                i += 1;
                // Collect actions until END or another keyword
                while i < args.len() {
                    let action_or_keyword = get_string(&args[i])?;
                    let upper = action_or_keyword.to_uppercase();
                    if upper == "END" || upper == "WASM" || upper == "PRIORITY" || upper == "DESC" {
                        break;
                    }
                    actions.push(get_bytes(&args[i])?);
                    i += 1;
                }
                continue;
            }
            "WASM" => {
                i += 1;
                if i + 1 >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                wasm_module = Some(get_bytes(&args[i])?);
                i += 1;
                wasm_function = Some(get_bytes(&args[i])?);
            }
            "PRIORITY" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                priority = Some(get_int(&args[i])? as i32);
            }
            "DESC" | "DESCRIPTION" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                description = Some(get_bytes(&args[i])?);
            }
            "END" => {
                // End of action block
            }
            _ => {
                // Unknown keyword, treat as action
                actions.push(get_bytes(&args[i])?);
            }
        }
        i += 1;
    }

    Ok(Command::TriggerCreate {
        name,
        event_type,
        pattern,
        actions,
        wasm_module,
        wasm_function,
        priority,
        description,
    })
}

pub(crate) fn parse_trigger_delete(args: &[Frame]) -> Result<Command> {
    // TRIGGER.DELETE name
    if args.is_empty() {
        return Err(FerriteError::WrongArity("TRIGGER.DELETE".to_string()));
    }

    let name = get_bytes(&args[0])?;
    Ok(Command::TriggerDelete { name })
}

pub(crate) fn parse_trigger_get(args: &[Frame]) -> Result<Command> {
    // TRIGGER.GET name
    if args.is_empty() {
        return Err(FerriteError::WrongArity("TRIGGER.GET".to_string()));
    }

    let name = get_bytes(&args[0])?;
    Ok(Command::TriggerGet { name })
}

pub(crate) fn parse_trigger_list(args: &[Frame]) -> Result<Command> {
    // TRIGGER.LIST [pattern]
    let pattern = args.first().map(get_bytes).transpose()?;
    Ok(Command::TriggerList { pattern })
}

pub(crate) fn parse_trigger_enable(args: &[Frame]) -> Result<Command> {
    // TRIGGER.ENABLE name
    if args.is_empty() {
        return Err(FerriteError::WrongArity("TRIGGER.ENABLE".to_string()));
    }

    let name = get_bytes(&args[0])?;
    Ok(Command::TriggerEnable { name })
}

pub(crate) fn parse_trigger_disable(args: &[Frame]) -> Result<Command> {
    // TRIGGER.DISABLE name
    if args.is_empty() {
        return Err(FerriteError::WrongArity("TRIGGER.DISABLE".to_string()));
    }

    let name = get_bytes(&args[0])?;
    Ok(Command::TriggerDisable { name })
}

pub(crate) fn parse_trigger_fire(args: &[Frame]) -> Result<Command> {
    // TRIGGER.FIRE name key [value] [TTL seconds]
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("TRIGGER.FIRE".to_string()));
    }

    let name = get_bytes(&args[0])?;
    let key = get_bytes(&args[1])?;

    let mut value = None;
    let mut ttl = None;

    let mut i = 2;
    while i < args.len() {
        let arg = get_string(&args[i])?.to_uppercase();
        match arg.as_str() {
            "TTL" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                ttl = Some(get_int(&args[i])?);
            }
            _ => {
                // Treat as value if not a keyword
                if value.is_none() {
                    value = Some(get_bytes(&args[i])?);
                }
            }
        }
        i += 1;
    }

    Ok(Command::TriggerFire {
        name,
        key,
        value,
        ttl,
    })
}

pub(crate) fn parse_trigger_config(args: &[Frame]) -> Result<Command> {
    // TRIGGER.CONFIG GET|SET [param] [value]
    if args.is_empty() {
        return Err(FerriteError::WrongArity("TRIGGER.CONFIG".to_string()));
    }

    let operation = get_bytes(&args[0])?;
    let param = args.get(1).map(get_bytes).transpose()?;
    let value = args.get(2).map(get_bytes).transpose()?;

    Ok(Command::TriggerConfig {
        operation,
        param,
        value,
    })
}

pub(crate) fn parse_timeseries_command(cmd: &str, args: &[Frame]) -> Result<Command> {
    // Extract subcommand from "TS.SUBCOMMAND"
    let subcommand = cmd.strip_prefix("TS.").unwrap_or(cmd).to_string();

    // Collect all args as Bytes
    let parsed_args: Vec<Bytes> = args.iter().filter_map(|f| get_bytes(f).ok()).collect();

    Ok(Command::TimeSeries {
        subcommand,
        args: parsed_args,
    })
}

pub(crate) fn parse_document_command(cmd: &str, args: &[Frame]) -> Result<Command> {
    // Extract subcommand from "DOC.SUBCOMMAND"
    let subcommand = cmd.strip_prefix("DOC.").unwrap_or(cmd).to_string();

    // Collect all args as Bytes
    let parsed_args: Vec<Bytes> = args.iter().filter_map(|f| get_bytes(f).ok()).collect();

    Ok(Command::Document {
        subcommand,
        args: parsed_args,
    })
}

pub(crate) fn parse_graph_command(cmd: &str, args: &[Frame]) -> Result<Command> {
    // Extract subcommand from "GRAPH.SUBCOMMAND"
    let subcommand = cmd.strip_prefix("GRAPH.").unwrap_or(cmd).to_string();

    // Collect all args as Bytes
    let parsed_args: Vec<Bytes> = args.iter().filter_map(|f| get_bytes(f).ok()).collect();

    Ok(Command::Graph {
        subcommand,
        args: parsed_args,
    })
}

pub(crate) fn parse_rag_command(cmd: &str, args: &[Frame]) -> Result<Command> {
    // Extract subcommand from "RAG.SUBCOMMAND"
    let subcommand = cmd.strip_prefix("RAG.").unwrap_or(cmd).to_string();

    // Collect all args as Bytes
    let parsed_args: Vec<Bytes> = args.iter().filter_map(|f| get_bytes(f).ok()).collect();

    Ok(Command::Rag {
        subcommand,
        args: parsed_args,
    })
}

pub(crate) fn parse_query_command(subcommand: &str, args: &[Frame]) -> Result<Command> {
    let parsed_args: Vec<Bytes> = args.iter().filter_map(|f| get_bytes(f).ok()).collect();

    Ok(Command::Query {
        subcommand: subcommand.to_uppercase(),
        args: parsed_args,
    })
}

pub(crate) fn parse_ferrite_advisor(args: &[Frame]) -> Result<Command> {
    let subcommand = if args.is_empty() {
        "STATUS".to_string()
    } else {
        get_string(&args[0])?.to_uppercase()
    };
    let rest_args = args
        .iter()
        .skip(1)
        .filter_map(|f| get_string(f).ok())
        .collect();
    Ok(Command::FerriteAdvisor {
        subcommand,
        args: rest_args,
    })
}

pub(crate) fn parse_vector_hybrid(args: &[Frame]) -> Result<Command> {
    if args.len() < 3 {
        return Err(FerriteError::WrongArity("VECTOR.HYBRID".to_string()));
    }

    let index = get_bytes(&args[0])?;

    // Parse query vector (comma-separated floats)
    let vec_str = get_string(&args[1])?;
    let query_vector: Vec<f32> = vec_str
        .split([',', ' '])
        .filter(|s| !s.is_empty())
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    let query_text = get_string(&args[2])?;

    let mut top_k = 10usize;
    let mut alpha = 0.5f64;
    let mut strategy = "rrf".to_string();

    let mut i = 3;
    while i < args.len() {
        let arg = get_string(&args[i])?.to_uppercase();
        match arg.as_str() {
            "TOP" | "K" => {
                i += 1;
                if i < args.len() {
                    top_k = get_int(&args[i])? as usize;
                    i += 1;
                }
            }
            "ALPHA" => {
                i += 1;
                if i < args.len() {
                    alpha = get_float(&args[i])?;
                    i += 1;
                }
            }
            "STRATEGY" => {
                i += 1;
                if i < args.len() {
                    strategy = get_string(&args[i])?.to_lowercase();
                    i += 1;
                }
            }
            _ => {
                i += 1;
            }
        }
    }

    Ok(Command::VectorHybridSearch {
        index,
        query_vector,
        query_text,
        top_k,
        alpha,
        strategy,
    })
}

pub(crate) fn parse_vector_rerank(args: &[Frame]) -> Result<Command> {
    if args.len() < 3 {
        return Err(FerriteError::WrongArity("VECTOR.RERANK".to_string()));
    }

    let index = get_bytes(&args[0])?;
    let query_text = get_string(&args[1])?;

    let mut doc_ids = Vec::new();
    let mut top_k = 10usize;

    let mut i = 2;
    while i < args.len() {
        let arg = get_string(&args[i])?;
        if arg.to_uppercase() == "TOP" {
            i += 1;
            if i < args.len() {
                top_k = get_int(&args[i])? as usize;
                i += 1;
            }
        } else {
            doc_ids.push(arg);
            i += 1;
        }
    }

    Ok(Command::VectorRerank {
        index,
        query_text,
        doc_ids,
        top_k,
    })
}

// ── Materialized view command parsers ────────────────────────────────────────

pub(crate) fn parse_view_create(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("VIEW.CREATE".to_string()));
    }

    let name = get_bytes(&args[0])?;
    let query = get_string(&args[1])?;
    let mut strategy = "lazy".to_string();
    let mut interval: Option<u64> = None;

    let mut i = 2;
    while i < args.len() {
        let arg = get_string(&args[i])?.to_uppercase();
        match arg.as_str() {
            "STRATEGY" => {
                i += 1;
                if i < args.len() {
                    strategy = get_string(&args[i])?.to_lowercase();
                    i += 1;
                }
            }
            "INTERVAL" => {
                i += 1;
                if i < args.len() {
                    interval = Some(get_int(&args[i])? as u64);
                    i += 1;
                }
            }
            _ => {
                i += 1;
            }
        }
    }

    Ok(Command::ViewCreate {
        name,
        query,
        strategy,
        interval,
    })
}

pub(crate) fn parse_view_drop(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("VIEW.DROP".to_string()));
    }
    let name = get_bytes(&args[0])?;
    Ok(Command::ViewDrop { name })
}

pub(crate) fn parse_view_query(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("VIEW.QUERY".to_string()));
    }
    let name = get_bytes(&args[0])?;
    Ok(Command::ViewQuery { name })
}

#[allow(unused_variables)]
pub(crate) fn parse_view_list(args: &[Frame]) -> Result<Command> {
    Ok(Command::ViewList)
}

pub(crate) fn parse_view_refresh(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("VIEW.REFRESH".to_string()));
    }
    let name = get_bytes(&args[0])?;
    Ok(Command::ViewRefresh { name })
}

pub(crate) fn parse_view_info(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("VIEW.INFO".to_string()));
    }
    let name = get_bytes(&args[0])?;
    Ok(Command::ViewInfo { name })
}

/// Parse `MIGRATE.START source_uri [BATCH size] [WORKERS n] [VERIFY] [DRY-RUN]`
pub(crate) fn parse_migrate_start(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("MIGRATE.START".to_string()));
    }

    let source_uri = get_string(&args[0])?;
    let mut batch_size: Option<usize> = None;
    let mut workers: Option<usize> = None;
    let mut verify = false;
    let mut dry_run = false;

    let mut i = 1;
    while i < args.len() {
        let arg = get_string(&args[i])?.to_uppercase();
        match arg.as_str() {
            "BATCH" => {
                i += 1;
                if i < args.len() {
                    batch_size = Some(get_int(&args[i])? as usize);
                }
            }
            "WORKERS" => {
                i += 1;
                if i < args.len() {
                    workers = Some(get_int(&args[i])? as usize);
                }
            }
            "VERIFY" => {
                verify = true;
            }
            "DRY-RUN" => {
                dry_run = true;
            }
            _ => {}
        }
        i += 1;
    }

    Ok(Command::MigrateStart {
        source_uri,
        batch_size,
        workers,
        verify,
        dry_run,
    })
}

/// Parse `MIGRATE.VERIFY [SAMPLE pct]`
pub(crate) fn parse_migrate_verify(args: &[Frame]) -> Result<Command> {
    let mut sample_pct: Option<f64> = None;
    let mut i = 0;
    while i < args.len() {
        let arg = get_string(&args[i])?.to_uppercase();
        if arg == "SAMPLE" {
            i += 1;
            if i < args.len() {
                sample_pct = Some(get_float(&args[i])?);
            }
        }
        i += 1;
    }
    Ok(Command::MigrateVerify { sample_pct })
}

// ── Kafka-compatible streaming parsers ──────────────────────────────────

/// STREAM.CREATE topic [PARTITIONS n] [RETENTION ms] [REPLICATION n]
pub(crate) fn parse_stream_create(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("STREAM.CREATE".to_string()));
    }
    let topic = get_string(&args[0])?;
    let mut partitions: u32 = 4;
    let mut retention_ms: i64 = -1;
    let mut replication: u16 = 1;

    let mut i = 1;
    while i < args.len() {
        let arg = get_string(&args[i])?.to_uppercase();
        match arg.as_str() {
            "PARTITIONS" => {
                i += 1;
                if i < args.len() {
                    partitions = get_int(&args[i])? as u32;
                }
            }
            "RETENTION" => {
                i += 1;
                if i < args.len() {
                    retention_ms = get_int(&args[i])?;
                }
            }
            "REPLICATION" => {
                i += 1;
                if i < args.len() {
                    replication = get_int(&args[i])? as u16;
                }
            }
            _ => {}
        }
        i += 1;
    }
    Ok(Command::StreamCreate {
        topic,
        partitions,
        retention_ms,
        replication,
    })
}

/// STREAM.DELETE topic
pub(crate) fn parse_stream_delete(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("STREAM.DELETE".to_string()));
    }
    let topic = get_string(&args[0])?;
    Ok(Command::StreamDelete { topic })
}

/// STREAM.PRODUCE topic [KEY key] value [PARTITION n]
pub(crate) fn parse_stream_produce(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("STREAM.PRODUCE".to_string()));
    }
    let topic = get_string(&args[0])?;
    let mut key: Option<Bytes> = None;
    let mut partition: Option<u32> = None;
    let mut value: Option<Bytes> = None;

    let mut i = 1;
    while i < args.len() {
        let arg_str = get_string(&args[i])?.to_uppercase();
        match arg_str.as_str() {
            "KEY" => {
                i += 1;
                if i < args.len() {
                    key = Some(get_bytes(&args[i])?);
                }
            }
            "PARTITION" => {
                i += 1;
                if i < args.len() {
                    partition = Some(get_int(&args[i])? as u32);
                }
            }
            _ => {
                // Treat as value if not yet set
                if value.is_none() {
                    value = Some(get_bytes(&args[i])?);
                }
            }
        }
        i += 1;
    }

    let value = value.ok_or_else(|| FerriteError::WrongArity("STREAM.PRODUCE".to_string()))?;
    Ok(Command::StreamProduce {
        topic,
        key,
        value,
        partition,
    })
}

/// STREAM.FETCH topic partition offset [COUNT n]
pub(crate) fn parse_stream_fetch(args: &[Frame]) -> Result<Command> {
    if args.len() < 3 {
        return Err(FerriteError::WrongArity("STREAM.FETCH".to_string()));
    }
    let topic = get_string(&args[0])?;
    let partition = get_int(&args[1])? as u32;
    let offset = get_int(&args[2])?;
    let mut count: usize = 100;

    let mut i = 3;
    while i < args.len() {
        let arg = get_string(&args[i])?.to_uppercase();
        if arg == "COUNT" {
            i += 1;
            if i < args.len() {
                count = get_int(&args[i])? as usize;
            }
        }
        i += 1;
    }
    Ok(Command::StreamFetch {
        topic,
        partition,
        offset,
        count,
    })
}

/// STREAM.COMMIT group topic partition offset
pub(crate) fn parse_stream_commit(args: &[Frame]) -> Result<Command> {
    if args.len() < 4 {
        return Err(FerriteError::WrongArity("STREAM.COMMIT".to_string()));
    }
    let group = get_string(&args[0])?;
    let topic = get_string(&args[1])?;
    let partition = get_int(&args[2])? as u32;
    let offset = get_int(&args[3])?;
    Ok(Command::StreamCommit {
        group,
        topic,
        partition,
        offset,
    })
}

/// STREAM.DESCRIBE topic
pub(crate) fn parse_stream_describe(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("STREAM.DESCRIBE".to_string()));
    }
    let topic = get_string(&args[0])?;
    Ok(Command::StreamDescribe { topic })
}

/// STREAM.GROUPS [topic]
pub(crate) fn parse_stream_groups(args: &[Frame]) -> Result<Command> {
    let topic = if args.is_empty() {
        None
    } else {
        Some(get_string(&args[0])?)
    };
    Ok(Command::StreamGroups { topic })
}

/// STREAM.OFFSETS topic partition
pub(crate) fn parse_stream_offsets(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("STREAM.OFFSETS".to_string()));
    }
    let topic = get_string(&args[0])?;
    let partition = get_int(&args[1])? as u32;
    Ok(Command::StreamOffsets { topic, partition })
}

/// REGION.ADD id name endpoint
pub(crate) fn parse_region_add(args: &[Frame]) -> Result<Command> {
    if args.len() < 3 {
        return Err(FerriteError::WrongArity("REGION.ADD".to_string()));
    }
    let id = get_string(&args[0])?;
    let name = get_string(&args[1])?;
    let endpoint = get_string(&args[2])?;
    Ok(Command::RegionAdd { id, name, endpoint })
}

/// REGION.REMOVE id
pub(crate) fn parse_region_remove(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("REGION.REMOVE".to_string()));
    }
    let id = get_string(&args[0])?;
    Ok(Command::RegionRemove { id })
}

/// REGION.STATUS [id]
pub(crate) fn parse_region_status(args: &[Frame]) -> Result<Command> {
    let id = if args.is_empty() {
        None
    } else {
        Some(get_string(&args[0])?)
    };
    Ok(Command::RegionStatus { id })
}

/// REGION.CONFLICTS [LIMIT n]
pub(crate) fn parse_region_conflicts(args: &[Frame]) -> Result<Command> {
    let mut limit: usize = 10;
    let mut i = 0;
    while i < args.len() {
        let arg = get_string(&args[i])?.to_uppercase();
        if arg == "LIMIT" {
            i += 1;
            if i < args.len() {
                limit = get_int(&args[i])? as usize;
            }
        }
        i += 1;
    }
    Ok(Command::RegionConflicts { limit })
}

/// REGION.STRATEGY [strategy]
pub(crate) fn parse_region_strategy(args: &[Frame]) -> Result<Command> {
    let strategy = if args.is_empty() {
        None
    } else {
        Some(get_string(&args[0])?)
    };
    Ok(Command::RegionStrategy { strategy })
}

/// FERRITE.DEBUG subcommand [args...]
pub(crate) fn parse_ferrite_debug(args: &[Frame]) -> Result<Command> {
    let subcommand = if args.is_empty() {
        "STATS".to_string()
    } else {
        get_string(&args[0])?.to_uppercase()
    };
    let rest_args = args
        .iter()
        .skip(1)
        .filter_map(|f| get_string(f).ok())
        .collect();
    Ok(Command::FerriteDebug {
        subcommand,
        args: rest_args,
    })
}

// ── Data Mesh / Federation gateway parsers ────────────────────────────

/// FEDERATION.ADD id TYPE type URI uri [NAME name]
pub(crate) fn parse_federation_add(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("FEDERATION.ADD".to_string()));
    }
    let id = get_string(&args[0])?;
    let mut source_type = String::new();
    let mut uri = String::new();
    let mut name: Option<String> = None;

    let mut i = 1;
    while i < args.len() {
        let arg = get_string(&args[i])?.to_uppercase();
        match arg.as_str() {
            "TYPE" => {
                i += 1;
                if i < args.len() {
                    source_type = get_string(&args[i])?;
                }
            }
            "URI" => {
                i += 1;
                if i < args.len() {
                    uri = get_string(&args[i])?;
                }
            }
            "NAME" => {
                i += 1;
                if i < args.len() {
                    name = Some(get_string(&args[i])?);
                }
            }
            _ => {}
        }
        i += 1;
    }

    if source_type.is_empty() || uri.is_empty() {
        return Err(FerriteError::WrongArity("FEDERATION.ADD".to_string()));
    }

    Ok(Command::FederationAdd {
        id,
        source_type,
        uri,
        name,
    })
}

/// FEDERATION.REMOVE id
pub(crate) fn parse_federation_remove(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("FEDERATION.REMOVE".to_string()));
    }
    let id = get_string(&args[0])?;
    Ok(Command::FederationRemove { id })
}

/// FEDERATION.STATUS [id]
pub(crate) fn parse_federation_status(args: &[Frame]) -> Result<Command> {
    let id = if args.is_empty() {
        None
    } else {
        Some(get_string(&args[0])?)
    };
    Ok(Command::FederationStatus { id })
}

/// FEDERATION.NAMESPACE namespace source_id
pub(crate) fn parse_federation_namespace(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity(
            "FEDERATION.NAMESPACE".to_string(),
        ));
    }
    let namespace = get_string(&args[0])?;
    let source_id = get_string(&args[1])?;
    Ok(Command::FederationNamespace {
        namespace,
        source_id,
    })
}

/// FEDERATION.QUERY query_string
pub(crate) fn parse_federation_query(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("FEDERATION.QUERY".to_string()));
    }
    let query = get_string(&args[0])?;
    Ok(Command::FederationQuery { query })
}

/// FEDERATION.CONTRACT name source_id schema_json
pub(crate) fn parse_federation_contract(args: &[Frame]) -> Result<Command> {
    if args.len() < 3 {
        return Err(FerriteError::WrongArity(
            "FEDERATION.CONTRACT".to_string(),
        ));
    }
    let name = get_string(&args[0])?;
    let source_id = get_string(&args[1])?;
    let schema_json = get_string(&args[2])?;
    Ok(Command::FederationContract {
        name,
        source_id,
        schema_json,
    })
}

// ── Studio developer-experience command parsers ─────────────────────────────

/// Parse `STUDIO.SCHEMA [DB n]`
pub(crate) fn parse_studio_schema(args: &[Frame]) -> Result<Command> {
    let mut db = None;
    let mut i = 0;
    while i < args.len() {
        let arg = get_string(&args[i])?.to_uppercase();
        if arg == "DB" {
            i += 1;
            if i < args.len() {
                let n: u8 = get_string(&args[i])?
                    .parse()
                    .map_err(|_| FerriteError::Protocol("invalid DB index".to_string()))?;
                db = Some(n);
            }
        }
        i += 1;
    }
    Ok(Command::StudioSchema { db })
}

/// Parse `STUDIO.TEMPLATES [name]`
pub(crate) fn parse_studio_templates(args: &[Frame]) -> Result<Command> {
    let name = args.first().map(get_string).transpose()?;
    Ok(Command::StudioTemplates { name })
}

/// Parse `STUDIO.SETUP template_name`
pub(crate) fn parse_studio_setup(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("STUDIO.SETUP".to_string()));
    }
    let template = get_string(&args[0])?;
    Ok(Command::StudioSetup { template })
}

/// Parse `STUDIO.COMPAT [redis_info]`
pub(crate) fn parse_studio_compat(args: &[Frame]) -> Result<Command> {
    let redis_info = args.first().map(get_string).transpose()?;
    Ok(Command::StudioCompat { redis_info })
}

/// Parse `STUDIO.HELP command`
pub(crate) fn parse_studio_help(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("STUDIO.HELP".to_string()));
    }
    let command = get_string(&args[0])?;
    Ok(Command::StudioHelp { command })
}

/// Parse `STUDIO.SUGGEST [context]`
pub(crate) fn parse_studio_suggest(args: &[Frame]) -> Result<Command> {
    let context = args.first().map(get_string).transpose()?;
    Ok(Command::StudioSuggest { context })
}
