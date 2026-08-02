use bytes::Bytes;

use super::{get_bytes, get_float, get_int, get_string};
use crate::commands::parser::Command;
use crate::error::{FerriteError, Result};
use crate::protocol::Frame;

mod crdt_wasm_parsers;
mod federation_parsers;
mod search_parsers;
mod streaming_parsers;
mod trigger_parsers;

pub(crate) use crdt_wasm_parsers::*;
pub(crate) use federation_parsers::*;
pub(crate) use search_parsers::*;
pub(crate) use streaming_parsers::*;
pub(crate) use trigger_parsers::*;

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

pub(crate) fn parse_autotune(args: &[Frame]) -> Result<Command> {
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
    Ok(Command::AutoTune {
        subcommand,
        args: rest_args,
    })
}

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

pub(crate) fn parse_view_subscribe(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("VIEW.SUBSCRIBE".to_string()));
    }
    let name = get_string(&args[0])?;
    Ok(Command::ViewSubscribe { name })
}

pub(crate) fn parse_view_unsubscribe(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("VIEW.UNSUBSCRIBE".to_string()));
    }
    let name = get_string(&args[0])?;
    Ok(Command::ViewUnsubscribe { name })
}

pub(crate) fn parse_view_maintenance(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("VIEW.MAINTENANCE".to_string()));
    }
    let name = get_string(&args[0])?;
    Ok(Command::ViewMaintenance { name })
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

/// Parse `GATEWAY subcommand [args...]`
pub(crate) fn parse_gateway(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("GATEWAY".to_string()));
    }
    let subcommand = get_string(&args[0])?.to_uppercase();
    let gateway_args: Vec<String> = args[1..]
        .iter()
        .filter_map(|f| get_string(f).ok())
        .collect();
    Ok(Command::Gateway {
        subcommand,
        args: gateway_args,
    })
}

/// Parse `BUDGET subcommand [args...]`
pub(crate) fn parse_budget(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("BUDGET".to_string()));
    }
    let subcommand = get_string(&args[0])?.to_uppercase();
    let budget_args: Vec<String> = args[1..]
        .iter()
        .filter_map(|f| get_string(f).ok())
        .collect();
    Ok(Command::Budget {
        subcommand,
        args: budget_args,
    })
}
