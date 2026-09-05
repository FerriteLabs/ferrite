use super::{get_bytes, get_int, get_string};
use crate::commands::parser::Command;
use crate::error::{FerriteError, Result};
use crate::protocol::Frame;

mod temporal_parsers;
pub(crate) use temporal_parsers::*;

pub(crate) fn parse_cluster(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("CLUSTER".to_string()));
    }

    let subcommand = get_string(&args[0])?.to_uppercase();
    let cluster_args: Vec<String> = args[1..]
        .iter()
        .filter_map(|f| get_string(f).ok())
        .collect();

    Ok(Command::Cluster {
        subcommand,
        args: cluster_args,
    })
}

pub(crate) fn parse_tiering(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("TIERING".to_string()));
    }

    let subcommand = get_string(&args[0])?.to_uppercase();

    // Subcommands that require a key as first argument
    let (key, remaining_args) = match subcommand.as_str() {
        "COST" | "TIER" | "PIN" | "UNPIN" | "MIGRATE" | "PRIORITY" | "STATS" => {
            // These subcommands may have a key after the subcommand name
            // TIERING COST <key> or TIERING COST PATTERN <pattern>
            // TIERING TIER <key>
            // etc.
            if args.len() >= 2 {
                let second_arg = get_string(&args[1])?.to_uppercase();
                // If second arg is a sub-subcommand like PATTERN, TOTAL, SET, etc.
                if matches!(
                    second_arg.as_str(),
                    "PATTERN" | "TOTAL" | "SIMULATE" | "SET" | "IMPORT"
                ) {
                    (None, &args[1..])
                } else {
                    // Second arg is likely a key
                    (Some(get_bytes(&args[1])?), &args[2..])
                }
            } else {
                (None, &args[1..])
            }
        }
        _ => (None, &args[1..]),
    };

    let tiering_args: Vec<String> = remaining_args
        .iter()
        .filter_map(|f| get_string(f).ok())
        .collect();

    Ok(Command::Tiering {
        subcommand,
        args: tiering_args,
        key,
    })
}

pub(crate) fn parse_cdc(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("CDC".to_string()));
    }

    let subcommand = get_string(&args[0])?.to_uppercase();

    let cdc_args: Vec<String> = args[1..]
        .iter()
        .filter_map(|f| get_string(f).ok())
        .collect();

    Ok(Command::Cdc {
        subcommand,
        args: cdc_args,
    })
}

pub(crate) fn parse_tenant(command_name: &str, args: &[Frame]) -> Result<Command> {
    // Handle both "TENANT subcommand" and "TENANT.SUBCOMMAND" forms
    if command_name.contains('.') {
        // TENANT.CREATE, TENANT.LIST, etc.
        let subcommand = command_name
            .split_once('.')
            .map(|x| x.1)
            .unwrap_or("HELP")
            .to_uppercase();
        let tenant_args: Vec<String> = args.iter().filter_map(|f| get_string(f).ok()).collect();
        Ok(Command::Tenant {
            subcommand,
            args: tenant_args,
        })
    } else {
        // TENANT subcommand [args...]
        if args.is_empty() {
            return Err(FerriteError::WrongArity("TENANT".to_string()));
        }
        let subcommand = get_string(&args[0])?.to_uppercase();
        let tenant_args: Vec<String> = args[1..]
            .iter()
            .filter_map(|f| get_string(f).ok())
            .collect();
        Ok(Command::Tenant {
            subcommand,
            args: tenant_args,
        })
    }
}

pub(crate) fn parse_edge(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("EDGE".to_string()));
    }

    let subcommand = get_string(&args[0])?.to_uppercase();

    let edge_args: Vec<bytes::Bytes> = args[1..].iter().filter_map(|f| get_bytes(f).ok()).collect();

    Ok(Command::Edge {
        subcommand,
        args: edge_args,
    })
}

pub(crate) fn parse_ebpf(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("EBPF".to_string()));
    }

    let subcommand = get_string(&args[0])?.to_uppercase();

    let ebpf_args: Vec<String> = args[1..]
        .iter()
        .filter_map(|f| get_string(f).ok())
        .collect();

    Ok(Command::Ebpf {
        subcommand,
        args: ebpf_args,
    })
}

pub(crate) fn parse_cloud(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("CLOUD".to_string()));
    }

    let subcommand = get_string(&args[0])?.to_uppercase();

    let cloud_args: Vec<String> = args[1..]
        .iter()
        .filter_map(|f| get_string(f).ok())
        .collect();

    Ok(Command::Cloud {
        subcommand,
        args: cloud_args,
    })
}

pub(crate) fn parse_observe(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("OBSERVE".to_string()));
    }

    let subcommand = get_string(&args[0])?.to_uppercase();

    let observe_args: Vec<String> = args[1..]
        .iter()
        .filter_map(|f| get_string(f).ok())
        .collect();

    Ok(Command::Observe {
        subcommand,
        args: observe_args,
    })
}
