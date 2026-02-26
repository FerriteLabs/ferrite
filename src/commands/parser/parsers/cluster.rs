use super::{get_bytes, get_int, get_string};
use crate::commands::parser::Command;
use crate::error::{FerriteError, Result};
use crate::protocol::Frame;

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

pub(crate) fn parse_edge(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("EDGE".to_string()));
    }

    let subcommand = get_string(&args[0])?.to_uppercase();

    let edge_args: Vec<bytes::Bytes> = args[1..]
        .iter()
        .filter_map(|f| get_bytes(f).ok())
        .collect();

    Ok(Command::Edge {
        subcommand,
        args: edge_args,
    })
}

pub(crate) fn parse_history(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("HISTORY".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let mut from = None;
    let mut to = None;
    let mut limit = None;
    let mut ascending = false;
    let mut with_values = false;

    let mut i = 1;
    while i < args.len() {
        let arg = get_string(&args[i])?.to_uppercase();
        match arg.as_str() {
            "FROM" => {
                i += 1;
                if i < args.len() {
                    from = Some(get_string(&args[i])?);
                }
            }
            "TO" => {
                i += 1;
                if i < args.len() {
                    to = Some(get_string(&args[i])?);
                }
            }
            "LIMIT" => {
                i += 1;
                if i < args.len() {
                    limit = Some(get_int(&args[i])? as usize);
                }
            }
            "ORDER" => {
                i += 1;
                if i < args.len() {
                    let order = get_string(&args[i])?.to_uppercase();
                    ascending = order == "ASC";
                }
            }
            "WITHVALUES" => {
                with_values = true;
            }
            "ASC" => {
                ascending = true;
            }
            "DESC" => {
                ascending = false;
            }
            _ => {}
        }
        i += 1;
    }

    Ok(Command::History {
        key,
        from,
        to,
        limit,
        ascending,
        with_values,
    })
}

pub(crate) fn parse_history_count(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("HISTORY.COUNT".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let mut from = None;
    let mut to = None;

    let mut i = 1;
    while i < args.len() {
        let arg = get_string(&args[i])?.to_uppercase();
        match arg.as_str() {
            "FROM" => {
                i += 1;
                if i < args.len() {
                    from = Some(get_string(&args[i])?);
                }
            }
            "TO" => {
                i += 1;
                if i < args.len() {
                    to = Some(get_string(&args[i])?);
                }
            }
            _ => {}
        }
        i += 1;
    }

    Ok(Command::HistoryCount { key, from, to })
}

pub(crate) fn parse_history_first(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("HISTORY.FIRST".to_string()));
    }

    let key = get_bytes(&args[0])?;
    Ok(Command::HistoryFirst { key })
}

pub(crate) fn parse_history_last(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("HISTORY.LAST".to_string()));
    }

    let key = get_bytes(&args[0])?;
    Ok(Command::HistoryLast { key })
}

pub(crate) fn parse_diff(args: &[Frame]) -> Result<Command> {
    if args.len() < 3 {
        return Err(FerriteError::WrongArity("DIFF".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let timestamp1 = get_string(&args[1])?;
    let timestamp2 = get_string(&args[2])?;

    Ok(Command::Diff {
        key,
        timestamp1,
        timestamp2,
    })
}

pub(crate) fn parse_restore_from(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("RESTORE.FROM".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let timestamp = get_string(&args[1])?;
    let mut target = None;

    // Check for NEWKEY option
    let mut i = 2;
    while i < args.len() {
        let arg = get_string(&args[i])?.to_uppercase();
        if arg == "NEWKEY" {
            i += 1;
            if i < args.len() {
                target = Some(get_bytes(&args[i])?);
            }
        }
        i += 1;
    }

    Ok(Command::RestoreFrom {
        key,
        timestamp,
        target,
    })
}

pub(crate) fn parse_temporal(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("TEMPORAL".to_string()));
    }

    let subcommand = get_string(&args[0])?.to_uppercase();

    let temporal_args: Vec<String> = args[1..]
        .iter()
        .filter_map(|f| get_string(f).ok())
        .collect();

    Ok(Command::Temporal {
        subcommand,
        args: temporal_args,
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
