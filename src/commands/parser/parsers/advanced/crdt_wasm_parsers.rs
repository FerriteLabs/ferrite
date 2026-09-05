//! CRDT and WASM plugin command parsers.

use super::{get_bytes, get_int, get_string};
use crate::commands::parser::Command;
use crate::error::{FerriteError, Result};
use crate::protocol::Frame;

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
    let numkeys = usize::try_from(get_int(&args[1])?).map_err(|_| FerriteError::NotInteger)?;
    let key_count = numkeys.min(args.len() - 2);
    let args_start = key_count + 2;
    let keys = args[2..args_start]
        .iter()
        .map(get_bytes)
        .collect::<Result<Vec<_>>>()?;
    let wasm_args = args[args_start..]
        .iter()
        .map(get_bytes)
        .collect::<Result<Vec<_>>>()?;

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
    let numkeys = usize::try_from(get_int(&args[1])?).map_err(|_| FerriteError::NotInteger)?;
    let key_count = numkeys.min(args.len() - 2);
    let args_start = key_count + 2;
    let keys = args[2..args_start]
        .iter()
        .map(get_bytes)
        .collect::<Result<Vec<_>>>()?;
    let wasm_args = args[args_start..]
        .iter()
        .map(get_bytes)
        .collect::<Result<Vec<_>>>()?;

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
