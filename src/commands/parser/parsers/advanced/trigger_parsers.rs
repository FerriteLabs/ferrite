//! Trigger command parsers.

use super::{get_bytes, get_int, get_string};
use crate::commands::parser::Command;
use crate::error::{FerriteError, Result};
use crate::protocol::Frame;

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
