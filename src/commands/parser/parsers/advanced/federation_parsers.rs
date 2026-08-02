//! Federation and multi-region command parsers.

use super::{get_int, get_string};
use crate::commands::parser::Command;
use crate::error::{FerriteError, Result};
use crate::protocol::Frame;

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
        return Err(FerriteError::WrongArity("FEDERATION.NAMESPACE".to_string()));
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
        return Err(FerriteError::WrongArity("FEDERATION.CONTRACT".to_string()));
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
