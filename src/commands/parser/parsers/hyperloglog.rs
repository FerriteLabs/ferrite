use bytes::Bytes;

use crate::error::{FerriteError, Result};
use crate::protocol::Frame;
use super::{get_bytes};
use crate::commands::parser::{Command};

/// Parse PFADD key element [element ...]
pub(crate) fn parse_pfadd(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("pfadd".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let elements: Vec<Bytes> = args[1..]
        .iter()
        .map(get_bytes)
        .collect::<Result<Vec<_>>>()?;

    Ok(Command::PfAdd { key, elements })
}

/// Parse PFCOUNT key [key ...]
pub(crate) fn parse_pfcount(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("pfcount".to_string()));
    }

    let keys: Vec<Bytes> = args.iter().map(get_bytes).collect::<Result<Vec<_>>>()?;

    Ok(Command::PfCount { keys })
}

/// Parse PFMERGE destkey sourcekey [sourcekey ...]
pub(crate) fn parse_pfmerge(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("pfmerge".to_string()));
    }

    let destkey = get_bytes(&args[0])?;
    let sourcekeys: Vec<Bytes> = args[1..]
        .iter()
        .map(get_bytes)
        .collect::<Result<Vec<_>>>()?;

    Ok(Command::PfMerge {
        destkey,
        sourcekeys,
    })
}
