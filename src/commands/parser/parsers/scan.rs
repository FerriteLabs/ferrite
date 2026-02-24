use super::{get_bytes, get_int, get_string};
use crate::commands::parser::Command;
use crate::error::{FerriteError, Result};
use crate::protocol::Frame;

pub(crate) fn parse_scan(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("scan".to_string()));
    }

    let cursor = get_int(&args[0])? as u64;
    let mut pattern = None;
    let mut count = None;
    let mut type_filter = None;

    let mut i = 1;
    while i < args.len() {
        let opt = get_string(&args[i])?.to_ascii_uppercase();
        match opt.as_str() {
            "MATCH" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                pattern = Some(get_string(&args[i])?);
            }
            "COUNT" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                count = Some(get_int(&args[i])? as usize);
            }
            "TYPE" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                type_filter = Some(get_string(&args[i])?);
            }
            _ => return Err(FerriteError::Syntax),
        }
        i += 1;
    }

    Ok(Command::Scan {
        cursor,
        pattern,
        count,
        type_filter,
    })
}

pub(crate) fn parse_hscan(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("hscan".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let cursor = get_int(&args[1])? as u64;
    let mut pattern = None;
    let mut count = None;
    let mut novalues = false;

    let mut i = 2;
    while i < args.len() {
        let opt = get_string(&args[i])?.to_ascii_uppercase();
        match opt.as_str() {
            "MATCH" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                pattern = Some(get_string(&args[i])?);
            }
            "COUNT" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                count = Some(get_int(&args[i])? as usize);
            }
            "NOVALUES" => {
                novalues = true;
            }
            _ => return Err(FerriteError::Syntax),
        }
        i += 1;
    }

    Ok(Command::HScan {
        key,
        cursor,
        pattern,
        count,
        novalues,
    })
}

pub(crate) fn parse_sscan(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("sscan".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let cursor = get_int(&args[1])? as u64;
    let mut pattern = None;
    let mut count = None;

    let mut i = 2;
    while i < args.len() {
        let opt = get_string(&args[i])?.to_ascii_uppercase();
        match opt.as_str() {
            "MATCH" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                pattern = Some(get_string(&args[i])?);
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

    Ok(Command::SScan {
        key,
        cursor,
        pattern,
        count,
    })
}

pub(crate) fn parse_zscan(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("zscan".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let cursor = get_int(&args[1])? as u64;
    let mut pattern = None;
    let mut count = None;

    let mut i = 2;
    while i < args.len() {
        let opt = get_string(&args[i])?.to_ascii_uppercase();
        match opt.as_str() {
            "MATCH" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                pattern = Some(get_string(&args[i])?);
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

    Ok(Command::ZScan {
        key,
        cursor,
        pattern,
        count,
    })
}
