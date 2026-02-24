use bytes::Bytes;

use super::{get_bytes, get_int, get_string};
use crate::commands::parser::{Command, SortOptions};
use crate::error::{FerriteError, Result};
use crate::protocol::Frame;

pub(crate) fn parse_expire(args: &[Frame]) -> Result<Command> {
    if args.len() != 2 {
        return Err(FerriteError::WrongArity("EXPIRE".to_string()));
    }
    Ok(Command::Expire {
        key: get_bytes(&args[0])?,
        seconds: get_int(&args[1])? as u64,
    })
}

pub(crate) fn parse_pexpire(args: &[Frame]) -> Result<Command> {
    if args.len() != 2 {
        return Err(FerriteError::WrongArity("PEXPIRE".to_string()));
    }
    Ok(Command::PExpire {
        key: get_bytes(&args[0])?,
        milliseconds: get_int(&args[1])? as u64,
    })
}

pub(crate) fn parse_ttl(args: &[Frame]) -> Result<Command> {
    if args.len() != 1 {
        return Err(FerriteError::WrongArity("TTL".to_string()));
    }
    Ok(Command::Ttl {
        key: get_bytes(&args[0])?,
    })
}

pub(crate) fn parse_pttl(args: &[Frame]) -> Result<Command> {
    if args.len() != 1 {
        return Err(FerriteError::WrongArity("PTTL".to_string()));
    }
    Ok(Command::PTtl {
        key: get_bytes(&args[0])?,
    })
}

pub(crate) fn parse_persist(args: &[Frame]) -> Result<Command> {
    if args.len() != 1 {
        return Err(FerriteError::WrongArity("PERSIST".to_string()));
    }
    Ok(Command::Persist {
        key: get_bytes(&args[0])?,
    })
}

pub(crate) fn parse_expireat(args: &[Frame]) -> Result<Command> {
    if args.len() != 2 {
        return Err(FerriteError::WrongArity("EXPIREAT".to_string()));
    }
    Ok(Command::ExpireAt {
        key: get_bytes(&args[0])?,
        timestamp: get_int(&args[1])? as u64,
    })
}

pub(crate) fn parse_pexpireat(args: &[Frame]) -> Result<Command> {
    if args.len() != 2 {
        return Err(FerriteError::WrongArity("PEXPIREAT".to_string()));
    }
    Ok(Command::PExpireAt {
        key: get_bytes(&args[0])?,
        timestamp_ms: get_int(&args[1])? as u64,
    })
}

pub(crate) fn parse_expiretime(args: &[Frame]) -> Result<Command> {
    if args.len() != 1 {
        return Err(FerriteError::WrongArity("EXPIRETIME".to_string()));
    }
    Ok(Command::ExpireTime {
        key: get_bytes(&args[0])?,
    })
}

pub(crate) fn parse_pexpiretime(args: &[Frame]) -> Result<Command> {
    if args.len() != 1 {
        return Err(FerriteError::WrongArity("PEXPIRETIME".to_string()));
    }
    Ok(Command::PExpireTime {
        key: get_bytes(&args[0])?,
    })
}

pub(crate) fn parse_keys(args: &[Frame]) -> Result<Command> {
    if args.len() != 1 {
        return Err(FerriteError::WrongArity("KEYS".to_string()));
    }
    Ok(Command::Keys {
        pattern: get_string(&args[0])?,
    })
}

pub(crate) fn parse_type(args: &[Frame]) -> Result<Command> {
    if args.len() != 1 {
        return Err(FerriteError::WrongArity("TYPE".to_string()));
    }
    Ok(Command::Type {
        key: get_bytes(&args[0])?,
    })
}

pub(crate) fn parse_rename(args: &[Frame]) -> Result<Command> {
    if args.len() != 2 {
        return Err(FerriteError::WrongArity("RENAME".to_string()));
    }
    Ok(Command::Rename {
        key: get_bytes(&args[0])?,
        new_key: get_bytes(&args[1])?,
    })
}

pub(crate) fn parse_renamenx(args: &[Frame]) -> Result<Command> {
    if args.len() != 2 {
        return Err(FerriteError::WrongArity("RENAMENX".to_string()));
    }
    Ok(Command::RenameNx {
        key: get_bytes(&args[0])?,
        new_key: get_bytes(&args[1])?,
    })
}

pub(crate) fn parse_copy(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("COPY".to_string()));
    }

    let source = get_bytes(&args[0])?;
    let destination = get_bytes(&args[1])?;
    let mut dest_db = None;
    let mut replace = false;

    let mut i = 2;
    while i < args.len() {
        let opt = get_string(&args[i])?.to_uppercase();
        match opt.as_str() {
            "DB" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                dest_db = Some(get_int(&args[i])? as u8);
            }
            "REPLACE" => {
                replace = true;
            }
            _ => return Err(FerriteError::Syntax),
        }
        i += 1;
    }

    Ok(Command::Copy {
        source,
        destination,
        dest_db,
        replace,
    })
}

pub(crate) fn parse_unlink(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("UNLINK".to_string()));
    }
    let keys: Result<Vec<Bytes>> = args.iter().map(get_bytes).collect();
    Ok(Command::Unlink { keys: keys? })
}

pub(crate) fn parse_touch(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("TOUCH".to_string()));
    }
    let keys: Result<Vec<Bytes>> = args.iter().map(get_bytes).collect();
    Ok(Command::Touch { keys: keys? })
}

pub(crate) fn parse_object(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("OBJECT".to_string()));
    }
    let subcommand = get_string(&args[0])?.to_uppercase();
    let key = if args.len() > 1 {
        Some(get_bytes(&args[1])?)
    } else {
        None
    };
    Ok(Command::Object { subcommand, key })
}

pub(crate) fn parse_dump(args: &[Frame]) -> Result<Command> {
    if args.len() != 1 {
        return Err(FerriteError::WrongArity("DUMP".to_string()));
    }
    Ok(Command::Dump {
        key: get_bytes(&args[0])?,
    })
}

pub(crate) fn parse_restore(args: &[Frame]) -> Result<Command> {
    if args.len() < 3 {
        return Err(FerriteError::WrongArity("RESTORE".to_string()));
    }
    let key = get_bytes(&args[0])?;
    let ttl = get_int(&args[1])? as u64;
    let data = get_bytes(&args[2])?;

    let mut replace = false;
    for item in args.iter().skip(3) {
        let opt = get_string(item)?.to_uppercase();
        if opt == "REPLACE" {
            replace = true;
        }
    }

    Ok(Command::Restore {
        key,
        ttl,
        data,
        replace,
    })
}

pub(crate) fn parse_sort(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("SORT".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let mut options = SortOptions::default();

    let mut i = 1;
    while i < args.len() {
        let opt = get_string(&args[i])?.to_uppercase();
        match opt.as_str() {
            "BY" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                options.by = Some(get_string(&args[i])?);
            }
            "LIMIT" => {
                i += 1;
                if i + 1 >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                let offset = get_int(&args[i])?;
                i += 1;
                let count = get_int(&args[i])?;
                options.limit = Some((offset, count));
            }
            "GET" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                options.get.push(get_string(&args[i])?);
            }
            "ASC" => {
                options.desc = false;
            }
            "DESC" => {
                options.desc = true;
            }
            "ALPHA" => {
                options.alpha = true;
            }
            "STORE" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                options.store = Some(get_bytes(&args[i])?);
            }
            _ => return Err(FerriteError::Syntax),
        }
        i += 1;
    }

    Ok(Command::Sort { key, options })
}
