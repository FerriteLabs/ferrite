use bytes::Bytes;

use super::{get_bytes, get_float, get_int, get_string};
use crate::commands::parser::{Command, ListDirection};
use crate::error::{FerriteError, Result};
use crate::protocol::Frame;

pub(crate) fn parse_lpush(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("LPUSH".to_string()));
    }
    let key = get_bytes(&args[0])?;
    let values: Result<Vec<Bytes>> = args[1..].iter().map(get_bytes).collect();
    Ok(Command::LPush {
        key,
        values: values?,
    })
}

pub(crate) fn parse_rpush(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("RPUSH".to_string()));
    }
    let key = get_bytes(&args[0])?;
    let values: Result<Vec<Bytes>> = args[1..].iter().map(get_bytes).collect();
    Ok(Command::RPush {
        key,
        values: values?,
    })
}

pub(crate) fn parse_lpop(args: &[Frame]) -> Result<Command> {
    if args.is_empty() || args.len() > 2 {
        return Err(FerriteError::WrongArity("LPOP".to_string()));
    }
    let key = get_bytes(&args[0])?;
    let count = if args.len() == 2 {
        Some(get_int(&args[1])? as usize)
    } else {
        None
    };
    Ok(Command::LPop { key, count })
}

pub(crate) fn parse_rpop(args: &[Frame]) -> Result<Command> {
    if args.is_empty() || args.len() > 2 {
        return Err(FerriteError::WrongArity("RPOP".to_string()));
    }
    let key = get_bytes(&args[0])?;
    let count = if args.len() == 2 {
        Some(get_int(&args[1])? as usize)
    } else {
        None
    };
    Ok(Command::RPop { key, count })
}

pub(crate) fn parse_blpop(args: &[Frame]) -> Result<Command> {
    // BLPOP key [key ...] timeout
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("BLPOP".to_string()));
    }
    // Last argument is timeout
    let timeout = get_float(&args[args.len() - 1])?;
    // All other arguments are keys
    let keys: Result<Vec<Bytes>> = args[..args.len() - 1].iter().map(get_bytes).collect();
    Ok(Command::BLPop {
        keys: keys?,
        timeout,
    })
}

pub(crate) fn parse_brpop(args: &[Frame]) -> Result<Command> {
    // BRPOP key [key ...] timeout
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("BRPOP".to_string()));
    }
    // Last argument is timeout
    let timeout = get_float(&args[args.len() - 1])?;
    // All other arguments are keys
    let keys: Result<Vec<Bytes>> = args[..args.len() - 1].iter().map(get_bytes).collect();
    Ok(Command::BRPop {
        keys: keys?,
        timeout,
    })
}

pub(crate) fn parse_brpoplpush(args: &[Frame]) -> Result<Command> {
    // BRPOPLPUSH source destination timeout
    if args.len() != 3 {
        return Err(FerriteError::WrongArity("BRPOPLPUSH".to_string()));
    }
    Ok(Command::BRPopLPush {
        source: get_bytes(&args[0])?,
        destination: get_bytes(&args[1])?,
        timeout: get_float(&args[2])?,
    })
}

pub(crate) fn parse_direction(frame: &Frame) -> Result<ListDirection> {
    let s = get_string(frame)?.to_uppercase();
    match s.as_str() {
        "LEFT" => Ok(ListDirection::Left),
        "RIGHT" => Ok(ListDirection::Right),
        _ => Err(FerriteError::Syntax),
    }
}

pub(crate) fn parse_blmove(args: &[Frame]) -> Result<Command> {
    // BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout
    if args.len() != 5 {
        return Err(FerriteError::WrongArity("BLMOVE".to_string()));
    }
    Ok(Command::BLMove {
        source: get_bytes(&args[0])?,
        destination: get_bytes(&args[1])?,
        wherefrom: parse_direction(&args[2])?,
        whereto: parse_direction(&args[3])?,
        timeout: get_float(&args[4])?,
    })
}

pub(crate) fn parse_lmove(args: &[Frame]) -> Result<Command> {
    // LMOVE source destination LEFT|RIGHT LEFT|RIGHT
    if args.len() != 4 {
        return Err(FerriteError::WrongArity("LMOVE".to_string()));
    }
    Ok(Command::LMove {
        source: get_bytes(&args[0])?,
        destination: get_bytes(&args[1])?,
        wherefrom: parse_direction(&args[2])?,
        whereto: parse_direction(&args[3])?,
    })
}

pub(crate) fn parse_ltrim(args: &[Frame]) -> Result<Command> {
    if args.len() != 3 {
        return Err(FerriteError::WrongArity("LTRIM".to_string()));
    }
    Ok(Command::LTrim {
        key: get_bytes(&args[0])?,
        start: get_int(&args[1])?,
        stop: get_int(&args[2])?,
    })
}

pub(crate) fn parse_lpos(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("LPOS".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let element = get_bytes(&args[1])?;
    let mut rank = None;
    let mut count = None;
    let mut maxlen = None;

    let mut i = 2;
    while i < args.len() {
        let option = get_string(&args[i])?.to_uppercase();
        match option.as_str() {
            "RANK" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                rank = Some(get_int(&args[i])?);
            }
            "COUNT" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                count = Some(get_int(&args[i])? as usize);
            }
            "MAXLEN" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                maxlen = Some(get_int(&args[i])? as usize);
            }
            _ => return Err(FerriteError::Syntax),
        }
        i += 1;
    }

    Ok(Command::LPos {
        key,
        element,
        rank,
        count,
        maxlen,
    })
}

pub(crate) fn parse_rpoplpush(args: &[Frame]) -> Result<Command> {
    if args.len() != 2 {
        return Err(FerriteError::WrongArity("RPOPLPUSH".to_string()));
    }
    Ok(Command::RPopLPush {
        source: get_bytes(&args[0])?,
        destination: get_bytes(&args[1])?,
    })
}

pub(crate) fn parse_lmpop(args: &[Frame]) -> Result<Command> {
    // LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]
    if args.len() < 3 {
        return Err(FerriteError::WrongArity("LMPOP".to_string()));
    }

    let numkeys = get_int(&args[0])? as usize;
    if numkeys == 0 || args.len() < 2 + numkeys {
        return Err(FerriteError::Syntax);
    }

    let mut keys = Vec::with_capacity(numkeys);
    for item in args.iter().take(numkeys + 1).skip(1) {
        keys.push(get_bytes(item)?);
    }

    let direction_idx = 1 + numkeys;
    if direction_idx >= args.len() {
        return Err(FerriteError::Syntax);
    }
    let direction = parse_direction(&args[direction_idx])?;

    let mut count = 1;
    let mut i = direction_idx + 1;
    while i < args.len() {
        let option = get_string(&args[i])?.to_uppercase();
        if option == "COUNT" {
            i += 1;
            if i >= args.len() {
                return Err(FerriteError::Syntax);
            }
            count = get_int(&args[i])? as usize;
        } else {
            return Err(FerriteError::Syntax);
        }
        i += 1;
    }

    Ok(Command::LMPop {
        keys,
        direction,
        count,
    })
}

pub(crate) fn parse_blmpop(args: &[Frame]) -> Result<Command> {
    // BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]
    if args.len() < 4 {
        return Err(FerriteError::WrongArity("BLMPOP".to_string()));
    }

    let timeout = get_float(&args[0])?;
    if timeout < 0.0 {
        return Err(FerriteError::InvalidArgument(
            "timeout is negative".to_string(),
        ));
    }

    let numkeys = get_int(&args[1])? as usize;
    if numkeys == 0 || args.len() < 3 + numkeys {
        return Err(FerriteError::Syntax);
    }

    let mut keys = Vec::with_capacity(numkeys);
    for item in args.iter().skip(2).take(numkeys) {
        keys.push(get_bytes(item)?);
    }

    let direction_idx = 2 + numkeys;
    if direction_idx >= args.len() {
        return Err(FerriteError::Syntax);
    }
    let direction = parse_direction(&args[direction_idx])?;

    let mut count = 1;
    let mut i = direction_idx + 1;
    while i < args.len() {
        let option = get_string(&args[i])?.to_uppercase();
        if option == "COUNT" {
            i += 1;
            if i >= args.len() {
                return Err(FerriteError::Syntax);
            }
            count = get_int(&args[i])? as usize;
        } else {
            return Err(FerriteError::Syntax);
        }
        i += 1;
    }

    Ok(Command::BLMPop {
        timeout,
        keys,
        direction,
        count,
    })
}

pub(crate) fn parse_lrange(args: &[Frame]) -> Result<Command> {
    if args.len() != 3 {
        return Err(FerriteError::WrongArity("LRANGE".to_string()));
    }
    Ok(Command::LRange {
        key: get_bytes(&args[0])?,
        start: get_int(&args[1])?,
        stop: get_int(&args[2])?,
    })
}

pub(crate) fn parse_llen(args: &[Frame]) -> Result<Command> {
    if args.len() != 1 {
        return Err(FerriteError::WrongArity("LLEN".to_string()));
    }
    Ok(Command::LLen {
        key: get_bytes(&args[0])?,
    })
}

pub(crate) fn parse_lindex(args: &[Frame]) -> Result<Command> {
    if args.len() != 2 {
        return Err(FerriteError::WrongArity("LINDEX".to_string()));
    }
    Ok(Command::LIndex {
        key: get_bytes(&args[0])?,
        index: get_int(&args[1])?,
    })
}

pub(crate) fn parse_lset(args: &[Frame]) -> Result<Command> {
    if args.len() != 3 {
        return Err(FerriteError::WrongArity("LSET".to_string()));
    }
    Ok(Command::LSet {
        key: get_bytes(&args[0])?,
        index: get_int(&args[1])?,
        value: get_bytes(&args[2])?,
    })
}

pub(crate) fn parse_lrem(args: &[Frame]) -> Result<Command> {
    if args.len() != 3 {
        return Err(FerriteError::WrongArity("LREM".to_string()));
    }
    Ok(Command::LRem {
        key: get_bytes(&args[0])?,
        count: get_int(&args[1])?,
        value: get_bytes(&args[2])?,
    })
}

pub(crate) fn parse_linsert(args: &[Frame]) -> Result<Command> {
    if args.len() != 4 {
        return Err(FerriteError::WrongArity("LINSERT".to_string()));
    }
    let position = get_string(&args[1])?.to_ascii_uppercase();
    let before = match position.as_str() {
        "BEFORE" => true,
        "AFTER" => false,
        _ => return Err(FerriteError::Syntax),
    };
    Ok(Command::LInsert {
        key: get_bytes(&args[0])?,
        before,
        pivot: get_bytes(&args[2])?,
        value: get_bytes(&args[3])?,
    })
}

pub(crate) fn parse_lpushx(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("LPUSHX".to_string()));
    }
    let key = get_bytes(&args[0])?;
    let values: Result<Vec<Bytes>> = args[1..].iter().map(get_bytes).collect();
    Ok(Command::LPushX {
        key,
        values: values?,
    })
}

pub(crate) fn parse_rpushx(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("RPUSHX".to_string()));
    }
    let key = get_bytes(&args[0])?;
    let values: Result<Vec<Bytes>> = args[1..].iter().map(get_bytes).collect();
    Ok(Command::RPushX {
        key,
        values: values?,
    })
}
