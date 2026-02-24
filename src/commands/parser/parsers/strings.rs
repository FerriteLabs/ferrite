use bytes::Bytes;

use super::{get_bytes, get_float, get_int, get_string};
use crate::commands::parser::{Command, SetOptions};
use crate::error::{FerriteError, Result};
use crate::protocol::Frame;

pub(crate) fn parse_get(args: &[Frame]) -> Result<Command> {
    if args.len() != 1 {
        return Err(FerriteError::WrongArity("GET".to_string()));
    }
    Ok(Command::Get {
        key: get_bytes(&args[0])?,
    })
}

pub(crate) fn parse_set(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("SET".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let value = get_bytes(&args[1])?;
    let mut options = SetOptions::default();

    let mut i = 2;
    while i < args.len() {
        let opt = get_string(&args[i])?.to_ascii_uppercase();
        match opt.as_str() {
            "EX" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                let seconds = get_int(&args[i])? as u64;
                options.expire_ms = Some(seconds * 1000);
            }
            "PX" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                let ms = get_int(&args[i])? as u64;
                options.expire_ms = Some(ms);
            }
            "NX" => options.nx = true,
            "XX" => options.xx = true,
            "GET" => options.get = true,
            _ => return Err(FerriteError::Syntax),
        }
        i += 1;
    }

    Ok(Command::Set {
        key,
        value,
        options,
    })
}

pub(crate) fn parse_del(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("DEL".to_string()));
    }
    let keys: Result<Vec<Bytes>> = args.iter().map(get_bytes).collect();
    Ok(Command::Del { keys: keys? })
}

pub(crate) fn parse_exists(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("EXISTS".to_string()));
    }
    let keys: Result<Vec<Bytes>> = args.iter().map(get_bytes).collect();
    Ok(Command::Exists { keys: keys? })
}

pub(crate) fn parse_incr(args: &[Frame]) -> Result<Command> {
    if args.len() != 1 {
        return Err(FerriteError::WrongArity("INCR".to_string()));
    }
    Ok(Command::Incr {
        key: get_bytes(&args[0])?,
    })
}

pub(crate) fn parse_decr(args: &[Frame]) -> Result<Command> {
    if args.len() != 1 {
        return Err(FerriteError::WrongArity("DECR".to_string()));
    }
    Ok(Command::Decr {
        key: get_bytes(&args[0])?,
    })
}

pub(crate) fn parse_incrby(args: &[Frame]) -> Result<Command> {
    if args.len() != 2 {
        return Err(FerriteError::WrongArity("INCRBY".to_string()));
    }
    Ok(Command::IncrBy {
        key: get_bytes(&args[0])?,
        delta: get_int(&args[1])?,
    })
}

pub(crate) fn parse_decrby(args: &[Frame]) -> Result<Command> {
    if args.len() != 2 {
        return Err(FerriteError::WrongArity("DECRBY".to_string()));
    }
    Ok(Command::DecrBy {
        key: get_bytes(&args[0])?,
        delta: get_int(&args[1])?,
    })
}

pub(crate) fn parse_mget(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("MGET".to_string()));
    }
    let keys: Result<Vec<Bytes>> = args.iter().map(get_bytes).collect();
    Ok(Command::MGet { keys: keys? })
}

pub(crate) fn parse_mset(args: &[Frame]) -> Result<Command> {
    if args.is_empty() || args.len() % 2 != 0 {
        return Err(FerriteError::WrongArity("MSET".to_string()));
    }
    let mut pairs = Vec::with_capacity(args.len() / 2);
    for chunk in args.chunks(2) {
        pairs.push((get_bytes(&chunk[0])?, get_bytes(&chunk[1])?));
    }
    Ok(Command::MSet { pairs })
}

pub(crate) fn parse_append(args: &[Frame]) -> Result<Command> {
    if args.len() != 2 {
        return Err(FerriteError::WrongArity("APPEND".to_string()));
    }
    Ok(Command::Append {
        key: get_bytes(&args[0])?,
        value: get_bytes(&args[1])?,
    })
}

pub(crate) fn parse_strlen(args: &[Frame]) -> Result<Command> {
    if args.len() != 1 {
        return Err(FerriteError::WrongArity("STRLEN".to_string()));
    }
    Ok(Command::StrLen {
        key: get_bytes(&args[0])?,
    })
}

pub(crate) fn parse_getrange(args: &[Frame]) -> Result<Command> {
    if args.len() != 3 {
        return Err(FerriteError::WrongArity("GETRANGE".to_string()));
    }
    Ok(Command::GetRange {
        key: get_bytes(&args[0])?,
        start: get_int(&args[1])?,
        end: get_int(&args[2])?,
    })
}

pub(crate) fn parse_setrange(args: &[Frame]) -> Result<Command> {
    if args.len() != 3 {
        return Err(FerriteError::WrongArity("SETRANGE".to_string()));
    }
    let offset = get_int(&args[1])?;
    if offset < 0 {
        return Err(FerriteError::Protocol("offset is out of range".to_string()));
    }
    Ok(Command::SetRange {
        key: get_bytes(&args[0])?,
        offset,
        value: get_bytes(&args[2])?,
    })
}

pub(crate) fn parse_setnx(args: &[Frame]) -> Result<Command> {
    if args.len() != 2 {
        return Err(FerriteError::WrongArity("SETNX".to_string()));
    }
    Ok(Command::SetNx {
        key: get_bytes(&args[0])?,
        value: get_bytes(&args[1])?,
    })
}

pub(crate) fn parse_setex(args: &[Frame]) -> Result<Command> {
    if args.len() != 3 {
        return Err(FerriteError::WrongArity("SETEX".to_string()));
    }
    let seconds = get_int(&args[1])?;
    if seconds <= 0 {
        return Err(FerriteError::Protocol(
            "invalid expire time in SETEX".to_string(),
        ));
    }
    Ok(Command::SetEx {
        key: get_bytes(&args[0])?,
        seconds: seconds as u64,
        value: get_bytes(&args[2])?,
    })
}

pub(crate) fn parse_psetex(args: &[Frame]) -> Result<Command> {
    if args.len() != 3 {
        return Err(FerriteError::WrongArity("PSETEX".to_string()));
    }
    let milliseconds = get_int(&args[1])?;
    if milliseconds <= 0 {
        return Err(FerriteError::Protocol(
            "invalid expire time in PSETEX".to_string(),
        ));
    }
    Ok(Command::PSetEx {
        key: get_bytes(&args[0])?,
        milliseconds: milliseconds as u64,
        value: get_bytes(&args[2])?,
    })
}

pub(crate) fn parse_getset(args: &[Frame]) -> Result<Command> {
    if args.len() != 2 {
        return Err(FerriteError::WrongArity("GETSET".to_string()));
    }
    Ok(Command::GetSet {
        key: get_bytes(&args[0])?,
        value: get_bytes(&args[1])?,
    })
}

pub(crate) fn parse_getdel(args: &[Frame]) -> Result<Command> {
    if args.len() != 1 {
        return Err(FerriteError::WrongArity("GETDEL".to_string()));
    }
    Ok(Command::GetDel {
        key: get_bytes(&args[0])?,
    })
}

pub(crate) fn parse_incrbyfloat(args: &[Frame]) -> Result<Command> {
    if args.len() != 2 {
        return Err(FerriteError::WrongArity("INCRBYFLOAT".to_string()));
    }
    Ok(Command::IncrByFloat {
        key: get_bytes(&args[0])?,
        delta: get_float(&args[1])?,
    })
}

pub(crate) fn parse_getex(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("GETEX".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let mut ex = None;
    let mut px = None;
    let mut exat = None;
    let mut pxat = None;
    let mut persist = false;

    let mut i = 1;
    while i < args.len() {
        let option = get_string(&args[i])?.to_uppercase();
        match option.as_str() {
            "EX" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                ex = Some(get_int(&args[i])? as u64);
            }
            "PX" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                px = Some(get_int(&args[i])? as u64);
            }
            "EXAT" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                exat = Some(get_int(&args[i])? as u64);
            }
            "PXAT" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                pxat = Some(get_int(&args[i])? as u64);
            }
            "PERSIST" => {
                persist = true;
            }
            _ => return Err(FerriteError::Syntax),
        }
        i += 1;
    }

    Ok(Command::GetEx {
        key,
        ex,
        px,
        exat,
        pxat,
        persist,
    })
}

pub(crate) fn parse_msetnx(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 || args.len() % 2 != 0 {
        return Err(FerriteError::WrongArity("MSETNX".to_string()));
    }

    let mut pairs = Vec::with_capacity(args.len() / 2);
    let mut i = 0;
    while i < args.len() {
        let key = get_bytes(&args[i])?;
        let value = get_bytes(&args[i + 1])?;
        pairs.push((key, value));
        i += 2;
    }

    Ok(Command::MSetNx { pairs })
}

pub(crate) fn parse_lcs(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("LCS".to_string()));
    }

    let key1 = get_bytes(&args[0])?;
    let key2 = get_bytes(&args[1])?;
    let mut len_only = false;
    let mut idx = false;
    let mut min_match_len = 0usize;
    let mut with_match_len = false;

    let mut i = 2;
    while i < args.len() {
        let option = get_string(&args[i])?.to_uppercase();
        match option.as_str() {
            "LEN" => {
                len_only = true;
            }
            "IDX" => {
                idx = true;
            }
            "MINMATCHLEN" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                min_match_len = get_int(&args[i])? as usize;
            }
            "WITHMATCHLEN" => {
                with_match_len = true;
            }
            _ => return Err(FerriteError::Syntax),
        }
        i += 1;
    }

    Ok(Command::Lcs {
        key1,
        key2,
        len_only,
        idx,
        min_match_len,
        with_match_len,
    })
}
