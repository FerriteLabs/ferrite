use bytes::Bytes;

use crate::error::{FerriteError, Result};
use crate::protocol::Frame;
use super::{get_string, get_bytes, get_int};
use crate::commands::parser::{Command};

pub(crate) fn parse_hset(args: &[Frame]) -> Result<Command> {
    if args.len() < 3 || (args.len() - 1) % 2 != 0 {
        return Err(FerriteError::WrongArity("HSET".to_string()));
    }
    let key = get_bytes(&args[0])?;
    let mut pairs = Vec::with_capacity((args.len() - 1) / 2);
    for chunk in args[1..].chunks(2) {
        pairs.push((get_bytes(&chunk[0])?, get_bytes(&chunk[1])?));
    }
    Ok(Command::HSet { key, pairs })
}

pub(crate) fn parse_hget(args: &[Frame]) -> Result<Command> {
    if args.len() != 2 {
        return Err(FerriteError::WrongArity("HGET".to_string()));
    }
    Ok(Command::HGet {
        key: get_bytes(&args[0])?,
        field: get_bytes(&args[1])?,
    })
}

pub(crate) fn parse_hmset(args: &[Frame]) -> Result<Command> {
    if args.len() < 3 || (args.len() - 1) % 2 != 0 {
        return Err(FerriteError::WrongArity("HMSET".to_string()));
    }
    let key = get_bytes(&args[0])?;
    let mut pairs = Vec::with_capacity((args.len() - 1) / 2);
    for chunk in args[1..].chunks(2) {
        pairs.push((get_bytes(&chunk[0])?, get_bytes(&chunk[1])?));
    }
    Ok(Command::HMSet { key, pairs })
}

pub(crate) fn parse_hmget(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("HMGET".to_string()));
    }
    let key = get_bytes(&args[0])?;
    let fields: Result<Vec<Bytes>> = args[1..].iter().map(get_bytes).collect();
    Ok(Command::HMGet {
        key,
        fields: fields?,
    })
}

pub(crate) fn parse_hdel(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("HDEL".to_string()));
    }
    let key = get_bytes(&args[0])?;
    let fields: Result<Vec<Bytes>> = args[1..].iter().map(get_bytes).collect();
    Ok(Command::HDel {
        key,
        fields: fields?,
    })
}

pub(crate) fn parse_hexists(args: &[Frame]) -> Result<Command> {
    if args.len() != 2 {
        return Err(FerriteError::WrongArity("HEXISTS".to_string()));
    }
    Ok(Command::HExists {
        key: get_bytes(&args[0])?,
        field: get_bytes(&args[1])?,
    })
}

pub(crate) fn parse_hgetall(args: &[Frame]) -> Result<Command> {
    if args.len() != 1 {
        return Err(FerriteError::WrongArity("HGETALL".to_string()));
    }
    Ok(Command::HGetAll {
        key: get_bytes(&args[0])?,
    })
}

pub(crate) fn parse_hkeys(args: &[Frame]) -> Result<Command> {
    if args.len() != 1 {
        return Err(FerriteError::WrongArity("HKEYS".to_string()));
    }
    Ok(Command::HKeys {
        key: get_bytes(&args[0])?,
    })
}

pub(crate) fn parse_hvals(args: &[Frame]) -> Result<Command> {
    if args.len() != 1 {
        return Err(FerriteError::WrongArity("HVALS".to_string()));
    }
    Ok(Command::HVals {
        key: get_bytes(&args[0])?,
    })
}

pub(crate) fn parse_hlen(args: &[Frame]) -> Result<Command> {
    if args.len() != 1 {
        return Err(FerriteError::WrongArity("HLEN".to_string()));
    }
    Ok(Command::HLen {
        key: get_bytes(&args[0])?,
    })
}

pub(crate) fn parse_hsetnx(args: &[Frame]) -> Result<Command> {
    if args.len() != 3 {
        return Err(FerriteError::WrongArity("HSETNX".to_string()));
    }
    Ok(Command::HSetNx {
        key: get_bytes(&args[0])?,
        field: get_bytes(&args[1])?,
        value: get_bytes(&args[2])?,
    })
}

pub(crate) fn parse_hincrby(args: &[Frame]) -> Result<Command> {
    if args.len() != 3 {
        return Err(FerriteError::WrongArity("HINCRBY".to_string()));
    }
    Ok(Command::HIncrBy {
        key: get_bytes(&args[0])?,
        field: get_bytes(&args[1])?,
        delta: get_int(&args[2])?,
    })
}

pub(crate) fn parse_hincrbyfloat(args: &[Frame]) -> Result<Command> {
    if args.len() != 3 {
        return Err(FerriteError::WrongArity("HINCRBYFLOAT".to_string()));
    }
    let delta_str = get_string(&args[2])?;
    let delta = delta_str
        .parse::<f64>()
        .map_err(|_| FerriteError::Protocol("value is not a valid float".to_string()))?;
    Ok(Command::HIncrByFloat {
        key: get_bytes(&args[0])?,
        field: get_bytes(&args[1])?,
        delta,
    })
}

pub(crate) fn parse_hstrlen(args: &[Frame]) -> Result<Command> {
    if args.len() != 2 {
        return Err(FerriteError::WrongArity("HSTRLEN".to_string()));
    }
    Ok(Command::HStrLen {
        key: get_bytes(&args[0])?,
        field: get_bytes(&args[1])?,
    })
}

pub(crate) fn parse_hrandfield(args: &[Frame]) -> Result<Command> {
    // HRANDFIELD key [count [WITHVALUES]]
    if args.is_empty() || args.len() > 3 {
        return Err(FerriteError::WrongArity("HRANDFIELD".to_string()));
    }

    let key = get_bytes(&args[0])?;

    if args.len() == 1 {
        return Ok(Command::HRandField {
            key,
            count: None,
            withvalues: false,
        });
    }

    let count = Some(get_int(&args[1])?);

    let withvalues = if args.len() == 3 {
        let option = get_string(&args[2])?.to_uppercase();
        if option == "WITHVALUES" {
            true
        } else {
            return Err(FerriteError::Syntax);
        }
    } else {
        false
    };

    Ok(Command::HRandField {
        key,
        count,
        withvalues,
    })
}
