use bytes::Bytes;

use crate::error::{FerriteError, Result};
use crate::protocol::Frame;
use super::{get_string, get_bytes, get_int};
use crate::commands::parser::{Command};

pub(crate) fn parse_sadd(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("SADD".to_string()));
    }
    let key = get_bytes(&args[0])?;
    let members: Result<Vec<Bytes>> = args[1..].iter().map(get_bytes).collect();
    Ok(Command::SAdd {
        key,
        members: members?,
    })
}

pub(crate) fn parse_srem(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("SREM".to_string()));
    }
    let key = get_bytes(&args[0])?;
    let members: Result<Vec<Bytes>> = args[1..].iter().map(get_bytes).collect();
    Ok(Command::SRem {
        key,
        members: members?,
    })
}

pub(crate) fn parse_smembers(args: &[Frame]) -> Result<Command> {
    if args.len() != 1 {
        return Err(FerriteError::WrongArity("SMEMBERS".to_string()));
    }
    Ok(Command::SMembers {
        key: get_bytes(&args[0])?,
    })
}

pub(crate) fn parse_sismember(args: &[Frame]) -> Result<Command> {
    if args.len() != 2 {
        return Err(FerriteError::WrongArity("SISMEMBER".to_string()));
    }
    Ok(Command::SIsMember {
        key: get_bytes(&args[0])?,
        member: get_bytes(&args[1])?,
    })
}

pub(crate) fn parse_smismember(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("SMISMEMBER".to_string()));
    }
    let key = get_bytes(&args[0])?;
    let members: Result<Vec<Bytes>> = args[1..].iter().map(get_bytes).collect();
    Ok(Command::SMIsMember {
        key,
        members: members?,
    })
}

pub(crate) fn parse_scard(args: &[Frame]) -> Result<Command> {
    if args.len() != 1 {
        return Err(FerriteError::WrongArity("SCARD".to_string()));
    }
    Ok(Command::SCard {
        key: get_bytes(&args[0])?,
    })
}

pub(crate) fn parse_sunion(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("SUNION".to_string()));
    }
    let keys: Result<Vec<Bytes>> = args.iter().map(get_bytes).collect();
    Ok(Command::SUnion { keys: keys? })
}

pub(crate) fn parse_sunionstore(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("SUNIONSTORE".to_string()));
    }
    let destination = get_bytes(&args[0])?;
    let keys: Result<Vec<Bytes>> = args[1..].iter().map(get_bytes).collect();
    Ok(Command::SUnionStore {
        destination,
        keys: keys?,
    })
}

pub(crate) fn parse_sinter(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("SINTER".to_string()));
    }
    let keys: Result<Vec<Bytes>> = args.iter().map(get_bytes).collect();
    Ok(Command::SInter { keys: keys? })
}

pub(crate) fn parse_sinterstore(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("SINTERSTORE".to_string()));
    }
    let destination = get_bytes(&args[0])?;
    let keys: Result<Vec<Bytes>> = args[1..].iter().map(get_bytes).collect();
    Ok(Command::SInterStore {
        destination,
        keys: keys?,
    })
}

/// SINTERCARD numkeys key [key ...] [LIMIT limit]
pub(crate) fn parse_sintercard(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("SINTERCARD".to_string()));
    }

    // First argument is numkeys
    let numkeys_str = get_string(&args[0])?;
    let numkeys: usize = numkeys_str.parse().map_err(|_| FerriteError::NotInteger)?;

    if args.len() < 1 + numkeys {
        return Err(FerriteError::WrongArity("SINTERCARD".to_string()));
    }

    let keys: Result<Vec<Bytes>> = args[1..=numkeys].iter().map(get_bytes).collect();
    let keys = keys?;

    let mut limit = None;
    let mut i = 1 + numkeys;
    while i < args.len() {
        let opt = get_string(&args[i])?.to_uppercase();
        match opt.as_str() {
            "LIMIT" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                let limit_str = get_string(&args[i])?;
                limit = Some(limit_str.parse().map_err(|_| FerriteError::NotInteger)?);
            }
            _ => return Err(FerriteError::Syntax),
        }
        i += 1;
    }

    Ok(Command::SInterCard { keys, limit })
}

pub(crate) fn parse_sdiff(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("SDIFF".to_string()));
    }
    let keys: Result<Vec<Bytes>> = args.iter().map(get_bytes).collect();
    Ok(Command::SDiff { keys: keys? })
}

pub(crate) fn parse_sdiffstore(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("SDIFFSTORE".to_string()));
    }
    let destination = get_bytes(&args[0])?;
    let keys: Result<Vec<Bytes>> = args[1..].iter().map(get_bytes).collect();
    Ok(Command::SDiffStore {
        destination,
        keys: keys?,
    })
}

pub(crate) fn parse_spop(args: &[Frame]) -> Result<Command> {
    if args.is_empty() || args.len() > 2 {
        return Err(FerriteError::WrongArity("SPOP".to_string()));
    }
    let key = get_bytes(&args[0])?;
    let count = if args.len() == 2 {
        Some(get_int(&args[1])? as usize)
    } else {
        None
    };
    Ok(Command::SPop { key, count })
}

pub(crate) fn parse_srandmember(args: &[Frame]) -> Result<Command> {
    if args.is_empty() || args.len() > 2 {
        return Err(FerriteError::WrongArity("SRANDMEMBER".to_string()));
    }
    let key = get_bytes(&args[0])?;
    let count = if args.len() == 2 {
        Some(get_int(&args[1])?)
    } else {
        None
    };
    Ok(Command::SRandMember { key, count })
}

pub(crate) fn parse_smove(args: &[Frame]) -> Result<Command> {
    if args.len() != 3 {
        return Err(FerriteError::WrongArity("SMOVE".to_string()));
    }
    Ok(Command::SMove {
        source: get_bytes(&args[0])?,
        destination: get_bytes(&args[1])?,
        member: get_bytes(&args[2])?,
    })
}
