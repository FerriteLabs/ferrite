use bytes::Bytes;

use super::{get_bytes, get_string};
use crate::commands::parser::Command;
use crate::error::{FerriteError, Result};
use crate::protocol::Frame;

/// GETBIT key offset
pub(crate) fn parse_getbit(args: &[Frame]) -> Result<Command> {
    if args.len() != 2 {
        return Err(FerriteError::WrongArity("GETBIT".to_string()));
    }
    let key = get_bytes(&args[0])?;
    let offset: u64 = get_string(&args[1])?.parse().map_err(|_| {
        FerriteError::Protocol("ERR bit offset is not an integer or out of range".to_string())
    })?;
    Ok(Command::GetBit { key, offset })
}

/// SETBIT key offset value
pub(crate) fn parse_setbit(args: &[Frame]) -> Result<Command> {
    if args.len() != 3 {
        return Err(FerriteError::WrongArity("SETBIT".to_string()));
    }
    let key = get_bytes(&args[0])?;
    let offset: u64 = get_string(&args[1])?.parse().map_err(|_| {
        FerriteError::Protocol("ERR bit offset is not an integer or out of range".to_string())
    })?;
    let value: u8 = get_string(&args[2])?.parse().map_err(|_| {
        FerriteError::Protocol("ERR bit is not an integer or out of range".to_string())
    })?;
    Ok(Command::SetBit { key, offset, value })
}

/// BITCOUNT key [start end [BYTE|BIT]]
pub(crate) fn parse_bitcount(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("BITCOUNT".to_string()));
    }
    let key = get_bytes(&args[0])?;
    let mut start = None;
    let mut end = None;
    let mut bit_mode = false;

    if args.len() >= 3 {
        start = Some(get_string(&args[1])?.parse().map_err(|_| {
            FerriteError::Protocol("ERR value is not an integer or out of range".to_string())
        })?);
        end = Some(get_string(&args[2])?.parse().map_err(|_| {
            FerriteError::Protocol("ERR value is not an integer or out of range".to_string())
        })?);
    }

    if args.len() >= 4 {
        let mode = get_string(&args[3])?.to_ascii_uppercase();
        bit_mode = match mode.as_str() {
            "BIT" => true,
            "BYTE" => false,
            _ => return Err(FerriteError::Protocol("ERR syntax error".to_string())),
        };
    }

    Ok(Command::BitCount {
        key,
        start,
        end,
        bit_mode,
    })
}

/// BITOP operation destkey key [key ...]
pub(crate) fn parse_bitop(args: &[Frame]) -> Result<Command> {
    if args.len() < 3 {
        return Err(FerriteError::WrongArity("BITOP".to_string()));
    }

    let op_str = get_string(&args[0])?.to_ascii_uppercase();
    let operation = match op_str.as_str() {
        "AND" => crate::commands::bitmap::BitOperation::And,
        "OR" => crate::commands::bitmap::BitOperation::Or,
        "XOR" => crate::commands::bitmap::BitOperation::Xor,
        "NOT" => crate::commands::bitmap::BitOperation::Not,
        _ => return Err(FerriteError::Protocol("ERR syntax error".to_string())),
    };

    let destkey = get_bytes(&args[1])?;
    let keys: Vec<Bytes> = args[2..].iter().map(get_bytes).collect::<Result<_>>()?;

    Ok(Command::BitOp {
        operation,
        destkey,
        keys,
    })
}

/// BITPOS key bit [start [end [BYTE|BIT]]]
pub(crate) fn parse_bitpos(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("BITPOS".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let bit: u8 = get_string(&args[1])?.parse().map_err(|_| {
        FerriteError::Protocol("ERR bit is not an integer or out of range".to_string())
    })?;

    let mut start = None;
    let mut end = None;
    let mut bit_mode = false;

    if args.len() >= 3 {
        start = Some(get_string(&args[2])?.parse().map_err(|_| {
            FerriteError::Protocol("ERR value is not an integer or out of range".to_string())
        })?);
    }

    if args.len() >= 4 {
        end = Some(get_string(&args[3])?.parse().map_err(|_| {
            FerriteError::Protocol("ERR value is not an integer or out of range".to_string())
        })?);
    }

    if args.len() >= 5 {
        let mode = get_string(&args[4])?.to_ascii_uppercase();
        bit_mode = match mode.as_str() {
            "BIT" => true,
            "BYTE" => false,
            _ => return Err(FerriteError::Protocol("ERR syntax error".to_string())),
        };
    }

    Ok(Command::BitPos {
        key,
        bit,
        start,
        end,
        bit_mode,
    })
}

/// BITFIELD key [GET encoding offset] [SET encoding offset value] [INCRBY encoding offset increment] [OVERFLOW WRAP|SAT|FAIL]
pub(crate) fn parse_bitfield(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("BITFIELD".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let mut subcommands = Vec::new();
    let mut i = 1;

    while i < args.len() {
        let subcmd = get_string(&args[i])?.to_ascii_uppercase();
        match subcmd.as_str() {
            "GET" => {
                if i + 2 >= args.len() {
                    return Err(FerriteError::Protocol(
                        "ERR BITFIELD GET requires encoding and offset".to_string(),
                    ));
                }
                let encoding_str = get_string(&args[i + 1])?;
                let offset_str = get_string(&args[i + 2])?;
                let encoding = crate::commands::bitmap::BitFieldEncoding::parse(&encoding_str)
                    .ok_or_else(|| {
                        FerriteError::Protocol("ERR invalid bitfield type".to_string())
                    })?;
                let offset = crate::commands::bitmap::BitFieldOffset::parse(&offset_str)
                    .ok_or_else(|| {
                        FerriteError::Protocol("ERR invalid bitfield offset".to_string())
                    })?;
                subcommands
                    .push(crate::commands::bitmap::BitFieldSubCommand::Get { encoding, offset });
                i += 3;
            }
            "SET" => {
                if i + 3 >= args.len() {
                    return Err(FerriteError::Protocol(
                        "ERR BITFIELD SET requires encoding, offset and value".to_string(),
                    ));
                }
                let encoding_str = get_string(&args[i + 1])?;
                let offset_str = get_string(&args[i + 2])?;
                let value: i64 = get_string(&args[i + 3])?.parse().map_err(|_| {
                    FerriteError::Protocol(
                        "ERR value is not an integer or out of range".to_string(),
                    )
                })?;
                let encoding = crate::commands::bitmap::BitFieldEncoding::parse(&encoding_str)
                    .ok_or_else(|| {
                        FerriteError::Protocol("ERR invalid bitfield type".to_string())
                    })?;
                let offset = crate::commands::bitmap::BitFieldOffset::parse(&offset_str)
                    .ok_or_else(|| {
                        FerriteError::Protocol("ERR invalid bitfield offset".to_string())
                    })?;
                subcommands.push(crate::commands::bitmap::BitFieldSubCommand::Set {
                    encoding,
                    offset,
                    value,
                });
                i += 4;
            }
            "INCRBY" => {
                if i + 3 >= args.len() {
                    return Err(FerriteError::Protocol(
                        "ERR BITFIELD INCRBY requires encoding, offset and increment".to_string(),
                    ));
                }
                let encoding_str = get_string(&args[i + 1])?;
                let offset_str = get_string(&args[i + 2])?;
                let increment: i64 = get_string(&args[i + 3])?.parse().map_err(|_| {
                    FerriteError::Protocol(
                        "ERR increment is not an integer or out of range".to_string(),
                    )
                })?;
                let encoding = crate::commands::bitmap::BitFieldEncoding::parse(&encoding_str)
                    .ok_or_else(|| {
                        FerriteError::Protocol("ERR invalid bitfield type".to_string())
                    })?;
                let offset = crate::commands::bitmap::BitFieldOffset::parse(&offset_str)
                    .ok_or_else(|| {
                        FerriteError::Protocol("ERR invalid bitfield offset".to_string())
                    })?;
                subcommands.push(crate::commands::bitmap::BitFieldSubCommand::IncrBy {
                    encoding,
                    offset,
                    increment,
                });
                i += 4;
            }
            "OVERFLOW" => {
                if i + 1 >= args.len() {
                    return Err(FerriteError::Protocol(
                        "ERR BITFIELD OVERFLOW requires type".to_string(),
                    ));
                }
                let overflow_type = get_string(&args[i + 1])?.to_ascii_uppercase();
                let overflow = match overflow_type.as_str() {
                    "WRAP" => crate::commands::bitmap::OverflowType::Wrap,
                    "SAT" => crate::commands::bitmap::OverflowType::Sat,
                    "FAIL" => crate::commands::bitmap::OverflowType::Fail,
                    _ => {
                        return Err(FerriteError::Protocol(
                            "ERR invalid overflow type".to_string(),
                        ))
                    }
                };
                subcommands.push(crate::commands::bitmap::BitFieldSubCommand::Overflow(
                    overflow,
                ));
                i += 2;
            }
            _ => {
                return Err(FerriteError::Protocol(format!(
                    "ERR unknown subcommand: {}",
                    subcmd
                )))
            }
        }
    }

    Ok(Command::BitField { key, subcommands })
}

/// BITFIELD_RO key GET encoding offset [GET encoding offset ...]
pub(crate) fn parse_bitfield_ro(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("BITFIELD_RO".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let mut subcommands = Vec::new();
    let mut i = 1;

    while i < args.len() {
        let subcmd = get_string(&args[i])?.to_ascii_uppercase();
        if subcmd != "GET" {
            return Err(FerriteError::Protocol(
                "ERR BITFIELD_RO only supports GET subcommand".to_string(),
            ));
        }
        if i + 2 >= args.len() {
            return Err(FerriteError::Protocol(
                "ERR BITFIELD_RO GET requires encoding and offset".to_string(),
            ));
        }
        let encoding_str = get_string(&args[i + 1])?;
        let offset_str = get_string(&args[i + 2])?;
        let encoding = crate::commands::bitmap::BitFieldEncoding::parse(&encoding_str)
            .ok_or_else(|| FerriteError::Protocol("ERR invalid bitfield type".to_string()))?;
        let offset = crate::commands::bitmap::BitFieldOffset::parse(&offset_str)
            .ok_or_else(|| FerriteError::Protocol("ERR invalid bitfield offset".to_string()))?;
        subcommands.push(crate::commands::bitmap::BitFieldSubCommand::Get { encoding, offset });
        i += 3;
    }

    Ok(Command::BitFieldRo { key, subcommands })
}
