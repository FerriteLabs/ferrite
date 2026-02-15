
use bytes::Bytes;
use crate::error::{FerriteError, Result};
use crate::protocol::Frame;
use super::{get_string, get_bytes, get_int};
use crate::commands::parser::{Command};

pub(crate) fn parse_xadd(args: &[Frame]) -> Result<Command> {
    if args.len() < 4 {
        return Err(FerriteError::WrongArity("XADD".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let mut idx = 1;
    let mut maxlen: Option<usize> = None;
    let mut minid: Option<String> = None;
    let mut nomkstream = false;

    // Parse optional flags: NOMKSTREAM, MAXLEN, MINID, LIMIT
    while idx < args.len() {
        let arg = get_string(&args[idx])?.to_uppercase();
        if arg == "MAXLEN" {
            idx += 1;
            // Skip optional ~ or =
            if idx < args.len() {
                let next = get_string(&args[idx])?;
                if next == "~" || next == "=" {
                    idx += 1;
                }
            }
            if idx < args.len() {
                maxlen = Some(get_int(&args[idx])? as usize);
                idx += 1;
            }
        } else if arg == "MINID" {
            idx += 1;
            // Skip optional ~ or =
            if idx < args.len() {
                let next = get_string(&args[idx])?;
                if next == "~" || next == "=" {
                    idx += 1;
                }
            }
            if idx < args.len() {
                minid = Some(get_string(&args[idx])?);
                idx += 1;
            }
        } else if arg == "NOMKSTREAM" {
            nomkstream = true;
            idx += 1;
        } else if arg == "LIMIT" {
            idx += 2; // Skip LIMIT and count
        } else {
            break;
        }
    }

    // Parse ID (could be * for auto-generate)
    if idx >= args.len() {
        return Err(FerriteError::WrongArity("XADD".to_string()));
    }
    let id_str = get_string(&args[idx])?;
    let id = if id_str == "*" { None } else { Some(id_str) };
    idx += 1;

    // Parse field-value pairs
    if (args.len() - idx) % 2 != 0 || args.len() - idx < 2 {
        return Err(FerriteError::Protocol(
            "wrong number of arguments for 'xadd' command".to_string(),
        ));
    }

    let mut fields = Vec::new();
    while idx < args.len() {
        let field = get_bytes(&args[idx])?;
        let value = get_bytes(&args[idx + 1])?;
        fields.push((field, value));
        idx += 2;
    }

    Ok(Command::XAdd {
        key,
        id,
        fields,
        maxlen,
        minid,
        nomkstream,
    })
}

pub(crate) fn parse_xlen(args: &[Frame]) -> Result<Command> {
    if args.len() != 1 {
        return Err(FerriteError::WrongArity("XLEN".to_string()));
    }
    Ok(Command::XLen {
        key: get_bytes(&args[0])?,
    })
}

pub(crate) fn parse_xrange(args: &[Frame]) -> Result<Command> {
    if args.len() < 3 {
        return Err(FerriteError::WrongArity("XRANGE".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let start = get_string(&args[1])?;
    let end = get_string(&args[2])?;

    let mut count = None;
    if args.len() >= 5
        && get_string(&args[3])?.to_uppercase() == "COUNT" {
            count = Some(get_int(&args[4])? as usize);
        }

    Ok(Command::XRange {
        key,
        start,
        end,
        count,
    })
}

pub(crate) fn parse_xrevrange(args: &[Frame]) -> Result<Command> {
    if args.len() < 3 {
        return Err(FerriteError::WrongArity("XREVRANGE".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let end = get_string(&args[1])?;
    let start = get_string(&args[2])?;

    let mut count = None;
    if args.len() >= 5
        && get_string(&args[3])?.to_uppercase() == "COUNT" {
            count = Some(get_int(&args[4])? as usize);
        }

    Ok(Command::XRevRange {
        key,
        start,
        end,
        count,
    })
}

pub(crate) fn parse_xread(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("XREAD".to_string()));
    }

    let mut idx = 0;
    let mut count = None;
    let mut block = None;

    // Parse options
    while idx < args.len() {
        let arg = get_string(&args[idx])?.to_uppercase();
        match arg.as_str() {
            "COUNT" => {
                idx += 1;
                if idx < args.len() {
                    count = Some(get_int(&args[idx])? as usize);
                    idx += 1;
                }
            }
            "BLOCK" => {
                idx += 1;
                if idx < args.len() {
                    block = Some(get_int(&args[idx])? as u64);
                    idx += 1;
                }
            }
            "STREAMS" => {
                idx += 1;
                break;
            }
            _ => {
                return Err(FerriteError::Protocol(format!(
                    "Unrecognized XREAD argument: {}",
                    arg
                )));
            }
        }
    }

    // Remaining args are keys followed by IDs
    let remaining = &args[idx..];
    if remaining.is_empty() || remaining.len() % 2 != 0 {
        return Err(FerriteError::WrongArity("XREAD".to_string()));
    }

    let half = remaining.len() / 2;
    let mut streams = Vec::with_capacity(half);
    for i in 0..half {
        let key = get_bytes(&remaining[i])?;
        let id = get_string(&remaining[half + i])?;
        streams.push((key, id));
    }

    Ok(Command::XRead {
        streams,
        count,
        block,
    })
}

pub(crate) fn parse_xdel(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("XDEL".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let ids: Result<Vec<String>> = args[1..].iter().map(get_string).collect();

    Ok(Command::XDel { key, ids: ids? })
}

pub(crate) fn parse_xtrim(args: &[Frame]) -> Result<Command> {
    if args.len() < 3 {
        return Err(FerriteError::WrongArity("XTRIM".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let strategy = get_string(&args[1])?.to_uppercase();

    let mut idx = 2;
    let mut approximate = false;

    // Skip optional ~ or =
    if idx < args.len() {
        let next = get_string(&args[idx])?;
        if next == "~" {
            approximate = true;
            idx += 1;
        } else if next == "=" {
            idx += 1;
        }
    }

    match strategy.as_str() {
        "MAXLEN" => {
            let maxlen = get_int(&args[idx])? as usize;
            Ok(Command::XTrim {
                key,
                maxlen: Some(maxlen),
                minid: None,
                approximate,
            })
        }
        "MINID" => {
            let minid_val = get_string(&args[idx])?;
            Ok(Command::XTrim {
                key,
                maxlen: None,
                minid: Some(minid_val),
                approximate,
            })
        }
        _ => Err(FerriteError::Protocol(
            "ERR unsupported XTRIM strategy. Try MAXLEN or MINID.".to_string(),
        )),
    }
}

pub(crate) fn parse_xinfo(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("XINFO".to_string()));
    }

    let subcommand = get_string(&args[0])?.to_uppercase();

    // HELP subcommand doesn't require a key
    if subcommand == "HELP" {
        return Ok(Command::XInfo {
            key: Bytes::new(),
            subcommand,
            group_name: None,
        });
    }

    if args.len() < 2 {
        return Err(FerriteError::WrongArity("XINFO".to_string()));
    }

    let key = get_bytes(&args[1])?;

    // CONSUMERS requires a group name as third argument
    let group_name = if subcommand == "CONSUMERS" {
        if args.len() < 3 {
            return Err(FerriteError::WrongArity("XINFO CONSUMERS".to_string()));
        }
        Some(get_bytes(&args[2])?)
    } else {
        None
    };

    Ok(Command::XInfo {
        key,
        subcommand,
        group_name,
    })
}

pub(crate) fn parse_xgroup(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("XGROUP".to_string()));
    }

    let subcommand = get_string(&args[0])?.to_uppercase();

    match subcommand.as_str() {
        "CREATE" => {
            if args.len() < 4 {
                return Err(FerriteError::WrongArity("XGROUP CREATE".to_string()));
            }
            let key = get_bytes(&args[1])?;
            let group = get_bytes(&args[2])?;
            let id = get_string(&args[3])?;

            let mut mkstream = false;
            let mut idx = 4;
            while idx < args.len() {
                let opt = get_string(&args[idx])?.to_uppercase();
                match opt.as_str() {
                    "MKSTREAM" => {
                        mkstream = true;
                        idx += 1;
                    }
                    "ENTRIESREAD" => {
                        // Skip ENTRIESREAD option
                        idx += 2;
                    }
                    _ => idx += 1,
                }
            }

            Ok(Command::XGroupCreate {
                key,
                group,
                id,
                mkstream,
            })
        }
        "DESTROY" => {
            if args.len() < 3 {
                return Err(FerriteError::WrongArity("XGROUP DESTROY".to_string()));
            }
            let key = get_bytes(&args[1])?;
            let group = get_bytes(&args[2])?;

            Ok(Command::XGroupDestroy { key, group })
        }
        "CREATECONSUMER" => {
            if args.len() < 4 {
                return Err(FerriteError::WrongArity(
                    "XGROUP CREATECONSUMER".to_string(),
                ));
            }
            let key = get_bytes(&args[1])?;
            let group = get_bytes(&args[2])?;
            let consumer = get_bytes(&args[3])?;

            Ok(Command::XGroupCreateConsumer {
                key,
                group,
                consumer,
            })
        }
        "DELCONSUMER" => {
            if args.len() < 4 {
                return Err(FerriteError::WrongArity("XGROUP DELCONSUMER".to_string()));
            }
            let key = get_bytes(&args[1])?;
            let group = get_bytes(&args[2])?;
            let consumer = get_bytes(&args[3])?;

            Ok(Command::XGroupDelConsumer {
                key,
                group,
                consumer,
            })
        }
        "SETID" => {
            if args.len() < 4 {
                return Err(FerriteError::WrongArity("XGROUP SETID".to_string()));
            }
            let key = get_bytes(&args[1])?;
            let group = get_bytes(&args[2])?;
            let id = get_string(&args[3])?;

            Ok(Command::XGroupSetId { key, group, id })
        }
        _ => Err(FerriteError::UnknownCommand(format!(
            "XGROUP {}",
            subcommand
        ))),
    }
}

pub(crate) fn parse_xack(args: &[Frame]) -> Result<Command> {
    if args.len() < 3 {
        return Err(FerriteError::WrongArity("XACK".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let group = get_bytes(&args[1])?;
    let ids: Vec<String> = args[2..]
        .iter()
        .map(get_string)
        .collect::<Result<Vec<_>>>()?;

    Ok(Command::XAck { key, group, ids })
}

pub(crate) fn parse_xpending(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("XPENDING".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let group = get_bytes(&args[1])?;

    // Summary form: XPENDING key group
    if args.len() == 2 {
        return Ok(Command::XPending {
            key,
            group,
            start: None,
            end: None,
            count: None,
            consumer: None,
            idle: None,
        });
    }

    // Extended form: XPENDING key group [[IDLE min-idle-time] start end count [consumer]]
    let mut idx = 2;
    let mut idle = None;
    let mut start = None;
    let mut end = None;
    let mut count = None;
    let mut consumer = None;

    // Check for IDLE option
    if idx < args.len() {
        let opt = get_string(&args[idx])?.to_uppercase();
        if opt == "IDLE" {
            idx += 1;
            if idx < args.len() {
                idle = Some(
                    get_string(&args[idx])?
                        .parse::<u64>()
                        .map_err(|_| FerriteError::NotInteger)?,
                );
                idx += 1;
            }
        }
    }

    // Parse start, end, count
    if idx < args.len() {
        start = Some(get_string(&args[idx])?);
        idx += 1;
    }
    if idx < args.len() {
        end = Some(get_string(&args[idx])?);
        idx += 1;
    }
    if idx < args.len() {
        count = Some(
            get_string(&args[idx])?
                .parse::<usize>()
                .map_err(|_| FerriteError::NotInteger)?,
        );
        idx += 1;
    }

    // Optional consumer filter
    if idx < args.len() {
        consumer = Some(get_bytes(&args[idx])?);
    }

    Ok(Command::XPending {
        key,
        group,
        start,
        end,
        count,
        consumer,
        idle,
    })
}

pub(crate) fn parse_xclaim(args: &[Frame]) -> Result<Command> {
    if args.len() < 5 {
        return Err(FerriteError::WrongArity("XCLAIM".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let group = get_bytes(&args[1])?;
    let consumer = get_bytes(&args[2])?;
    let min_idle_time = get_string(&args[3])?
        .parse::<u64>()
        .map_err(|_| FerriteError::NotInteger)?;

    // Parse message IDs until we hit an option
    let mut idx = 4;
    let mut ids = Vec::new();

    while idx < args.len() {
        let s = get_string(&args[idx])?;
        let upper = s.to_uppercase();
        if upper == "IDLE"
            || upper == "TIME"
            || upper == "RETRYCOUNT"
            || upper == "FORCE"
            || upper == "JUSTID"
            || upper == "LASTID"
        {
            break;
        }
        ids.push(s);
        idx += 1;
    }

    let mut idle = None;
    let mut time = None;
    let mut retrycount = None;
    let mut force = false;
    let mut justid = false;

    while idx < args.len() {
        let opt = get_string(&args[idx])?.to_uppercase();
        match opt.as_str() {
            "IDLE" => {
                idx += 1;
                if idx < args.len() {
                    idle = Some(
                        get_string(&args[idx])?
                            .parse::<u64>()
                            .map_err(|_| FerriteError::NotInteger)?,
                    );
                    idx += 1;
                }
            }
            "TIME" => {
                idx += 1;
                if idx < args.len() {
                    time = Some(
                        get_string(&args[idx])?
                            .parse::<u64>()
                            .map_err(|_| FerriteError::NotInteger)?,
                    );
                    idx += 1;
                }
            }
            "RETRYCOUNT" => {
                idx += 1;
                if idx < args.len() {
                    retrycount = Some(
                        get_string(&args[idx])?
                            .parse::<u64>()
                            .map_err(|_| FerriteError::NotInteger)?,
                    );
                    idx += 1;
                }
            }
            "FORCE" => {
                force = true;
                idx += 1;
            }
            "JUSTID" => {
                justid = true;
                idx += 1;
            }
            "LASTID" => {
                // Skip LASTID option
                idx += 2;
            }
            _ => idx += 1,
        }
    }

    Ok(Command::XClaim {
        key,
        group,
        consumer,
        min_idle_time,
        ids,
        idle,
        time,
        retrycount,
        force,
        justid,
    })
}

pub(crate) fn parse_xautoclaim(args: &[Frame]) -> Result<Command> {
    if args.len() < 5 {
        return Err(FerriteError::WrongArity("XAUTOCLAIM".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let group = get_bytes(&args[1])?;
    let consumer = get_bytes(&args[2])?;
    let min_idle_time = get_string(&args[3])?
        .parse::<u64>()
        .map_err(|_| FerriteError::NotInteger)?;
    let start = get_string(&args[4])?;

    let mut idx = 5;
    let mut count = None;
    let mut justid = false;

    while idx < args.len() {
        let opt = get_string(&args[idx])?.to_uppercase();
        match opt.as_str() {
            "COUNT" => {
                idx += 1;
                if idx < args.len() {
                    count = Some(
                        get_string(&args[idx])?
                            .parse::<usize>()
                            .map_err(|_| FerriteError::NotInteger)?,
                    );
                    idx += 1;
                }
            }
            "JUSTID" => {
                justid = true;
                idx += 1;
            }
            _ => idx += 1,
        }
    }

    Ok(Command::XAutoClaim {
        key,
        group,
        consumer,
        min_idle_time,
        start,
        count,
        justid,
    })
}

pub(crate) fn parse_xreadgroup(args: &[Frame]) -> Result<Command> {
    if args.len() < 6 {
        return Err(FerriteError::WrongArity("XREADGROUP".to_string()));
    }

    let mut idx = 0;
    let mut group = None;
    let mut consumer = None;
    let mut count = None;
    let mut block = None;
    let mut noack = false;

    // Parse GROUP groupname consumername
    let first = get_string(&args[idx])?.to_uppercase();
    if first != "GROUP" {
        return Err(FerriteError::Syntax);
    }
    idx += 1;

    if idx < args.len() {
        group = Some(get_bytes(&args[idx])?);
        idx += 1;
    }
    if idx < args.len() {
        consumer = Some(get_bytes(&args[idx])?);
        idx += 1;
    }

    // Parse optional COUNT, BLOCK, NOACK
    while idx < args.len() {
        let opt = get_string(&args[idx])?.to_uppercase();
        match opt.as_str() {
            "COUNT" => {
                idx += 1;
                if idx < args.len() {
                    count = Some(
                        get_string(&args[idx])?
                            .parse::<usize>()
                            .map_err(|_| FerriteError::NotInteger)?,
                    );
                    idx += 1;
                }
            }
            "BLOCK" => {
                idx += 1;
                if idx < args.len() {
                    block = Some(
                        get_string(&args[idx])?
                            .parse::<u64>()
                            .map_err(|_| FerriteError::NotInteger)?,
                    );
                    idx += 1;
                }
            }
            "NOACK" => {
                noack = true;
                idx += 1;
            }
            "STREAMS" => {
                idx += 1;
                break;
            }
            _ => idx += 1,
        }
    }

    // Remaining args are keys and IDs
    let remaining = args.len() - idx;
    if remaining == 0 || remaining % 2 != 0 {
        return Err(FerriteError::Syntax);
    }

    let half = remaining / 2;
    let mut streams = Vec::with_capacity(half);

    for i in 0..half {
        let key = get_bytes(&args[idx + i])?;
        let id = get_string(&args[idx + half + i])?;
        streams.push((key, id));
    }

    let group = group.ok_or(FerriteError::Syntax)?;
    let consumer = consumer.ok_or(FerriteError::Syntax)?;

    Ok(Command::XReadGroup {
        group,
        consumer,
        count,
        block,
        noack,
        streams,
    })
}
