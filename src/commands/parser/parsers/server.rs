use bytes::Bytes;

use crate::error::{FerriteError, Result};
use crate::protocol::Frame;
use super::{get_string, get_bytes, get_int};
use crate::commands::parser::{Command};

pub(crate) fn parse_ping(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        Ok(Command::Ping { message: None })
    } else if args.len() == 1 {
        Ok(Command::Ping {
            message: Some(get_bytes(&args[0])?),
        })
    } else {
        Err(FerriteError::WrongArity("PING".to_string()))
    }
}

pub(crate) fn parse_echo(args: &[Frame]) -> Result<Command> {
    if args.len() != 1 {
        return Err(FerriteError::WrongArity("ECHO".to_string()));
    }
    Ok(Command::Echo {
        message: get_bytes(&args[0])?,
    })
}

pub(crate) fn parse_select(args: &[Frame]) -> Result<Command> {
    if args.len() != 1 {
        return Err(FerriteError::WrongArity("SELECT".to_string()));
    }
    let db = get_int(&args[0])?;
    if !(0..=15).contains(&db) {
        return Err(FerriteError::Protocol("invalid DB index".to_string()));
    }
    Ok(Command::Select { db: db as u8 })
}

pub(crate) fn parse_info(args: &[Frame]) -> Result<Command> {
    let section = if args.is_empty() {
        None
    } else {
        Some(get_string(&args[0])?)
    };
    Ok(Command::Info { section })
}

pub(crate) fn parse_command_cmd(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Ok(Command::CommandCmd {
            subcommand: None,
            args: vec![],
        });
    }

    let subcommand = Some(get_string(&args[0])?.to_uppercase());
    let cmd_args: Vec<Bytes> = args[1..].iter().filter_map(|f| get_bytes(f).ok()).collect();

    Ok(Command::CommandCmd {
        subcommand,
        args: cmd_args,
    })
}

pub(crate) fn parse_debug(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("DEBUG".to_string()));
    }

    let subcommand = get_string(&args[0])?.to_uppercase();
    let debug_args: Vec<Bytes> = args[1..].iter().filter_map(|f| get_bytes(f).ok()).collect();

    Ok(Command::Debug {
        subcommand,
        args: debug_args,
    })
}

pub(crate) fn parse_memory(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("MEMORY".to_string()));
    }

    let subcommand = get_string(&args[0])?.to_uppercase();
    let key = if args.len() > 1 {
        Some(get_bytes(&args[1])?)
    } else {
        None
    };

    Ok(Command::Memory { subcommand, key })
}

pub(crate) fn parse_config(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("CONFIG".to_string()));
    }

    let subcommand = get_string(&args[0])?.to_uppercase();
    let config_args: Vec<Bytes> = args[1..].iter().filter_map(|f| get_bytes(f).ok()).collect();

    Ok(Command::Config {
        subcommand,
        args: config_args,
    })
}

pub(crate) fn parse_module(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("MODULE".to_string()));
    }

    let subcommand = get_string(&args[0])?.to_uppercase();
    let module_args: Vec<Bytes> = args[1..].iter().filter_map(|f| get_bytes(f).ok()).collect();

    Ok(Command::Module {
        subcommand,
        args: module_args,
    })
}

pub(crate) fn parse_bgsave(args: &[Frame]) -> Result<Command> {
    let schedule = if !args.is_empty() {
        let opt = get_string(&args[0])?.to_uppercase();
        opt == "SCHEDULE"
    } else {
        false
    };
    Ok(Command::BgSave { schedule })
}

pub(crate) fn parse_slowlog(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("SLOWLOG".to_string()));
    }
    let subcommand = get_string(&args[0])?.to_uppercase();
    let argument = if args.len() > 1 {
        Some(get_int(&args[1])?)
    } else {
        None
    };
    Ok(Command::SlowLog {
        subcommand,
        argument,
    })
}

pub(crate) fn parse_latency(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("LATENCY".to_string()));
    }
    let subcommand = get_string(&args[0])?.to_uppercase();
    let latency_args: Vec<Bytes> = args[1..].iter().filter_map(|f| get_bytes(f).ok()).collect();
    Ok(Command::Latency {
        subcommand,
        args: latency_args,
    })
}

pub(crate) fn parse_wait(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("WAIT".to_string()));
    }
    let numreplicas = get_int(&args[0])?;
    let timeout = get_int(&args[1])?;
    Ok(Command::Wait {
        numreplicas,
        timeout,
    })
}

pub(crate) fn parse_shutdown(args: &[Frame]) -> Result<Command> {
    let mut nosave = false;
    let mut save = false;
    let mut now = false;
    let mut force = false;
    let mut abort = false;

    for arg in args {
        let opt = get_string(arg)?.to_uppercase();
        match opt.as_str() {
            "NOSAVE" => nosave = true,
            "SAVE" => save = true,
            "NOW" => now = true,
            "FORCE" => force = true,
            "ABORT" => abort = true,
            _ => {}
        }
    }
    Ok(Command::Shutdown {
        nosave,
        save,
        now,
        force,
        abort,
    })
}

pub(crate) fn parse_flushdb(args: &[Frame]) -> Result<Command> {
    let mut is_async = false;
    if !args.is_empty() {
        let opt = get_string(&args[0])?.to_uppercase();
        match opt.as_str() {
            "ASYNC" => is_async = true,
            "SYNC" => is_async = false,
            _ => return Err(FerriteError::Protocol(
                format!("ERR FLUSHDB called with unknown option '{}'", opt),
            )),
        }
    }
    Ok(Command::FlushDb { r#async: is_async })
}

pub(crate) fn parse_flushall(args: &[Frame]) -> Result<Command> {
    let mut is_async = false;
    if !args.is_empty() {
        let opt = get_string(&args[0])?.to_uppercase();
        match opt.as_str() {
            "ASYNC" => is_async = true,
            "SYNC" => is_async = false,
            _ => return Err(FerriteError::Protocol(
                format!("ERR FLUSHALL called with unknown option '{}'", opt),
            )),
        }
    }
    Ok(Command::FlushAll { r#async: is_async })
}

pub(crate) fn parse_swapdb(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("SWAPDB".to_string()));
    }
    let index1 = get_int(&args[0])? as u8;
    let index2 = get_int(&args[1])? as u8;
    Ok(Command::SwapDb { index1, index2 })
}

pub(crate) fn parse_move(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("MOVE".to_string()));
    }
    let key = get_bytes(&args[0])?;
    let db = get_int(&args[1])? as u8;
    Ok(Command::Move { key, db })
}

pub(crate) fn parse_migrate(args: &[Frame]) -> Result<Command> {
    if args.len() < 5 {
        return Err(FerriteError::WrongArity("MIGRATE".to_string()));
    }
    let host = get_string(&args[0])?;
    let port = get_int(&args[1])? as u16;
    let key_str = get_string(&args[2])?;
    let key = if key_str.is_empty() {
        None
    } else {
        Some(get_bytes(&args[2])?)
    };
    let destination_db = get_int(&args[3])? as u8;
    let timeout = get_int(&args[4])?;

    let mut copy = false;
    let mut replace = false;
    let mut auth = None;
    let mut keys = Vec::new();

    let mut i = 5;
    while i < args.len() {
        let opt = get_string(&args[i])?.to_uppercase();
        match opt.as_str() {
            "COPY" => copy = true,
            "REPLACE" => replace = true,
            "AUTH" => {
                i += 1;
                if i < args.len() {
                    auth = Some(get_string(&args[i])?);
                }
            }
            "AUTH2" => {
                i += 2; // Skip username and password
            }
            "KEYS" => {
                i += 1;
                while i < args.len() {
                    keys.push(get_bytes(&args[i])?);
                    i += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }

    Ok(Command::Migrate {
        host,
        port,
        key,
        destination_db,
        timeout,
        copy,
        replace,
        auth,
        keys,
    })
}
