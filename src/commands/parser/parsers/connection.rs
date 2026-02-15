use bytes::Bytes;

use crate::error::{FerriteError, Result};
use crate::protocol::Frame;
use super::{get_string, get_bytes, get_int};
use crate::commands::parser::{Command};

pub(crate) fn parse_client(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("CLIENT".to_string()));
    }

    let subcommand = get_string(&args[0])?.to_uppercase();
    let client_args: Vec<Bytes> = args[1..].iter().filter_map(|f| get_bytes(f).ok()).collect();

    Ok(Command::Client {
        subcommand,
        args: client_args,
    })
}

pub(crate) fn parse_hello(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Ok(Command::Hello {
            protocol_version: None,
            auth: None,
            client_name: None,
        });
    }

    let protocol_version = Some(get_int(&args[0])? as u8);
    let mut auth = None;
    let mut client_name = None;

    let mut idx = 1;
    while idx < args.len() {
        let opt = get_string(&args[idx])?.to_ascii_uppercase();
        match opt.as_str() {
            "AUTH" => {
                if idx + 2 >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                let username = get_bytes(&args[idx + 1])?;
                let password = get_bytes(&args[idx + 2])?;
                auth = Some((username, password));
                idx += 3;
            }
            "SETNAME" => {
                if idx + 1 >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                client_name = Some(get_bytes(&args[idx + 1])?);
                idx += 2;
            }
            _ => return Err(FerriteError::Syntax),
        }
    }

    Ok(Command::Hello {
        protocol_version,
        auth,
        client_name,
    })
}

pub(crate) fn parse_replicaof(args: &[Frame]) -> Result<Command> {
    if args.len() != 2 {
        return Err(FerriteError::WrongArity("REPLICAOF".to_string()));
    }

    let host = get_string(&args[0])?;
    let port_or_one = get_string(&args[1])?;

    // Check for "NO ONE" to stop replication
    if host.eq_ignore_ascii_case("NO") && port_or_one.eq_ignore_ascii_case("ONE") {
        return Ok(Command::ReplicaOf {
            host: None,
            port: None,
        });
    }

    let port: u16 = port_or_one
        .parse()
        .map_err(|_| FerriteError::Protocol("Invalid port number".to_string()))?;

    Ok(Command::ReplicaOf {
        host: Some(host),
        port: Some(port),
    })
}

pub(crate) fn parse_slaveof(args: &[Frame]) -> Result<Command> {
    if args.len() != 2 {
        return Err(FerriteError::WrongArity("SLAVEOF".to_string()));
    }

    let host = get_string(&args[0])?;
    let port_or_one = get_string(&args[1])?;

    // Check for "NO ONE" to stop replication
    if host.eq_ignore_ascii_case("NO") && port_or_one.eq_ignore_ascii_case("ONE") {
        return Ok(Command::SlaveOf {
            host: None,
            port: None,
        });
    }

    let port: u16 = port_or_one
        .parse()
        .map_err(|_| FerriteError::Protocol("Invalid port number".to_string()))?;

    Ok(Command::SlaveOf {
        host: Some(host),
        port: Some(port),
    })
}

pub(crate) fn parse_replconf(args: &[Frame]) -> Result<Command> {
    if args.is_empty() || args.len() % 2 != 0 {
        return Err(FerriteError::WrongArity("REPLCONF".to_string()));
    }

    let mut options = Vec::with_capacity(args.len() / 2);
    for chunk in args.chunks(2) {
        let key = get_string(&chunk[0])?;
        let value = get_string(&chunk[1])?;
        options.push((key, value));
    }

    Ok(Command::ReplConf { options })
}

pub(crate) fn parse_psync(args: &[Frame]) -> Result<Command> {
    if args.len() != 2 {
        return Err(FerriteError::WrongArity("PSYNC".to_string()));
    }

    let replication_id = get_string(&args[0])?;
    let offset = get_int(&args[1])?;

    Ok(Command::Psync {
        replication_id,
        offset,
    })
}

pub(crate) fn parse_auth(args: &[Frame]) -> Result<Command> {
    match args.len() {
        1 => {
            // AUTH password (default user)
            let password = get_string(&args[0])?;
            Ok(Command::Auth {
                username: None,
                password,
            })
        }
        2 => {
            // AUTH username password
            let username = get_string(&args[0])?;
            let password = get_string(&args[1])?;
            Ok(Command::Auth {
                username: Some(username),
                password,
            })
        }
        _ => Err(FerriteError::WrongArity("AUTH".to_string())),
    }
}

pub(crate) fn parse_acl(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("ACL".to_string()));
    }

    let subcommand = get_string(&args[0])?.to_uppercase();
    let acl_args: Vec<String> = args[1..]
        .iter()
        .filter_map(|f| get_string(f).ok())
        .collect();

    Ok(Command::Acl {
        subcommand,
        args: acl_args,
    })
}
