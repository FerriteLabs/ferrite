use bytes::Bytes;

use super::{get_bytes, get_string};
use crate::commands::parser::Command;
use crate::error::{FerriteError, Result};
use crate::protocol::Frame;

pub(crate) fn parse_subscribe(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("SUBSCRIBE".to_string()));
    }
    let channels: Result<Vec<Bytes>> = args.iter().map(get_bytes).collect();
    Ok(Command::Subscribe {
        channels: channels?,
    })
}

pub(crate) fn parse_unsubscribe(args: &[Frame]) -> Result<Command> {
    let channels: Result<Vec<Bytes>> = args.iter().map(get_bytes).collect();
    Ok(Command::Unsubscribe {
        channels: channels?,
    })
}

pub(crate) fn parse_psubscribe(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("PSUBSCRIBE".to_string()));
    }
    let patterns: Result<Vec<Bytes>> = args.iter().map(get_bytes).collect();
    Ok(Command::PSubscribe {
        patterns: patterns?,
    })
}

pub(crate) fn parse_punsubscribe(args: &[Frame]) -> Result<Command> {
    let patterns: Result<Vec<Bytes>> = args.iter().map(get_bytes).collect();
    Ok(Command::PUnsubscribe {
        patterns: patterns?,
    })
}

pub(crate) fn parse_publish(args: &[Frame]) -> Result<Command> {
    if args.len() != 2 {
        return Err(FerriteError::WrongArity("PUBLISH".to_string()));
    }
    Ok(Command::Publish {
        channel: get_bytes(&args[0])?,
        message: get_bytes(&args[1])?,
    })
}

pub(crate) fn parse_pubsub(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("PUBSUB".to_string()));
    }

    let subcommand = get_string(&args[0])?.to_ascii_uppercase();
    let subargs = &args[1..];

    match subcommand.as_str() {
        "CHANNELS" => {
            let pattern = if subargs.is_empty() {
                None
            } else {
                Some(get_bytes(&subargs[0])?)
            };
            Ok(Command::PubSubChannels { pattern })
        }
        "NUMSUB" => {
            let channels: Result<Vec<Bytes>> = subargs.iter().map(get_bytes).collect();
            Ok(Command::PubSubNumSub {
                channels: channels?,
            })
        }
        "NUMPAT" => {
            if !subargs.is_empty() {
                return Err(FerriteError::WrongArity("PUBSUB NUMPAT".to_string()));
            }
            Ok(Command::PubSubNumPat)
        }
        "SHARDCHANNELS" => {
            let pattern = if subargs.is_empty() {
                None
            } else {
                Some(get_bytes(&subargs[0])?)
            };
            Ok(Command::PubSubShardChannels { pattern })
        }
        "SHARDNUMSUB" => {
            let channels: Result<Vec<Bytes>> = subargs.iter().map(get_bytes).collect();
            Ok(Command::PubSubShardNumSub {
                channels: channels?,
            })
        }
        _ => Err(FerriteError::UnknownCommand(format!(
            "PUBSUB {}",
            subcommand
        ))),
    }
}

pub(crate) fn parse_ssubscribe(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("SSUBSCRIBE".to_string()));
    }
    let channels: Result<Vec<Bytes>> = args.iter().map(get_bytes).collect();
    Ok(Command::SSubscribe {
        channels: channels?,
    })
}

pub(crate) fn parse_sunsubscribe(args: &[Frame]) -> Result<Command> {
    let channels: Result<Vec<Bytes>> = args.iter().map(get_bytes).collect();
    Ok(Command::SUnsubscribe {
        channels: channels?,
    })
}

pub(crate) fn parse_spublish(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("SPUBLISH".to_string()));
    }
    let channel = get_bytes(&args[0])?;
    let message = get_bytes(&args[1])?;
    Ok(Command::SPublish { channel, message })
}
