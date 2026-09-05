//! Kafka-compatible streaming command parsers.

use bytes::Bytes;

use super::{get_bytes, get_int, get_string};
use crate::commands::parser::Command;
use crate::error::{FerriteError, Result};
use crate::protocol::Frame;

pub(crate) fn parse_stream_create(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("STREAM.CREATE".to_string()));
    }
    let topic = get_string(&args[0])?;
    let mut partitions: u32 = 4;
    let mut retention_ms: i64 = -1;
    let mut replication: u16 = 1;

    let mut i = 1;
    while i < args.len() {
        let arg = get_string(&args[i])?.to_uppercase();
        match arg.as_str() {
            "PARTITIONS" => {
                i += 1;
                if i < args.len() {
                    partitions = get_int(&args[i])? as u32;
                }
            }
            "RETENTION" => {
                i += 1;
                if i < args.len() {
                    retention_ms = get_int(&args[i])?;
                }
            }
            "REPLICATION" => {
                i += 1;
                if i < args.len() {
                    replication = get_int(&args[i])? as u16;
                }
            }
            _ => {}
        }
        i += 1;
    }
    Ok(Command::StreamCreate {
        topic,
        partitions,
        retention_ms,
        replication,
    })
}

/// STREAM.DELETE topic
pub(crate) fn parse_stream_delete(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("STREAM.DELETE".to_string()));
    }
    let topic = get_string(&args[0])?;
    Ok(Command::StreamDelete { topic })
}

/// STREAM.PRODUCE topic [KEY key] value [PARTITION n]
pub(crate) fn parse_stream_produce(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("STREAM.PRODUCE".to_string()));
    }
    let topic = get_string(&args[0])?;
    let mut key: Option<Bytes> = None;
    let mut partition: Option<u32> = None;
    let mut value: Option<Bytes> = None;

    let mut i = 1;
    while i < args.len() {
        let arg_str = get_string(&args[i])?.to_uppercase();
        match arg_str.as_str() {
            "KEY" => {
                i += 1;
                if i < args.len() {
                    key = Some(get_bytes(&args[i])?);
                }
            }
            "PARTITION" => {
                i += 1;
                if i < args.len() {
                    partition = Some(get_int(&args[i])? as u32);
                }
            }
            _ => {
                // Treat as value if not yet set
                if value.is_none() {
                    value = Some(get_bytes(&args[i])?);
                }
            }
        }
        i += 1;
    }

    let value = value.ok_or_else(|| FerriteError::WrongArity("STREAM.PRODUCE".to_string()))?;
    Ok(Command::StreamProduce {
        topic,
        key,
        value,
        partition,
    })
}

/// STREAM.FETCH topic partition offset [COUNT n]
pub(crate) fn parse_stream_fetch(args: &[Frame]) -> Result<Command> {
    if args.len() < 3 {
        return Err(FerriteError::WrongArity("STREAM.FETCH".to_string()));
    }
    let topic = get_string(&args[0])?;
    let partition = get_int(&args[1])? as u32;
    let offset = get_int(&args[2])?;
    let mut count: usize = 100;

    let mut i = 3;
    while i < args.len() {
        let arg = get_string(&args[i])?.to_uppercase();
        if arg == "COUNT" {
            i += 1;
            if i < args.len() {
                count = get_int(&args[i])? as usize;
            }
        }
        i += 1;
    }
    Ok(Command::StreamFetch {
        topic,
        partition,
        offset,
        count,
    })
}

/// STREAM.COMMIT group topic partition offset
pub(crate) fn parse_stream_commit(args: &[Frame]) -> Result<Command> {
    if args.len() < 4 {
        return Err(FerriteError::WrongArity("STREAM.COMMIT".to_string()));
    }
    let group = get_string(&args[0])?;
    let topic = get_string(&args[1])?;
    let partition = get_int(&args[2])? as u32;
    let offset = get_int(&args[3])?;
    Ok(Command::StreamCommit {
        group,
        topic,
        partition,
        offset,
    })
}

/// STREAM.DESCRIBE topic
pub(crate) fn parse_stream_describe(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("STREAM.DESCRIBE".to_string()));
    }
    let topic = get_string(&args[0])?;
    Ok(Command::StreamDescribe { topic })
}

/// STREAM.GROUPS [topic]
pub(crate) fn parse_stream_groups(args: &[Frame]) -> Result<Command> {
    let topic = if args.is_empty() {
        None
    } else {
        Some(get_string(&args[0])?)
    };
    Ok(Command::StreamGroups { topic })
}

/// STREAM.OFFSETS topic partition
pub(crate) fn parse_stream_offsets(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("STREAM.OFFSETS".to_string()));
    }
    let topic = get_string(&args[0])?;
    let partition = get_int(&args[1])? as u32;
    Ok(Command::StreamOffsets { topic, partition })
}
