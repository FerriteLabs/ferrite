use bytes::Bytes;

use crate::error::{FerriteError, Result};
use crate::protocol::Frame;
use super::{get_string, get_bytes, get_int};
use crate::commands::parser::{Command};

pub(crate) fn parse_eval(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("EVAL".to_string()));
    }

    let script = get_string(&args[0])?;
    let numkeys = get_int(&args[1])? as usize;

    if numkeys > args.len() - 2 {
        return Err(FerriteError::Protocol(
            "Number of keys can't be greater than number of args".to_string(),
        ));
    }

    let keys: Result<Vec<Bytes>> = args[2..2 + numkeys].iter().map(get_bytes).collect();
    let script_args: Result<Vec<Bytes>> = args[2 + numkeys..].iter().map(get_bytes).collect();

    Ok(Command::Eval {
        script,
        keys: keys?,
        args: script_args?,
    })
}

pub(crate) fn parse_evalsha(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("EVALSHA".to_string()));
    }

    let sha1 = get_string(&args[0])?;
    let numkeys = get_int(&args[1])? as usize;

    if numkeys > args.len() - 2 {
        return Err(FerriteError::Protocol(
            "Number of keys can't be greater than number of args".to_string(),
        ));
    }

    let keys: Result<Vec<Bytes>> = args[2..2 + numkeys].iter().map(get_bytes).collect();
    let script_args: Result<Vec<Bytes>> = args[2 + numkeys..].iter().map(get_bytes).collect();

    Ok(Command::EvalSha {
        sha1,
        keys: keys?,
        args: script_args?,
    })
}

pub(crate) fn parse_script(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("SCRIPT".to_string()));
    }

    let subcommand = get_string(&args[0])?.to_uppercase();
    let script_args: Vec<String> = args[1..]
        .iter()
        .filter_map(|f| get_string(f).ok())
        .collect();

    Ok(Command::Script {
        subcommand,
        args: script_args,
    })
}

pub(crate) fn parse_eval_ro(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("EVAL_RO".to_string()));
    }

    let script = get_string(&args[0])?;
    let numkeys = get_int(&args[1])? as usize;

    if numkeys > args.len() - 2 {
        return Err(FerriteError::Protocol(
            "Number of keys can't be greater than number of args".to_string(),
        ));
    }

    let keys: Result<Vec<Bytes>> = args[2..2 + numkeys].iter().map(get_bytes).collect();
    let script_args: Result<Vec<Bytes>> = args[2 + numkeys..].iter().map(get_bytes).collect();

    Ok(Command::EvalRo {
        script,
        keys: keys?,
        args: script_args?,
    })
}

pub(crate) fn parse_evalsha_ro(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("EVALSHA_RO".to_string()));
    }

    let sha1 = get_string(&args[0])?;
    let numkeys = get_int(&args[1])? as usize;

    if numkeys > args.len() - 2 {
        return Err(FerriteError::Protocol(
            "Number of keys can't be greater than number of args".to_string(),
        ));
    }

    let keys: Result<Vec<Bytes>> = args[2..2 + numkeys].iter().map(get_bytes).collect();
    let script_args: Result<Vec<Bytes>> = args[2 + numkeys..].iter().map(get_bytes).collect();

    Ok(Command::EvalShaRo {
        sha1,
        keys: keys?,
        args: script_args?,
    })
}

pub(crate) fn parse_function(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("FUNCTION".to_string()));
    }
    let subcommand = get_string(&args[0])?.to_uppercase();
    let function_args: Vec<Bytes> = args[1..].iter().filter_map(|f| get_bytes(f).ok()).collect();
    Ok(Command::Function {
        subcommand,
        args: function_args,
    })
}

pub(crate) fn parse_fcall(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("FCALL".to_string()));
    }
    let function = get_string(&args[0])?;
    let numkeys = get_int(&args[1])? as usize;

    if numkeys > args.len() - 2 {
        return Err(FerriteError::Protocol(
            "Number of keys can't be greater than number of args".to_string(),
        ));
    }

    let keys: Result<Vec<Bytes>> = args[2..2 + numkeys].iter().map(get_bytes).collect();
    let fcall_args: Result<Vec<Bytes>> = args[2 + numkeys..].iter().map(get_bytes).collect();

    Ok(Command::FCall {
        function,
        keys: keys?,
        args: fcall_args?,
    })
}

pub(crate) fn parse_fcall_ro(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("FCALL_RO".to_string()));
    }
    let function = get_string(&args[0])?;
    let numkeys = get_int(&args[1])? as usize;

    if numkeys > args.len() - 2 {
        return Err(FerriteError::Protocol(
            "Number of keys can't be greater than number of args".to_string(),
        ));
    }

    let keys: Result<Vec<Bytes>> = args[2..2 + numkeys].iter().map(get_bytes).collect();
    let fcall_args: Result<Vec<Bytes>> = args[2 + numkeys..].iter().map(get_bytes).collect();

    Ok(Command::FCallRo {
        function,
        keys: keys?,
        args: fcall_args?,
    })
}
