use bytes::Bytes;

use crate::error::{FerriteError, Result};
use crate::protocol::Frame;
use super::get_bytes;
use crate::commands::parser::{Command};

pub(crate) fn parse_watch(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("WATCH".to_string()));
    }
    let keys: Result<Vec<Bytes>> = args.iter().map(get_bytes).collect();
    Ok(Command::Watch { keys: keys? })
}
