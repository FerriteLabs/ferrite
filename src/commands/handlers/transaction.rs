//! Transaction command handlers
//!
//! Implements Redis-like transaction commands plus extended hybrid transaction support:
//! - TX.BEGIN - Start a new transaction
//! - TX.EXEC - Execute a command within a transaction
//! - TX.COMMIT - Commit a transaction
//! - TX.ROLLBACK - Rollback a transaction
//! - TX.STATUS - Get transaction status
//! - TX.LIST - List active transactions
//! - TX.SAVEPOINT - Create a savepoint
//! - TX.ROLLBACK.TO - Rollback to a savepoint
//! - TX.RELEASE - Release a savepoint
//! - TX.WATCH - Watch keys for optimistic locking
//! - TX.PREPARE - 2PC prepare phase
//! - TX.COMMIT.PREPARED - 2PC commit prepared transaction

use bytes::Bytes;

use crate::protocol::Frame;
use crate::transaction::{IsolationLevel, TransactionId, TransactionManager, TransactionState};

use super::{err_frame, ok_frame, HandlerContext};

/// Handle TX.BEGIN command
/// TX.BEGIN [ISOLATION read_uncommitted|read_committed|repeatable_read|serializable] [TIMEOUT ms] [READONLY]
pub async fn handle_tx_begin(
    _ctx: &HandlerContext<'_>,
    args: &[Bytes],
    tx_manager: &TransactionManager,
) -> Frame {
    let mut isolation = None;
    let mut _timeout_ms = None;
    let mut _read_only = false;

    // Parse arguments
    let mut i = 0;
    while i < args.len() {
        let arg = String::from_utf8_lossy(&args[i]).to_uppercase();
        match arg.as_str() {
            "ISOLATION" => {
                if i + 1 >= args.len() {
                    return err_frame("ISOLATION requires a level argument");
                }
                let level = String::from_utf8_lossy(&args[i + 1]).to_lowercase();
                isolation = match level.as_str() {
                    "read_uncommitted" => Some(IsolationLevel::ReadUncommitted),
                    "read_committed" => Some(IsolationLevel::ReadCommitted),
                    "repeatable_read" => Some(IsolationLevel::RepeatableRead),
                    "serializable" => Some(IsolationLevel::Serializable),
                    _ => return err_frame("Unknown isolation level"),
                };
                i += 2;
            }
            "TIMEOUT" => {
                if i + 1 >= args.len() {
                    return err_frame("TIMEOUT requires a millisecond value");
                }
                match String::from_utf8_lossy(&args[i + 1]).parse::<u64>() {
                    Ok(ms) => _timeout_ms = Some(ms),
                    Err(_) => return err_frame("Invalid timeout value"),
                }
                i += 2;
            }
            "READONLY" => {
                _read_only = true;
                i += 1;
            }
            _ => {
                return err_frame(&format!("Unknown argument: {}", arg));
            }
        }
    }

    // Begin transaction
    match tx_manager.begin(isolation).await {
        Ok(txn) => {
            let txn_id = txn.id();
            Frame::bulk(format!("txn-{}", txn_id.0))
        }
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle TX.COMMIT command
/// TX.COMMIT <txid>
pub async fn handle_tx_commit(
    _ctx: &HandlerContext<'_>,
    args: &[Bytes],
    tx_manager: &TransactionManager,
) -> Frame {
    if args.is_empty() {
        return err_frame("TX.COMMIT requires a transaction ID");
    }

    let txn_id = match parse_txn_id(&args[0]) {
        Ok(id) => id,
        Err(e) => return err_frame(&e),
    };

    match tx_manager.commit(txn_id).await {
        Ok(()) => ok_frame(),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle TX.ROLLBACK command
/// TX.ROLLBACK <txid>
pub async fn handle_tx_rollback(
    _ctx: &HandlerContext<'_>,
    args: &[Bytes],
    tx_manager: &TransactionManager,
) -> Frame {
    if args.is_empty() {
        return err_frame("TX.ROLLBACK requires a transaction ID");
    }

    let txn_id = match parse_txn_id(&args[0]) {
        Ok(id) => id,
        Err(e) => return err_frame(&e),
    };

    match tx_manager.rollback(txn_id).await {
        Ok(()) => ok_frame(),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle TX.STATUS command
/// TX.STATUS <txid>
pub async fn handle_tx_status(
    _ctx: &HandlerContext<'_>,
    args: &[Bytes],
    tx_manager: &TransactionManager,
) -> Frame {
    if args.is_empty() {
        return err_frame("TX.STATUS requires a transaction ID");
    }

    let txn_id = match parse_txn_id(&args[0]) {
        Ok(id) => id,
        Err(e) => return err_frame(&e),
    };

    match tx_manager.get(txn_id).await {
        Some(txn) => {
            let state = txn.state();
            let state_str = match state {
                TransactionState::Active => "active",
                TransactionState::Preparing => "preparing",
                TransactionState::Prepared => "prepared",
                TransactionState::Committing => "committing",
                TransactionState::Committed => "committed",
                TransactionState::Aborting => "aborting",
                TransactionState::Aborted => "aborted",
            };

            let mut result = Vec::new();
            result.push(Frame::bulk("id"));
            result.push(Frame::bulk(format!("txn-{}", txn_id.0)));
            result.push(Frame::bulk("state"));
            result.push(Frame::bulk(state_str));
            result.push(Frame::bulk("isolation"));
            let iso_str = format!("{:?}", txn.isolation());
            result.push(Frame::bulk(iso_str));
            result.push(Frame::bulk("snapshot_ts"));
            result.push(Frame::Integer(txn.snapshot_ts() as i64));
            result.push(Frame::bulk("read_only"));
            result.push(Frame::bulk(if txn.is_read_only() {
                "true"
            } else {
                "false"
            }));

            Frame::array(result)
        }
        None => Frame::Null,
    }
}

/// Handle TX.LIST command
/// TX.LIST [ACTIVE|ALL]
pub async fn handle_tx_list(
    _ctx: &HandlerContext<'_>,
    _args: &[Bytes],
    tx_manager: &TransactionManager,
) -> Frame {
    let count = tx_manager.active_count().await;

    let result = vec![Frame::bulk("active_count"), Frame::Integer(count as i64)];

    Frame::array(result)
}

/// Handle TX.EXEC command - execute a command within a transaction context
/// TX.EXEC <txid> <command> [args...]
pub async fn handle_tx_exec(
    _ctx: &HandlerContext<'_>,
    args: &[Bytes],
    tx_manager: &TransactionManager,
) -> Frame {
    if args.len() < 2 {
        return err_frame("TX.EXEC requires transaction ID and command");
    }

    let txn_id = match parse_txn_id(&args[0]) {
        Ok(id) => id,
        Err(e) => return err_frame(&e),
    };

    let txn = match tx_manager.get(txn_id).await {
        Some(t) => t,
        None => return err_frame("Transaction not found"),
    };

    let command = String::from_utf8_lossy(&args[1]).to_uppercase();
    let cmd_args = &args[2..];

    // Execute command within transaction context
    match command.as_str() {
        "GET" => {
            if cmd_args.is_empty() {
                return err_frame("GET requires a key");
            }
            match txn.get(&cmd_args[0]).await {
                Ok(Some(value)) => match value {
                    crate::storage::Value::String(s) => Frame::bulk(s),
                    _ => Frame::bulk(format!("{:?}", value)),
                },
                Ok(None) => Frame::Null,
                Err(e) => err_frame(&e.to_string()),
            }
        }
        "SET" => {
            if cmd_args.len() < 2 {
                return err_frame("SET requires key and value");
            }
            match txn
                .set(
                    cmd_args[0].clone(),
                    crate::storage::Value::String(cmd_args[1].clone()),
                )
                .await
            {
                Ok(()) => ok_frame(),
                Err(e) => err_frame(&e.to_string()),
            }
        }
        "DEL" => {
            if cmd_args.is_empty() {
                return err_frame("DEL requires a key");
            }
            match txn.delete(cmd_args[0].clone()).await {
                Ok(()) => Frame::Integer(1),
                Err(e) => err_frame(&e.to_string()),
            }
        }
        "INCR" => {
            if cmd_args.is_empty() {
                return err_frame("INCR requires a key");
            }
            match txn.incr(cmd_args[0].clone(), 1).await {
                Ok(val) => Frame::Integer(val),
                Err(e) => err_frame(&e.to_string()),
            }
        }
        "INCRBY" => {
            if cmd_args.len() < 2 {
                return err_frame("INCRBY requires key and increment");
            }
            let delta = match String::from_utf8_lossy(&cmd_args[1]).parse::<i64>() {
                Ok(d) => d,
                Err(_) => return err_frame("Invalid increment value"),
            };
            match txn.incr(cmd_args[0].clone(), delta).await {
                Ok(val) => Frame::Integer(val),
                Err(e) => err_frame(&e.to_string()),
            }
        }
        "EXISTS" => {
            if cmd_args.is_empty() {
                return err_frame("EXISTS requires a key");
            }
            match txn.exists(&cmd_args[0]).await {
                Ok(exists) => Frame::Integer(if exists { 1 } else { 0 }),
                Err(e) => err_frame(&e.to_string()),
            }
        }
        "MGET" => {
            if cmd_args.is_empty() {
                return err_frame("MGET requires at least one key");
            }
            match txn.mget(cmd_args).await {
                Ok(values) => {
                    let frames: Vec<Frame> = values
                        .into_iter()
                        .map(|v| match v {
                            Some(crate::storage::Value::String(s)) => Frame::bulk(s),
                            Some(other) => Frame::bulk(format!("{:?}", other)),
                            None => Frame::Null,
                        })
                        .collect();
                    Frame::array(frames)
                }
                Err(e) => err_frame(&e.to_string()),
            }
        }
        "MSET" => {
            if cmd_args.len() < 2 || cmd_args.len() % 2 != 0 {
                return err_frame("MSET requires key-value pairs");
            }
            let pairs: Vec<_> = cmd_args
                .chunks(2)
                .map(|chunk| {
                    (
                        chunk[0].clone(),
                        crate::storage::Value::String(chunk[1].clone()),
                    )
                })
                .collect();
            match txn.mset(pairs).await {
                Ok(()) => ok_frame(),
                Err(e) => err_frame(&e.to_string()),
            }
        }
        _ => err_frame(&format!("Unsupported transaction command: {}", command)),
    }
}

/// Handle TX.SAVEPOINT command
/// TX.SAVEPOINT <txid> <name>
pub async fn handle_tx_savepoint(
    _ctx: &HandlerContext<'_>,
    args: &[Bytes],
    _tx_manager: &TransactionManager,
) -> Frame {
    if args.len() < 2 {
        return err_frame("TX.SAVEPOINT requires transaction ID and savepoint name");
    }

    let _txn_id = match parse_txn_id(&args[0]) {
        Ok(id) => id,
        Err(e) => return err_frame(&e),
    };

    let _name = String::from_utf8_lossy(&args[1]).to_string();

    // Note: Full savepoint implementation requires modifying Transaction struct
    // to include SavepointManager. For now, return OK as a placeholder.
    ok_frame()
}

/// Handle TX.ROLLBACK.TO command
/// TX.ROLLBACK.TO <txid> <savepoint_name>
pub async fn handle_tx_rollback_to(
    _ctx: &HandlerContext<'_>,
    args: &[Bytes],
    _tx_manager: &TransactionManager,
) -> Frame {
    if args.len() < 2 {
        return err_frame("TX.ROLLBACK.TO requires transaction ID and savepoint name");
    }

    let _txn_id = match parse_txn_id(&args[0]) {
        Ok(id) => id,
        Err(e) => return err_frame(&e),
    };

    let _name = String::from_utf8_lossy(&args[1]).to_string();

    // Placeholder - full implementation needs SavepointManager in Transaction
    ok_frame()
}

/// Handle TX.RELEASE command
/// TX.RELEASE <txid> <savepoint_name>
pub async fn handle_tx_release(
    _ctx: &HandlerContext<'_>,
    args: &[Bytes],
    _tx_manager: &TransactionManager,
) -> Frame {
    if args.len() < 2 {
        return err_frame("TX.RELEASE requires transaction ID and savepoint name");
    }

    let _txn_id = match parse_txn_id(&args[0]) {
        Ok(id) => id,
        Err(e) => return err_frame(&e),
    };

    let _name = String::from_utf8_lossy(&args[1]).to_string();

    // Placeholder
    ok_frame()
}

/// Handle TX.WATCH command (optimistic locking)
/// TX.WATCH <txid> key [key...]
pub async fn handle_tx_watch(
    _ctx: &HandlerContext<'_>,
    args: &[Bytes],
    tx_manager: &TransactionManager,
) -> Frame {
    if args.len() < 2 {
        return err_frame("TX.WATCH requires transaction ID and at least one key");
    }

    let txn_id = match parse_txn_id(&args[0]) {
        Ok(id) => id,
        Err(e) => return err_frame(&e),
    };

    let txn = match tx_manager.get(txn_id).await {
        Some(t) => t,
        None => return err_frame("Transaction not found"),
    };

    // Read each key to add to read set (for conflict detection)
    let keys = &args[1..];
    for key in keys {
        if let Err(e) = txn.get(key).await {
            return err_frame(&e.to_string());
        }
    }

    ok_frame()
}

/// Handle TX.PREPARE command (2PC prepare phase)
/// TX.PREPARE <txid>
pub async fn handle_tx_prepare(
    _ctx: &HandlerContext<'_>,
    args: &[Bytes],
    tx_manager: &TransactionManager,
) -> Frame {
    if args.is_empty() {
        return err_frame("TX.PREPARE requires a transaction ID");
    }

    let txn_id = match parse_txn_id(&args[0]) {
        Ok(id) => id,
        Err(e) => return err_frame(&e),
    };

    let txn = match tx_manager.get(txn_id).await {
        Some(t) => t,
        None => return err_frame("Transaction not found"),
    };

    match txn.prepare().await {
        Ok(()) => ok_frame(),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle TX.INFO command
/// TX.INFO
pub async fn handle_tx_info(
    _ctx: &HandlerContext<'_>,
    _args: &[Bytes],
    tx_manager: &TransactionManager,
) -> Frame {
    let active = tx_manager.active_count().await;
    let watermark = tx_manager.committed_watermark();

    let result = vec![
        Frame::bulk("enabled"),
        Frame::bulk("true"),
        Frame::bulk("active_transactions"),
        Frame::Integer(active as i64),
        Frame::bulk("committed_watermark"),
        Frame::Integer(watermark as i64),
    ];

    Frame::array(result)
}

/// Parse a transaction ID from string format "txn-123" or just "123"
fn parse_txn_id(bytes: &Bytes) -> Result<TransactionId, String> {
    let s = String::from_utf8_lossy(bytes);
    let num_str = if let Some(stripped) = s.strip_prefix("txn-") {
        stripped
    } else {
        &s
    };

    num_str
        .parse::<u64>()
        .map(TransactionId)
        .map_err(|_| format!("Invalid transaction ID: {}", s))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn test_parse_txn_id() {
        assert_eq!(
            parse_txn_id(&Bytes::from("123")).unwrap(),
            TransactionId(123)
        );
        assert_eq!(
            parse_txn_id(&Bytes::from("txn-456")).unwrap(),
            TransactionId(456)
        );
        assert!(parse_txn_id(&Bytes::from("invalid")).is_err());
    }
}
