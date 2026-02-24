//! Request handler
//!
//! This module implements the request handler that processes commands.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tracing::{debug, error, Instrument};

use bytes::Bytes;

use crate::audit::{AuditEntry, SharedAuditHandle};
use crate::auth::SharedAcl;
use crate::commands::{Command, CommandExecutor, SharedBlockingListManager};
use crate::config::SharedConfig;
use crate::error::Result;
use crate::protocol::Frame;
use crate::replication::{
    ReplicationCommand, ReplicationPrimary, ReplicationRole, SharedReplicationState,
};
use crate::storage::Store;

use super::connection::{Connection, ProtocolVersion};
use crate::runtime::{
    ConnectionId, SharedClientRegistry, SharedSlowLog, SharedSubscriptionManager,
    SharedWatchRegistry,
};

/// Transaction state for a connection
#[derive(Default)]
struct TransactionState {
    /// Whether we're currently in a transaction (after MULTI)
    in_transaction: bool,
    /// Queued commands to execute on EXEC
    queue: Vec<Command>,
    /// Keys being watched (for optimistic locking)
    watched_keys: HashSet<Bytes>,
    /// Whether watched keys have been modified (transaction should abort)
    watch_dirty: bool,
}

impl TransactionState {
    /// Reset the transaction state (after EXEC or DISCARD)
    fn reset(&mut self) {
        self.in_transaction = false;
        self.queue.clear();
        self.watched_keys.clear();
        self.watch_dirty = false;
    }
}

/// Dependency bundle for building a Handler.
pub struct HandlerDependencies {
    pub store: Arc<Store>,
    pub pubsub_manager: SharedSubscriptionManager,
    pub acl: SharedAcl,
    pub blocking_manager: SharedBlockingListManager,
    pub blocking_stream_manager: crate::commands::SharedBlockingStreamManager,
    pub blocking_zset_manager: crate::commands::SharedBlockingSortedSetManager,
    pub replication_state: SharedReplicationState,
    pub replication_primary: Arc<ReplicationPrimary>,
    pub audit_handle: SharedAuditHandle,
    pub shutdown_rx: broadcast::Receiver<()>,
    pub slowlog: SharedSlowLog,
    pub client_registry: SharedClientRegistry,
    pub client_id: u64,
    pub watch_registry: SharedWatchRegistry,
    pub config: Option<SharedConfig>,
}

/// Handler for a single client connection
pub struct Handler {
    /// The client connection
    connection: Connection,

    /// Command executor
    executor: CommandExecutor,

    /// Shared store reference for WATCH functionality
    #[allow(dead_code)] // Planned for v0.2 — reserved for WATCH command functionality
    store: Arc<Store>,

    /// Shared ACL reference for authentication
    acl: SharedAcl,

    /// Shared configuration for hot reload
    #[allow(dead_code)] // Planned for v0.2 — reserved for hot-reload configuration support
    config: Option<SharedConfig>,

    /// Replication state
    replication_state: SharedReplicationState,

    /// Replication primary handler (for command propagation)
    replication_primary: Arc<ReplicationPrimary>,

    /// Audit logging handle
    audit_handle: SharedAuditHandle,

    /// Currently authenticated user
    current_user: String,

    /// Transaction state
    transaction: TransactionState,

    /// Shutdown signal receiver
    shutdown_rx: broadcast::Receiver<()>,

    /// Slow query log
    slowlog: SharedSlowLog,

    /// Client registry
    client_registry: SharedClientRegistry,

    /// This client's ID in the registry
    client_id: u64,

    /// Global WATCH registry for cross-connection key invalidation
    watch_registry: SharedWatchRegistry,

    /// This connection's ID in the watch registry
    watch_connection_id: ConnectionId,
}

/// RESP3 response transformation type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Resp3Transform {
    /// No transformation needed
    None,
    /// Transform flat array [f1, v1, f2, v2, ...] to Map
    ArrayToMap,
    /// Transform array to Set
    ArrayToSet,
}

impl Handler {
    /// Create a new handler
    #[allow(dead_code)] // Planned for v0.2 — constructor used by server connection handling
    pub fn new(connection: Connection, deps: HandlerDependencies) -> Self {
        let executor = match deps.config.clone() {
            Some(config) => CommandExecutor::with_shared_state(
                deps.store.clone(),
                deps.pubsub_manager.clone(),
                deps.acl.clone(),
                deps.blocking_manager.clone(),
                deps.blocking_stream_manager.clone(),
                deps.blocking_zset_manager.clone(),
                config,
                deps.slowlog.clone(),
                deps.client_registry.clone(),
            ),
            None => CommandExecutor::new(
                deps.store.clone(),
                deps.pubsub_manager.clone(),
                deps.acl.clone(),
                deps.blocking_manager.clone(),
                deps.blocking_stream_manager.clone(),
                deps.blocking_zset_manager.clone(),
            ),
        };
        // Register with watch registry and get connection ID
        let watch_connection_id = ConnectionId::new();
        Self {
            connection,
            executor,
            store: deps.store,
            acl: deps.acl,
            config: deps.config,
            replication_state: deps.replication_state,
            replication_primary: deps.replication_primary,
            audit_handle: deps.audit_handle,
            current_user: "default".to_string(),
            transaction: TransactionState::default(),
            shutdown_rx: deps.shutdown_rx,
            slowlog: deps.slowlog,
            client_registry: deps.client_registry,
            client_id: deps.client_id,
            watch_registry: deps.watch_registry,
            watch_connection_id,
        }
    }

    /// Create a new handler with shared configuration
    #[allow(dead_code)] // Planned for v0.2 — reserved for shared configuration support
    pub fn with_config(
        connection: Connection,
        mut deps: HandlerDependencies,
        config: SharedConfig,
    ) -> Self {
        deps.config = Some(config);
        Self::new(connection, deps)
    }

    /// Run the handler loop
    ///
    /// Uses batch writes for pipelined commands to improve throughput.
    /// When multiple commands are available in the read buffer, responses
    /// are buffered and flushed together, reducing syscall overhead.
    pub async fn run(mut self) -> Result<()> {
        loop {
            tokio::select! {
                result = self.connection.read_frame() => {
                    match result {
                        Ok(Some(frame)) => {
                            // Process this frame
                            let response = self.handle_frame(frame).await;

                            // Check if there are more pipelined commands waiting
                            if self.connection.has_pending_data() {
                                // Buffer this response and process more commands
                                self.connection.buffer_frame(&response);

                                // Process all buffered pipelined commands
                                loop {
                                    match self.connection.try_parse_buffered() {
                                        Ok(Some(next_frame)) => {
                                            let next_response = self.handle_frame(next_frame).await;
                                            self.connection.buffer_frame(&next_response);
                                        }
                                        Ok(None) => {
                                            // No more complete frames in buffer
                                            break;
                                        }
                                        Err(e) => {
                                            // Protocol error in buffered data
                                            let error_response = Frame::error(e.to_resp_error());
                                            self.connection.buffer_frame(&error_response);
                                            break;
                                        }
                                    }
                                }

                                // Flush all buffered responses at once
                                self.connection.flush_buffered().await?;
                            } else {
                                // No pipelining, write immediately
                                self.connection.write_frame(&response).await?;
                            }
                        }
                        Ok(None) => {
                            // Connection closed cleanly
                            debug!("Connection closed");
                            break;
                        }
                        Err(e) => {
                            if e.is_fatal() {
                                error!("Fatal error: {}", e);
                                break;
                            } else {
                                let response = Frame::error(e.to_resp_error());
                                self.connection.write_frame(&response).await?;
                            }
                        }
                    }
                }
                _ = self.shutdown_rx.recv() => {
                    debug!("Shutdown signal received, closing connection");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Handle a single frame
    async fn handle_frame(&mut self, frame: Frame) -> Frame {
        // Start timing for audit
        let start_time = Instant::now();

        // Parse the command from the frame
        let command = match Command::from_frame(frame) {
            Ok(cmd) => cmd,
            Err(e) => return Frame::error(e.to_resp_error()),
        };

        // Extract audit info (command name and keys)
        let (cmd_name, keys) = extract_audit_info(&command);
        let cmd_name_for_tracking = cmd_name.clone();
        let keys_for_otel = keys.clone();

        // Create audit entry
        let audit_entry = AuditEntry::new(
            self.connection.peer_addr,
            self.current_user.clone(),
            self.connection.database,
            cmd_name,
            keys,
            self.connection.protocol_version.version_number(),
        );

        // Handle AUTH command specially - update authenticated user
        if let Command::Auth { username, password } = &command {
            let result = self.handle_auth(username.as_deref(), password).await;
            self.log_audit_result(audit_entry, start_time, &result)
                .await;
            return result;
        }

        // Handle HELLO command specially - update protocol version
        if let Command::Hello {
            protocol_version,
            auth,
            client_name,
        } = &command
        {
            let result = self
                .handle_hello(*protocol_version, auth.clone(), client_name.clone())
                .await;
            self.log_audit_result(audit_entry, start_time, &result)
                .await;
            return result;
        }

        // Handle RESET command specially - resets connection state
        if matches!(command, Command::Reset) {
            let result = self.handle_reset();
            self.log_audit_result(audit_entry, start_time, &result)
                .await;
            return result;
        }

        // Handle transaction commands specially
        match &command {
            Command::Multi => {
                let result = self.handle_multi();
                self.log_audit_result(audit_entry, start_time, &result)
                    .await;
                return result;
            }
            Command::Exec => {
                let result = self.handle_exec().await;
                self.log_audit_result(audit_entry, start_time, &result)
                    .await;
                return result;
            }
            Command::Discard => {
                let result = self.handle_discard();
                self.log_audit_result(audit_entry, start_time, &result)
                    .await;
                return result;
            }
            Command::Watch { keys } => {
                let result = self.handle_watch(keys.clone());
                self.log_audit_result(audit_entry, start_time, &result)
                    .await;
                return result;
            }
            Command::Unwatch => {
                let result = self.handle_unwatch();
                self.log_audit_result(audit_entry, start_time, &result)
                    .await;
                return result;
            }
            _ => {}
        }

        // If in transaction, queue the command instead of executing
        if self.transaction.in_transaction {
            let result = self.queue_command(command);
            self.log_audit_result(audit_entry, start_time, &result)
                .await;
            return result;
        }

        // Handle SELECT command specially since it updates connection state
        if let Command::Select { db } = &command {
            if *db > 15 {
                let result = Frame::error("ERR invalid DB index");
                self.log_audit_result(audit_entry, start_time, &result)
                    .await;
                return result;
            }
            self.connection.database = *db;
            let result = Frame::simple("OK");
            self.log_audit_result(audit_entry, start_time, &result)
                .await;
            return result;
        }

        // Check if this is a write command and convert to replication command
        let repl_command = self.command_to_replication(&command);

        // Check if this command needs RESP3 response transformation
        let resp3_transform = self.get_resp3_transform(&command);

        // Create OTel span for this command and execute within it
        let first_key = keys_for_otel.first().map(|k| k.as_bytes());
        let otel_span =
            ferrite_core::telemetry::traces::command_span(&cmd_name_for_tracking, first_key);
        let result = self
            .executor
            .execute_with_acl(command, self.connection.database, &self.current_user)
            .instrument(otel_span)
            .await;

        // Log audit entry with result
        let elapsed = start_time.elapsed();
        self.log_audit_result(audit_entry, start_time, &result)
            .await;
        self.record_post_execution(elapsed, &cmd_name_for_tracking);

        // Record OTel command metrics
        let success = !matches!(result, Frame::Error(_));
        ferrite_core::telemetry::metrics::record_command(
            &cmd_name_for_tracking,
            elapsed.as_secs_f64(),
            success,
        );

        // If we're a primary and this was a write command that succeeded, propagate it
        if let Some(repl_cmd) = repl_command {
            if !matches!(result, Frame::Error(_))
                && self.replication_state.role().await == ReplicationRole::Primary
            {
                self.replication_primary.propagate(repl_cmd).await;
            }
        }

        // Apply RESP3 transformation if needed
        if self.connection.protocol_version.is_resp3() {
            self.transform_for_resp3(result, resp3_transform)
        } else {
            result
        }
    }

    /// Determine if a command needs RESP3 response transformation
    fn get_resp3_transform(&self, command: &Command) -> Resp3Transform {
        match command {
            // Hash commands that return field-value pairs as flat arrays
            Command::HGetAll { .. } => Resp3Transform::ArrayToMap,

            // Set commands that return set members
            Command::SMembers { .. }
            | Command::SInter { .. }
            | Command::SUnion { .. }
            | Command::SDiff { .. } => Resp3Transform::ArrayToSet,

            _ => Resp3Transform::None,
        }
    }

    /// Transform response frame for RESP3 protocol
    fn transform_for_resp3(&self, frame: Frame, transform: Resp3Transform) -> Frame {
        transform_for_resp3(frame, transform)
    }

    async fn log_audit_result(
        &self,
        mut audit_entry: AuditEntry,
        start_time: Instant,
        result: &Frame,
    ) {
        let success = !matches!(result, Frame::Error(_));
        let error = if let Frame::Error(e) = result {
            Some(String::from_utf8_lossy(e).into_owned())
        } else {
            None
        };
        audit_entry.complete(start_time.elapsed(), success, error);
        self.audit_handle.log(audit_entry).await;
    }

    fn record_post_execution(&self, elapsed: Duration, cmd_name: &str) {
        let client_addr = self
            .connection
            .peer_addr
            .map(|a| a.to_string())
            .unwrap_or_default();
        let client_name = self.connection.client_name.clone().unwrap_or_default();
        self.slowlog.record(
            elapsed,
            vec![Bytes::from(cmd_name.to_string())],
            client_addr,
            client_name,
        );

        self.client_registry.update(self.client_id, |client| {
            client.last_cmd = cmd_name.to_string();
            client.last_interaction = std::time::Instant::now();
            client.db = self.connection.database;
        });
    }
}

/// Transform response frame for RESP3 protocol (standalone function)
fn transform_for_resp3(frame: Frame, transform: Resp3Transform) -> Frame {
    match transform {
        Resp3Transform::None => frame,
        Resp3Transform::ArrayToMap => array_to_map(frame),
        Resp3Transform::ArrayToSet => array_to_set(frame),
    }
}

/// Convert a flat array [f1, v1, f2, v2, ...] to a Map
fn array_to_map(frame: Frame) -> Frame {
    match frame {
        Frame::Array(Some(arr)) => {
            let mut map = std::collections::HashMap::new();
            let mut iter = arr.into_iter();
            while let (Some(key_frame), Some(value_frame)) = (iter.next(), iter.next()) {
                let key = match key_frame {
                    Frame::Bulk(Some(b)) | Frame::Simple(b) => b,
                    _ => continue,
                };
                map.insert(key, value_frame);
            }
            Frame::Map(map)
        }
        // Return as-is if not an array (e.g., error)
        other => other,
    }
}

/// Convert an array to a Set
fn array_to_set(frame: Frame) -> Frame {
    match frame {
        Frame::Array(Some(arr)) => Frame::Set(arr),
        // Return as-is if not an array (e.g., error)
        other => other,
    }
}

/// Extract command name and keys for audit logging
fn extract_audit_info(command: &Command) -> (String, Vec<String>) {
    let meta = command.meta();
    let keys = meta
        .keys
        .iter()
        .map(|b| String::from_utf8_lossy(b).to_string())
        .collect();
    (meta.name.to_string(), keys)
}

impl Handler {
    fn lex_bound_bytes(bound: &crate::commands::sorted_sets::LexBound, is_min: bool) -> Bytes {
        match bound {
            crate::commands::sorted_sets::LexBound::Unbounded => {
                if is_min {
                    Bytes::from_static(b"-")
                } else {
                    Bytes::from_static(b"+")
                }
            }
            crate::commands::sorted_sets::LexBound::Inclusive(value) => {
                let mut buf = Vec::with_capacity(1 + value.len());
                buf.push(b'[');
                buf.extend_from_slice(value);
                Bytes::from(buf)
            }
            crate::commands::sorted_sets::LexBound::Exclusive(value) => {
                let mut buf = Vec::with_capacity(1 + value.len());
                buf.push(b'(');
                buf.extend_from_slice(value);
                Bytes::from(buf)
            }
        }
    }

    /// Convert a command to a replication command (if it's a write operation)
    fn command_to_replication(&self, command: &Command) -> Option<ReplicationCommand> {
        match command {
            Command::Set {
                key,
                value,
                options,
            } => {
                if let Some(ms) = options.expire_ms {
                    Some(ReplicationCommand::SetEx {
                        key: key.clone(),
                        value: value.clone(),
                        expire_ms: ms,
                    })
                } else {
                    Some(ReplicationCommand::Set {
                        key: key.clone(),
                        value: value.clone(),
                    })
                }
            }
            Command::SetNx { key, value } => Some(ReplicationCommand::SetNx {
                key: key.clone(),
                value: value.clone(),
            }),
            Command::SetEx {
                key,
                seconds,
                value,
            } => Some(ReplicationCommand::SetExSeconds {
                key: key.clone(),
                seconds: *seconds,
                value: value.clone(),
            }),
            Command::PSetEx {
                key,
                milliseconds,
                value,
            } => Some(ReplicationCommand::PSetEx {
                key: key.clone(),
                milliseconds: *milliseconds,
                value: value.clone(),
            }),
            Command::MSet { pairs } => Some(ReplicationCommand::MSet {
                pairs: pairs.clone(),
            }),
            Command::Append { key, value } => Some(ReplicationCommand::Append {
                key: key.clone(),
                value: value.clone(),
            }),
            Command::SetRange { key, offset, value } => Some(ReplicationCommand::SetRange {
                key: key.clone(),
                offset: *offset as usize,
                value: value.clone(),
            }),
            Command::Incr { key } => Some(ReplicationCommand::Incr { key: key.clone() }),
            Command::Decr { key } => Some(ReplicationCommand::Decr { key: key.clone() }),
            Command::IncrBy { key, delta } => Some(ReplicationCommand::IncrBy {
                key: key.clone(),
                delta: *delta,
            }),
            Command::DecrBy { key, delta } => Some(ReplicationCommand::DecrBy {
                key: key.clone(),
                delta: *delta,
            }),
            Command::IncrByFloat { key, delta } => Some(ReplicationCommand::IncrByFloat {
                key: key.clone(),
                delta: *delta,
            }),
            Command::Del { keys } => Some(ReplicationCommand::Del { keys: keys.clone() }),
            Command::Unlink { keys } => Some(ReplicationCommand::Unlink { keys: keys.clone() }),
            Command::Expire { key, seconds } => Some(ReplicationCommand::Expire {
                key: key.clone(),
                seconds: *seconds,
            }),
            Command::PExpire { key, milliseconds } => Some(ReplicationCommand::PExpire {
                key: key.clone(),
                milliseconds: *milliseconds,
            }),
            Command::ExpireAt { key, timestamp } => Some(ReplicationCommand::ExpireAt {
                key: key.clone(),
                timestamp: *timestamp,
            }),
            Command::PExpireAt { key, timestamp_ms } => Some(ReplicationCommand::PExpireAt {
                key: key.clone(),
                timestamp_ms: *timestamp_ms,
            }),
            Command::Persist { key } => Some(ReplicationCommand::Persist { key: key.clone() }),
            Command::Rename { key, new_key } => Some(ReplicationCommand::Rename {
                key: key.clone(),
                newkey: new_key.clone(),
            }),
            Command::LPush { key, values } => Some(ReplicationCommand::LPush {
                key: key.clone(),
                values: values.clone(),
            }),
            Command::RPush { key, values } => Some(ReplicationCommand::RPush {
                key: key.clone(),
                values: values.clone(),
            }),
            Command::LPushX { key, values } => {
                if values.len() == 1 {
                    values.first().map(|value| ReplicationCommand::LPushX {
                        key: key.clone(),
                        value: value.clone(),
                    })
                } else if values.is_empty() {
                    None
                } else {
                    let mut command_vec = Vec::with_capacity(values.len() + 2);
                    command_vec.push(Bytes::from_static(b"LPUSHX"));
                    command_vec.push(key.clone());
                    command_vec.extend(values.iter().cloned());
                    Some(ReplicationCommand::Raw {
                        command: command_vec,
                    })
                }
            }
            Command::RPushX { key, values } => {
                if values.len() == 1 {
                    values.first().map(|value| ReplicationCommand::RPushX {
                        key: key.clone(),
                        value: value.clone(),
                    })
                } else if values.is_empty() {
                    None
                } else {
                    let mut command_vec = Vec::with_capacity(values.len() + 2);
                    command_vec.push(Bytes::from_static(b"RPUSHX"));
                    command_vec.push(key.clone());
                    command_vec.extend(values.iter().cloned());
                    Some(ReplicationCommand::Raw {
                        command: command_vec,
                    })
                }
            }
            Command::LPop { key, count } => Some(ReplicationCommand::LPop {
                key: key.clone(),
                count: *count,
            }),
            Command::RPop { key, count } => Some(ReplicationCommand::RPop {
                key: key.clone(),
                count: *count,
            }),
            Command::LSet { key, index, value } => Some(ReplicationCommand::LSet {
                key: key.clone(),
                index: *index,
                value: value.clone(),
            }),
            Command::LTrim { key, start, stop } => Some(ReplicationCommand::LTrim {
                key: key.clone(),
                start: *start,
                stop: *stop,
            }),
            Command::LInsert {
                key,
                before,
                pivot,
                value,
            } => Some(ReplicationCommand::LInsert {
                key: key.clone(),
                before: *before,
                pivot: pivot.clone(),
                value: value.clone(),
            }),
            Command::LRem { key, count, value } => Some(ReplicationCommand::LRem {
                key: key.clone(),
                count: *count,
                value: value.clone(),
            }),
            Command::HSet { key, pairs } => Some(ReplicationCommand::HSet {
                key: key.clone(),
                pairs: pairs.clone(),
            }),
            Command::HSetNx { key, field, value } => Some(ReplicationCommand::HSetNx {
                key: key.clone(),
                field: field.clone(),
                value: value.clone(),
            }),
            Command::HDel { key, fields } => Some(ReplicationCommand::HDel {
                key: key.clone(),
                fields: fields.clone(),
            }),
            Command::HIncrBy { key, field, delta } => Some(ReplicationCommand::HIncrBy {
                key: key.clone(),
                field: field.clone(),
                delta: *delta,
            }),
            Command::HIncrByFloat { key, field, delta } => Some(ReplicationCommand::HIncrByFloat {
                key: key.clone(),
                field: field.clone(),
                delta: *delta,
            }),
            Command::SAdd { key, members } => Some(ReplicationCommand::SAdd {
                key: key.clone(),
                members: members.clone(),
            }),
            Command::SRem { key, members } => Some(ReplicationCommand::SRem {
                key: key.clone(),
                members: members.clone(),
            }),
            Command::SPop { key, count } => Some(ReplicationCommand::SPop {
                key: key.clone(),
                count: *count,
            }),
            Command::SMove {
                source,
                destination,
                member,
            } => Some(ReplicationCommand::SMove {
                source: source.clone(),
                dest: destination.clone(),
                member: member.clone(),
            }),
            Command::ZAdd { key, pairs, .. } => Some(ReplicationCommand::ZAdd {
                key: key.clone(),
                pairs: pairs.clone(),
            }),
            Command::ZRem { key, members } => Some(ReplicationCommand::ZRem {
                key: key.clone(),
                members: members.clone(),
            }),
            Command::ZIncrBy {
                key,
                increment,
                member,
            } => Some(ReplicationCommand::ZIncrBy {
                key: key.clone(),
                delta: *increment,
                member: member.clone(),
            }),
            Command::ZRemRangeByRank { key, start, stop } => {
                Some(ReplicationCommand::ZRemRangeByRank {
                    key: key.clone(),
                    start: *start,
                    stop: *stop,
                })
            }
            Command::ZRemRangeByScore { key, min, max } => {
                Some(ReplicationCommand::ZRemRangeByScore {
                    key: key.clone(),
                    min: *min,
                    max: *max,
                })
            }
            Command::ZRemRangeByLex { key, min, max } => Some(ReplicationCommand::ZRemRangeByLex {
                key: key.clone(),
                min: Self::lex_bound_bytes(min, true),
                max: Self::lex_bound_bytes(max, false),
            }),
            Command::XAdd {
                key, id, fields, ..
            } => Some(ReplicationCommand::XAdd {
                key: key.clone(),
                id: Bytes::from(id.clone().unwrap_or_else(|| "*".to_string())),
                fields: fields.clone(),
            }),
            Command::XDel { key, ids } => Some(ReplicationCommand::XDel {
                key: key.clone(),
                ids: ids.iter().map(|id| Bytes::from(id.clone())).collect(),
            }),
            Command::XTrim {
                key, maxlen, minid, ..
            } => {
                if let Some(maxlen) = maxlen {
                    Some(ReplicationCommand::XTrim {
                        key: key.clone(),
                        strategy: Bytes::from_static(b"MAXLEN"),
                        threshold: Bytes::from(maxlen.to_string()),
                    })
                } else {
                    minid.as_ref().map(|minid| ReplicationCommand::XTrim {
                        key: key.clone(),
                        strategy: Bytes::from_static(b"MINID"),
                        threshold: Bytes::from(minid.clone()),
                    })
                }
            }
            Command::PfAdd { key, elements } => Some(ReplicationCommand::PfAdd {
                key: key.clone(),
                elements: elements.clone(),
            }),
            Command::PfMerge {
                destkey,
                sourcekeys,
            } => Some(ReplicationCommand::PfMerge {
                dest: destkey.clone(),
                sources: sourcekeys.clone(),
            }),
            Command::FlushDb { .. } => Some(ReplicationCommand::FlushDb {
                db: self.connection.database,
            }),
            Command::FlushAll { .. } => Some(ReplicationCommand::FlushAll),
            Command::Select { db } => Some(ReplicationCommand::Select { db: *db }),
            // Other commands are not replicated (read commands, admin, etc.)
            _ => None,
        }
    }

    /// Handle AUTH command for user authentication
    async fn handle_auth(&mut self, username: Option<&str>, password: &str) -> Frame {
        let username = username.unwrap_or("default");

        // Attempt to authenticate
        match self.acl.authenticate(username, password).await {
            Ok(()) => {
                self.current_user = username.to_string();
                Frame::simple("OK")
            }
            Err(e) => Frame::error(format!("WRONGPASS {}", e)),
        }
    }

    /// Handle HELLO command for protocol version negotiation
    async fn handle_hello(
        &mut self,
        protocol_version: Option<u8>,
        auth: Option<(Bytes, Bytes)>,
        client_name: Option<Bytes>,
    ) -> Frame {
        use std::collections::HashMap;

        // Default to RESP2 if no version specified
        let proto = protocol_version.unwrap_or(2);

        // Validate and set protocol version
        let version = match ProtocolVersion::from_version(proto) {
            Some(v) => v,
            None => return Frame::error("NOPROTO unsupported protocol version"),
        };

        // Handle authentication if provided
        if let Some((username, password)) = auth {
            let username_str = String::from_utf8_lossy(&username);
            let password_str = String::from_utf8_lossy(&password);
            if let Err(e) = self.acl.authenticate(&username_str, &password_str).await {
                return Frame::error(format!("WRONGPASS {}", e));
            }
            self.current_user = username_str.to_string();
        }

        // Set protocol version on connection
        self.connection.set_protocol_version(version);

        // Set client name if provided
        if let Some(name) = client_name {
            self.connection
                .set_client_name(Some(String::from_utf8_lossy(&name).to_string()));
        }

        // Return server info as a Map (RESP3 style response)
        let mut info = HashMap::new();
        info.insert(Bytes::from_static(b"server"), Frame::bulk("ferrite"));
        info.insert(Bytes::from_static(b"version"), Frame::bulk("0.1.0"));
        info.insert(Bytes::from_static(b"proto"), Frame::Integer(proto as i64));
        info.insert(Bytes::from_static(b"id"), Frame::Integer(1)); // Connection ID placeholder
        info.insert(Bytes::from_static(b"mode"), Frame::bulk("standalone"));
        info.insert(Bytes::from_static(b"role"), Frame::bulk("master"));
        info.insert(Bytes::from_static(b"modules"), Frame::array(vec![]));

        Frame::Map(info)
    }

    /// Handle MULTI command
    fn handle_multi(&mut self) -> Frame {
        if self.transaction.in_transaction {
            return Frame::error("ERR MULTI calls can not be nested");
        }
        self.transaction.in_transaction = true;
        Frame::simple("OK")
    }

    /// Handle EXEC command
    async fn handle_exec(&mut self) -> Frame {
        if !self.transaction.in_transaction {
            return Frame::error("ERR EXEC without MULTI");
        }

        // Check if watched keys were modified (WATCH was aborted) using global registry
        if self.watch_registry.is_dirty(self.watch_connection_id) {
            self.watch_registry.unwatch_all(self.watch_connection_id);
            self.transaction.reset();
            return Frame::null();
        }

        // Execute all queued commands with ACL checking
        let commands = std::mem::take(&mut self.transaction.queue);
        let mut results = Vec::with_capacity(commands.len());
        let is_primary = self.replication_state.role().await == ReplicationRole::Primary;

        for cmd in commands {
            // Get replication command before execution
            let repl_cmd = self.command_to_replication(&cmd);

            let result = self
                .executor
                .execute_with_acl(cmd, self.connection.database, &self.current_user)
                .await;

            // Propagate successful write commands
            if let Some(repl) = repl_cmd {
                if !matches!(result, Frame::Error(_)) && is_primary {
                    self.replication_primary.propagate(repl).await;
                }
            }

            results.push(result);
        }

        // Clean up watch state
        self.watch_registry.unwatch_all(self.watch_connection_id);
        self.transaction.reset();
        Frame::array(results)
    }

    /// Handle DISCARD command
    fn handle_discard(&mut self) -> Frame {
        if !self.transaction.in_transaction {
            return Frame::error("ERR DISCARD without MULTI");
        }
        // Clean up watch state in global registry
        self.watch_registry.unwatch_all(self.watch_connection_id);
        self.transaction.reset();
        Frame::simple("OK")
    }

    /// Handle WATCH command
    fn handle_watch(&mut self, keys: Vec<Bytes>) -> Frame {
        if self.transaction.in_transaction {
            return Frame::error("ERR WATCH inside MULTI is not allowed");
        }
        // Register watches in global registry for cross-connection notification
        let db = self.connection.database;
        for key in keys.clone() {
            self.transaction.watched_keys.insert(key.clone());
            self.watch_registry.watch(self.watch_connection_id, db, key);
        }
        Frame::simple("OK")
    }

    /// Handle UNWATCH command
    fn handle_unwatch(&mut self) -> Frame {
        // Clear both local state and global registry
        self.watch_registry.unwatch_all(self.watch_connection_id);
        self.transaction.watched_keys.clear();
        self.transaction.watch_dirty = false;
        Frame::simple("OK")
    }

    /// Handle RESET command - resets connection to initial state
    ///
    /// Per Redis spec, RESET:
    /// - Discards any transaction state (MULTI)
    /// - Unwatches all keys
    /// - Exits pub/sub mode (TODO: when pub/sub state is tracked in handler)
    /// - Resets client name
    /// - Switches to database 0
    /// - Resets authentication to default user
    fn handle_reset(&mut self) -> Frame {
        // Clean up watch state in global registry
        self.watch_registry.unwatch_all(self.watch_connection_id);
        // Reset transaction state
        self.transaction.reset();

        // Reset to database 0
        self.connection.database = 0;

        // Clear client name
        self.connection.set_client_name(None);

        // Reset to default user
        self.current_user = "default".to_string();

        // Reset protocol version to RESP2 (default)
        self.connection.set_protocol_version(ProtocolVersion::Resp2);

        // Update client registry
        self.client_registry.update(self.client_id, |client| {
            client.db = 0;
            client.last_cmd = "RESET".to_string();
            client.last_interaction = std::time::Instant::now();
        });

        // Return RESET as per Redis spec
        Frame::simple("RESET")
    }

    /// Queue a command for later execution in EXEC
    fn queue_command(&mut self, command: Command) -> Frame {
        // Certain commands cannot be queued
        match &command {
            Command::Watch { .. } => {
                return Frame::error("ERR WATCH inside MULTI is not allowed");
            }
            Command::Unwatch => {
                return Frame::error("ERR UNWATCH inside MULTI is not allowed");
            }
            _ => {}
        }

        self.transaction.queue.push(command);
        Frame::simple("QUEUED")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_array_to_map_basic() {
        // HGETALL returns [field1, value1, field2, value2, ...]
        let arr = Frame::Array(Some(vec![
            Frame::bulk("field1"),
            Frame::bulk("value1"),
            Frame::bulk("field2"),
            Frame::bulk("value2"),
        ]));

        let result = array_to_map(arr);
        match result {
            Frame::Map(map) => {
                assert_eq!(map.len(), 2);
                assert!(map.contains_key(&Bytes::from_static(b"field1")));
                assert!(map.contains_key(&Bytes::from_static(b"field2")));
                match map.get(&Bytes::from_static(b"field1")) {
                    Some(Frame::Bulk(Some(v))) => assert_eq!(*v, Bytes::from("value1")),
                    _ => panic!("Expected bulk string value"),
                }
            }
            _ => panic!("Expected Map frame"),
        }
    }

    #[test]
    fn test_array_to_map_empty() {
        let arr = Frame::Array(Some(vec![]));
        let result = array_to_map(arr);
        match result {
            Frame::Map(map) => assert!(map.is_empty()),
            _ => panic!("Expected empty Map frame"),
        }
    }

    #[test]
    fn test_array_to_map_error_passthrough() {
        let err = Frame::error("ERR some error");
        let result = array_to_map(err);
        match result {
            Frame::Error(msg) => {
                let err_str = String::from_utf8_lossy(&msg);
                assert!(err_str.contains("some error"));
            }
            _ => panic!("Expected error to pass through"),
        }
    }

    #[test]
    fn test_array_to_set_basic() {
        // SMEMBERS returns [member1, member2, ...]
        let arr = Frame::Array(Some(vec![
            Frame::bulk("member1"),
            Frame::bulk("member2"),
            Frame::bulk("member3"),
        ]));

        let result = array_to_set(arr);
        match result {
            Frame::Set(members) => {
                assert_eq!(members.len(), 3);
            }
            _ => panic!("Expected Set frame"),
        }
    }

    #[test]
    fn test_array_to_set_empty() {
        let arr = Frame::Array(Some(vec![]));
        let result = array_to_set(arr);
        match result {
            Frame::Set(members) => assert!(members.is_empty()),
            _ => panic!("Expected empty Set frame"),
        }
    }

    #[test]
    fn test_array_to_set_error_passthrough() {
        let err = Frame::error("WRONGTYPE error");
        let result = array_to_set(err);
        match result {
            Frame::Error(msg) => {
                let err_str = String::from_utf8_lossy(&msg);
                assert!(err_str.contains("WRONGTYPE"));
            }
            _ => panic!("Expected error to pass through"),
        }
    }

    #[test]
    fn test_transform_for_resp3_none() {
        let frame = Frame::simple("OK");
        let result = transform_for_resp3(frame.clone(), Resp3Transform::None);
        match result {
            Frame::Simple(s) => assert_eq!(s, "OK"),
            _ => panic!("Expected simple string"),
        }
    }

    #[test]
    fn test_transform_for_resp3_array_to_map() {
        let arr = Frame::Array(Some(vec![Frame::bulk("key"), Frame::bulk("value")]));
        let result = transform_for_resp3(arr, Resp3Transform::ArrayToMap);
        match result {
            Frame::Map(map) => {
                assert_eq!(map.len(), 1);
                assert!(map.contains_key(&Bytes::from_static(b"key")));
            }
            _ => panic!("Expected Map frame"),
        }
    }

    #[test]
    fn test_transform_for_resp3_array_to_set() {
        let arr = Frame::Array(Some(vec![Frame::bulk("a"), Frame::bulk("b")]));
        let result = transform_for_resp3(arr, Resp3Transform::ArrayToSet);
        match result {
            Frame::Set(set) => assert_eq!(set.len(), 2),
            _ => panic!("Expected Set frame"),
        }
    }

    #[test]
    fn test_resp3_transform_enum() {
        assert_eq!(Resp3Transform::None, Resp3Transform::None);
        assert_ne!(Resp3Transform::ArrayToMap, Resp3Transform::ArrayToSet);
    }
}
