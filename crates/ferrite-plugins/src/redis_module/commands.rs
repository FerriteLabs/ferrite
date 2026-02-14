//! Bridge between Redis module commands and Ferrite's command executor
//!
//! Translates module command callbacks into entries that can be registered
//! in Ferrite's command table and dispatched through the async command model.


use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use parking_lot::RwLock;

use super::api::{RedisModuleCallReply, RedisModuleCtx, RedisModuleString, Status};

/// Command flags mirroring Redis module command flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandFlag {
    /// Command may modify the data set
    Write,
    /// Command only reads data
    ReadOnly,
    /// Command is for administrator use
    Admin,
    /// Command may block the client
    DenyOom,
    /// Command should not appear in MONITOR output
    NoMonitor,
    /// Command is fast (O(1) or O(log N))
    Fast,
    /// Command may load data from disk
    Loading,
    /// Command may run while server is loading
    AllowLoading,
    /// Command operates on pub/sub channels
    PubSub,
}

impl fmt::Display for CommandFlag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommandFlag::Write => write!(f, "write"),
            CommandFlag::ReadOnly => write!(f, "readonly"),
            CommandFlag::Admin => write!(f, "admin"),
            CommandFlag::DenyOom => write!(f, "deny-oom"),
            CommandFlag::NoMonitor => write!(f, "no-monitor"),
            CommandFlag::Fast => write!(f, "fast"),
            CommandFlag::Loading => write!(f, "loading"),
            CommandFlag::AllowLoading => write!(f, "allow-loading"),
            CommandFlag::PubSub => write!(f, "pubsub"),
        }
    }
}

/// Signature for a module command callback (`RedisModuleCmdFunc`).
///
/// The callback receives the module context and the argument list
/// and must return [`Status::Ok`] or [`Status::Err`].
pub type ModuleCommandFn =
    Arc<dyn Fn(&mut RedisModuleCtx, &[RedisModuleString]) -> Status + Send + Sync>;

/// Metadata for a single module command registered via `RedisModule_CreateCommand`.
#[derive(Clone)]
pub struct ModuleCommand {
    /// Command name (e.g. `"MYMOD.SET"`)
    pub name: String,
    /// Callback function
    pub callback: ModuleCommandFn,
    /// Command flags
    pub flags: Vec<CommandFlag>,
    /// First key argument position (1-indexed, 0 = no keys)
    pub first_key: i32,
    /// Last key argument position (negative = relative to end)
    pub last_key: i32,
    /// Step between key arguments
    pub key_step: i32,
}

impl ModuleCommand {
    /// Create a new module command.
    pub fn new<F>(name: &str, callback: F, flags: Vec<CommandFlag>) -> Self
    where
        F: Fn(&mut RedisModuleCtx, &[RedisModuleString]) -> Status + Send + Sync + 'static,
    {
        Self {
            name: name.to_uppercase(),
            callback: Arc::new(callback),
            flags,
            first_key: 1,
            last_key: 1,
            key_step: 1,
        }
    }

    /// Set key specification (first, last, step) for the command.
    pub fn with_keys(mut self, first: i32, last: i32, step: i32) -> Self {
        self.first_key = first;
        self.last_key = last;
        self.key_step = step;
        self
    }

    /// Execute the command callback.
    pub fn execute(
        &self,
        ctx: &mut RedisModuleCtx,
        args: &[RedisModuleString],
    ) -> Status {
        (self.callback)(ctx, args)
    }

    /// Whether the command is write-capable.
    pub fn is_write(&self) -> bool {
        self.flags.contains(&CommandFlag::Write)
    }

    /// Whether the command is read-only.
    pub fn is_readonly(&self) -> bool {
        self.flags.contains(&CommandFlag::ReadOnly)
    }
}

impl fmt::Debug for ModuleCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModuleCommand")
            .field("name", &self.name)
            .field("flags", &self.flags)
            .field("first_key", &self.first_key)
            .field("last_key", &self.last_key)
            .field("key_step", &self.key_step)
            .finish()
    }
}

/// Registry of module commands that bridges into Ferrite's command table.
///
/// Module authors register commands via `RedisModule_CreateCommand`; those
/// entries are stored here and can later be looked up during command dispatch.
pub struct ModuleCommandRegistry {
    /// Registered commands keyed by uppercase name
    commands: RwLock<HashMap<String, ModuleCommand>>,
}

impl ModuleCommandRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            commands: RwLock::new(HashMap::new()),
        }
    }

    /// Register a command (`RedisModule_CreateCommand`).
    pub fn register(&self, command: ModuleCommand) -> Status {
        let mut commands = self.commands.write();
        if commands.contains_key(&command.name) {
            return Status::Err;
        }
        commands.insert(command.name.clone(), command);
        Status::Ok
    }

    /// Unregister a command by name.
    pub fn unregister(&self, name: &str) -> Status {
        let mut commands = self.commands.write();
        if commands.remove(&name.to_uppercase()).is_some() {
            Status::Ok
        } else {
            Status::Err
        }
    }

    /// Look up a command by name.
    pub fn get(&self, name: &str) -> Option<ModuleCommand> {
        self.commands.read().get(&name.to_uppercase()).cloned()
    }

    /// Execute a registered module command.
    pub fn execute(
        &self,
        name: &str,
        ctx: &mut RedisModuleCtx,
        args: &[RedisModuleString],
    ) -> Result<Status, ModuleCommandError> {
        let cmd = self
            .get(name)
            .ok_or_else(|| ModuleCommandError::NotFound(name.to_string()))?;
        Ok(cmd.execute(ctx, args))
    }

    /// Return the number of registered commands.
    pub fn len(&self) -> usize {
        self.commands.read().len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.commands.read().is_empty()
    }

    /// List all registered command names.
    pub fn command_names(&self) -> Vec<String> {
        self.commands.read().keys().cloned().collect()
    }

    /// Remove all commands belonging to a specific module.
    pub fn unregister_module(&self, module_name: &str) {
        let prefix = format!("{}.", module_name.to_uppercase());
        self.commands
            .write()
            .retain(|name, _| !name.starts_with(&prefix));
    }
}

impl Default for ModuleCommandRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Errors produced by the module command registry.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ModuleCommandError {
    /// Command not found in the registry
    #[error("module command not found: {0}")]
    NotFound(String),
    /// Command execution failed
    #[error("module command execution failed: {0}")]
    ExecutionFailed(String),
}

/// Convert a `RedisModuleCallReply` to a basic string for debugging.
pub fn call_reply_to_string(reply: &RedisModuleCallReply) -> String {
    match reply {
        RedisModuleCallReply::String(s) => s.clone(),
        RedisModuleCallReply::Error(e) => format!("ERR {}", e),
        RedisModuleCallReply::Integer(i) => i.to_string(),
        RedisModuleCallReply::Nil => "(nil)".to_string(),
        RedisModuleCallReply::Array(arr) => {
            let items: Vec<String> = arr.iter().map(call_reply_to_string).collect();
            format!("[{}]", items.join(", "))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_execute() {
        let registry = ModuleCommandRegistry::new();

        let cmd = ModuleCommand::new(
            "MYMOD.PING",
            |ctx, _args| {
                ctx.reply_with_simple_string("PONG");
                Status::Ok
            },
            vec![CommandFlag::ReadOnly, CommandFlag::Fast],
        );
        assert_eq!(registry.register(cmd), Status::Ok);
        assert_eq!(registry.len(), 1);

        let mut ctx = RedisModuleCtx::new(0, 1);
        let result = registry.execute("MYMOD.PING", &mut ctx, &[]);
        assert_eq!(result.unwrap(), Status::Ok);

        let replies = ctx.take_replies();
        assert_eq!(replies.len(), 1);
    }

    #[test]
    fn test_duplicate_registration() {
        let registry = ModuleCommandRegistry::new();

        let cmd1 = ModuleCommand::new("CMD", |_, _| Status::Ok, vec![]);
        let cmd2 = ModuleCommand::new("CMD", |_, _| Status::Ok, vec![]);

        assert_eq!(registry.register(cmd1), Status::Ok);
        assert_eq!(registry.register(cmd2), Status::Err);
    }

    #[test]
    fn test_unregister() {
        let registry = ModuleCommandRegistry::new();
        let cmd = ModuleCommand::new("TEST.CMD", |_, _| Status::Ok, vec![]);
        registry.register(cmd);

        assert_eq!(registry.unregister("TEST.CMD"), Status::Ok);
        assert!(registry.is_empty());
        assert_eq!(registry.unregister("TEST.CMD"), Status::Err);
    }

    #[test]
    fn test_unregister_module() {
        let registry = ModuleCommandRegistry::new();
        registry.register(ModuleCommand::new("MOD.A", |_, _| Status::Ok, vec![]));
        registry.register(ModuleCommand::new("MOD.B", |_, _| Status::Ok, vec![]));
        registry.register(ModuleCommand::new("OTHER.X", |_, _| Status::Ok, vec![]));

        registry.unregister_module("MOD");
        assert_eq!(registry.len(), 1);
        assert!(registry.get("OTHER.X").is_some());
    }

    #[test]
    fn test_command_flags() {
        let cmd = ModuleCommand::new(
            "TEST",
            |_, _| Status::Ok,
            vec![CommandFlag::Write, CommandFlag::DenyOom],
        );
        assert!(cmd.is_write());
        assert!(!cmd.is_readonly());
    }

    #[test]
    fn test_command_with_keys() {
        let cmd = ModuleCommand::new("TEST", |_, _| Status::Ok, vec![])
            .with_keys(1, -1, 2);
        assert_eq!(cmd.first_key, 1);
        assert_eq!(cmd.last_key, -1);
        assert_eq!(cmd.key_step, 2);
    }

    #[test]
    fn test_execute_not_found() {
        let registry = ModuleCommandRegistry::new();
        let mut ctx = RedisModuleCtx::new(0, 1);
        let result = registry.execute("NONEXISTENT", &mut ctx, &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_command_names() {
        let registry = ModuleCommandRegistry::new();
        registry.register(ModuleCommand::new("A.CMD", |_, _| Status::Ok, vec![]));
        registry.register(ModuleCommand::new("B.CMD", |_, _| Status::Ok, vec![]));

        let mut names = registry.command_names();
        names.sort();
        assert_eq!(names, vec!["A.CMD", "B.CMD"]);
    }

    #[test]
    fn test_call_reply_to_string() {
        assert_eq!(
            call_reply_to_string(&RedisModuleCallReply::String("OK".into())),
            "OK"
        );
        assert_eq!(
            call_reply_to_string(&RedisModuleCallReply::Integer(42)),
            "42"
        );
        assert_eq!(
            call_reply_to_string(&RedisModuleCallReply::Nil),
            "(nil)"
        );
    }
}
