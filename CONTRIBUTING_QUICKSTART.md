# Contributing Quick Start: Adding a Redis Command

This guide walks through adding a new command to Ferrite in 5 steps using `GETDEL` as a concrete example. For full contribution guidelines, see [CONTRIBUTING.md](CONTRIBUTING.md).

## Prerequisites

```bash
make setup    # One-time environment check
make test-fast  # Verify everything works (~6s)
```

## Step 1: Add the Command variant (`src/commands/parser.rs`)

Find the appropriate category comment (e.g., `// String commands`) and add a variant to the `Command` enum:

```rust
// In the Command enum, under "// String commands":
/// GETDEL key
GetDel { key: Bytes },
```

## Step 2: Add the parser (`src/commands/parser.rs`)

Add a match arm in `Command::from_frame` and a parse function:

```rust
// In the from_frame match:
"GETDEL" => parse_getdel(args),

// Parse function (placed near other string parsers):
fn parse_getdel(args: &[Frame]) -> Result<Command> {
    if args.len() != 1 {
        return Err(FerriteError::WrongArity("GETDEL".to_string()));
    }
    Ok(Command::GetDel {
        key: get_bytes(&args[0])?,
    })
}
```

## Step 3: Add ACL metadata (`src/commands/executor.rs`)

Find the `meta()` method and add a match arm under the right category:

```rust
Command::GetDel { key } => CommandMeta {
    name: "GETDEL",
    category: "string",
    keys: vec![key.clone()],
    permission: Permission::Write,
},
```

## Step 4: Add execution (`src/commands/executor.rs`)

Find `execute_internal()` and add a match arm that delegates to your handler:

```rust
// Under "// String commands":
Command::GetDel { key } => strings::getdel(&self.store, db, &key),
```

## Step 5: Implement the handler (`src/commands/strings.rs`)

Add the actual logic in the appropriate handler module:

```rust
pub fn getdel(store: &Arc<Store>, db: u8, key: &Bytes) -> Frame {
    match store.get(db, key) {
        Some(Value::String(data)) => {
            store.del(db, &[key.clone()]);
            Frame::bulk(data)
        }
        _ => Frame::null(),
    }
}
```

## Verify

```bash
make test-fast     # Unit tests (~6s)
make test          # Full test suite
make lint          # Clippy + formatting
```

## File Summary

| File | What to add |
|------|------------|
| `src/commands/parser.rs` | `Command` variant + parse function |
| `src/commands/executor.rs` | ACL metadata match arm + execution match arm |
| `src/commands/{category}.rs` | Handler implementation |

## Tips

- Search for an existing similar command (e.g., `GETSET`) to see the full pattern
- The `parser.rs` and `executor.rs` files are large — use section comments like `// String commands` to navigate
- `Frame::bulk(data)` for bulk string responses, `Frame::null()` for nil, `Frame::error(msg)` for errors
- Run `make dev-test` for continuous test feedback while developing
