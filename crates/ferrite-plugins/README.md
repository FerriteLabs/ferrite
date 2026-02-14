# Ferrite Plugins

Plugin system, WASM runtime, and conflict-free replicated data types for Ferrite.

Part of the [Ferrite](https://github.com/ferritelabs/ferrite) workspace.

## Modules

- `plugin` — Plugin system and lifecycle management
- `wasm` — WebAssembly user-defined functions
- `marketplace` — Plugin marketplace
- `crdt` — Conflict-free replicated data types (GCounter, PNCounter, OR-Set, LWW-Register)
- `adaptive` — ML-based auto-tuning

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
ferrite-plugins = { git = "https://github.com/ferritelabs/ferrite" }
```

## License

Apache-2.0 — see [LICENSE](../../LICENSE) for details.
