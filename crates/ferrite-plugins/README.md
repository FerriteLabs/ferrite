# Ferrite Plugins

Plugin system, WASM runtime, and conflict-free replicated data types for Ferrite.

Part of the [Ferrite](https://github.com/ferritelabs/ferrite) workspace.

## Features

- **Plugin lifecycle management** — Load, unload, enable, and disable plugins with event hooks for `BeforeCommand`, `AfterCommand`, `OnKeyWrite`, and custom events
- **WebAssembly runtime** — Sandboxed WASM execution with resource limits (memory, fuel, time), permission control, module pooling, and hot-reload support
- **CRDTs** — Conflict-free replicated data types (`GCounter`, `PNCounter`, `OrSet`, `OrMap`, `LwwRegister`, `MvRegister`) with hybrid-clock causality tracking for geo-replicated deployments
- **Plugin marketplace** — Discover, install, and manage plugins with dependency resolution, security scanning, and version tracking
- **Adaptive auto-tuning** — ML-based workload detection, load prediction, tier classification, and reinforcement-learning-driven configuration optimization
- **Type-safe SDK** — Builder API for plugin development with structured manifests, command contexts, and response types

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
ferrite-plugins = { git = "https://github.com/ferritelabs/ferrite" }
```

### Creating a plugin manifest and registering a command

```rust,ignore
use ferrite_plugins::plugin::{PluginManifest, PluginManager, PluginConfig, EventHook, EventType};
use ferrite_plugins::sdk::{PluginSdk, CommandContext, PluginResponse};

fn main() {
    // Define a plugin manifest
    let manifest = PluginManifest {
        name: "my-plugin".into(),
        version: "0.1.0".into(),
        description: "A custom Ferrite plugin".into(),
        ..Default::default()
    };

    // Initialize the plugin manager
    let config = PluginConfig::development();
    let mut manager = PluginManager::new(config);

    // Register an event hook
    let hook = EventHook::new(EventType::AfterCommand, |ctx| {
        println!("Command executed: {:?}", ctx.command_name());
        Ok(())
    });
    manager.register_hook(hook);
}
```

### Using CRDTs for distributed counters

```rust,ignore
use ferrite_plugins::crdt::{PNCounter, SiteId};

fn main() {
    let site_a = SiteId::new(1);
    let site_b = SiteId::new(2);

    let mut counter_a = PNCounter::new();
    let mut counter_b = PNCounter::new();

    // Increment on site A, decrement on site B
    counter_a.increment(site_a, 5);
    counter_b.decrement(site_b, 2);

    // Merge replicas — converges to 3
    counter_a.merge(&counter_b);
    assert_eq!(counter_a.value(), 3);
}
```

## Module Overview

| Module | Description |
|--------|-------------|
| `plugin` | `PluginManager`, `PluginConfig`, `PluginManifest`, `PluginRegistry`, `EventHook`, `EventType`, `CommandResult` — Plugin lifecycle, event hooks, and command registration |
| `wasm` | `FunctionRegistry`, `WasmExecutor`, `FunctionMetadata`, `FunctionPermissions`, `ResourceLimits`, `HotReloadConfig` — Sandboxed WASM execution with resource control |
| `crdt` | `PNCounter`, `GCounter`, `LwwRegister`, `MvRegister`, `OrSet`, `OrMap`, `HybridClock`, `SiteId` — Conflict-free replicated data types for eventual consistency |
| `marketplace` | `Marketplace`, `MarketplaceConfig`, `PluginMetadata`, `PluginType`, `SecurityStatus`, `InstalledPlugin` — Plugin discovery, installation, and security scanning |
| `adaptive` | `AdaptiveEngine`, `AdaptiveConfig`, `PatternDetector`, `LoadPredictor`, `TierClassifier`, `ConfigOptimizer`, `RLTieringAgent` — ML-based auto-tuning and workload optimization |
| `sdk` | `PluginSdk`, `PluginManifest`, `CommandContext`, `PluginResponse` — Type-safe plugin authoring API |
| `redis_module` | Redis module compatibility layer |

## License

Apache-2.0 — see [LICENSE](../../LICENSE) for details.
