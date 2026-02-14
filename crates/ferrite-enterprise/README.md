# Ferrite Enterprise

Enterprise features — multi-tenancy, governance, audit, and federation for Ferrite.

Part of the [Ferrite](https://github.com/ferritelabs/ferrite) workspace.

## Modules

- `tenancy` — Multi-tenant isolation and resource limits
- `governance` — Compliance and governance framework
- `policy` — Data governance policies
- `federation` — Multi-cluster federation
- `proxy` — Redis proxy mode

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
ferrite-enterprise = { git = "https://github.com/ferritelabs/ferrite" }
```

## License

Apache-2.0 — see [LICENSE](../../LICENSE) for details.
