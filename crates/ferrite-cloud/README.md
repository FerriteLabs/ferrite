# Ferrite Cloud

Cloud storage integration and deployment modes for Ferrite.

Part of the [Ferrite](https://github.com/ferritelabs/ferrite) workspace.

## Modules

- `cloud` — Cloud storage integration (S3/GCS/Azure)
- `s3` — S3-compatible API layer
- `multicloud` — Multi-cloud abstraction layer
- `serverless` — Serverless deployment mode
- `edge` — Edge deployment mode
- `costoptimizer` — Storage cost optimization

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
ferrite-cloud = { git = "https://github.com/ferritelabs/ferrite" }
```

## License

Apache-2.0 — see [LICENSE](../../LICENSE) for details.
