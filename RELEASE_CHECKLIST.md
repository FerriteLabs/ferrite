# Ferrite v0.2.0 Release Checklist

## Pre-Release

- [ ] All CI checks pass: `cargo fmt --all --check && cargo clippy --workspace -- -D warnings`
- [ ] All tests pass: `cargo test --workspace --lib` (expect 5,500+ passing)
- [ ] Integration tests pass: `cargo test --test integration_harness` (24 tests)
- [ ] No `cargo-deny` advisories: `cargo deny check`
- [ ] No `cargo-audit` vulnerabilities: `cargo audit`
- [ ] CHANGELOG.md updated with v0.2.0 section
- [ ] Version bumped in workspace `Cargo.toml`: `version = "0.2.0"`
- [ ] README quick-start instructions verified on fresh machine
- [ ] License headers present in all source files

## Build Artifacts

- [ ] Linux x86_64 binary: `cargo build --release --target x86_64-unknown-linux-gnu`
- [ ] Linux aarch64 binary: `cross build --release --target aarch64-unknown-linux-gnu`
- [ ] macOS x86_64 binary: `cargo build --release --target x86_64-apple-darwin`
- [ ] macOS aarch64 binary: `cargo build --release --target aarch64-apple-darwin`
- [ ] Docker image: `docker build -t ferritelabs/ferrite:0.2.0 -f ../ferrite-ops/Dockerfile .`
- [ ] Docker multi-arch: `docker buildx build --platform linux/amd64,linux/arm64`
- [ ] SHA-256 checksums generated for all binaries

## Publish

- [ ] `git tag v0.2.0 && git push origin v0.2.0`
- [ ] GitHub Release created with binaries + checksums + changelog
- [ ] Docker Hub: `docker push ferritelabs/ferrite:0.2.0 && docker push ferritelabs/ferrite:latest`
- [ ] crates.io: `cargo publish -p ferrite-core` (then each crate in dependency order)
- [ ] Homebrew formula updated: `homebrew-tap/ferrite.rb` → version 0.2.0

## Post-Release

- [ ] Verify `cargo install ferrite-server` works
- [ ] Verify `docker run ferritelabs/ferrite` works
- [ ] Verify `brew install ferritelabs/tap/ferrite` works
- [ ] Announce on GitHub Discussions
- [ ] Post to r/rust, r/redis, Hacker News
- [ ] Update ferrite-docs website with release notes
- [ ] Bump workspace version to 0.3.0-dev

## Crate Publish Order (respecting dependencies)

1. `ferrite-core` (no internal deps)
2. `ferrite-search` (no deps on core)
3. `ferrite-ai` (no deps on core)
4. `ferrite-graph` (no deps on core)
5. `ferrite-timeseries` (no deps on core)
6. `ferrite-document` (no deps on core)
7. `ferrite-streaming` (no deps on core)
8. `ferrite-cloud` (no deps on core)
9. `ferrite-plugins` (no deps on core)
10. `ferrite-k8s` (no deps on core)
11. `ferrite-enterprise` (no deps on core)
12. `ferrite-studio` (no deps on core)
13. `ferrite` (top-level, depends on all)
