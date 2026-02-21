#!/usr/bin/env bash
set -euo pipefail

echo "==> Installing system packages..."
sudo apt-get update
sudo apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    liburing-dev \
    redis-tools \
    git \
    curl \
    jq

sudo rm -rf /var/lib/apt/lists/*

echo "==> Installing Rust toolchain components..."
rustup component add clippy rustfmt llvm-tools-preview
rustup target add wasm32-wasi 2>/dev/null || true

echo "==> Installing cargo dev tools..."
cargo install cargo-watch --locked || true
cargo install cargo-deny --locked || true
cargo install cargo-audit --locked || true
cargo install cargo-chef --locked || true
cargo install cargo-tarpaulin --locked || true

echo "==> Fetching project dependencies..."
cargo fetch

echo "==> Dev container setup complete!"
