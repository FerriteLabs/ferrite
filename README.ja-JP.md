# Ferrite

[English](README.md) | [简体中文](README.zh-CN.md) | [日本語](README.ja-JP.md) | [한국어](README.ko-KR.md)

高性能な階層型ストレージを備えた Key-Value ストアです。Redis のドロップイン代替として設計されました。Rust で構築され、エポックベースの並行制御と io_uring ファーストの永続化を採用しています。

**メモリの速度、ディスクの容量、クラウドの経済性。**

[![Build Status](https://github.com/ferritelabs/ferrite/actions/workflows/ci.yml/badge.svg)](https://github.com/ferritelabs/ferrite/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/ferrite.svg)](https://crates.io/crates/ferrite)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.80%2B-orange)](https://www.rust-lang.org/)

## クイックスタート（60秒）

```bash
# ビルドして実行（macOS/Linux）— 設定ファイル不要
cargo build --release && ./target/release/ferrite

# または Docker Compose を使用
docker compose up -d

# 動作確認
redis-cli PING                    # 任意の Redis クライアントが使用可能
```

## 主な機能

| 機能 | 説明 |
|------|------|
| 🔄 **Redis 互換** | RESP2/RESP3 プロトコル、約72%のコマンドカバレッジ、全 Redis クライアント対応 |
| 💾 **階層型ストレージ** | 3層 HybridLog：メモリ → mmap → ディスク（io_uring） |
| 🧠 **AI ネイティブ** | ベクトル検索（HNSW/IVF）、セマンティックキャッシュ、RAG サポートを内蔵 |
| 📊 **マルチモデル** | ドキュメントストア、グラフDB、時系列、全文検索 |
| 🔌 **WASM プラグイン** | Rust/Go/AssemblyScript で拡張、サンドボックス実行 |
| 🔍 **FerriteQL** | SQL ライクなクエリ言語（オプティマイザ・実行計画付き） |
| 🏢 **マルチテナンシー** | テナント分離、リソースクォータ、監査ログをネイティブサポート |
| 📦 **組み込みモード** | Rust ライブラリとしてアプリケーションに組み込み可能 |

## アーキテクチャ

```
┌─────────────────────────────────────────────┐
│              RESP プロトコル層                 │
├─────────────────────────────────────────────┤
│   コマンドルーティング │ 認証/ACL │ Pub/Sub   │
├─────────────────────────────────────────────┤
│           HybridLog ストレージエンジン         │
│  ┌──────────┬──────────┬─────────────┐      │
│  │ 可変領域  │ 読取専用  │ ディスク     │      │
│  │ (メモリ)  │ (mmap)  │ (io_uring)  │      │
│  └──────────┴──────────┴─────────────┘      │
├─────────────────────────────────────────────┤
│ クラスタ │ レプリケーション │ 永続化 │ 監視    │
└─────────────────────────────────────────────┘
```

## 競合との比較

| 機能 | Ferrite | Redis | Dragonfly | Valkey |
|------|:---:|:---:|:---:|:---:|
| **言語** | Rust 🦀 | C | C++ | C |
| **ベクトル検索** | ✅ ネイティブ | ⚠️ モジュール | ❌ | ⚠️ モジュール |
| **階層型ストレージ** | ✅ 3層 | ❌ | ⚠️ SSD | ❌ |
| **WASM プラグイン** | ✅ | ❌ | ❌ | ❌ |
| **マルチモデル** | ✅ 6種類 | ⚠️ モジュール | ❌ | ⚠️ モジュール |
| **ライセンス** | Apache 2.0 | RSALv2 | BSL 1.1 | BSD-3 |

## インストール

```bash
# Homebrew
brew tap ferritelabs/ferrite && brew install ferrite

# またはインストールスクリプト
curl -fsSL https://raw.githubusercontent.com/ferritelabs/ferrite/main/scripts/install.sh | bash

# またはソースからビルド
cargo install ferrite
```

## ドキュメント

- 📖 [完全なドキュメント](https://ferrite.rs)
- 🏗️ [アーキテクチャ概要](docs/ARCHITECTURE.md)
- 🔄 [Redis 互換性](docs/REDIS_COMPAT.md)
- 🗺️ [ロードマップ](ROADMAP.md)
- 🤝 [コントリビューションガイド](CONTRIBUTING.md)

## コントリビューション

コントリビューションを歓迎します！詳しくは [CONTRIBUTING.md](CONTRIBUTING.md) をご覧ください。

## ライセンス

Apache License 2.0 — 詳細は [LICENSE](LICENSE) をご覧ください
