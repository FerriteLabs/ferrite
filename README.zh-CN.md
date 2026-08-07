# Ferrite

[English](README.md) | [简体中文](README.zh-CN.md) | [日本語](README.ja-JP.md) | [한국어](README.ko-KR.md)

高性能分层存储键值数据库，专为替代 Redis 而设计。使用 Rust 构建，采用基于 epoch 的并发控制和 io_uring 优先的持久化方案。

**内存般的速度，磁盘级的容量，云端的经济性。**

[![Build Status](https://github.com/ferritelabs/ferrite/actions/workflows/ci.yml/badge.svg)](https://github.com/ferritelabs/ferrite/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/ferrite.svg)](https://crates.io/crates/ferrite)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.88%2B-orange)](https://www.rust-lang.org/)

## 快速开始（60 秒）

```bash
# 构建并运行（macOS/Linux）— 无需配置文件
cargo build --release && ./target/release/ferrite

# 或使用 Docker Compose
docker compose up -d

# 验证
redis-cli PING                    # 任何 Redis 客户端均可使用
```

## 核心特性

| 特性 | 描述 |
|------|------|
| 🔄 **Redis 兼容** | RESP2/RESP3 协议，~72% 命令覆盖率，支持所有 Redis 客户端 |
| 💾 **分层存储** | 三层 HybridLog：内存 → mmap → 磁盘（io_uring） |
| 🧠 **AI 原生** | 内置向量搜索（HNSW/IVF）、语义缓存、RAG 支持 |
| 📊 **多模型** | 文档存储、图数据库、时序数据、全文搜索 |
| 🔌 **WASM 插件** | 使用 Rust/Go/AssemblyScript 扩展，沙盒执行 |
| 🔍 **FerriteQL** | 类 SQL 查询语言，带优化器和执行计划 |
| 🏢 **多租户** | 原生支持租户隔离、资源配额、审计日志 |
| 📦 **嵌入模式** | 作为 Rust 库嵌入你的应用程序 |

## 架构

```
┌─────────────────────────────────────────────┐
│              RESP 协议层                      │
├─────────────────────────────────────────────┤
│   命令路由  │  认证/ACL  │  Pub/Sub         │
├─────────────────────────────────────────────┤
│              HybridLog 存储引擎              │
│  ┌──────────┬──────────┬─────────────┐      │
│  │ 可变区   │ 只读区    │ 磁盘区      │      │
│  │ (内存)   │ (mmap)   │ (io_uring)  │      │
│  └──────────┴──────────┴─────────────┘      │
├─────────────────────────────────────────────┤
│  集群 │ 复制 │ 持久化 │ Prometheus 监控      │
└─────────────────────────────────────────────┘
```

## 与竞品对比

| 特性 | Ferrite | Redis | Dragonfly | Valkey |
|------|:---:|:---:|:---:|:---:|
| **语言** | Rust 🦀 | C | C++ | C |
| **向量搜索** | ✅ 原生 | ⚠️ 模块 | ❌ | ⚠️ 模块 |
| **分层存储** | ✅ 三层 | ❌ | ⚠️ SSD | ❌ |
| **WASM 插件** | ✅ | ❌ | ❌ | ❌ |
| **多模型** | ✅ 6 种 | ⚠️ 模块 | ❌ | ⚠️ 模块 |
| **许可证** | Apache 2.0 | RSALv2 | BSL 1.1 | BSD-3 |

## 安装

```bash
# Homebrew
brew tap ferritelabs/ferrite && brew install ferrite

# 或安装脚本
curl -fsSL https://raw.githubusercontent.com/ferritelabs/ferrite/main/scripts/install.sh | bash

# 或从源码构建
cargo install ferrite
```

## 文档

- 📖 [完整文档](https://ferrite.rs)
- 🏗️ [架构概述](docs/ARCHITECTURE.md)
- 🔄 [Redis 兼容性](docs/REDIS_COMPAT.md)
- 🗺️ [发展路线图](ROADMAP.md)
- 🤝 [贡献指南](CONTRIBUTING.md)

## 贡献

欢迎贡献！请阅读 [CONTRIBUTING.md](CONTRIBUTING.md) 了解如何开始。

## 许可证

Apache License 2.0 — 详见 [LICENSE](LICENSE)
