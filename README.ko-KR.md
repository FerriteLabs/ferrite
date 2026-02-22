# Ferrite

[English](README.md) | [简体中文](README.zh-CN.md) | [日本語](README.ja-JP.md) | [한국어](README.ko-KR.md)

고성능 계층형 스토리지 키-값 저장소로, Redis의 드롭인 대체를 목표로 설계되었습니다. Rust로 구축되었으며, 에포크 기반 동시성 제어와 io_uring 우선 영속화를 채택하고 있습니다.

**메모리의 속도, 디스크의 용량, 클라우드의 경제성.**

[![Build Status](https://github.com/ferritelabs/ferrite/actions/workflows/ci.yml/badge.svg)](https://github.com/ferritelabs/ferrite/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/ferrite.svg)](https://crates.io/crates/ferrite)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.80%2B-orange)](https://www.rust-lang.org/)

## 빠른 시작 (60초)

```bash
# 빌드 및 실행 (macOS/Linux) — 설정 파일 불필요
cargo build --release && ./target/release/ferrite

# 또는 Docker Compose 사용
docker compose up -d

# 확인
redis-cli PING                    # 모든 Redis 클라이언트 사용 가능
```

## 주요 기능

| 기능 | 설명 |
|------|------|
| 🔄 **Redis 호환** | RESP2/RESP3 프로토콜, ~72% 명령어 커버리지, 모든 Redis 클라이언트 지원 |
| 💾 **계층형 스토리지** | 3계층 HybridLog: 메모리 → mmap → 디스크 (io_uring) |
| 🧠 **AI 네이티브** | 벡터 검색(HNSW/IVF), 시맨틱 캐싱, RAG 지원 내장 |
| 📊 **멀티모델** | 문서 저장소, 그래프 DB, 시계열, 전문 검색 |
| 🔌 **WASM 플러그인** | Rust/Go/AssemblyScript로 확장, 샌드박스 실행 |
| 🔍 **FerriteQL** | SQL 유사 쿼리 언어 (옵티마이저 및 실행 계획 포함) |
| 🏢 **멀티테넌시** | 테넌트 격리, 리소스 쿼터, 감사 로그 네이티브 지원 |
| 📦 **임베디드 모드** | Rust 라이브러리로 애플리케이션에 임베드 가능 |

## 아키텍처

```
┌─────────────────────────────────────────────┐
│              RESP 프로토콜 계층                │
├─────────────────────────────────────────────┤
│   명령 라우팅  │  인증/ACL  │  Pub/Sub       │
├─────────────────────────────────────────────┤
│            HybridLog 스토리지 엔진            │
│  ┌──────────┬──────────┬─────────────┐      │
│  │ 가변 영역 │ 읽기전용  │ 디스크 영역  │      │
│  │ (메모리)  │ (mmap)   │ (io_uring)  │      │
│  └──────────┴──────────┴─────────────┘      │
├─────────────────────────────────────────────┤
│  클러스터 │ 복제 │ 영속화 │ Prometheus 모니터링 │
└─────────────────────────────────────────────┘
```

## 경쟁 제품 비교

| 기능 | Ferrite | Redis | Dragonfly | Valkey |
|------|:---:|:---:|:---:|:---:|
| **언어** | Rust 🦀 | C | C++ | C |
| **벡터 검색** | ✅ 네이티브 | ⚠️ 모듈 | ❌ | ⚠️ 모듈 |
| **계층형 스토리지** | ✅ 3계층 | ❌ | ⚠️ SSD | ❌ |
| **WASM 플러그인** | ✅ | ❌ | ❌ | ❌ |
| **멀티모델** | ✅ 6종류 | ⚠️ 모듈 | ❌ | ⚠️ 모듈 |
| **라이선스** | Apache 2.0 | RSALv2 | BSL 1.1 | BSD-3 |

## 설치

```bash
# Homebrew
brew tap ferritelabs/ferrite && brew install ferrite

# 또는 설치 스크립트
curl -fsSL https://raw.githubusercontent.com/ferritelabs/ferrite/main/scripts/install.sh | bash

# 또는 소스에서 빌드
cargo install ferrite
```

## 문서

- 📖 [전체 문서](https://ferrite.rs)
- 🏗️ [아키텍처 개요](docs/ARCHITECTURE.md)
- 🔄 [Redis 호환성](docs/REDIS_COMPAT.md)
- 🗺️ [로드맵](ROADMAP.md)
- 🤝 [기여 가이드](CONTRIBUTING.md)

## 기여

기여를 환영합니다! 자세한 내용은 [CONTRIBUTING.md](CONTRIBUTING.md)를 참조하세요.

## 라이선스

Apache License 2.0 — 자세한 내용은 [LICENSE](LICENSE)를 참조하세요
