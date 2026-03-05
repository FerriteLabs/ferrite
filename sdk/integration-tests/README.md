# SDK Integration Tests

Run all Ferrite SDK tests against a live Ferrite server to verify cross-language compatibility.

## Quick Start

```bash
# Start Ferrite and run all SDK tests
./run-all.sh

# Or manually:
docker compose up -d --wait
./run-all.sh
docker compose down
```

## What It Tests

Each SDK is tested against a real Ferrite server instance to verify:
- Connection and authentication
- Core Redis commands (GET, SET, DEL, etc.)
- Ferrite-specific extensions (VECTOR.*, SEMANTIC.*, TS.*, DOC.*)
- Error handling and edge cases

## SDKs Tested

| SDK | Language | Test Command |
|-----|----------|-------------|
| ferrite-rs | Rust | `cargo test` |
| ferrite-py | Python | `pytest tests/` |
| python (client + AI) | Python | `pytest tests/` |
| nodejs | TypeScript | `npm test` |
| typescript (AI SDK) | TypeScript | `npm test` |
| go | Go | `go test ./...` |
| java | Java | `mvn test` |
| dotnet | C# | `dotnet test` |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `FERRITE_PORT` | `6399` | Port the test Ferrite instance listens on |

## Prerequisites

- Docker with Compose v2
- Language runtimes for SDKs you want to test (Rust, Python, Node.js, Go, Java, .NET)
- `redis-cli` for health checks and FLUSHALL between tests

SDKs whose runtime is not installed will be skipped automatically.

## How It Works

1. Starts Ferrite in Docker on port 6399 (if not already running)
2. Runs each SDK's test suite sequentially
3. Calls `FLUSHALL` between SDK test runs to reset state
4. Reports pass/fail/skip counts
5. Stops the Docker container on exit (if it started one)
