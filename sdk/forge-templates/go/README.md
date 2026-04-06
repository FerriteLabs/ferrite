# Ferrite Forge — Go Function Template

Write Ferrite Forge serverless functions in Go, compiled to WASM via TinyGo.

## Prerequisites

```bash
# Install TinyGo (>= 0.33 for WASI P2 support)
brew install tinygo  # macOS
# or see https://tinygo.org/getting-started/install/
```

## Create a New Function

```bash
ferrite-fn new my-function --lang go
```

## Project Structure

```
my-function/
├── go.mod      # Go module definition
└── main.go     # Implements the process() export
```

## Build

```bash
cd my-function
tinygo build -o function.wasm -target=wasip2 main.go
```

## Deploy

```bash
ferrite-cli fn load my-function function.wasm

# Or via the Redis protocol
redis-cli -p 6379 FN.LOAD my-function /path/to/function.wasm
```

## Host Imports

Inside your `process` function you can call host-provided imports once
WIT bindings are generated:

| Import          | Description                       |
|-----------------|-----------------------------------|
| `kv_get(key)`   | Read a key from the Ferrite store |
| `kv_set(k, v)`  | Write a key to the Ferrite store  |
| `time_now_ms()` | Current wall-clock time (ms)      |
| `log_info(msg)` | Emit an info-level log line       |

## Notes

- TinyGo's WASI P2 support is evolving; check release notes for updates.
- The `//export` directive tells TinyGo to expose the function to the host.
- Keep `func main() {}` — it is required by TinyGo but not called at runtime.
