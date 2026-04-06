# Ferrite Forge — TypeScript Function Template

Write Ferrite Forge serverless functions in TypeScript, compiled to WASM
via [jco](https://github.com/nicolo-ribaudo/jco) componentize.

## Prerequisites

```bash
# Node.js >= 20
node --version

# Install dependencies
npm install
```

## Create a New Function

```bash
ferrite-fn new my-function --lang ts
```

## Project Structure

```
my-function/
├── package.json   # jco / componentize-js dependencies
├── index.ts       # Implements the process() export
└── function.wasm  # Built artifact (after npm run build)
```

## Build

```bash
cd my-function
npm install
npm run build
```

This compiles TypeScript → JavaScript → WASM component via `jco componentize`.

## Deploy

```bash
ferrite-cli fn load my-function function.wasm

# Or via the Redis protocol
redis-cli -p 6379 FN.LOAD my-function /path/to/function.wasm
```

## Host Imports

Inside your `process` function you can call host-provided imports:

| Import          | Description                       |
|-----------------|-----------------------------------|
| `kv.get(key)`   | Read a key from the Ferrite store |
| `kv.set(k, v)`  | Write a key to the Ferrite store  |
| `time.nowMs()`  | Current wall-clock time (ms)      |
| `log.info(msg)` | Emit an info-level log line       |

## Notes

- `jco componentize` converts a JS module into a WASI P2 component.
- The WIT world definition must match the one in `crates/ferrite-forge/wit`.
- You can test your function logic with any standard TS test runner
  (Jest, Vitest, etc.) before compiling to WASM.
