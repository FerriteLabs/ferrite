# hot_keys — Streaming Top-K Hot Key Detector

A streaming top-K hot key detector using the **Space-Saving** algorithm,
implemented as a Ferrite Forge WASM module.

## What it does

Maintains a fixed-size summary of the most frequently accessed keys. On each
call:

1. Loads the Space-Saving data structure from KV.
2. Records the observed key with `count` increments.
3. Persists the updated structure back to KV.
4. Returns the current top-K keys with estimated frequencies.

The Space-Saving algorithm guarantees that any key whose true frequency exceeds
`N/k` (where N is total observations, k is the number of counters) will appear
in the top-K list. It uses O(k) memory regardless of the number of distinct keys.

## Build

```bash
cargo component build --release
```

## Deploy

```bash
redis-cli FN.LOAD hot_keys $(cat target/wasm32-wasip2/release/ferrite_fn_hot_keys.wasm | base64)
```

## Use

```bash
# Record an observation (key = the observed key, input = config)
redis-cli FN.CALL hot_keys observed:user:42 '{"k":100,"count":1}'
redis-cli FN.CALL hot_keys observed:product:7 '{"k":100,"count":5}'

# Query the current top-K
redis-cli FN.CALL hot_keys __query '{"k":100,"top":10}'
# => {"top_keys":[{"key":"product:7","count":5,"error":0},{"key":"user:42","count":1,"error":0}]}
```

### Input JSON

| Field | Type | Description |
|-------|------|-------------|
| `k` | `u32` | Number of counters to maintain |
| `count` | `u64` | Increment amount (default: 1) |
| `top` | `u32` | (query mode) How many top keys to return |

### Output JSON

| Field | Type | Description |
|-------|------|-------------|
| `top_keys` | `array` | Sorted list of top keys |
| `top_keys[].key` | `string` | The key |
| `top_keys[].count` | `u64` | Estimated frequency |
| `top_keys[].error` | `u64` | Maximum overcount error bound |
| `total` | `u64` | Total observations recorded |
