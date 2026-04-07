# rate_limit — Token Bucket Rate Limiter

A per-key token bucket rate limiter implemented as a Ferrite Forge WASM module.

## What it does

Each key gets an independent token bucket. On every call the module:

1. Reads the bucket state from KV (`tokens`, `last_refill_ms`).
2. Refills tokens based on elapsed time and the configured `refill_rate`.
3. If tokens ≥ 1, decrements and allows the request (`allowed: true`).
4. Persists the updated bucket back to KV with a TTL so idle buckets expire.

## Build

```bash
cargo component build --release
```

## Deploy

```bash
redis-cli FN.LOAD rate_limit $(cat target/wasm32-wasip2/release/ferrite_fn_rate_limit.wasm | base64)
```

## Use

```bash
# Allow up to 10 requests per second for user:42
redis-cli FN.CALL rate_limit user:42 '{"capacity":10,"refill_rate":1.0}'
# => {"allowed":true,"remaining":9}

redis-cli FN.CALL rate_limit user:42 '{"capacity":10,"refill_rate":1.0}'
# => {"allowed":true,"remaining":8}
```

### Input JSON

| Field | Type | Description |
|-------|------|-------------|
| `capacity` | `u64` | Maximum tokens in the bucket |
| `refill_rate` | `f64` | Tokens added per millisecond |

### Output JSON

| Field | Type | Description |
|-------|------|-------------|
| `allowed` | `bool` | Whether the request was allowed |
| `remaining` | `u64` | Tokens left after this call |
