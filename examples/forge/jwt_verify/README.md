# jwt_verify — JWT Verification with Cached JWKS

An RFC 7519 JWT verification module that caches JSON Web Key Sets (JWKS) in
Ferrite KV for fast repeated validations.

## What it does

1. Parses the incoming JWT (header + payload + signature).
2. Extracts the `kid` (Key ID) from the JWT header.
3. Looks up `__jwks:<issuer>` in KV for a cached JWKS document.
4. If the cache is missing or expired, returns an error prompting refresh.
5. Verifies the signature using the matching public key (RS256 / ES256).
6. Validates standard claims: `exp`, `nbf`, `iss`, `aud`.
7. Returns the decoded payload on success.

## Build

```bash
cargo component build --release
```

## Deploy

```bash
redis-cli FN.LOAD jwt_verify $(cat target/wasm32-wasip2/release/ferrite_fn_jwt_verify.wasm | base64)
```

## Use

```bash
# Pre-cache the JWKS for your issuer
redis-cli SET __jwks:https://auth.example.com '{"keys":[...]}'

# Verify a token
redis-cli FN.CALL jwt_verify session:abc123 '<base64-encoded-jwt>'
# => {"valid":true,"sub":"user@example.com","exp":1700000000}
```

### Input

Raw JWT bytes (compact serialization: `header.payload.signature`).

### Output JSON

| Field | Type | Description |
|-------|------|-------------|
| `valid` | `bool` | Whether the token is valid |
| `sub` | `string` | Subject claim |
| `exp` | `u64` | Expiration timestamp |
| `claims` | `object` | Full decoded payload |
