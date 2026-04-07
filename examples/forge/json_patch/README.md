# json_patch — RFC 6902 JSON Patch

Applies RFC 6902 JSON Patch operations to a JSON document stored in Ferrite KV,
atomically within a single `FN.CALL` invocation.

## What it does

1. Reads the target JSON document from KV using the call key.
2. Parses the patch array from the input.
3. Applies each operation (`add`, `remove`, `replace`, `move`, `copy`, `test`)
   sequentially, aborting on failure.
4. Writes the patched document back to KV.
5. Returns the patched document.

## Build

```bash
cargo component build --release
```

## Deploy

```bash
redis-cli FN.LOAD json_patch $(cat target/wasm32-wasip2/release/ferrite_fn_json_patch.wasm | base64)
```

## Use

```bash
# Store an initial document
redis-cli SET doc:1 '{"name":"Alice","age":30}'

# Apply a patch
redis-cli FN.CALL json_patch doc:1 '[{"op":"replace","path":"/age","value":31},{"op":"add","path":"/city","value":"NYC"}]'
# => {"name":"Alice","age":31,"city":"NYC"}

# Atomic test-and-set
redis-cli FN.CALL json_patch doc:1 '[{"op":"test","path":"/age","value":31},{"op":"replace","path":"/age","value":32}]'
# => {"name":"Alice","age":32,"city":"NYC"}
```

### Input

JSON array of RFC 6902 patch operations.

### Output

The patched JSON document (full document after all operations applied).
