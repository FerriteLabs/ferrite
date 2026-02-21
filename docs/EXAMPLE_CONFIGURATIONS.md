# Example Configurations

## Development (Minimal)

```toml
[server]
bind = "127.0.0.1"
port = 6379

[storage]
databases = 16

[persistence]
aof_enabled = false

[metrics]
enabled = true
bind = "127.0.0.1"
port = 9090
```

## Production (Recommended)

```toml
[server]
bind = "0.0.0.0"
port = 6379
max_connections = 10000
timeout = 300
tcp_keepalive = 300

[storage]
databases = 16
max_memory = 4294967296  # 4GB

[persistence]
aof_enabled = true
aof_sync = "everysec"
checkpoint_interval = 3600

[tls]
enabled = true
cert_file = "/etc/ferrite/tls/server.crt"
key_file = "/etc/ferrite/tls/server.key"

[auth]
enabled = true
require_pass = true

[metrics]
enabled = true
bind = "127.0.0.1"
port = 9090

[logging]
level = "info"
audit_enabled = true
```

## AI/ML Workloads

```toml
[server]
bind = "0.0.0.0"
port = 6379
max_connections = 5000

[storage]
databases = 4
max_memory = 8589934592  # 8GB — larger for vector indexes

[persistence]
aof_enabled = true
aof_sync = "everysec"
checkpoint_interval = 1800

[metrics]
enabled = true
bind = "127.0.0.1"
port = 9090
```

Build with AI features:
```bash
cargo build --release --features "onnx"
```

## Edge / Embedded

```toml
[server]
bind = "127.0.0.1"
port = 6379
max_connections = 100

[storage]
databases = 1
max_memory = 268435456  # 256MB

[persistence]
aof_enabled = true
aof_sync = "everysec"
checkpoint_interval = 600
```

## Semantic Caching for ChatGPT API

```toml
[server]
bind = "0.0.0.0"
port = 6379
max_connections = 5000

[storage]
databases = 4
max_memory = 4294967296  # 4GB

[semantic]
enabled = true

[semantic.cache]
max_entries = 100000
default_threshold = 0.85
default_ttl_secs = 3600

[semantic.index]
type = "hnsw"
dimensions = 1536          # OpenAI text-embedding-3-small output
metric = "cosine"

[semantic.embedding]
provider = "openai"
model = "text-embedding-3-small"
api_key = "${OPENAI_API_KEY}"

[persistence]
aof_enabled = true
aof_sync = "everysec"
checkpoint_interval = 1800

[metrics]
enabled = true
bind = "127.0.0.1"
port = 9090
```

## Semantic Caching for RAG Pipelines

```toml
[server]
bind = "0.0.0.0"
port = 6379
max_connections = 5000

[storage]
databases = 4
max_memory = 8589934592  # 8GB — larger for vector indexes + document chunks

[semantic]
enabled = true

[semantic.cache]
max_entries = 500000       # RAG caches tend to be larger
default_threshold = 0.90   # Stricter for retrieval accuracy
default_ttl_secs = 86400   # 24 hours — document context changes less often

[semantic.index]
type = "hnsw"
dimensions = 384           # Local ONNX model output
metric = "cosine"
m = 32                     # Higher M for better recall on large indexes
ef_construction = 400

[semantic.embedding]
provider = "local"         # ONNX — no API costs for embeddings
model = "all-MiniLM-L6-v2"

[persistence]
aof_enabled = true
aof_sync = "everysec"
checkpoint_interval = 1800

[metrics]
enabled = true
bind = "127.0.0.1"
port = 9090
```

Build with ONNX support:
```bash
cargo build --release --features "onnx"
```

## Multi-Model Semantic Caching

```toml
[server]
bind = "0.0.0.0"
port = 6379
max_connections = 10000

[storage]
databases = 8              # Separate DBs per model/use-case
max_memory = 8589934592    # 8GB

[semantic]
enabled = true

[semantic.cache]
max_entries = 200000
default_threshold = 0.85
default_ttl_secs = 7200    # 2 hours

[semantic.index]
type = "hnsw"
dimensions = 1536
metric = "cosine"

[semantic.embedding]
provider = "openai"
model = "text-embedding-3-small"
api_key = "${OPENAI_API_KEY}"

[persistence]
aof_enabled = true
aof_sync = "everysec"
checkpoint_interval = 3600

[tls]
enabled = true
cert_file = "/etc/ferrite/tls/server.crt"
key_file = "/etc/ferrite/tls/server.key"

[auth]
enabled = true
require_pass = true

[metrics]
enabled = true
bind = "127.0.0.1"
port = 9090

[logging]
level = "info"
audit_enabled = true
```

Use separate Redis databases to isolate caches per model:
```bash
# DB 0: GPT-4o responses
SELECT 0
SEMANTIC.CACHE.SET "Explain quantum computing" "..." EX 7200

# DB 1: Claude responses
SELECT 1
SEMANTIC.CACHE.SET "Explain quantum computing" "..." EX 7200

# DB 2: Embeddings-only (RAG retrieval cache)
SELECT 2
SEMANTIC.CACHE.SET "quantum computing papers" "..." EX 86400
```

## High Availability (Cluster)

```toml
[server]
bind = "0.0.0.0"
port = 7000

[cluster]
enabled = true
node_timeout = 5000

[storage]
databases = 1
max_memory = 4294967296

[persistence]
aof_enabled = true
aof_sync = "everysec"

[replication]
min_replicas_to_write = 1
min_replicas_max_lag = 10
```
