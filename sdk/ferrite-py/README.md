# ferrite-py

Python SDK for [Ferrite](https://github.com/FerriteLabs/ferrite) — a Redis-compatible tiered-storage key-value store.

## Install

```bash
pip install ferrite-py
```

## Quick Start

```python
from ferrite import Ferrite, VectorStore

# Standard Redis commands work out of the box
client = Ferrite(host="localhost", port=6379)
client.set("hello", "world")
print(client.get("hello"))  # b"world"

# Create a search index
client.create_index(
    "movies", on="HASH", prefix="movie:",
    "title", "TEXT", "year", "NUMERIC",
)

# Search
results = client.search("movies", "@title:inception")

# Vector search
store = VectorStore(client)
store.create_index("embeddings", dimension=128, metric="COSINE")
store.add("embeddings", "doc:1", [0.1] * 128, {"title": "Example"})
results = store.search("embeddings", [0.1] * 128, k=5)

# Plugin management
client.plugin_list()
client.plugin_install("my-plugin")
```

## Optional: AI Integration

```bash
pip install ferrite-py[ai]
```

## Development

```bash
pip install -e ".[ai]"
python -m pytest tests/
```
