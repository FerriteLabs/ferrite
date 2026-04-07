# Agent Memory Examples

Runnable examples demonstrating Ferrite Mnemo — the agent memory SDK that
gives AI agents persistent, server-side memory via Ferrite's `MEM.*` commands.

## Prerequisites

```bash
# Start Ferrite (drop-in Redis replacement)
cargo run --release          # or: docker run -p 6379:6379 ferrite:latest

# Install the Python SDK with framework extras
pip install ferrite-py[mnemo-langgraph,mnemo-llamaindex,mnemo-letta]
```

## Examples

| Script | Framework | What it demonstrates |
|--------|-----------|---------------------|
| [`langgraph_chatbot.py`](langgraph_chatbot.py) | LangGraph | Multi-turn chatbot with persistent episodic memory |
| [`llamaindex_rag.py`](llamaindex_rag.py) | LlamaIndex | RAG agent storing & retrieving chat history |
| [`letta_passthrough.py`](letta_passthrough.py) | Letta (MemGPT) | Archival memory backend for Letta agents |

## Architecture

```
┌──────────────────┐     MEM.PUT / MEM.RECALL / MEM.FORGET
│  Your AI Agent   │ ──────────────────────────────────────►  ┌────────────┐
│  (LangGraph /    │                                          │  Ferrite   │
│   LlamaIndex /   │ ◄──────────────────────────────────────  │  Server    │
│   Letta)         │         Memory records (KV arrays)       │  :6379     │
└──────────────────┘                                          └────────────┘
        │
        ▼
  ferrite_mnemo.langgraph.FerriteLangGraphMemory
  ferrite_mnemo.llamaindex.FerriteLlamaIndexMemory
  ferrite_mnemo.letta.FerriteLettaMemory
```

## Quick Test

```bash
# Run any example (Ferrite must be running on localhost:6379)
python langgraph_chatbot.py
python llamaindex_rag.py
python letta_passthrough.py
```

## GDPR / Right to Forget

Every adapter exposes a `clear()` / `reset()` / `delete()` method that calls
`MEM.FORGET <agent_id>`, removing all memories for a given agent in a single
atomic operation.

## TypeScript / Node.js

See [`../../sdk/node/ferrite-mnemo/`](../../sdk/node/ferrite-mnemo/) for the
TypeScript adapter with LangChain.js support.
