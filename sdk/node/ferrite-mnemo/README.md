# Ferrite Mnemo — Agent Memory SDK for Node.js / TypeScript

Persistent agent memory backed by Ferrite's `MEM.*` commands, with a
first-class LangChain.js adapter.

## Installation

```bash
npm install ferrite-mnemo
# or
yarn add ferrite-mnemo
```

## Quick Start

```ts
import { FerriteMemoryClient } from "ferrite-mnemo";

const client = new FerriteMemoryClient(); // localhost:6379

// Store a memory
const id = await client.put("agent-1", "session-1", "episodic", "User likes dark mode");

// Recall memories
const memories = await client.recall("agent-1", 5);

// GDPR forget
await client.forget("agent-1");

await client.disconnect();
```

## LangChain.js Adapter

```ts
import { FerriteLangChainMemory } from "ferrite-mnemo";

const memory = new FerriteLangChainMemory({ agentId: "my-agent" });

await memory.saveContext({ input: "Hello" }, { output: "Hi!" });
const vars = await memory.loadMemoryVariables({});
console.log(vars.history);

await memory.disconnect();
```

## Requirements

- Node.js ≥ 18
- Ferrite server running (drop-in Redis replacement)
- `ioredis` ≥ 5.4 (bundled)
- `langchain` ≥ 0.2 (optional peer dependency)
