/**
 * @ferrite/ai — Semantic caching SDKs for LLM frameworks.
 *
 * @example
 * ```ts
 * import { FerriteClient } from "@ferrite/ai";
 *
 * const client = new FerriteClient({ host: "127.0.0.1", port: 6380 });
 * await client.semanticSet("What is Ferrite?", "A high-performance KV store.");
 * const result = await client.semanticGet("Tell me about Ferrite");
 * ```
 *
 * @module
 */

export { FerriteClient } from "./client";
export type {
  FerriteClientOptions,
  SemanticResult,
  SemanticStats,
} from "./client";

export { FerriteSemanticCache } from "./langchain";
export type { FerriteSemanticCacheOptions } from "./langchain";

export { cachedCompletion } from "./openai";
export type { CachedCompletionOptions } from "./openai";
