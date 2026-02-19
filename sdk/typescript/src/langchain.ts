/**
 * LangChain.js integration — use Ferrite as a semantic cache for LLM calls.
 *
 * @example
 * ```ts
 * import { FerriteSemanticCache } from "@ferrite/ai";
 * import { ChatOpenAI } from "@langchain/openai";
 *
 * const cache = new FerriteSemanticCache({ similarityThreshold: 0.85 });
 * const llm = new ChatOpenAI({ model: "gpt-4o-mini", cache });
 * ```
 */

import { createHash } from "crypto";
import { FerriteClient, type FerriteClientOptions } from "./client";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface FerriteSemanticCacheOptions extends FerriteClientOptions {
  similarityThreshold?: number;
  ttl?: number;
  client?: FerriteClient;
}

/** Minimal Generation shape expected by LangChain caches. */
interface Generation {
  text: string;
  generationInfo?: Record<string, unknown>;
}

// ---------------------------------------------------------------------------
// Cache
// ---------------------------------------------------------------------------

/**
 * LangChain.js-compatible semantic cache backed by Ferrite.
 *
 * Implements the `lookup` / `update` / `clear` interface expected by
 * `@langchain/core` BaseCache.
 */
export class FerriteSemanticCache {
  private readonly client: FerriteClient;
  private readonly similarityThreshold: number;
  private readonly ttl?: number;

  constructor(options: FerriteSemanticCacheOptions = {}) {
    const { similarityThreshold = 0.85, ttl, client, ...clientOpts } = options;

    this.client =
      client ??
      new FerriteClient({
        ...clientOpts,
        namespace: clientOpts.namespace ?? "lc_cache",
      });
    this.similarityThreshold = similarityThreshold;
    this.ttl = ttl;
  }

  /**
   * Look up a cached LLM response for the given prompt.
   *
   * @returns An array of Generation objects on a hit, or `null`.
   */
  async lookup(
    prompt: string,
    llmString: string,
  ): Promise<Generation[] | null> {
    const cacheKey = this.makeKey(prompt, llmString);
    const result = await this.client.semanticGet(
      cacheKey,
      this.similarityThreshold,
    );
    if (result === null) {
      return null;
    }

    try {
      const data = JSON.parse(result.response) as Array<{
        text: string;
        generationInfo?: Record<string, unknown>;
      }>;
      return data.map((g) => ({
        text: g.text,
        generationInfo: g.generationInfo,
      }));
    } catch {
      return [{ text: result.response }];
    }
  }

  /**
   * Store an LLM response in the semantic cache.
   */
  async update(
    prompt: string,
    llmString: string,
    returnVal: Generation[],
  ): Promise<void> {
    const cacheKey = this.makeKey(prompt, llmString);
    const data = returnVal.map((g) => ({
      text: g.text,
      generationInfo: g.generationInfo,
    }));
    await this.client.semanticSet(
      cacheKey,
      JSON.stringify(data),
      { llm: llmString.slice(0, 128) },
      this.ttl,
    );
  }

  /**
   * Clear all cached entries (best-effort).
   */
  async clear(): Promise<void> {
    try {
      await this.client.ioRedis.call("SEMANTIC.FLUSH", "lc_cache");
    } catch {
      // flush not supported yet
    }
  }

  private makeKey(prompt: string, llmString: string): string {
    const tag = createHash("sha256")
      .update(llmString)
      .digest("hex")
      .slice(0, 8);
    return `${tag}:${prompt}`;
  }
}
