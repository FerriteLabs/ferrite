/**
 * OpenAI SDK wrapper — transparently cache chat completions via Ferrite.
 *
 * @example
 * ```ts
 * import { cachedCompletion } from "@ferrite/ai";
 *
 * const response = await cachedCompletion({
 *   model: "gpt-4o-mini",
 *   messages: [{ role: "user", content: "What is Ferrite?" }],
 * });
 * ```
 */

import { FerriteClient, type FerriteClientOptions } from "./client";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface CachedCompletionOptions extends FerriteClientOptions {
  /** OpenAI model name (default `"gpt-4o-mini"`). */
  model?: string;
  /** Chat messages in the OpenAI format. */
  messages: Array<{ role: string; content: string }>;
  /** Sampling temperature (default `0`). */
  temperature?: number;
  /** Semantic similarity threshold (default `0.85`). */
  threshold?: number;
  /** Cache TTL in seconds (default `3600`). */
  ttl?: number;
  /** Pre-configured Ferrite client. */
  client?: FerriteClient;
  /** Additional options forwarded to `openai.chat.completions.create`. */
  openaiOptions?: Record<string, unknown>;
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

function extractQuery(
  messages: Array<{ role: string; content: string }>,
): string {
  const userParts = messages
    .filter((m) => m.role === "user")
    .map((m) => m.content);
  return userParts.length > 0 ? userParts.join(" ") : JSON.stringify(messages);
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Call `openai.chat.completions.create` with transparent semantic caching.
 *
 * On a cache hit the OpenAI API is **not** called — the cached response is
 * returned immediately, saving latency and cost.
 *
 * @returns The completion response with an extra `_cacheHit` boolean.
 */
export async function cachedCompletion(
  options: CachedCompletionOptions,
): Promise<Record<string, unknown> & { _cacheHit: boolean }> {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  let openai: typeof import("openai");
  try {
    openai = await import("openai");
  } catch {
    throw new Error(
      "The openai package is required. Install it with: npm install openai",
    );
  }

  const {
    model = "gpt-4o-mini",
    messages,
    temperature = 0,
    threshold = 0.85,
    ttl = 3600,
    client: providedClient,
    openaiOptions = {},
    ...clientOpts
  } = options;

  const fc =
    providedClient ??
    new FerriteClient({
      ...clientOpts,
      namespace: clientOpts.namespace ?? "openai_cache",
    });

  const queryText = extractQuery(messages);
  const cacheKey = `${model}:${queryText}`;

  // Try cache
  const cached = await fc.semanticGet(cacheKey, threshold);
  if (cached !== null) {
    try {
      const parsed = JSON.parse(cached.response) as Record<string, unknown>;
      return { ...parsed, _cacheHit: true };
    } catch {
      // malformed cache entry — fall through
    }
  }

  // Cache miss — call OpenAI
  const client = new openai.default();
  const response = await client.chat.completions.create({
    model,
    messages: messages as Parameters<
      typeof client.chat.completions.create
    >[0]["messages"],
    temperature,
    ...openaiOptions,
  });

  const responseObj = JSON.parse(JSON.stringify(response)) as Record<
    string,
    unknown
  >;

  // Store in cache
  await fc.semanticSet(cacheKey, JSON.stringify(responseObj), { model }, ttl);

  return { ...responseObj, _cacheHit: false };
}
