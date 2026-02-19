/**
 * Core Ferrite AI client for semantic caching operations.
 *
 * Wraps Ferrite's SEMANTIC.SET / SEMANTIC.GET / SEMANTIC.DEL commands
 * and adds convenience helpers for cache statistics.
 */

import Redis from "ioredis";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface FerriteClientOptions {
  host?: string;
  port?: number;
  password?: string;
  db?: number;
  namespace?: string;
}

export interface SemanticResult {
  query: string;
  response: string;
  similarity: number;
  metadata?: Record<string, unknown>;
}

export interface SemanticStats {
  hitCount: number;
  missCount: number;
  entryCount: number;
  avgSimilarity: number;
  hitRate: number;
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/**
 * High-level client for Ferrite semantic caching.
 */
export class FerriteClient {
  private readonly redis: Redis;
  private readonly namespace: string;
  private hits = 0;
  private misses = 0;

  constructor(options: FerriteClientOptions = {}) {
    const {
      host = "127.0.0.1",
      port = 6380,
      password,
      db = 0,
      namespace = "sem",
    } = options;

    this.redis = new Redis({
      host,
      port,
      password,
      db,
      lazyConnect: true,
    });
    this.namespace = namespace;
  }

  /** Return the underlying ioredis instance. */
  get ioRedis(): Redis {
    return this.redis;
  }

  /** Explicitly connect (lazy by default). */
  async connect(): Promise<void> {
    await this.redis.connect();
  }

  /** Gracefully disconnect. */
  async disconnect(): Promise<void> {
    await this.redis.quit();
  }

  /** Check connectivity. */
  async ping(): Promise<string> {
    return this.redis.ping();
  }

  // -- semantic operations -------------------------------------------------

  /**
   * Store a query/response pair in the semantic cache.
   *
   * @param query - Natural-language key.
   * @param response - Cached LLM response.
   * @param metadata - Optional JSON metadata.
   * @param ttl - Time-to-live in seconds.
   */
  async semanticSet(
    query: string,
    response: string,
    metadata?: Record<string, unknown>,
    ttl?: number,
  ): Promise<void> {
    const args: string[] = [this.namespace, query, response];
    if (metadata !== undefined) {
      args.push("METADATA", JSON.stringify(metadata));
    }
    if (ttl !== undefined) {
      args.push("TTL", String(ttl));
    }
    await this.redis.call("SEMANTIC.SET", ...args);
  }

  /**
   * Look up a cached response by semantic similarity.
   *
   * @param query - The query to search for.
   * @param threshold - Minimum cosine similarity (0.0–1.0).
   * @returns The best matching entry, or `null` on a miss.
   */
  async semanticGet(
    query: string,
    threshold: number = 0.85,
  ): Promise<SemanticResult | null> {
    const raw = await this.redis.call(
      "SEMANTIC.GET",
      this.namespace,
      query,
      "THRESHOLD",
      String(threshold),
    );

    if (raw === null || (Array.isArray(raw) && raw.length === 0)) {
      this.misses++;
      return null;
    }

    this.hits++;
    return this.parseResult(raw);
  }

  /**
   * Delete a cached entry by its original query text.
   *
   * @returns Number of entries removed.
   */
  async semanticDelete(query: string): Promise<number> {
    const result = await this.redis.call(
      "SEMANTIC.DEL",
      this.namespace,
      query,
    );
    return typeof result === "number" ? result : Number(result) || 0;
  }

  /** Return cache hit/miss statistics and entry count. */
  async semanticStats(): Promise<SemanticStats> {
    let entryCount = 0;
    let avgSimilarity = 0;

    try {
      const raw = await this.redis.call("SEMANTIC.STATS", this.namespace);
      if (Array.isArray(raw)) {
        const info = this.pairsToMap(raw);
        entryCount = Number(info.get("entries")) || 0;
        avgSimilarity = Number(info.get("avg_similarity")) || 0;
      }
    } catch {
      // stats not available
    }

    const total = this.hits + this.misses;
    return {
      hitCount: this.hits,
      missCount: this.misses,
      entryCount,
      avgSimilarity,
      hitRate: total > 0 ? this.hits / total : 0,
    };
  }

  // -- helpers -------------------------------------------------------------

  private parseResult(raw: unknown): SemanticResult {
    if (Array.isArray(raw)) {
      const pairs = this.pairsToMap(raw);
      let metadata: Record<string, unknown> | undefined;
      const metaStr = pairs.get("metadata");
      if (metaStr) {
        try {
          metadata = JSON.parse(metaStr);
        } catch {
          metadata = { raw: metaStr };
        }
      }

      return {
        query: pairs.get("query") ?? "",
        response: pairs.get("response") ?? "",
        similarity: Number(pairs.get("similarity")) || 0,
        metadata,
      };
    }

    return { query: "", response: String(raw), similarity: 1.0 };
  }

  private pairsToMap(pairs: unknown[]): Map<string, string> {
    const map = new Map<string, string>();
    for (let i = 0; i < pairs.length; i += 2) {
      const k = String(pairs[i]);
      const v = String(pairs[i + 1]);
      map.set(k, v);
    }
    return map;
  }
}
