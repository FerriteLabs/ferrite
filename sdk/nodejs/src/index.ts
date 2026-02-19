/**
 * @ferrite/client -- Official Node.js / TypeScript client for Ferrite.
 *
 * Ferrite is a high-performance tiered-storage key-value store designed as a
 * drop-in Redis replacement.  This package wraps
 * [ioredis](https://github.com/redis/ioredis) and adds first-class support
 * for every Ferrite extension: vector search, semantic caching, time series,
 * document store, graph queries, FerriteQL, and time-travel queries.
 *
 * @example
 * ```ts
 * import { FerriteClient } from "@ferrite/client";
 *
 * const client = new FerriteClient({ host: "localhost", port: 6379 });
 *
 * await client.set("key", "value");
 * console.log(await client.get("key"));
 *
 * await client.disconnect();
 * ```
 *
 * @module
 */

import Redis from "ioredis";

import * as cmd from "./commands";
import type {
  DocumentResult,
  FerriteClientOptions,
  GraphResult,
  QueryResult,
  SemanticSearchResult,
  TimeSeriesLabels,
  TimeSeriesSample,
  TimeTravelEntry,
  TlsOptions,
  VectorSearchOptions,
  VectorSearchResult,
} from "./types";

// Re-export types for consumer convenience
export type {
  DocumentResult,
  FerriteClientOptions,
  GraphResult,
  QueryResult,
  SemanticSearchResult,
  TimeSeriesLabels,
  TimeSeriesSample,
  TimeTravelEntry,
  TlsOptions,
  VectorSearchOptions,
  VectorSearchResult,
};

// ---------------------------------------------------------------------------
// Custom errors
// ---------------------------------------------------------------------------

/** Base class for all Ferrite client errors. */
export class FerriteError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "FerriteError";
  }
}

/** Raised when the client cannot establish a connection. */
export class FerriteConnectionError extends FerriteError {
  constructor(message: string) {
    super(message);
    this.name = "FerriteConnectionError";
  }
}

/** Raised when a command returns a server-side error. */
export class FerriteCommandError extends FerriteError {
  constructor(message: string) {
    super(message);
    this.name = "FerriteCommandError";
  }
}

/** Raised when an operation exceeds its timeout. */
export class FerriteTimeoutError extends FerriteError {
  constructor(message: string) {
    super(message);
    this.name = "FerriteTimeoutError";
  }
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/**
 * Ferrite client for Node.js / TypeScript.
 *
 * Wraps an [ioredis](https://github.com/redis/ioredis) instance and adds
 * typed helpers for every Ferrite extension command.  All standard Redis
 * commands are available through the {@link ioRedis} property.
 */
export class FerriteClient {
  private readonly redis: Redis;

  /**
   * Create a new Ferrite client.
   *
   * @param options - Connection and behaviour options.
   */
  constructor(options: FerriteClientOptions = {}) {
    const {
      host = "localhost",
      port = 6379,
      password,
      username,
      db = 0,
      tls,
      connectTimeout = 5000,
      commandTimeout = 0,
      maxRetriesPerRequest = 3,
      retryStrategy,
      keyPrefix = "",
    } = options;

    const redisOptions: Record<string, unknown> = {
      host,
      port,
      password,
      username,
      db,
      connectTimeout,
      commandTimeout: commandTimeout || undefined,
      maxRetriesPerRequest,
      keyPrefix,
      lazyConnect: true,
      retryStrategy: retryStrategy ?? ((times: number) => Math.min(times * 50, 2000)),
    };

    if (tls) {
      redisOptions.tls = {
        ca: tls.ca,
        cert: tls.cert,
        key: tls.key,
        rejectUnauthorized: tls.rejectUnauthorized ?? true,
      };
    }

    this.redis = new Redis(redisOptions as never);
  }

  // -- lifecycle -----------------------------------------------------------

  /**
   * Return the underlying `ioredis` instance so callers can issue any
   * standard Redis command directly.
   */
  get ioRedis(): Redis {
    return this.redis;
  }

  /**
   * Explicitly open the connection.
   *
   * The client uses `lazyConnect` by default, so the first command will
   * connect automatically.  Call this method if you want to eagerly verify
   * connectivity on startup.
   */
  async connect(): Promise<void> {
    await this.redis.connect();
  }

  /**
   * Gracefully disconnect from the Ferrite server.
   */
  async disconnect(): Promise<void> {
    await this.redis.quit();
  }

  /**
   * Check that the server is reachable.
   */
  async ping(): Promise<string> {
    return this.redis.ping();
  }

  // -- standard Redis pass-through -----------------------------------------

  /** Set the value of a key. */
  async set(
    key: string,
    value: string | number,
    options?: { ex?: number; px?: number; nx?: boolean; xx?: boolean },
  ): Promise<string | null> {
    const args: (string | number)[] = [key, value];
    if (options?.ex !== undefined) {
      args.push("EX", options.ex);
    }
    if (options?.px !== undefined) {
      args.push("PX", options.px);
    }
    if (options?.nx) {
      args.push("NX");
    }
    if (options?.xx) {
      args.push("XX");
    }
    return this.redis.set(...(args as [string, string | number]));
  }

  /** Get the value of a key. */
  async get(key: string): Promise<string | null> {
    return this.redis.get(key);
  }

  /** Delete one or more keys. */
  async del(...keys: string[]): Promise<number> {
    return this.redis.del(...keys);
  }

  /** Check how many of the given keys exist. */
  async exists(...keys: string[]): Promise<number> {
    return this.redis.exists(...keys);
  }

  // -- vector search -------------------------------------------------------

  /**
   * Create a vector index.
   *
   * @param index - Name of the index.
   * @param dimensions - Vector dimensionality.
   * @param distance - Distance metric: `"cosine"`, `"l2"`, or `"ip"`.
   * @param indexType - Index algorithm: `"hnsw"`, `"ivf"`, or `"flat"`.
   */
  async vectorCreateIndex(
    index: string,
    dimensions: number,
    distance: string = "cosine",
    indexType: string = "hnsw",
  ): Promise<void> {
    await cmd.vectorCreateIndex(this.redis, index, dimensions, distance, indexType);
  }

  /**
   * Add a vector to an index with optional metadata.
   *
   * @param index - The index to add to.
   * @param id - Unique identifier for this vector.
   * @param vector - The vector data as an array of numbers.
   * @param metadata - Optional JSON metadata.
   */
  async vectorSet(
    index: string,
    id: string,
    vector: number[],
    metadata?: Record<string, unknown>,
  ): Promise<void> {
    await cmd.vectorSet(this.redis, index, id, vector, metadata);
  }

  /**
   * Perform a k-nearest-neighbor vector similarity search.
   *
   * @param index - The index to search.
   * @param queryVector - The query vector.
   * @param options - Search options (topK, filter, includeVectors).
   * @returns An array of search results sorted by similarity.
   */
  async vectorSearch(
    index: string,
    queryVector: number[],
    options: VectorSearchOptions = {},
  ): Promise<VectorSearchResult[]> {
    const { topK = 10, filter, includeVectors } = options;
    return cmd.vectorSearch(this.redis, index, queryVector, topK, filter, includeVectors);
  }

  /**
   * Shorthand for {@link vectorSearch} with only `topK`.
   */
  async vectorKnn(
    index: string,
    queryVector: number[],
    k: number = 10,
  ): Promise<VectorSearchResult[]> {
    return this.vectorSearch(index, queryVector, { topK: k });
  }

  // -- semantic caching ----------------------------------------------------

  /**
   * Store a value with a semantic (natural-language) key.
   *
   * @param text - The natural-language key.
   * @param value - The cached value.
   */
  async semanticSet(text: string, value: string): Promise<void> {
    await cmd.semanticSet(this.redis, text, value);
  }

  /**
   * Retrieve cached values whose semantic key is similar to `text`.
   *
   * @param text - The query text.
   * @param threshold - Minimum similarity score (0.0--1.0). Default `0.85`.
   */
  async semanticSearch(
    text: string,
    threshold: number = 0.85,
  ): Promise<SemanticSearchResult[]> {
    return cmd.semanticSearch(this.redis, text, threshold);
  }

  // -- time series ---------------------------------------------------------

  /**
   * Append a sample to a time-series key.
   *
   * @param key - The time-series key.
   * @param timestamp - Timestamp in ms or `"*"` for server auto-assign.
   * @param value - The sample value.
   * @param labels - Optional labels.
   * @returns The timestamp assigned by the server.
   */
  async tsAdd(
    key: string,
    timestamp: string | number = "*",
    value: number = 0,
    labels?: TimeSeriesLabels,
  ): Promise<number> {
    return cmd.tsAdd(this.redis, key, String(timestamp), value, labels);
  }

  /**
   * Query a range of samples.
   *
   * @param key - The time-series key.
   * @param from - Start (`"-"` for earliest).
   * @param to - End (`"+"` for latest).
   */
  async tsRange(
    key: string,
    from: string = "-",
    to: string = "+",
  ): Promise<TimeSeriesSample[]> {
    return cmd.tsRange(this.redis, key, from, to);
  }

  /**
   * Fetch the latest sample for a time-series key.
   */
  async tsGet(key: string): Promise<TimeSeriesSample | null> {
    return cmd.tsGet(this.redis, key);
  }

  // -- document store ------------------------------------------------------

  /**
   * Insert or replace a JSON document.
   *
   * @param collection - The collection name.
   * @param id - Unique document identifier.
   * @param document - The JSON document.
   */
  async docSet(
    collection: string,
    id: string,
    document: Record<string, unknown>,
  ): Promise<void> {
    await cmd.docSet(this.redis, collection, id, document);
  }

  /**
   * Search documents in a collection.
   *
   * @param collection - The collection name.
   * @param query - A JSON query object.
   */
  async docSearch(
    collection: string,
    query: Record<string, unknown>,
  ): Promise<DocumentResult[]> {
    return cmd.docSearch(this.redis, collection, query);
  }

  // -- graph ---------------------------------------------------------------

  /**
   * Execute a Cypher-like graph query.
   *
   * @param graph - The graph name.
   * @param cypher - The query string.
   */
  async graphQuery(graph: string, cypher: string): Promise<GraphResult> {
    return cmd.graphQuery(this.redis, graph, cypher);
  }

  // -- FerriteQL -----------------------------------------------------------

  /**
   * Execute a FerriteQL query.
   *
   * @param ferriteQl - The query string, e.g.
   *   `"FROM users:* WHERE $.active = true SELECT $.name LIMIT 10"`.
   */
  async query(ferriteQl: string): Promise<QueryResult> {
    return cmd.ferriteQuery(this.redis, ferriteQl);
  }

  // -- time travel ---------------------------------------------------------

  /**
   * Read a key's value at a past point in time.
   *
   * @param key - The key name.
   * @param asOf - A human-friendly offset such as `"-1h"` or a Unix timestamp
   *   in milliseconds.
   */
  async timeTravel(key: string, asOf: string): Promise<string | null> {
    return cmd.timeTravel(this.redis, key, asOf);
  }

  /**
   * Retrieve the mutation history of a key.
   *
   * @param key - The key name.
   * @param since - How far back to look, e.g. `"-24h"`.
   */
  async history(key: string, since: string): Promise<TimeTravelEntry[]> {
    return cmd.history(this.redis, key, since);
  }

  // -- raw command ---------------------------------------------------------

  /**
   * Execute an arbitrary raw command.
   *
   * Useful for Ferrite commands not yet covered by typed helpers.
   *
   * @param args - Command name followed by arguments.
   * @returns The raw Redis response.
   */
  async executeCommand(...args: string[]): Promise<unknown> {
    return this.redis.call(args[0], ...args.slice(1));
  }
}
