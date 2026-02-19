/**
 * TypeScript type definitions for the Ferrite Node.js client SDK.
 *
 * @module types
 */

// ---------------------------------------------------------------------------
// Connection options
// ---------------------------------------------------------------------------

/** TLS configuration for secure connections. */
export interface TlsOptions {
  /** PEM-encoded CA certificate (or path). */
  ca?: string | Buffer;
  /** PEM-encoded client certificate (or path). */
  cert?: string | Buffer;
  /** PEM-encoded client private key (or path). */
  key?: string | Buffer;
  /** Reject connections to servers with invalid certificates. Default true. */
  rejectUnauthorized?: boolean;
}

/** Options accepted by the {@link FerriteClient} constructor. */
export interface FerriteClientOptions {
  /** Server hostname. Default `"localhost"`. */
  host?: string;
  /** Server port. Default `6379`. */
  port?: number;
  /** Authentication password. */
  password?: string;
  /** ACL username. Default `"default"`. */
  username?: string;
  /** Database index. Default `0`. */
  db?: number;
  /** TLS configuration. When set the connection uses TLS. */
  tls?: TlsOptions;
  /** Connection timeout in milliseconds. Default `5000`. */
  connectTimeout?: number;
  /** Command timeout in milliseconds. Default `0` (no timeout). */
  commandTimeout?: number;
  /** Maximum number of reconnection attempts. Default `10`. */
  maxRetriesPerRequest?: number | null;
  /**
   * Custom retry strategy.  Receives the attempt number and should return
   * the delay in ms before the next retry, or `null` to stop retrying.
   */
  retryStrategy?: (times: number) => number | null;
  /** Key prefix applied to every command. Default `""`. */
  keyPrefix?: string;
}

// ---------------------------------------------------------------------------
// Vector search
// ---------------------------------------------------------------------------

/** Options for {@link FerriteClient.vectorSearch}. */
export interface VectorSearchOptions {
  /** Maximum number of results to return. Default `10`. */
  topK?: number;
  /** Metadata filter expression. */
  filter?: string;
  /** Whether to include the raw vectors in the results. Default `false`. */
  includeVectors?: boolean;
}

/** A single result from a vector similarity search. */
export interface VectorSearchResult {
  /** The key / identifier of the matched vector. */
  id: string;
  /** Similarity score. */
  score: number;
  /** Optional metadata stored alongside the vector. */
  metadata?: Record<string, unknown>;
}

// ---------------------------------------------------------------------------
// Semantic search
// ---------------------------------------------------------------------------

/** A single result from a semantic search. */
export interface SemanticSearchResult {
  id: string;
  score: number;
  text?: string;
  metadata?: Record<string, unknown>;
}

// ---------------------------------------------------------------------------
// Time series
// ---------------------------------------------------------------------------

/** A single time-series data point. */
export interface TimeSeriesSample {
  /** Timestamp in milliseconds since Unix epoch. */
  timestamp: number;
  /** The sample value. */
  value: number;
}

/** Labels to attach to a time-series sample via {@link FerriteClient.tsAdd}. */
export interface TimeSeriesLabels {
  [key: string]: string;
}

// ---------------------------------------------------------------------------
// Document store
// ---------------------------------------------------------------------------

/** A single document returned from a document store query. */
export interface DocumentResult {
  id: string;
  document: Record<string, unknown>;
  score?: number;
}

// ---------------------------------------------------------------------------
// Graph
// ---------------------------------------------------------------------------

/** The result set from a graph query. */
export interface GraphResult {
  columns: string[];
  rows: unknown[][];
}

// ---------------------------------------------------------------------------
// FerriteQL
// ---------------------------------------------------------------------------

/** The result set from a FerriteQL query. */
export interface QueryResult {
  columns: string[];
  rows: unknown[][];
  count: number;
}

// ---------------------------------------------------------------------------
// Time travel
// ---------------------------------------------------------------------------

/** A historical version of a key's value. */
export interface TimeTravelEntry {
  /** Timestamp in milliseconds since Unix epoch. */
  timestamp: number;
  /** The value at that point in time. */
  value: string;
}
