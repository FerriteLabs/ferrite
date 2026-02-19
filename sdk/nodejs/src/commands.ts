/**
 * Ferrite extension command implementations.
 *
 * These helpers build raw Redis commands and parse the responses into the
 * typed structures defined in {@link types}.
 *
 * @module commands
 */

import type Redis from "ioredis";
import type {
  DocumentResult,
  GraphResult,
  QueryResult,
  SemanticSearchResult,
  TimeSeriesSample,
  TimeTravelEntry,
  VectorSearchResult,
} from "./types";

// ---------------------------------------------------------------------------
// Vector search
// ---------------------------------------------------------------------------

/**
 * Send a `VECTOR.INDEX.CREATE` command.
 */
export async function vectorCreateIndex(
  redis: Redis,
  index: string,
  dimensions: number,
  distance: string,
  indexType: string,
): Promise<void> {
  await redis.call(
    "VECTOR.INDEX.CREATE",
    index,
    "DIM",
    String(dimensions),
    "DISTANCE",
    distance.toUpperCase(),
    "TYPE",
    indexType.toUpperCase(),
  );
}

/**
 * Send a `VECTOR.ADD` command.
 */
export async function vectorSet(
  redis: Redis,
  index: string,
  id: string,
  vector: number[],
  metadata?: Record<string, unknown>,
): Promise<void> {
  const args: string[] = [index, id, JSON.stringify(vector)];
  if (metadata !== undefined) {
    args.push(JSON.stringify(metadata));
  }
  await redis.call("VECTOR.ADD", ...args);
}

/**
 * Send a `VECTOR.SEARCH` command and parse the results.
 */
export async function vectorSearch(
  redis: Redis,
  index: string,
  queryVector: number[],
  topK: number,
  filter?: string,
  includeVectors?: boolean,
): Promise<VectorSearchResult[]> {
  const args: string[] = [index, JSON.stringify(queryVector), "TOP", String(topK)];
  if (filter) {
    args.push("FILTER", filter);
  }
  if (includeVectors) {
    args.push("INCLUDE_VECTORS");
  }
  const raw = (await redis.call("VECTOR.SEARCH", ...args)) as unknown;
  return parseVectorResults(raw);
}

// ---------------------------------------------------------------------------
// Semantic caching
// ---------------------------------------------------------------------------

export async function semanticSet(
  redis: Redis,
  text: string,
  value: string,
): Promise<void> {
  await redis.call("SEMANTIC.SET", text, value);
}

export async function semanticSearch(
  redis: Redis,
  text: string,
  threshold: number,
): Promise<SemanticSearchResult[]> {
  const raw = (await redis.call("SEMANTIC.GET", text, String(threshold))) as unknown;
  return parseSemanticResults(raw);
}

// ---------------------------------------------------------------------------
// Time series
// ---------------------------------------------------------------------------

export async function tsAdd(
  redis: Redis,
  key: string,
  timestamp: string,
  value: number,
  labels?: Record<string, string>,
): Promise<number> {
  const args: string[] = [key, timestamp, String(value)];
  if (labels) {
    args.push("LABELS");
    for (const [k, v] of Object.entries(labels)) {
      args.push(k, v);
    }
  }
  const raw = await redis.call("TS.ADD", ...args);
  return Number(raw);
}

export async function tsRange(
  redis: Redis,
  key: string,
  from: string,
  to: string,
): Promise<TimeSeriesSample[]> {
  const raw = (await redis.call("TS.RANGE", key, from, to)) as unknown;
  return parseTsSamples(raw);
}

export async function tsGet(
  redis: Redis,
  key: string,
): Promise<TimeSeriesSample | null> {
  const raw = (await redis.call("TS.GET", key)) as unknown;
  if (raw === null || raw === undefined) {
    return null;
  }
  if (Array.isArray(raw) && raw.length >= 2) {
    return { timestamp: Number(raw[0]), value: Number(raw[1]) };
  }
  return null;
}

// ---------------------------------------------------------------------------
// Document store
// ---------------------------------------------------------------------------

export async function docSet(
  redis: Redis,
  collection: string,
  id: string,
  document: Record<string, unknown>,
): Promise<void> {
  await redis.call("DOC.INSERT", collection, id, JSON.stringify(document));
}

export async function docSearch(
  redis: Redis,
  collection: string,
  query: Record<string, unknown>,
): Promise<DocumentResult[]> {
  const raw = (await redis.call(
    "DOC.FIND",
    collection,
    JSON.stringify(query),
  )) as unknown;
  return parseDocumentResults(raw);
}

// ---------------------------------------------------------------------------
// Graph
// ---------------------------------------------------------------------------

export async function graphQuery(
  redis: Redis,
  graph: string,
  cypher: string,
): Promise<GraphResult> {
  const raw = (await redis.call("GRAPH.QUERY", graph, cypher)) as unknown;
  return parseGraphResult(raw);
}

// ---------------------------------------------------------------------------
// FerriteQL
// ---------------------------------------------------------------------------

export async function ferriteQuery(
  redis: Redis,
  ql: string,
): Promise<QueryResult> {
  const raw = (await redis.call("QUERY", ql)) as unknown;
  return parseQueryResult(raw);
}

// ---------------------------------------------------------------------------
// Time travel
// ---------------------------------------------------------------------------

export async function timeTravel(
  redis: Redis,
  key: string,
  asOf: string,
): Promise<string | null> {
  const raw = (await redis.call("GET", key, "AS", "OF", asOf)) as unknown;
  if (raw === null || raw === undefined) {
    return null;
  }
  return String(raw);
}

export async function history(
  redis: Redis,
  key: string,
  since: string,
): Promise<TimeTravelEntry[]> {
  const raw = (await redis.call("HISTORY", key, "SINCE", since)) as unknown;
  return parseHistoryResults(raw);
}

// ---------------------------------------------------------------------------
// Parsers (private)
// ---------------------------------------------------------------------------

function parseVectorResults(raw: unknown): VectorSearchResult[] {
  const results: VectorSearchResult[] = [];
  if (!Array.isArray(raw)) return results;
  for (let i = 0; i < raw.length; i += 3) {
    const id = String(raw[i]);
    const score = Number(raw[i + 1] ?? 0);
    let metadata: Record<string, unknown> | undefined;
    if (raw[i + 2] != null) {
      try {
        metadata = JSON.parse(String(raw[i + 2]));
      } catch {
        metadata = undefined;
      }
    }
    results.push({ id, score, metadata });
  }
  return results;
}

function parseSemanticResults(raw: unknown): SemanticSearchResult[] {
  const results: SemanticSearchResult[] = [];
  if (raw === null || raw === undefined) return results;
  if (typeof raw === "string") {
    return [{ id: "", score: 1, text: raw }];
  }
  if (!Array.isArray(raw)) return results;
  for (let i = 0; i < raw.length; i += 3) {
    results.push({
      id: String(raw[i]),
      score: Number(raw[i + 1] ?? 0),
      text: raw[i + 2] != null ? String(raw[i + 2]) : undefined,
    });
  }
  return results;
}

function parseTsSamples(raw: unknown): TimeSeriesSample[] {
  const samples: TimeSeriesSample[] = [];
  if (!Array.isArray(raw)) return samples;
  for (const item of raw) {
    if (Array.isArray(item) && item.length >= 2) {
      samples.push({ timestamp: Number(item[0]), value: Number(item[1]) });
    }
  }
  return samples;
}

function parseDocumentResults(raw: unknown): DocumentResult[] {
  const results: DocumentResult[] = [];
  if (!Array.isArray(raw)) return results;
  for (const item of raw) {
    if (Array.isArray(item) && item.length >= 2) {
      let document: Record<string, unknown>;
      try {
        document = JSON.parse(String(item[1]));
      } catch {
        document = {};
      }
      results.push({ id: String(item[0]), document });
    }
  }
  return results;
}

function parseGraphResult(raw: unknown): GraphResult {
  const result: GraphResult = { columns: [], rows: [] };
  if (!Array.isArray(raw)) return result;
  if (raw.length > 0 && Array.isArray(raw[0])) {
    result.columns = raw[0].map(String);
  }
  for (let i = 1; i < raw.length; i++) {
    if (Array.isArray(raw[i])) {
      result.rows.push(raw[i].map(String));
    }
  }
  return result;
}

function parseQueryResult(raw: unknown): QueryResult {
  const result: QueryResult = { columns: [], rows: [], count: 0 };
  if (!Array.isArray(raw)) return result;
  if (raw.length > 0 && Array.isArray(raw[0])) {
    result.columns = raw[0].map(String);
  }
  for (let i = 1; i < raw.length; i++) {
    if (Array.isArray(raw[i])) {
      result.rows.push(raw[i].map(String));
    }
  }
  result.count = result.rows.length;
  return result;
}

function parseHistoryResults(raw: unknown): TimeTravelEntry[] {
  const entries: TimeTravelEntry[] = [];
  if (!Array.isArray(raw)) return entries;
  for (const item of raw) {
    if (Array.isArray(item) && item.length >= 2) {
      entries.push({ timestamp: Number(item[0]), value: String(item[1]) });
    }
  }
  return entries;
}
