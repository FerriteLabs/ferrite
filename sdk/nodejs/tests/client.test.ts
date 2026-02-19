/**
 * Unit tests for the Ferrite Node.js client.
 *
 * All tests mock the underlying ioredis connection so they can run without a
 * live Ferrite server.
 */

import { FerriteClient } from "../src/index";
import type {
  VectorSearchResult,
  SemanticSearchResult,
  TimeSeriesSample,
  DocumentResult,
  GraphResult,
  QueryResult,
  TimeTravelEntry,
} from "../src/types";

// ---------------------------------------------------------------------------
// Mock ioredis
// ---------------------------------------------------------------------------

const mockCall = jest.fn();
const mockPing = jest.fn().mockResolvedValue("PONG");
const mockSet = jest.fn().mockResolvedValue("OK");
const mockGet = jest.fn().mockResolvedValue("value");
const mockDel = jest.fn().mockResolvedValue(1);
const mockExists = jest.fn().mockResolvedValue(1);
const mockQuit = jest.fn().mockResolvedValue("OK");
const mockConnect = jest.fn().mockResolvedValue(undefined);

jest.mock("ioredis", () => {
  return jest.fn().mockImplementation(() => ({
    call: mockCall,
    ping: mockPing,
    set: mockSet,
    get: mockGet,
    del: mockDel,
    exists: mockExists,
    quit: mockQuit,
    connect: mockConnect,
  }));
});

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

let client: FerriteClient;

beforeEach(() => {
  jest.clearAllMocks();
  client = new FerriteClient({ host: "localhost", port: 6379 });
});

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

describe("lifecycle", () => {
  it("should ping the server", async () => {
    const result = await client.ping();
    expect(result).toBe("PONG");
    expect(mockPing).toHaveBeenCalled();
  });

  it("should connect explicitly", async () => {
    await client.connect();
    expect(mockConnect).toHaveBeenCalled();
  });

  it("should disconnect gracefully", async () => {
    await client.disconnect();
    expect(mockQuit).toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// Standard Redis commands
// ---------------------------------------------------------------------------

describe("standard commands", () => {
  it("should set a key", async () => {
    const result = await client.set("key", "value");
    expect(mockSet).toHaveBeenCalled();
  });

  it("should get a key", async () => {
    const result = await client.get("key");
    expect(result).toBe("value");
  });

  it("should delete keys", async () => {
    const count = await client.del("k1", "k2");
    expect(count).toBe(1);
  });

  it("should check key existence", async () => {
    const count = await client.exists("key");
    expect(count).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Vector search
// ---------------------------------------------------------------------------

describe("vector search", () => {
  it("should create a vector index", async () => {
    mockCall.mockResolvedValueOnce("OK");
    await client.vectorCreateIndex("idx", 384, "cosine", "hnsw");
    expect(mockCall).toHaveBeenCalledWith(
      "VECTOR.INDEX.CREATE",
      "idx",
      "DIM",
      "384",
      "DISTANCE",
      "COSINE",
      "TYPE",
      "HNSW",
    );
  });

  it("should add a vector", async () => {
    mockCall.mockResolvedValueOnce("OK");
    await client.vectorSet("idx", "doc:1", [0.1, 0.2], { tag: "test" });
    expect(mockCall).toHaveBeenCalledWith(
      "VECTOR.ADD",
      "idx",
      "doc:1",
      JSON.stringify([0.1, 0.2]),
      JSON.stringify({ tag: "test" }),
    );
  });

  it("should search vectors", async () => {
    mockCall.mockResolvedValueOnce([
      "doc:1", "0.95", '{"tag":"test"}',
      "doc:2", "0.80", null,
    ]);
    const results = await client.vectorSearch("idx", [0.1, 0.2], { topK: 2 });
    expect(results).toHaveLength(2);
    expect(results[0].id).toBe("doc:1");
    expect(results[0].score).toBe(0.95);
    expect(results[0].metadata).toEqual({ tag: "test" });
    expect(results[1].metadata).toBeUndefined();
  });

  it("should do knn search", async () => {
    mockCall.mockResolvedValueOnce(["doc:1", "0.9", null]);
    const results = await client.vectorKnn("idx", [0.1], 1);
    expect(results).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// Semantic caching
// ---------------------------------------------------------------------------

describe("semantic caching", () => {
  it("should set a semantic value", async () => {
    mockCall.mockResolvedValueOnce("OK");
    await client.semanticSet("capital of France?", "Paris");
    expect(mockCall).toHaveBeenCalledWith("SEMANTIC.SET", "capital of France?", "Paris");
  });

  it("should search semantically with single string result", async () => {
    mockCall.mockResolvedValueOnce("Paris");
    const results = await client.semanticSearch("France capital?", 0.85);
    expect(results).toHaveLength(1);
    expect(results[0].text).toBe("Paris");
  });

  it("should search semantically with array result", async () => {
    mockCall.mockResolvedValueOnce([
      "id1", "0.92", "Paris is the capital",
      "id2", "0.87", "France's capital is Paris",
    ]);
    const results = await client.semanticSearch("capital of France?");
    expect(results).toHaveLength(2);
    expect(results[0].score).toBe(0.92);
  });

  it("should handle null semantic result", async () => {
    mockCall.mockResolvedValueOnce(null);
    const results = await client.semanticSearch("unknown");
    expect(results).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// Time series
// ---------------------------------------------------------------------------

describe("time series", () => {
  it("should add a sample", async () => {
    mockCall.mockResolvedValueOnce(1700000000);
    const ts = await client.tsAdd("temp:room1", "*", 23.5);
    expect(ts).toBe(1700000000);
  });

  it("should add a sample with labels", async () => {
    mockCall.mockResolvedValueOnce(1700000000);
    await client.tsAdd("temp:room1", "*", 23.5, { loc: "office" });
    expect(mockCall).toHaveBeenCalledWith(
      "TS.ADD",
      "temp:room1",
      "*",
      "23.5",
      "LABELS",
      "loc",
      "office",
    );
  });

  it("should query a range", async () => {
    mockCall.mockResolvedValueOnce([
      [1700000000, "23.5"],
      [1700000060, "24.0"],
    ]);
    const samples = await client.tsRange("temp:room1");
    expect(samples).toHaveLength(2);
    expect(samples[0].timestamp).toBe(1700000000);
    expect(samples[0].value).toBe(23.5);
  });

  it("should get the latest sample", async () => {
    mockCall.mockResolvedValueOnce([1700000000, "23.5"]);
    const sample = await client.tsGet("temp:room1");
    expect(sample).not.toBeNull();
    expect(sample!.value).toBe(23.5);
  });

  it("should return null for empty ts_get", async () => {
    mockCall.mockResolvedValueOnce(null);
    const sample = await client.tsGet("empty");
    expect(sample).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Document store
// ---------------------------------------------------------------------------

describe("document store", () => {
  it("should insert a document", async () => {
    mockCall.mockResolvedValueOnce("OK");
    const doc = { title: "Hello", author: "Alice" };
    await client.docSet("articles", "a:1", doc);
    expect(mockCall).toHaveBeenCalledWith(
      "DOC.INSERT",
      "articles",
      "a:1",
      JSON.stringify(doc),
    );
  });

  it("should search documents", async () => {
    mockCall.mockResolvedValueOnce([["a:1", '{"title":"Hello"}']]);
    const results = await client.docSearch("articles", { author: "Alice" });
    expect(results).toHaveLength(1);
    expect(results[0].document.title).toBe("Hello");
  });
});

// ---------------------------------------------------------------------------
// Graph
// ---------------------------------------------------------------------------

describe("graph", () => {
  it("should execute a graph query", async () => {
    mockCall.mockResolvedValueOnce([["name", "age"], ["Bob", "28"]]);
    const result = await client.graphQuery(
      "social",
      "MATCH (a:User)-[:FOLLOWS]->(b) RETURN b.name, b.age",
    );
    expect(result.columns).toEqual(["name", "age"]);
    expect(result.rows).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// FerriteQL
// ---------------------------------------------------------------------------

describe("FerriteQL", () => {
  it("should execute a query", async () => {
    mockCall.mockResolvedValueOnce([["name"], ["Alice"], ["Bob"]]);
    const result = await client.query(
      "FROM users:* WHERE $.active = true SELECT $.name",
    );
    expect(result.columns).toEqual(["name"]);
    expect(result.count).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// Time travel
// ---------------------------------------------------------------------------

describe("time travel", () => {
  it("should read a past value", async () => {
    mockCall.mockResolvedValueOnce("old_value");
    const val = await client.timeTravel("mykey", "-1h");
    expect(val).toBe("old_value");
    expect(mockCall).toHaveBeenCalledWith("GET", "mykey", "AS", "OF", "-1h");
  });

  it("should return null for missing past value", async () => {
    mockCall.mockResolvedValueOnce(null);
    const val = await client.timeTravel("missing", "-1h");
    expect(val).toBeNull();
  });

  it("should retrieve key history", async () => {
    mockCall.mockResolvedValueOnce([
      [1700000000, "v1"],
      [1700003600, "v2"],
    ]);
    const entries = await client.history("mykey", "-24h");
    expect(entries).toHaveLength(2);
    expect(entries[1].value).toBe("v2");
  });
});

// ---------------------------------------------------------------------------
// Raw command
// ---------------------------------------------------------------------------

describe("raw command", () => {
  it("should execute an arbitrary command", async () => {
    mockCall.mockResolvedValueOnce("OK");
    const result = await client.executeCommand(
      "CRDT.GCOUNTER",
      "mycounter",
      "INCR",
      "5",
    );
    expect(result).toBe("OK");
    expect(mockCall).toHaveBeenCalledWith("CRDT.GCOUNTER", "mycounter", "INCR", "5");
  });
});
