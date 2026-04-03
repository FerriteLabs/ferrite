/**
 * Ferrite Mnemo — Agent Memory SDK for Node.js / TypeScript.
 *
 * Core client that communicates with Ferrite's MEM.* commands via ioredis.
 */
import Redis from "ioredis";

export interface MemoryRecord {
  [key: string]: string;
}

export interface FerriteMemoryClientOptions {
  host?: string;
  port?: number;
  password?: string;
  db?: number;
}

/**
 * Client for Ferrite's MEM.* commands.
 */
export class FerriteMemoryClient {
  private redis: Redis;

  constructor(opts: FerriteMemoryClientOptions = {}) {
    this.redis = new Redis({
      host: opts.host ?? "localhost",
      port: opts.port ?? 6379,
      password: opts.password,
      db: opts.db ?? 0,
    });
  }

  /**
   * Store a memory record. Returns the record ID.
   */
  async put(
    agentId: string,
    sessionId: string,
    kind: string,
    content: string,
    meta?: Record<string, unknown>
  ): Promise<string> {
    const args = [agentId, sessionId, kind, content];
    if (meta) {
      args.push("META", JSON.stringify(meta));
    }
    const result = await this.redis.call("MEM.PUT", ...args);
    return String(result);
  }

  /**
   * Retrieve a memory record by ID.
   */
  async get(recordId: string): Promise<MemoryRecord | null> {
    const result = await this.redis.call("MEM.GET", recordId);
    if (result === null) return null;
    return this.parseKvArray(result as string[]);
  }

  /**
   * Recall memories for an agent.
   */
  async recall(
    agentId: string,
    limit = 10,
    kind?: string
  ): Promise<MemoryRecord[]> {
    const args = [agentId, "LIMIT", String(limit)];
    if (kind) {
      args.push("KIND", kind);
    }
    const result = await this.redis.call("MEM.RECALL", ...args);
    return this.parseRecallResult(result);
  }

  /**
   * Forget all memories for an agent (GDPR).
   */
  async forget(agentId: string): Promise<number> {
    const result = await this.redis.call("MEM.FORGET", agentId);
    return Number(result);
  }

  /**
   * Get memory store statistics.
   */
  async stats(): Promise<MemoryRecord> {
    const result = await this.redis.call("MEM.STATS");
    return this.parseKvArray(result as string[]);
  }

  /**
   * Disconnect from Ferrite.
   */
  async disconnect(): Promise<void> {
    await this.redis.quit();
  }

  private parseKvArray(arr: string[]): MemoryRecord {
    const record: MemoryRecord = {};
    if (!Array.isArray(arr)) return record;
    for (let i = 0; i < arr.length - 1; i += 2) {
      record[arr[i]] = arr[i + 1];
    }
    return record;
  }

  private parseRecallResult(result: unknown): MemoryRecord[] {
    if (!Array.isArray(result)) return [];
    return result.map((r: unknown) =>
      Array.isArray(r) ? this.parseKvArray(r as string[]) : ({} as MemoryRecord)
    );
  }
}

export { FerriteLangChainMemory } from "./langchain";
